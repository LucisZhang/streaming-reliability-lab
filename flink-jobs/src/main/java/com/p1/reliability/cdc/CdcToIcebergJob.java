package com.p1.reliability.cdc;

import java.util.Collections;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.FlinkSink;

public final class CdcToIcebergJob {
  private CdcToIcebergJob() {}

  public static void main(String[] args) throws Exception {
    JobConfig config = JobConfig.fromArgs(args);
    IcebergTables.ensureTables(config);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));
    env.getCheckpointConfig().setCheckpointTimeout(10 * 60 * 1000L);
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(Math.max(1_000L, config.checkpointIntervalMs() / 3));
    env.getCheckpointConfig()
        .enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    MySqlSource<OrderChange> source =
        MySqlSource.<OrderChange>builder()
            .hostname(config.mysqlHost())
            .port(config.mysqlPort())
            .databaseList(config.mysqlDatabase())
            .tableList(config.mysqlTable())
            .username(config.mysqlUser())
            .password(config.mysqlPassword())
            .serverId(config.serverId())
            .startupOptions(StartupOptions.initial())
            .deserializer(new OrderDebeziumDeserializationSchema())
            .build();

    DataStream<OrderChange> changes =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-cdc-orders")
            .name("mysql-cdc-orders")
            .uid("mysql-cdc-orders-source")
            .setParallelism(1);

    if (config.taskCrashEventId() > 0) {
      changes =
          changes
              .map(new TaskCrashOnceMap(config.taskCrashEventId(), config.taskCrashMarkerPath()))
              .name("phase-1-3-task-crash-once")
              .uid("phase-1-3-task-crash-once")
              .returns(TypeInformation.of(OrderChange.class))
              .setParallelism(1);
    }

    if (config.checkpointCompleteFaultEventId() > 0) {
      changes =
          changes
              .map(
                  new CheckpointCompleteFaultOnceMap(
                      config.checkpointCompleteFaultEventId(),
                      config.checkpointCompleteFaultMarkerPath()))
              .name("phase-2-1-sink-commit-fault-once")
              .uid("phase-2-1-sink-commit-fault-once")
              .returns(TypeInformation.of(OrderChange.class))
              .setParallelism(1);
    }

    DataStream<RowData> currentRows =
        changes
            .filter(new CurrentTableChangeFilter())
            .name("orders-current-drop-update-before")
            .uid("orders-current-drop-update-before")
            .map(new CurrentRowDataMapper())
            .name("orders-current-rowdata")
            .uid("orders-current-rowdata")
            .returns(TypeInformation.of(RowData.class));

    FlinkSink.forRowData(currentRows)
        .tableLoader(IcebergTables.currentTableLoader(config))
        .upsert(true)
        .equalityFieldColumns(Collections.singletonList("order_id"))
        .uidPrefix("iceberg-orders-current")
        .append();

    DataStream<RowData> changelogRows =
        changes
            .map(new ChangelogRowDataMapper())
            .name("orders-changelog-rowdata")
            .uid("orders-changelog-rowdata")
            .returns(TypeInformation.of(RowData.class));

    FlinkSink.forRowData(changelogRows)
        .tableLoader(IcebergTables.changelogTableLoader(config))
        .uidPrefix("iceberg-orders-changelog")
        .append();

    env.execute("cdc-to-iceberg-v2-upsert");
  }
}
