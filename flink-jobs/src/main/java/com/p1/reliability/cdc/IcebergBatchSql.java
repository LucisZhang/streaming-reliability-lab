package com.p1.reliability.cdc;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public final class IcebergBatchSql {
  private static final DateTimeFormatter TS_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private IcebergBatchSql() {}

  public static void main(String[] args) throws Exception {
    JobConfig config = JobConfig.fromArgs(args);
    String query = requiredQuery(args);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    tableEnvironment.executeSql(config.icebergCatalogDdl());
    tableEnvironment.executeSql("USE CATALOG " + config.catalogName());
    tableEnvironment.executeSql("USE " + config.icebergDatabase());

    TableResult result = tableEnvironment.executeSql(query);
    try (CloseableIterator<Row> rows = result.collect()) {
      while (rows.hasNext()) {
        Row row = rows.next();
        printRow(row);
      }
    }
  }

  private static String requiredQuery(String[] args) {
    for (int index = 0; index < args.length - 1; index++) {
      if ("--query".equals(args[index])) {
        return args[index + 1];
      }
    }
    throw new IllegalArgumentException("--query is required");
  }

  private static void printRow(Row row) {
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < row.getArity(); index++) {
      if (index > 0) {
        builder.append('\t');
      }
      builder.append(format(row.getField(index)));
    }
    System.out.println(builder);
  }

  private static String format(Object value) {
    if (value == null) {
      return "NULL";
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).format(TS_FORMAT);
    }
    return String.valueOf(value);
  }
}
