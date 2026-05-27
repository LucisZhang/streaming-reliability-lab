package com.p1.reliability.cdc;

import io.debezium.data.Envelope;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public final class OrderDebeziumDeserializationSchema
    implements DebeziumDeserializationSchema<OrderChange> {
  private static final DateTimeFormatter MYSQL_DATETIME =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][.SSSSSS]", Locale.ROOT);

  @Override
  public void deserialize(SourceRecord sourceRecord, Collector<OrderChange> out) {
    if (!(sourceRecord.value() instanceof Struct)) {
      return;
    }

    Struct value = (Struct) sourceRecord.value();
    Struct source = value.getStruct("source");
    Long sourceTsMs = value.getInt64("ts_ms");
    Envelope.Operation operation = Envelope.operationFor(sourceRecord);

    if (operation == Envelope.Operation.CREATE) {
      out.collect(fromStruct(RowKind.INSERT, "insert", value.getStruct("after"), sourceTsMs));
    } else if (operation == Envelope.Operation.READ) {
      out.collect(fromStruct(RowKind.INSERT, "snapshot", value.getStruct("after"), sourceTsMs));
    } else if (operation == Envelope.Operation.UPDATE) {
      out.collect(fromStruct(RowKind.UPDATE_BEFORE, "update_before", value.getStruct("before"), sourceTsMs));
      out.collect(fromStruct(RowKind.UPDATE_AFTER, "update_after", value.getStruct("after"), sourceTsMs));
    } else if (operation == Envelope.Operation.DELETE) {
      out.collect(fromStruct(RowKind.DELETE, "delete", value.getStruct("before"), sourceTsMs));
    } else if (source != null) {
      throw new IllegalArgumentException("Unsupported Debezium operation: " + operation);
    }
  }

  @Override
  public TypeInformation<OrderChange> getProducedType() {
    return TypeInformation.of(OrderChange.class);
  }

  private static OrderChange fromStruct(RowKind rowKind, String operation, Struct row, Long sourceTsMs) {
    if (row == null) {
      throw new IllegalArgumentException("Missing row payload for " + operation);
    }

    return new OrderChange(
        rowKind,
        operation,
        number(row.get("order_id")).longValue(),
        row.getString("business_key"),
        number(row.get("event_id")).longValue(),
        number(row.get("customer_id")).longValue(),
        row.getString("status"),
        number(row.get("amount_cents")).longValue(),
        timestamp(row.get("updated_at")),
        number(row.get("seed")).intValue(),
        sourceTsMs);
  }

  private static Number number(Object value) {
    if (!(value instanceof Number)) {
      throw new IllegalArgumentException("Expected numeric value but got " + value);
    }
    return (Number) value;
  }

  private static LocalDateTime timestamp(Object value) {
    if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    }
    if (value instanceof java.util.Date) {
      return LocalDateTime.ofInstant(((java.util.Date) value).toInstant(), ZoneOffset.UTC);
    }
    if (value instanceof Number) {
      long epochValue = ((Number) value).longValue();
      if (Math.abs(epochValue) < 100_000_000_000_000L) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochValue), ZoneOffset.UTC);
      }
      long seconds = Math.floorDiv(epochValue, 1_000_000L);
      long nanos = Math.floorMod(epochValue, 1_000_000L) * 1_000L;
      return LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
    }
    if (value instanceof String) {
      String text = ((String) value).replace('T', ' ');
      if (text.endsWith("Z")) {
        return LocalDateTime.ofInstant(Instant.parse((String) value), ZoneOffset.UTC);
      }
      return LocalDateTime.parse(text, MYSQL_DATETIME);
    }
    throw new IllegalArgumentException("Unsupported timestamp value: " + value);
  }
}
