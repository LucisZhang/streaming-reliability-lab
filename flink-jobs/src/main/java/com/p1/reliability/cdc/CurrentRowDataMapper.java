package com.p1.reliability.cdc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public final class CurrentRowDataMapper implements MapFunction<OrderChange, RowData> {
  @Override
  public RowData map(OrderChange change) {
    return GenericRowData.ofKind(
        change.rowKind,
        change.orderId,
        StringData.fromString(change.businessKey),
        change.eventId,
        change.customerId,
        StringData.fromString(change.status),
        change.amountCents,
        TimestampData.fromLocalDateTime(change.updatedAt),
        change.seed);
  }
}
