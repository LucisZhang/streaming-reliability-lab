package com.p1.reliability.cdc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

public final class ChangelogRowDataMapper implements MapFunction<OrderChange, RowData> {
  @Override
  public RowData map(OrderChange change) {
    return GenericRowData.ofKind(
        RowKind.INSERT,
        change.orderId,
        StringData.fromString(change.businessKey),
        change.eventId,
        change.customerId,
        StringData.fromString(change.status),
        change.amountCents,
        TimestampData.fromLocalDateTime(change.updatedAt),
        change.seed,
        StringData.fromString(change.rowKind.shortString()),
        StringData.fromString(change.cdcOperation),
        change.sourceTsMs);
  }
}
