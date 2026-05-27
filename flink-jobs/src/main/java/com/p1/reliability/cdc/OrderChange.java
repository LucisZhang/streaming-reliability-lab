package com.p1.reliability.cdc;

import java.io.Serializable;
import java.time.LocalDateTime;
import org.apache.flink.types.RowKind;

public final class OrderChange implements Serializable {
  private static final long serialVersionUID = 1L;

  public RowKind rowKind;
  public String cdcOperation;
  public long orderId;
  public String businessKey;
  public long eventId;
  public long customerId;
  public String status;
  public long amountCents;
  public LocalDateTime updatedAt;
  public int seed;
  public Long sourceTsMs;

  public OrderChange() {}

  public OrderChange(
      RowKind rowKind,
      String cdcOperation,
      long orderId,
      String businessKey,
      long eventId,
      long customerId,
      String status,
      long amountCents,
      LocalDateTime updatedAt,
      int seed,
      Long sourceTsMs) {
    this.rowKind = rowKind;
    this.cdcOperation = cdcOperation;
    this.orderId = orderId;
    this.businessKey = businessKey;
    this.eventId = eventId;
    this.customerId = customerId;
    this.status = status;
    this.amountCents = amountCents;
    this.updatedAt = updatedAt;
    this.seed = seed;
    this.sourceTsMs = sourceTsMs;
  }
}
