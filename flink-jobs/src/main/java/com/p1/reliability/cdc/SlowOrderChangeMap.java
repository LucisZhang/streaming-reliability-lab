package com.p1.reliability.cdc;

import org.apache.flink.api.common.functions.MapFunction;

public final class SlowOrderChangeMap implements MapFunction<OrderChange, OrderChange> {
  private final long sleepMs;

  public SlowOrderChangeMap(long sleepMs) {
    if (sleepMs < 0) {
      throw new IllegalArgumentException("sleepMs must be non-negative");
    }
    this.sleepMs = sleepMs;
  }

  @Override
  public OrderChange map(OrderChange change) throws InterruptedException {
    if (sleepMs > 0) {
      Thread.sleep(sleepMs);
    }
    return change;
  }
}
