package com.p1.reliability.cdc;

import org.apache.flink.api.common.functions.MapFunction;

public final class IdentityOrderChangeMap implements MapFunction<OrderChange, OrderChange> {
  @Override
  public OrderChange map(OrderChange change) {
    return change;
  }
}
