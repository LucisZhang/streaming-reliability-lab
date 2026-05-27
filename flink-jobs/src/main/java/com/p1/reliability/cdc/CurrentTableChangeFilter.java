package com.p1.reliability.cdc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.RowKind;

public final class CurrentTableChangeFilter implements FilterFunction<OrderChange> {
  @Override
  public boolean filter(OrderChange change) {
    return change.rowKind != RowKind.UPDATE_BEFORE;
  }
}
