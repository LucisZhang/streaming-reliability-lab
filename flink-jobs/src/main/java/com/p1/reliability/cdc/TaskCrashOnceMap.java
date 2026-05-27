package com.p1.reliability.cdc;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.flink.api.common.functions.MapFunction;

public final class TaskCrashOnceMap implements MapFunction<OrderChange, OrderChange> {
  private final long crashEventId;
  private final String markerPath;

  public TaskCrashOnceMap(long crashEventId, String markerPath) {
    this.crashEventId = crashEventId;
    this.markerPath = markerPath;
  }

  @Override
  public OrderChange map(OrderChange change) throws IOException {
    if (change.eventId == crashEventId && markFirstCrash()) {
      throw new RuntimeException(
          "Intentional Phase 1.3 task crash at event_id=" + crashEventId);
    }
    return change;
  }

  private boolean markFirstCrash() throws IOException {
    Path marker = Paths.get(markerPath);
    if (Files.exists(marker)) {
      return false;
    }
    Path parent = marker.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    try {
      Files.writeString(
          marker,
          "crashed event_id=" + crashEventId + System.lineSeparator(),
          StandardOpenOption.CREATE_NEW);
      return true;
    } catch (FileAlreadyExistsException ignored) {
      return false;
    }
  }
}
