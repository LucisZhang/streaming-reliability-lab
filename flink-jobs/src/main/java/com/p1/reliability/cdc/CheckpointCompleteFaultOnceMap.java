package com.p1.reliability.cdc;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;

public final class CheckpointCompleteFaultOnceMap
    implements MapFunction<OrderChange, OrderChange>, CheckpointListener {
  private final long triggerEventId;
  private final String markerPath;
  private transient boolean triggerObserved;

  public CheckpointCompleteFaultOnceMap(long triggerEventId, String markerPath) {
    this.triggerEventId = triggerEventId;
    this.markerPath = markerPath;
  }

  @Override
  public OrderChange map(OrderChange change) {
    if (change.eventId == triggerEventId) {
      triggerObserved = true;
    }
    return change;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    if (triggerObserved && markFirstFault(checkpointId)) {
      throw new RuntimeException(
          "Intentional Phase 2.1 checkpoint-complete sink-commit fault after event_id="
              + triggerEventId
              + " at checkpoint="
              + checkpointId);
    }
  }

  private boolean markFirstFault(long checkpointId) throws IOException {
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
          "fault=checkpoint-complete"
              + System.lineSeparator()
              + "trigger_event_id="
              + triggerEventId
              + System.lineSeparator()
              + "checkpoint_id="
              + checkpointId
              + System.lineSeparator(),
          StandardOpenOption.CREATE_NEW);
      return true;
    } catch (FileAlreadyExistsException ignored) {
      return false;
    }
  }
}
