package com.p1.reliability.cdc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.flink.actions.RewriteDataFilesAction;
import org.apache.iceberg.io.CloseableIterable;

public final class IcebergAdmin {
  private static final String RESET_TABLES = "reset-tables";
  private static final String SMALL_FILE_METRICS = "small-file-metrics";
  private static final String REWRITE_DATA_FILES = "rewrite-data-files";
  private static final String REWRITE_MANIFESTS = "rewrite-manifests";

  private IcebergAdmin() {}

  public static void main(String[] args) throws Exception {
    int commandIndex = commandIndex(args);
    if (commandIndex < 0) {
      throw new IllegalArgumentException(
          "Usage: IcebergAdmin {reset-tables|small-file-metrics|rewrite-data-files|rewrite-manifests} [job args]");
    }

    String command = args[commandIndex];
    String[] configArgs = withoutIndex(args, commandIndex);
    JobConfig config = JobConfig.fromArgs(configArgs);

    if (RESET_TABLES.equals(command)) {
      IcebergTables.dropTables(config);
      return;
    }

    Table table = loadTable(config, option(args, "--table", IcebergTables.CURRENT_TABLE));
    if (SMALL_FILE_METRICS.equals(command)) {
      int repetitions = intOption(args, "--planning-repetitions", 5);
      System.out.println(collectMetrics(table, repetitions).toJson());
      return;
    }

    if (REWRITE_DATA_FILES.equals(command)) {
      long targetFileSizeBytes = longOption(args, "--target-file-size-bytes", 128L * 1024L * 1024L);
      int maxParallelism = intOption(args, "--max-parallelism", 2);
      System.out.println(rewriteDataFiles(table, targetFileSizeBytes, maxParallelism));
      return;
    }

    if (REWRITE_MANIFESTS.equals(command)) {
      System.out.println(rewriteManifests(table));
      return;
    }

    throw new IllegalArgumentException("Unhandled command: " + command);
  }

  private static String rewriteDataFiles(
      Table table, long targetFileSizeBytes, int maxParallelism) {
    RewriteDataFilesAction action = Actions.forTable(table).rewriteDataFiles();
    action.targetSizeInBytes(targetFileSizeBytes);
    action.maxParallelism(maxParallelism);
    action.useStartingSequenceNumber(true);

    RewriteDataFilesActionResult result = action.execute();
    long rewrittenBytes = 0L;
    for (DataFile file : result.deletedDataFiles()) {
      rewrittenBytes += file.fileSizeInBytes();
    }
    table.refresh();
    return "{"
        + "\"action\":\"rewrite_data_files\","
        + "\"target_file_size_bytes\":"
        + targetFileSizeBytes
        + ","
        + "\"max_parallelism\":"
        + maxParallelism
        + ","
        + "\"rewritten_data_files_count\":"
        + result.deletedDataFiles().size()
        + ","
        + "\"added_data_files_count\":"
        + result.addedDataFiles().size()
        + ","
        + "\"rewritten_bytes_count\":"
        + rewrittenBytes
        + "}";
  }

  private static String rewriteManifests(Table table) {
    int snapshotBefore = snapshotManifestCount(table);
    int liveBefore;
    try {
      liveBefore = dataFileStats(table).manifestLocations.size();
    } catch (IOException exc) {
      throw new RuntimeException("Failed to inspect live data manifests", exc);
    }
    if (snapshotBefore > 1) {
      table.rewriteManifests()
          .rewriteIf(manifest -> manifest.content() == ManifestContent.DATA)
          .commit();
      table.refresh();
    }
    int snapshotAfter = snapshotManifestCount(table);
    int liveAfter;
    try {
      liveAfter = dataFileStats(table).manifestLocations.size();
    } catch (IOException exc) {
      throw new RuntimeException("Failed to inspect live data manifests", exc);
    }
    return "{"
        + "\"action\":\"rewrite_manifests\","
        + "\"ran\":"
        + (snapshotBefore > 1)
        + ","
        + "\"live_manifest_count_before\":"
        + liveBefore
        + ","
        + "\"live_manifest_count_after\":"
        + liveAfter
        + ","
        + "\"snapshot_manifest_count_before\":"
        + snapshotBefore
        + ","
        + "\"snapshot_manifest_count_after\":"
        + snapshotAfter
        + "}";
  }

  private static Table loadTable(JobConfig config, String tableName) {
    Catalog catalog = IcebergTables.loadCatalog(config);
    return catalog.loadTable(tableIdentifier(config, tableName));
  }

  private static TableIdentifier tableIdentifier(JobConfig config, String tableName) {
    String[] parts = tableName.split("\\.");
    if (parts.length == 1) {
      return TableIdentifier.of(config.icebergDatabase(), parts[0]);
    }
    if (parts.length == 2) {
      return TableIdentifier.of(parts[0], parts[1]);
    }
    if (parts.length == 3) {
      return TableIdentifier.of(parts[1], parts[2]);
    }
    throw new IllegalArgumentException("Unsupported Iceberg table identifier: " + tableName);
  }

  private static Metrics collectMetrics(Table table, int planningRepetitions) throws IOException {
    table.refresh();
    DataFileStats dataFileStats = dataFileStats(table);
    List<Long> planningLatenciesNanos = new ArrayList<>();
    int plannedFileScanTasks = 0;
    for (int index = 0; index < planningRepetitions; index++) {
      PlanningMeasurement measurement = measurePlanning(table);
      planningLatenciesNanos.add(measurement.elapsedNanos);
      plannedFileScanTasks = measurement.fileScanTasks;
    }

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> dataManifests =
        snapshot == null ? Collections.emptyList() : snapshot.dataManifests(table.io());
    List<ManifestFile> deleteManifests =
        snapshot == null ? Collections.emptyList() : snapshot.deleteManifests(table.io());
    return new Metrics(
        table.name(),
        snapshot == null ? -1L : snapshot.snapshotId(),
        dataFileStats.fileSizes,
        dataFileStats.manifestLocations,
        dataManifests,
        deleteManifests,
        planningLatenciesNanos,
        planningRepetitions,
        plannedFileScanTasks);
  }

  private static DataFileStats dataFileStats(Table table) throws IOException {
    List<Long> sizes = new ArrayList<>();
    Set<String> manifestLocations = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        sizes.add(file.fileSizeInBytes());
        if (file.manifestLocation() != null) {
          manifestLocations.add(file.manifestLocation());
        }
      }
    }
    return new DataFileStats(sizes, manifestLocations);
  }

  private static PlanningMeasurement measurePlanning(Table table) throws IOException {
    long started = System.nanoTime();
    int fileScanTasks = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask ignored : tasks) {
        fileScanTasks++;
      }
    }
    return new PlanningMeasurement(System.nanoTime() - started, fileScanTasks);
  }

  private static int snapshotManifestCount(Table table) {
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return 0;
    }
    return snapshot.dataManifests(table.io()).size() + snapshot.deleteManifests(table.io()).size();
  }

  private static int commandIndex(String[] args) {
    for (int index = 0; index < args.length; index++) {
      if (RESET_TABLES.equals(args[index])
          || SMALL_FILE_METRICS.equals(args[index])
          || REWRITE_DATA_FILES.equals(args[index])
          || REWRITE_MANIFESTS.equals(args[index])) {
        return index;
      }
    }
    return -1;
  }

  private static String[] withoutIndex(String[] args, int skippedIndex) {
    String[] result = new String[args.length - 1];
    int output = 0;
    for (int index = 0; index < args.length; index++) {
      if (index != skippedIndex) {
        result[output] = args[index];
        output++;
      }
    }
    return result;
  }

  private static String option(String[] args, String name, String defaultValue) {
    for (int index = 0; index < args.length - 1; index++) {
      if (name.equals(args[index])) {
        return args[index + 1];
      }
    }
    return defaultValue;
  }

  private static int intOption(String[] args, String name, int defaultValue) {
    return Integer.parseInt(option(args, name, Integer.toString(defaultValue)));
  }

  private static long longOption(String[] args, String name, long defaultValue) {
    return Long.parseLong(option(args, name, Long.toString(defaultValue)));
  }

  private static String quote(String value) {
    return "\""
        + value.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
        + "\"";
  }

  private static String jsonNumber(double value) {
    return String.format(Locale.ROOT, "%.3f", value);
  }

  private static final class PlanningMeasurement {
    private final long elapsedNanos;
    private final int fileScanTasks;

    private PlanningMeasurement(long elapsedNanos, int fileScanTasks) {
      this.elapsedNanos = elapsedNanos;
      this.fileScanTasks = fileScanTasks;
    }
  }

  private static final class DataFileStats {
    private final List<Long> fileSizes;
    private final Set<String> manifestLocations;

    private DataFileStats(List<Long> fileSizes, Set<String> manifestLocations) {
      this.fileSizes = fileSizes;
      this.manifestLocations = manifestLocations;
    }
  }

  private static final class Metrics {
    private final String table;
    private final long snapshotId;
    private final List<Long> fileSizes;
    private final Set<String> liveManifestLocations;
    private final List<ManifestFile> dataManifests;
    private final List<ManifestFile> deleteManifests;
    private final List<Long> planningLatenciesNanos;
    private final int planningRepetitions;
    private final int plannedFileScanTasks;

    private Metrics(
        String table,
        long snapshotId,
        List<Long> fileSizes,
        Set<String> liveManifestLocations,
        List<ManifestFile> dataManifests,
        List<ManifestFile> deleteManifests,
        List<Long> planningLatenciesNanos,
        int planningRepetitions,
        int plannedFileScanTasks) {
      this.table = table;
      this.snapshotId = snapshotId;
      this.fileSizes = new ArrayList<>(fileSizes);
      this.liveManifestLocations = new HashSet<>(liveManifestLocations);
      this.dataManifests = new ArrayList<>(dataManifests);
      this.deleteManifests = new ArrayList<>(deleteManifests);
      this.planningLatenciesNanos = new ArrayList<>(planningLatenciesNanos);
      this.planningRepetitions = planningRepetitions;
      this.plannedFileScanTasks = plannedFileScanTasks;
    }

    private String toJson() {
      Collections.sort(fileSizes);
      Collections.sort(planningLatenciesNanos);
      return "{"
          + "\"table\":"
          + quote(table)
          + ","
          + "\"snapshot_id\":"
          + snapshotId
          + ","
          + "\"data_file_count\":"
          + fileSizes.size()
          + ","
          + "\"total_data_file_size_bytes\":"
          + sum(fileSizes)
          + ","
          + "\"median_file_size_bytes\":"
          + jsonNumber(median(fileSizes))
          + ","
          + "\"min_file_size_bytes\":"
          + (fileSizes.isEmpty() ? 0L : fileSizes.get(0))
          + ","
          + "\"max_file_size_bytes\":"
          + (fileSizes.isEmpty() ? 0L : fileSizes.get(fileSizes.size() - 1))
          + ","
          + "\"manifest_count\":"
          + liveManifestLocations.size()
          + ","
          + "\"live_data_manifest_count\":"
          + liveManifestLocations.size()
          + ","
          + "\"snapshot_manifest_count\":"
          + (dataManifests.size() + deleteManifests.size())
          + ","
          + "\"data_manifest_count\":"
          + dataManifests.size()
          + ","
          + "\"delete_manifest_count\":"
          + deleteManifests.size()
          + ","
          + "\"manifest_size_bytes\":"
          + (manifestBytes(dataManifests) + manifestBytes(deleteManifests))
          + ","
          + "\"planning_latency_ms\":"
          + jsonNumber(medianNanosAsMillis(planningLatenciesNanos))
          + ","
          + "\"planning_repetitions\":"
          + planningRepetitions
          + ","
          + "\"planned_file_scan_tasks\":"
          + plannedFileScanTasks
          + "}";
    }

    private static long sum(List<Long> values) {
      long total = 0L;
      for (long value : values) {
        total += value;
      }
      return total;
    }

    private static double median(List<Long> values) {
      if (values.isEmpty()) {
        return 0.0d;
      }
      int middle = values.size() / 2;
      if (values.size() % 2 == 1) {
        return values.get(middle);
      }
      return (values.get(middle - 1) + values.get(middle)) / 2.0d;
    }

    private static double medianNanosAsMillis(List<Long> values) {
      return median(values) / 1_000_000.0d;
    }

    private static long manifestBytes(List<ManifestFile> manifests) {
      long total = 0L;
      for (ManifestFile manifest : manifests) {
        total += manifest.length();
      }
      return total;
    }
  }
}
