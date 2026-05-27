package com.p1.reliability.cdc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Types;

public final class IcebergTables {
  public static final String CURRENT_TABLE = "orders_current";
  public static final String CHANGELOG_TABLE = "orders_changelog";

  private IcebergTables() {}

  public static void ensureTables(JobConfig config) {
    Catalog catalog = loadCatalog(config);
    Namespace namespace = Namespace.of(config.icebergDatabase());
    if (catalog instanceof SupportsNamespaces) {
      try {
        ((SupportsNamespaces) catalog).createNamespace(namespace);
      } catch (AlreadyExistsException ignored) {
        // Existing namespace is the normal path after the first run.
      }
    }

    TableIdentifier current = TableIdentifier.of(namespace, CURRENT_TABLE);
    if (!catalog.tableExists(current)) {
      catalog.createTable(
          current, currentSchema(), PartitionSpec.unpartitioned(), currentProperties(config));
    }

    TableIdentifier changelog = TableIdentifier.of(namespace, CHANGELOG_TABLE);
    if (!catalog.tableExists(changelog)) {
      catalog.createTable(
          changelog, changelogSchema(), PartitionSpec.unpartitioned(), changelogProperties(config));
    }
  }

  public static void dropTables(JobConfig config) {
    Catalog catalog = loadCatalog(config);
    Namespace namespace = Namespace.of(config.icebergDatabase());
    catalog.dropTable(TableIdentifier.of(namespace, CURRENT_TABLE), true);
    catalog.dropTable(TableIdentifier.of(namespace, CHANGELOG_TABLE), true);
  }

  public static TableLoader currentTableLoader(JobConfig config) {
    return withJdbcDriver(TableLoader.fromCatalog(catalogLoader(config), currentIdentifier(config)));
  }

  public static TableLoader changelogTableLoader(JobConfig config) {
    return withJdbcDriver(TableLoader.fromCatalog(catalogLoader(config), changelogIdentifier(config)));
  }

  public static TableIdentifier currentIdentifier(JobConfig config) {
    return TableIdentifier.of(config.icebergDatabase(), CURRENT_TABLE);
  }

  public static TableIdentifier changelogIdentifier(JobConfig config) {
    return TableIdentifier.of(config.icebergDatabase(), CHANGELOG_TABLE);
  }

  public static Schema currentSchema() {
    return new Schema(
        Arrays.asList(
            Types.NestedField.required(1, "order_id", Types.LongType.get()),
            Types.NestedField.required(2, "business_key", Types.StringType.get()),
            Types.NestedField.required(3, "event_id", Types.LongType.get()),
            Types.NestedField.required(4, "customer_id", Types.LongType.get()),
            Types.NestedField.required(5, "status", Types.StringType.get()),
            Types.NestedField.required(6, "amount_cents", Types.LongType.get()),
            Types.NestedField.required(7, "updated_at", Types.TimestampType.withoutZone()),
            Types.NestedField.required(8, "seed", Types.IntegerType.get())),
        new HashSet<>(Arrays.asList(1)));
  }

  public static Schema changelogSchema() {
    return new Schema(
        Arrays.asList(
            Types.NestedField.required(1, "order_id", Types.LongType.get()),
            Types.NestedField.required(2, "business_key", Types.StringType.get()),
            Types.NestedField.required(3, "event_id", Types.LongType.get()),
            Types.NestedField.required(4, "customer_id", Types.LongType.get()),
            Types.NestedField.required(5, "status", Types.StringType.get()),
            Types.NestedField.required(6, "amount_cents", Types.LongType.get()),
            Types.NestedField.required(7, "updated_at", Types.TimestampType.withoutZone()),
            Types.NestedField.required(8, "seed", Types.IntegerType.get()),
            Types.NestedField.required(9, "cdc_row_kind", Types.StringType.get()),
            Types.NestedField.required(10, "cdc_operation", Types.StringType.get()),
            Types.NestedField.optional(11, "source_ts_ms", Types.LongType.get())));
  }

  public static CatalogLoader catalogLoader(JobConfig config) {
    return CatalogLoader.custom(
        config.catalogName(),
        catalogProperties(config),
        new Configuration(),
        "org.apache.iceberg.jdbc.JdbcCatalog");
  }

  public static Map<String, String> catalogProperties(JobConfig config) {
    Map<String, String> properties = new HashMap<>(config.icebergCatalogProperties());
    properties.put(CatalogProperties.URI, config.catalogUri());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    return properties;
  }

  static Catalog loadCatalog(JobConfig config) {
    return CatalogUtil.loadCatalog(
        "org.apache.iceberg.jdbc.JdbcCatalog",
        config.catalogName(),
        catalogProperties(config),
        new Configuration());
  }

  private static Map<String, String> currentProperties(JobConfig config) {
    Map<String, String> properties = new HashMap<>();
    properties.put("format-version", "2");
    properties.put("write.upsert.enabled", "true");
    properties.put("write.delete.mode", "merge-on-read");
    properties.put("write.update.mode", "merge-on-read");
    properties.put("write.merge.mode", "merge-on-read");
    properties.put("write.format.default", "parquet");
    putIfConfigured(
        properties, "write.target-file-size-bytes", config.writeTargetFileSizeBytes());
    putIfConfigured(
        properties, "write.parquet.row-group-size-bytes", config.writeParquetRowGroupSizeBytes());
    putIfConfigured(
        properties,
        "commit.manifest.min-count-to-merge",
        config.commitManifestMinCountToMerge());
    return properties;
  }

  private static Map<String, String> changelogProperties(JobConfig config) {
    Map<String, String> properties = new HashMap<>();
    properties.put("format-version", "2");
    properties.put("write.format.default", "parquet");
    putIfConfigured(
        properties, "write.target-file-size-bytes", config.writeTargetFileSizeBytes());
    putIfConfigured(
        properties, "write.parquet.row-group-size-bytes", config.writeParquetRowGroupSizeBytes());
    putIfConfigured(
        properties,
        "commit.manifest.min-count-to-merge",
        config.commitManifestMinCountToMerge());
    return properties;
  }

  private static void putIfConfigured(
      Map<String, String> properties, String propertyName, String value) {
    if (value != null && !value.isBlank()) {
      properties.put(propertyName, value);
    }
  }

  private static TableLoader withJdbcDriver(TableLoader delegate) {
    return new DriverLoadingTableLoader(delegate);
  }

  private static void loadJdbcDriver() {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException exc) {
      throw new IllegalStateException("MySQL JDBC driver is required for Iceberg JDBC catalog", exc);
    }
  }

  private static final class DriverLoadingTableLoader implements TableLoader {
    private final TableLoader delegate;

    private DriverLoadingTableLoader(TableLoader delegate) {
      this.delegate = delegate;
    }

    @Override
    public void open() {
      loadJdbcDriver();
      delegate.open();
    }

    @Override
    public boolean isOpen() {
      return delegate.isOpen();
    }

    @Override
    public org.apache.iceberg.Table loadTable() {
      return delegate.loadTable();
    }

    @Override
    public TableLoader clone() {
      return new DriverLoadingTableLoader(delegate.clone());
    }

    @Override
    public void close() throws java.io.IOException {
      delegate.close();
    }
  }
}
