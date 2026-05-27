# Version Matrix

Selected on 2026-05-26 for a single-node local reliability lab.

| Component | Pinned family | Phase 1.1 use | Rationale / compatibility note |
| --- | --- | --- | --- |
| Java | 11 | Host toolchain, future Flink job build | Flink CDC 3.x requires JDK 11+; Java 11 keeps compatibility broad for Flink 1.20. |
| Maven | 3.9 | Host toolchain, future Java build | Current Maven 3.x line used for reproducible Flink job packaging. |
| Python | 3.11 | Harness, generator, SQL wrappers | Stable runtime for typed harness code and pyiceberg metadata tooling. |
| Node | 20 | Future static dashboard | LTS line for Vite-based dashboard builds. |
| MySQL | 8.0 | Core source DB and Iceberg JDBC catalog DB | CDC source uses ROW binlog, full row image, and GTID. |
| Apache Flink | 1.20.x | Core JobManager/TaskManager and future batch SQL reader | Required by the selected Flink CDC 3.x line; the Iceberg data reader must be Flink SQL batch or an equivalent equality-delete-aware engine. |
| Flink CDC | 3.6.0 | Phase 1.2+ | Latest selected CDC line; CDC 3.3+ dropped older Flink 1.17/1.18 support. |
| Apache Iceberg | 1.10.x | Phase 1.2+ | v2 tables are required for upsert/equality-delete behavior. |
| MinIO | RELEASE.2025-04-22T22-12-26Z | Core object store | Local S3-compatible warehouse with path-style access. |
| StarRocks | 3.3.x | M3+ only | Kept out of `core`; later used for internal Primary Key table serving and compaction benchmark. |
| pyiceberg | 0.9.x | Metadata-only wrapper | It is allowed for table metadata, file, manifest, and snapshot inspection only. It must not materialize current table data for reconciliation. |

## Known Incompatibilities

- PyIceberg is not a correctness reader for v2 upsert tables that contain equality deletes. Phase 1.1 fixes the split: `make sql-iceberg` identifies the Flink SQL batch data path, while `make sql-iceberg-meta` identifies the pyiceberg metadata-only path.
- StarRocks is excluded from `core`. Catalog smoke tests and internal table imports start in M3.
- No multi-node, GPU, or managed cloud assumptions are part of this stack.

