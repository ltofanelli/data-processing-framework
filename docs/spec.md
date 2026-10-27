# Specification — v1

## 0. How to use this document

- This document is **normative**: it describes the behavior the implementation must have. [architecture.md](architecture.md) explains the design with diagrams and examples. If they diverge, this document wins.
- **MUST** indicates an obligation, **MUST NOT** a prohibition and **MAY** an option.
- Every rule has an ID (for example, `CKP-06`). Code, tests and discussions should cite the ID.
- Anything not written here is not specified. Do not invent behavior: open points are listed in [section 10](#10-not-specified-in-v1).
- Metadata column names and default values cited here are the defaults in `enums.py` (see `ENUM-*`).

---

## 1. Glossary

| Term | Meaning |
|---|---|
| Run | One execution of the pipeline. |
| Batch | A set of data processed and written at once. Each batch produces a single commit on the target. |
| File source | `source` is a folder of files (`file_format` is set). |
| Table source | `source` is a Delta table (name or path). |
| Target | Delta table written by the pipeline (`target`). |
| Validity | From when a version of the row is valid. Comes from `modified_date`. |
| Version order | Which version of a key is the newest. Comes from `order_column`, or from validity when `order_column` is not set. |
| Checkpoint | What has not been read yet. It is the maximum of a column on the target. |
| Period | A slice of the target replaced as a whole by `replace_where` (for example, a month). |
| Final name | A column's name after the rename and the special-character fix. |
| Metadata generated in the run | A metadata column that the current run creates (see `META-01`). |

---

## 2. Principles

- **PRI-01** The common path MUST be configurable with text parameters only: `source`, `target`, `file_format`, `modified_date`, `write_mode`, `merge_key`, `scd2`, `rename_columns`, `data_types`. Out-of-pattern cases use sub-parameters; very specific cases use extension points in code (`transform`, `Reader`, `Writer`, `DQRule`).
- **PRI-02** The framework MUST NOT depend on a control table or a log process. The state is the target itself. The only exception is `discovery = "external_checkpoint"`, where the control table is provided by the user.
- **PRI-03** Listing, checking and moving files MUST go through the `FileSystemAdapter` interface. No other part of the framework accesses the file system directly.
- **PRI-04** Validity MUST come from the source or the extraction (column, file date, date in the path or a provided value). It MUST NOT come from the time the framework processes the data.
- **PRI-05** Every parameter that references a column uses the **final name**. Only `rename_columns` uses the source name. To filter the source, the framework translates the final name through the rename map.
- **PRI-06** Dates are the configurer's choice: the framework does not pick the validity column or the checkpoint column on its own.

---

## 3. Parameters

### 3.1 General

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `source` | str | required, except with `fixed_query` or `custom_query` | Name `schema.table` or path. With a query, it only identifies the source in the `RunResult` | SRC-01, SRC-02 |
| `target` | str | required | Name `schema.table` or path | SRC-02, SRC-03 |
| `write_mode` | str | required | `append`, `overwrite`, `replace_where`, `upsert` | WRT-* |
| `run_id` | str | generated UUID | Run identifier, usually sent by the orchestrator | META-04 |
| `transform` | function | — | `transform(df, ctx) -> df` | CONF-01 |

### 3.2 File source

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `file_format` | str | — | Spark read format (`parquet`, `csv`, `json`...). Setting it makes the source a file source | SRC-01 |
| `read_options` | dict | `{}` | Options passed to the Spark reader | — |
| `discovery` | str | `incremental` | `incremental`, `file_compare`, `external_checkpoint`, `folder_pattern` | DISC-* |
| `chunk_size` | int | no chunk: 1 batch | ≥ 1 | DISC-07 |
| `path_pattern` | str | — | Not specified in v1 | §10 |

### 3.3 Table source

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `strategy` | str | `checkpoint_column` | `checkpoint_column`, `fixed_query`, `custom_query` | CKP-09 |
| `query` | str | — | SQL. Required with `fixed_query` and `custom_query` | CKP-09 |
| `checkpoint_column` | str | the `modified_date` column | Final name of a `timestamp`, `date` or numeric column | CKP-02 to CKP-05 |
| `latest_load_only` | bool | `False` | — | CKP-08 |

### 3.4 Validity and order

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `modified_date` | str | — | Final column name (`column`) or fixed value (`value`) | VAL-01, VAL-02 |
| `modified_date_source` | str | `column` | `column`, `file_mtime`, `file_path`, `value` | VAL-02 |
| `modified_date_format` | str | `Defaults.DATETIME_FORMAT` | Spark date pattern | VAL-03 |
| `modified_date_regex` | str | — | Regex with one capture group. Required with `file_path` | VAL-02 |
| `order_column` | str | — | Final name of a column with an orderable type | VAL-07 to VAL-10 |

### 3.5 Conform

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `rename_columns` | list[dict] or dict | — | Pairs source name → final name | CONF-02 |
| `select_columns` | list | — | Only these columns (final names). Mutually exclusive with `drop_columns` | CONF-04 |
| `drop_columns` | list | — | All but these (final names). Mutually exclusive with `select_columns` | CONF-04 |
| `hash_include` | list | — | Only these columns in the hash. Mutually exclusive with `hash_exclude` | DED-02 |
| `hash_exclude` | list | — | All but these in the hash. Mutually exclusive with `hash_include` | DED-02 |

### 3.6 Data Quality

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `data_types` | dict | — | Column (final name) → Spark type | DQ-03 |
| `rules` | list[DQRule] | — | Custom rules, in code | DQ-04 |
| `quarantine_table` | str | — | Name or path of the quarantine table | DQ-05, DQ-06 |
| `gate_type` | str | — (no gate) | `count`, `percent` | DQ-07 |
| `gate_threshold` | float | — | `count`: number of rows. `percent`: 0 to 100 | DQ-07 |

### 3.7 Write

| Parameter | Type | Default | Values / format | Rules |
|---|---|---|---|---|
| `merge_key` | str or list | — | Final names | DED-01, WRT-05 |
| `scd2` | bool | `False` | — | SCD-* |
| `merge_schema` | bool | `True` | — | SCH-02 |
| `partition_by` | list | — | Partition columns used when creating the target | SRC-03 |
| `replace_period_source` | str | `path` (file source) / `column` (table source) | `path`, `value`, `column` | PER-01 |
| `replace_period` | str | — | Column name (`column`) or period value (`value`) | PER-05, PER-06 |
| `replace_period_regex` | str | — | Regex with one capture group applied to the path | PER-02 |
| `replace_period_format` | str | — | Period format (e.g. `yyyy-MM`, `yyyyMM`) | PER-02 |

---

## 4. Execution order

The pseudocode below is the reference for the order of the steps. The IDs point to the rule that governs each step.

```text
run(config):
    validate(config)                                  # CFG-*, ENUM-03
    run_id = config.run_id or new UUID                # META-04
    batches = plan(config)                            # SRC, DISC, CKP, PER
    if batches is empty:
        return RunResult(status = "no_new_data")      # CKP-06, DISC-08
    for each batch, in planned order:
        batch_ts = current time                       # META-02
        df = read(batch)                              # SCH-01
        if config.transform: df = transform(df, ctx)
        df = rename(df)                               # CONF-02
        df = fix_names(df)                            # CONF-03
        df = resolve_validity(df)                     # VAL-05
        df = select_columns(df)                       # CONF-04
        df = add_metadata(df, run_id, batch_ts)       # META-*
        valid, quarantine, failure = apply_dq(df)     # DQ-*
        write quarantine, if any                      # DQ-06
        if failure or gate exceeded:
            stop the run                              # ERR-01
        if config.merge_key:
            valid = dedup(valid)                      # DED-*
        write(valid) in a single commit               # WRT-*, SCD-*
    return RunResult(status = "success")              # RES-*

plan(config), table source:
    if strategy == custom_query: return [query result]                # CKP-09
    col = checkpoint column                                           # CKP-02
    if col does not exist: return [whole source]
    max_target = maximum on the target side, if the target exists     # CKP-03
    max_source = maximum of col on the source
    if max_target exists and max_source <= max_target: return []      # CKP-06
    if latest_load_only: return [col == max_source]                   # CKP-08
    if write_mode == overwrite: return [whole source]                 # CKP-07
    rows = col > max_target and col <= max_source
    if write_mode == replace_where:
        rows = all source rows of the periods present in rows         # PER-06
    return [rows]

plan(config), file source:
    files = list(source)                                              # DISC-01
    if replace_period_source == value:
        return [files whose period in the path == replace_period]     # PER-05
    if discovery == folder_pattern:
        return batches of whole selected folders                      # PER-04
    new = files selected by discovery                                 # DISC-02, DISC-05
    if new is empty: return []                                        # DISC-08
    if write_mode == overwrite: return [all listed files]             # DISC-09
    sort new by (modification time, path)                             # DISC-03
    return new grouped by chunk_size                                  # DISC-07
```

---

## 5. Rules

### 5.1 Source and target (SRC)

- **SRC-01** With `file_format`, the source is a file source and `source` is a folder. Without `file_format`, the source is a Delta table.
- **SRC-02** In `source`, `target` and `quarantine_table`, a value containing `/` is a path; without `/`, it is a table name `schema.table`.
- **SRC-03** If the target does not exist, it MUST be created on the first run with the schema of the final DataFrame, the metadata columns (and the SCD2 columns, when `scd2 = True`) and the `partition_by` partitions. If the schema in the name does not exist, it MUST be created first.
- **SRC-04** In a file source, each row's path and modification time MUST come from Spark's hidden `_metadata` column. These values MUST have the same normalization and precision as the values used in file discovery.

### 5.2 File discovery (DISC)

- **DISC-01** Listing is recursive and MUST ignore files whose name starts with `_` or `.`.
- **DISC-02** `incremental` (default): selects files whose modification time is **strictly greater** than the largest `_source_file_modified_ts` on the target. If the target does not exist, selects all.
- **DISC-03** Selected files MUST be sorted by (modification time, path) before being grouped into batches.
- **DISC-04** Ties in `incremental` are not handled: a file with the same time as the target's maximum is not read. This is the expected v1 behavior; `file_compare` avoids the problem.
- **DISC-05** `file_compare`: selects files whose path is not among the distinct `_source_file_path` values on the target. The file's identity is its path: a file rewritten with the same name is not read again.
- **DISC-06** `external_checkpoint`: compares the listing with a control table provided by the user. The format of that table is not specified in v1.
- **DISC-07** No `chunk_size`: all selected files in one batch. `chunk_size = N`: N files per batch. With `folder_pattern`, N counts folders (PER-04).
- **DISC-08** No file selected: the run ends with status `no_new_data`, without writing.
- **DISC-09** With `write_mode = "overwrite"`, discovery only decides whether there is a new file. If there is, all listed files MUST be read, in a single batch.
- **DISC-10** A file with no rows writes nothing to the target and is therefore selected again in later runs. This behavior is accepted.

### 5.3 Checkpoint (CKP)

- **CKP-01** The checkpoint is the maximum of a column on the target, queried at the start of each run. It MUST NOT be stored anywhere else.
- **CKP-02** Checkpoint column in a table source: `checkpoint_column`, if set; otherwise the `modified_date` column, when `modified_date_source = "column"`; otherwise none, and the source is read in full on every run.
- **CKP-03** Target side: with the default checkpoint (the `modified_date` column), it is `_modified_date`, and the source filter applies the same date conversion. With `checkpoint_column`, it is that column under its final name. In the source filter, the final name is translated through the rename map (PRI-05).
- **CKP-04** `checkpoint_column` MUST be `timestamp`, `date` or numeric.
- **CKP-05** `checkpoint_column` MUST NOT have the name of a metadata column generated in the run, because the target would store the run's own value. To use a metadata column from the previous layer (e.g. `_created_ts`), rename it in `rename_columns` or write it under another name in the previous layer (ENUM-02).
- **CKP-06** Closed window: query `max_target` and `max_source`. If `max_source <= max_target`, the run ends with `no_new_data`, without writing. Otherwise, read `column > max_target AND column <= max_source`. Without a target, read `column <= max_source`. Rows arriving during the read are left for the next run.
- **CKP-07** With `write_mode = "overwrite"`, CKP-06 only decides whether the run happens. The read covers the whole source.
- **CKP-08** With `latest_load_only = True`, read only `column = max_source`, after the CKP-06 check. The column must have the same value in every row of a load.
- **CKP-09** With `fixed_query`, the CKP-06 window is applied over the query result. With `custom_query`, there is no framework checkpoint: the query defines the slice and is read on every run.
- **CKP-10** Reprocessing means deleting from the target: removing rows lowers the maximum, and the next run re-reads from there.
- **CKP-11** In `append`, re-reading duplicates and skipping loses data. So append MUST have a way to identify what is new (CFG-12).

### 5.4 Validity and order (VAL)

- **VAL-01** With `merge_key`, validity MUST be defined: `modified_date` with `modified_date_source` equal to `column` or `value`, or `modified_date_source` equal to `file_mtime` or `file_path`. Without `merge_key`, validity is optional.
- **VAL-02** Validity sources:
  - `column`: the column given in `modified_date`;
  - `file_mtime`: the file's modification time;
  - `file_path`: the date extracted from the path by the capture group of `modified_date_regex` and converted with `modified_date_format`;
  - `value`: the value of `modified_date`, the same for every row of the run.
- **VAL-03** `timestamp` or `date` columns are not converted. Strings are converted with `modified_date_format` (default `Defaults.DATETIME_FORMAT`).
- **VAL-04** There is no time zone conversion. A string without a time zone is interpreted in the Spark session time zone.
- **VAL-05** Validity MUST be resolved into `_modified_date` before column selection and the other metadata. So the source column can be dropped afterwards and can have the name of a metadata column generated in the run.
- **VAL-06** `_modified_date` is written whenever validity is defined.
- **VAL-07** With `order_column`, it decides which version is the newest: in dedup, in upsert, for late data and in the SCD2 sequence. `_modified_date` then only provides the dates. Without `order_column`, `_modified_date` decides both.
- **VAL-08** `order_column` does not become metadata: it stays on the target under its final name, MUST NOT be dropped and is excluded from the hash.
- **VAL-09** Ties in version order within the same key: break by `order_column`, if set; otherwise deterministically by the row hash. Ties broken by the hash are counted in `ties_resolved`.
- **VAL-10** Order and date can disagree. Within a key, a version MUST be discarded when there is a newer version by order whose date is less than or equal to its own, because it was superseded before it became valid. The result MUST be the same whether the versions arrive in the same batch or in different runs.

### 5.5 Conform (CONF)

- **CONF-01** Fixed order: `transform` → rename → special-character fix → validity (`_modified_date`) → column selection → other metadata.
- **CONF-02** `rename_columns` accepts a list of single-pair dictionaries or a single dictionary. It uses source names.
- **CONF-03** Name fix. Always runs, after the rename:
  - each sequence of characters Delta does not accept (space, `,`, `;`, `{`, `}`, `(`, `)`, `=`, newline, tab) becomes `_`;
  - case, accents and other characters are kept;
  - changed names go to `warnings`;
  - if two names become equal case-insensitively, the run fails.
- **CONF-04** `select_columns` and `drop_columns` are mutually exclusive, use final names and are applied before the other metadata.
- **CONF-05** Source columns with the name of a metadata column generated in the run are overwritten, with a warning in `warnings`. Metadata the run does not generate (e.g. `_source_file_modified_ts` in a table read) is kept.

### 5.6 Data Quality (DQ)

- **DQ-01** DQ runs after Conform and before dedup.
- **DQ-02** Automatic rules, with the quarantine action: `merge_key`, `_modified_date` and `order_column` (when set) not null.
- **DQ-03** `data_types` converts the columns to the given types. A row whose value does not convert goes to quarantine; valid rows are written already converted.
- **DQ-04** Each `DQRule` has an action:
  - `FAIL`: a failing row fails the run;
  - `QUARANTINE`: the row goes to quarantine;
  - `WARN`: the row continues and is counted in `dq_by_rule`.
- **DQ-05** Without `quarantine_table`, a row that would go to quarantine fails the run.
- **DQ-06** Quarantined rows are written to `quarantine_table`, even when the gate is exceeded. They go back to quarantine on every run until they are fixed at the source.
- **DQ-07** The gate evaluates each whole batch. `count`: exceeded when the quarantined rows exceed `gate_threshold`. `percent`: exceeded when `quarantined rows / rows read × 100` exceeds `gate_threshold`. Gate exceeded: the batch is not written and the run ends with `gate_failed`. Without `gate_type`, there is no gate.

### 5.7 Dedup (DED)

- **DED-01** Dedup only runs when there is a `merge_key`, after DQ.
- **DED-02** The row hash considers all columns except the `modified_date` column, `order_column` and columns starting with `_`. `hash_include` and `hash_exclude`, mutually exclusive, replace this rule.
- **DED-03** The hash MUST NOT be written to the target. In SCD2, the hash of the current version is computed on the fly, with the same definition as the batch.
- **DED-04** Without SCD2: keeps one row per `merge_key`, the newest (VAL-07).
- **DED-05** With SCD2: sorts each key's rows by version order and removes only consecutive repeats (hash equal to the previous row's), keeping the oldest.

### 5.8 Write (WRT)

- **WRT-01** Each batch MUST be written in a single Delta commit. Each written batch advances the checkpoint.
- **WRT-02** `append`: inserts the rows.
- **WRT-03** `overwrite`: replaces the whole target, in a single batch (DISC-09, CKP-07).
- **WRT-04** `replace_where`: replaces on the target the periods present in the batch (PER-*).
- **WRT-05** `upsert`: MERGE by `merge_key`. A new key is inserted; an existing key is only updated when the source row is newer (VAL-07). Older rows are not applied and are counted in `rows_late`.
- **WRT-06** The same pipeline should not run in parallel. v1 has no protection against it: two simultaneous runs see the same checkpoint and, in append, write the same data twice.

### 5.9 SCD2 (SCD)

- **SCD-01** `scd2 = True` is only accepted with `upsert` (incremental history) or `overwrite` (rebuild).
- **SCD-02** Upsert with SCD2, for the keys in the batch:
  1. read the current versions (`_is_current = true`) on the target;
  2. compute their hash (DED-03);
  3. discard rows older than the current version (late data, counted in `rows_late`);
  4. if the key's first version in the batch has the same hash as the current one, discard that row;
  5. sort the remaining versions and apply VAL-10;
  6. compute the intervals (SCD-03);
  7. write everything in a single MERGE (SCD-05).
- **SCD-03** Intervals:
  - `_valid_from` = `_modified_date`; on the first version of a new key, `Defaults.SCD2_FIRST_VALID_FROM` (default `1900-01-01`; with `None`, uses `_modified_date`);
  - `_valid_to` = `_valid_from` of the next version; on the current version, `Defaults.SCD2_CURRENT_VALID_TO` (default `9999-12-31`; with `None`, it is null);
  - the interval is closed at the start and open at the end: the date join is `date >= _valid_from AND date < _valid_to`;
  - `_version` = current version + position; `_is_current = true` only on the last one.
- **SCD-04** A late row is not inserted in the middle of the history.
- **SCD-05** Closing the current version and inserting the new versions MUST happen in the same MERGE:
  - version to close: `_valid_to` = `_valid_from` of the first new version, `_is_current = false`, plus the batch's `_updated_ts` and `_run_id`;
  - new versions: inserted with the batch's `_created_ts`, `_updated_ts` and `_run_id`.
- **SCD-06** `overwrite` with SCD2:
  - reads the whole source, builds every version of each key with rules DED-05, VAL-10 and SCD-03 and replaces the target in a single commit;
  - the source must keep every version;
  - `_created_ts` and `_updated_ts` receive the rebuild time.

### 5.10 Periods (PER)

- **PER-01** The period source is `replace_period_source`: `path` (default in a file source), `value` (file source only) or `column` (default and only option in a table source).
- **PER-02** With `path`, the period is the capture group of `replace_period_regex` applied to the path, converted with `replace_period_format`.
- **PER-03** One file per period: the normal discovery (`incremental`) works, and `chunk_size` groups files normally.
- **PER-04** One folder per period (`discovery = "folder_pattern"`):
  - a folder is selected when it has a file whose modification time is greater than the largest `_source_file_modified_ts` written for that period on the target, or when the period does not exist on the target;
  - every file of the selected folder is read, including those already written;
  - a period MUST NOT be split across batches, and `chunk_size` counts folders;
  - a file deleted from the folder does not trigger reprocessing.
- **PER-05** With `value`, `source` is still the root:
  - only files whose period in the path (PER-02) equals `replace_period` are read;
  - the value uses the same format as the period in the path;
  - the checkpoint is ignored;
  - all files of the period go in a single batch.
- **PER-06** Table source, period from the `replace_period` column:
  - with a checkpoint, the window (CKP-06) only identifies the periods with new rows, and the framework MUST re-read every source row of those periods before replacing them;
  - without a checkpoint, the source is read in full;
  - with `custom_query`, the query must return complete periods.
- **PER-07** The period is written to `_period`, and `replaceWhere` replaces the `_period` values present in the batch. Replaced periods are reported in `periods_replaced`.

### 5.11 Schema (SCH)

- **SCH-01** The files of a batch MUST be read with the merged schema of all of them. Incompatible types between files of the same batch make the read fail.
- **SCH-02** New column relative to the target: with `merge_schema = True` (default), it is added; with `False`, the run fails listing the new columns. Added columns go to `warnings`.
- **SCH-03** A column that is no longer present is written as null in the new rows.
- **SCH-04** A type different from the target makes the run fail. The only conversion is the explicit one, done by `data_types`, by a `DQRule` or by `transform`.

### 5.12 Metadata (META)

- **META-01** Columns and when they exist:

  | Column | Exists when | Content |
  |---|---|---|
  | `_run_id` | Always | Run of the row's last write |
  | `_created_ts` | Always | Time of the batch that inserted the row. Never changes |
  | `_updated_ts` | `upsert` or `scd2 = True` | Time of the batch of the last write |
  | `_modified_date` | Validity defined | Row validity |
  | `_source_file_path` | File source | Path of the source file |
  | `_source_file_modified_ts` | File source | Modification time of the source file |
  | `_period` | `replace_where` | Row period |
  | `_valid_from`, `_valid_to`, `_is_current`, `_version` | `scd2 = True` | Version control |

- **META-02** `_created_ts` and `_updated_ts` receive the time the batch started being written, always after the previous batch's commit. The value is the same for every row of the batch.
- **META-03** On insert, `_updated_ts = _created_ts`. `_updated_ts` changes when the row is updated and, in SCD2, when the version is closed.
- **META-04** `_run_id` comes from the `run_id` parameter or from a UUID generated at the start of the run. In upsert, it changes together with `_updated_ts`.
- **META-05** Naming convention: every framework column starts with `_`; times recorded by the framework or by storage end in `_ts`; validity columns follow the concept (`_modified_date`, `_valid_from`, `_valid_to`).

### 5.13 Enums (ENUM)

- **ENUM-01** `enums.py` holds the metadata column names (`MetadataColumns`), the values accepted by parameters (`WriteMode`, `ModifiedDateSource`, `DiscoveryStrategy`, `ReadStrategy`, `ReplacePeriodSource`, `DQAction`, `GateType`) and the default values (`Defaults`).
- **ENUM-02** They are plain classes with attributes, NOT `enum.Enum`, because the values MAY be overridden in code before a run (e.g. `MetadataColumns.RUN_ID = "_bronze_run_id"`).
- **ENUM-03** The values MUST be validated at the start of the run with the same rule as CONF-03.
- **ENUM-04** An override applies to the whole Python process until it is undone.
- **ENUM-05** If the target exists and lacks a metadata column the run needs to read (checkpoint column, `_modified_date` in upsert or the SCD2 columns), the run MUST fail with a message instead of treating the target as empty.

Default values:

| Class | Member | Value |
|---|---|---|
| `MetadataColumns` | `RUN_ID`, `CREATED_TS`, `UPDATED_TS`, `MODIFIED_DATE`, `SOURCE_FILE_PATH`, `SOURCE_FILE_MODIFIED_TS`, `PERIOD`, `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, `VERSION` | `_run_id`, `_created_ts`, `_updated_ts`, `_modified_date`, `_source_file_path`, `_source_file_modified_ts`, `_period`, `_valid_from`, `_valid_to`, `_is_current`, `_version` |
| `Defaults` | `DATETIME_FORMAT` | `yyyy-MM-dd HH:mm:ss.SSSSSS` |
| `Defaults` | `SCD2_FIRST_VALID_FROM` | `1900-01-01` |
| `Defaults` | `SCD2_CURRENT_VALID_TO` | `9999-12-31` |

### 5.14 Errors and commits (ERR)

- **ERR-01** Any error stops the run: the current batch is not written and the following batches are not processed. Examples: read, schema, write, `FAIL` in DQ, quarantine without `quarantine_table`, gate exceeded.
- **ERR-02** Previous batches remain written. The next run resumes from the failed batch, through the checkpoint.
- **ERR-03** A row sent to quarantine is not an error.

---

## 6. Configuration validation (CFG)

The configuration MUST be validated before any read. Each combination below MUST be rejected with a message citing the ID.

| ID | Rejected combination | Reason |
|---|---|---|
| CFG-01 | `target` or `write_mode` missing, or `source` missing outside `fixed_query` and `custom_query` | Required parameters |
| CFG-02 | `merge_key` without defined validity (VAL-01) | No way to order the versions |
| CFG-03 | `write_mode = "upsert"` without `merge_key` | MERGE needs the key |
| CFG-04 | `scd2 = True` with `append` or `replace_where` | SCD-01 |
| CFG-05 | `order_column` without `merge_key` | No versions to order |
| CFG-06 | `hash_include` with `hash_exclude` | Mutually exclusive parameters |
| CFG-07 | `select_columns` with `drop_columns` | Mutually exclusive parameters |
| CFG-08 | `modified_date_source = "file_path"` without `modified_date_regex` | No way to extract the date |
| CFG-09 | `modified_date_regex` with `modified_date_source` other than `file_path` | The regex only applies to the path |
| CFG-10 | `file_mtime` or `file_path` with a table source | There is no source file |
| CFG-11 | `checkpoint_column` or `latest_load_only` with a file source | In a file source, what is new is decided by discovery |
| CFG-12 | `append` with a table source without `checkpoint_column`, without `modified_date` as a column and without `custom_query` | Every run would write the same data again |
| CFG-13 | `latest_load_only` without a checkpoint column (CKP-02) | No way to identify the most recent load |
| CFG-14 | `checkpoint_column` with the name of a metadata column generated in the run | CKP-05 |
| CFG-15 | Checkpoint column or `order_column` dropped by `drop_columns` or missing from `select_columns` | The next run needs it |
| CFG-16 | `chunk_size` with a table source | File batches only exist in a file source |
| CFG-17 | `chunk_size` with `write_mode = "overwrite"` | WRT-03 |
| CFG-18 | `replace_where` in a file source without `replace_period_regex` or without `replace_period_format` | No way to know each file's period |
| CFG-19 | `replace_where` in a table source without `replace_period` | No period column |
| CFG-20 | `replace_period_source = "value"` without `replace_period` | No period provided |
| CFG-21 | `replace_period_source` incompatible with the source (`path` or `value` in a table source, `column` in a file source) | PER-01 |
| CFG-22 | `discovery = "folder_pattern"` with a `write_mode` other than `replace_where` | Without replacing the period, data would be repeated |
| CFG-23 | `strategy` equal to `fixed_query` or `custom_query` without `query` | Nothing to read |
| CFG-24 | `gate_type` without `gate_threshold`, or the other way around | The gate needs both |

---

## 7. RunResult (RES)

- **RES-01** Every run MUST return a `RunResult` with the fields below, aggregated across all batches.
- **RES-02** `status` is `success`, `failed`, `gate_failed` or `no_new_data`.

| Field | Description |
|---|---|
| `run_id` | Run identifier |
| `source` / `target` | Source and target |
| `write_mode` | Write mode |
| `modified_date_source` | Validity source used |
| `started_at` / `finished_at` / `duration_s` | Run time |
| `status` | RES-02 |
| `batches` | Batches processed |
| `files_read` | Files read (file source) |
| `rows_read` | Rows read |
| `rows_quarantined` | Rows sent to quarantine |
| `dq_by_rule` | Count and % of failures per rule |
| `gate` | Type, threshold, measured value and whether it passed |
| `rows_after_dedup` | Rows after dedup |
| `ties_resolved` | Ties broken by the hash (VAL-09) |
| `rows_late` | Rows not applied because they are older than the target |
| `rows_inserted` / `rows_updated` / `rows_closed` | Write metrics (`rows_closed` = SCD2 versions closed) |
| `periods_replaced` | Periods replaced (`replace_where`) |
| `checkpoint` | Checkpoint value at the start of the run and last value processed |
| `warnings` | Fixed names, new columns on the target, overwritten metadata |
| `error` | Error message, when there is one |

---

## 8. Contracts

| Component | Kind | Responsibility |
|---|---|---|
| `PipelineConfig` | dataclass | Groups `SourceConfig`, `ValidityConfig`, `ConformConfig`, `DQConfig`, `TargetConfig`, `transform` and `run_id`. Exposes `from_dict(dict)` and `validate()` (CFG-*) |
| `SourceConfig` | abstract | `source`. Specializations: `FileSourceConfig` (3.2) and `TableSourceConfig` (3.3) |
| `ValidityConfig` | dataclass | Parameters in section 3.4 |
| `ConformConfig` | dataclass | Parameters in section 3.5 |
| `DQConfig` | dataclass | Parameters in section 3.6 |
| `TargetConfig` | dataclass | Parameters in section 3.7 |
| `Pipeline` | class | `run() -> RunResult`, following section 4 |
| `Reader` | extensible | `plan() -> list[Batch]`, `read(batch) -> DataFrame`. Implementations: `FileReader` and `TableReader` |
| `FileSystemAdapter` | interface | `list_files(path, recursive) -> list[FileInfo]`, `exists(path) -> bool`, `move(src, dst)` (PRI-03) |
| `FileInfo` | dataclass | `path`, `size`, `modification_time` |
| `Transformer` | protocol | `(df, ctx) -> DataFrame` |
| `ConformStep` | interface | `apply(df, ctx) -> DataFrame` |
| `DQRule` | extensible | `name`, `action` (`DQAction`), `condition() -> Column` |
| `Writer` | extensible | `write(df, ctx) -> WriteMetrics` |
| `RunResult` | dataclass | Section 7 |

Classes marked as extensible have a default implementation and MAY be overridden for specific cases.

---

## 9. Acceptance scenarios

Each scenario is a test. The "Rules" column shows what it covers.

| ID | Given | When | Then | Rules |
|---|---|---|---|---|
| AC-01 | Table source with `updated_at`; `write_mode = "upsert"`, `merge_key = "id"`, `modified_date = "updated_at"` | Run with new rows | Rows inserted or updated; `_modified_date` written; checkpoint = maximum of `_modified_date` on the target | CKP-02, CKP-03, WRT-05 |
| AC-02 | The same pipeline, with no new rows at the source | Run | Status `no_new_data`; target unchanged | CKP-06 |
| AC-03 | SCD2; key 1 with A(t1), A(t2), B(t3), A(t4) in the same batch | Dedup | A(t1), B(t3), A(t4) remain | DED-05 |
| AC-04 | File upsert with `modified_date = "dt_update"`; file A (arrival 10:00, id 1, `dt_update` 09:00, X) already written | File B arrives (arrival 11:00, id 1, `dt_update` 08:00, Y) | B is read but not applied; target keeps X; `rows_late = 1` | DISC-02, WRT-05 |
| AC-05 | SCD2 with `order_column = "seq"`; versions A (seq 3, date 11:00) and B (seq 5, date 10:00) of the same key | In the same batch, or B in one run and A in the next | In both cases, only B is in the history | VAL-10 |
| AC-06 | SCD2 with defaults; new key with versions v1 and v2 | Run | v1: `_valid_from = 1900-01-01`, `_valid_to = _valid_from` of v2. v2: `_valid_to = 9999-12-31`, `_is_current = true` | SCD-03 |
| AC-07 | SCD2; key already has a current version | A version with a different hash arrives | Current closed and new inserted in the same commit; `_updated_ts` and `_run_id` of the closed one updated | SCD-05 |
| AC-08 | Rows with the same business content and a different `_bronze_created_ts` | Upsert SCD2 | No new version | DED-02 |
| AC-09 | Source with accumulated full loads (`load_ts` 10/01 and 10/02); `overwrite`, `checkpoint_column = "load_ts"`, `latest_load_only = True` | Run | Target only has the 10/02 load. A new run without a new load ends with `no_new_data` | CKP-08 |
| AC-10 | `write_mode = "overwrite"` with `modified_date` as checkpoint | Run with new rows | Reads the whole source and writes in a single batch | CKP-07, WRT-03 |
| AC-11 | `upsert` without `merge_key` | Validation | Rejected: CFG-03 | CFG-03 |
| AC-12 | `checkpoint_column = "_created_ts"` without rename | Validation | Rejected: CFG-14. With `rename_columns = [{"_created_ts": "_bronze_created_ts"}]` and `checkpoint_column = "_bronze_created_ts"`, it is accepted | CKP-05 |
| AC-13 | File append with `chunk_size = 5`; the 4th file of the 2nd batch has an incompatible type | Run | Batch 1 written; batch 2 not written; following batches not processed; status `failed`. The next run re-reads the files of batch 2 | ERR-01, ERR-02, SCH-01 |
| AC-14 | Columns `Sale Date` and `Amount` at the source | Run | Columns written as `Sale_Date` and `Amount`; `warnings` records the change | CONF-03 |
| AC-15 | Columns `a b` and `a_b` at the source | Run | Run fails: the names collide after the fix | CONF-03 |
| AC-16 | Only the 4th file of a batch has the column `new_col` | Run with `merge_schema = True` | `new_col` added to the target; rows from the other files have `new_col` null | SCH-01, SCH-02 |
| AC-17 | Same case, with `merge_schema = False` | Run | Run fails listing `new_col` | SCH-02 |
| AC-18 | No `quarantine_table`; a row with a null `merge_key` | Run | Run fails; batch not written | DQ-02, DQ-05 |
| AC-19 | `gate_type = "percent"`, `gate_threshold = 2`; 3% of the rows go to quarantine | Run | Quarantine written; batch not written; status `gate_failed` | DQ-06, DQ-07 |
| AC-20 | Table source with a `_created_ts` column; the run also generates `_created_ts` | Run | Source value overwritten; warning in `warnings` | CONF-05 |
| AC-21 | `MetadataColumns.RUN_ID = "_bronze_run_id"` before the run | Run | Target receives `_bronze_run_id` | ENUM-02 |
| AC-22 | Existing target without the expected checkpoint column | Run | Run fails with a message; the target is not treated as empty | ENUM-05 |
| AC-23 | `discovery = "incremental"`; two files with the same modification time; only one was written | Run | The other is not read (expected v1 behavior) | DISC-04 |
| AC-24 | `discovery = "file_compare"`; a file already written is rewritten with the same name | Run | The file is not read again | DISC-05 |
| AC-25 | `folder_pattern`, `replace_where`, `chunk_size = 1`; a new file arrives in folder `2026-08/` | Run | Every file in `2026-08/` is read in one batch; period `2026-08` is replaced; `periods_replaced = ["2026-08"]` | PER-04, PER-07 |
| AC-26 | `replace_period_source = "value"`, `replace_period = "202608"`, no new file | Run | Reads only the files of period `202608` and replaces the period, ignoring the checkpoint | PER-05 |
| AC-27 | Table source, `replace_where` by `year_month`; the window returns 1 new row of `202608` | Run | Every row of `202608` is re-read from the source before the period is replaced | PER-06 |
| AC-28 | SCD2 with `overwrite`; source with every version, including one that arrived late | Run | History rebuilt with the late version in the right position; a single commit | SCD-06 |
| AC-29 | String `dt_change` without a time zone | Run | `_modified_date` interpreted in the Spark session time zone, without conversion | VAL-04 |
| AC-30 | `modified_date = "_created_ts"` coming from the previous layer | Run | `_modified_date` receives the previous layer's value; the run's own `_created_ts` is written normally | VAL-05, CONF-05 |

---

## 10. Not specified in v1

These points have no defined behavior. The implementation MUST NOT invent a rule for them; it must raise the question.

- Format of the `external_checkpoint` control table.
- Semantics of `path_pattern`.
- Schema of the quarantine table: columns beyond the row's own, such as the rule that failed.
- Declarative format for DQ rules beyond `data_types` and `DQRule` in code.
- Contents of the `ctx` received by `transform`, `ConformStep` and `Writer`.
- Data type written to `_period`.
