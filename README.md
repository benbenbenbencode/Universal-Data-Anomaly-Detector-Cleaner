# Universal Data Anomaly Detector & Cleaner work to any report by beny@wagely.app

Two Python scripts that (1) detect common data-quality problems in a folder of CSV files without any hardcoded schema, and (2) produce cleaned, database-ready CSVs with explicit column types, so they can be imported into PostgreSQL (via DBeaver) and analysed right away.

## 1. What is in the package

| File | Purpose |
|---|---|
| `universal_anomaly_detector.py` | Scans all CSVs in a folder and writes `universal_anomaly_report.csv`. Read-only: never changes your data. |
| `universal_data_cleaner.py` | Cleans the data and creates import-ready files. Imports helper functions from the detector, so **keep both files in the same folder**. |

## 2. Requirements

- Python 3.10+
- `pandas` and `numpy` (`pip install pandas numpy`)
- Tested with Python 3.12, pandas 3.0.2 and PostgreSQL 16. Not tested on pandas 2.x, MySQL, SQL Server, or Redshift.

## 3. How to run

1. Put both `.py` files in any folder.
2. Open each file and set `DATA_DIR` (at the bottom, under `if __name__ == "__main__":`) to the folder that contains your CSV files.
3. Run the detector (optional, report only):
   ```
   python universal_anomaly_detector.py
   ```
4. Run the cleaner:
   ```
   python universal_data_cleaner.py
   ```

The original CSVs are never modified.

## 4. What the cleaner produces

Three new sub-folders are created next to your input CSVs.

```
da_test_data_v2/
├── cleaned/                 <- what you import into the database
│   ├── <table>.csv          one cleaned CSV per input file
│   ├── schema.sql           CREATE TABLE statements with explicit types
│   └── load_postgres.sql    psql \copy commands (optional alternative to the DBeaver wizard)
├── quarantine/              <- rows removed from the clean table (exact duplicates), with the reason
└── cleaning_report/
    ├── change_summary.csv   counts per table / column / action
    ├── change_log_rows.csv  row-level audit trail (Row = 0-based index; CSV line = Row + 2)
    ├── reconciliation.csv   raw rows = cleaned rows + quarantined rows
    └── validation.csv       automatic checks that the exported files match schema.sql
```

If any validation check fails, the script stops with an error instead of writing files that could break the import.

## 5. Cleaning rules (what happens to bad data)

Principle: **fix what is deterministic, NULL what is unusable, flag what is ambiguous, drop only exact duplicates (and keep them in quarantine).**

| Action | Examples |
|---|---|
| **FIXED** | Trim whitespace; placeholder strings (`n/a`, `null`, `?`) become NULL; category spellings that differ only by case or whitespace are merged to the most frequent spelling (`Multi Kill` becomes `Multi kill`); column names normalised to `snake_case` (`user_Id` becomes `user_id`). |
| **NULLED** | Unparseable dates (e.g. `4350983` in a date column); non-numeric text in a numeric column (`abc`, `1,234`, `inf`). The original value is kept in `change_log_rows.csv` (column `Old`). |
| **DROPPED** | Exact duplicate rows (first copy kept). Dropped rows go to `quarantine/`, never deleted. |
| **FLAGGED** | Outliers, negative values, orphan foreign keys, rows that are duplicates except for the `id`, conflicting attributes for the same key, wrong date order, out-of-range dates, ambiguous day/month dates. Rows stay in the table and get a `dq_flag` value. |

Safety rules built in:
- Columns with leading zeros (zip codes, phone numbers) stay as text, so `00273` is not turned into `273`.
- Integers up to 18 digits are kept exactly (no float rounding).
- Dates are parsed strictly. If a column has only ambiguous values such as `05/06/2023`, it is parsed month-first **and flagged** (`ambiguous_date:<column>`), not silently guessed.
- Category merging never strips punctuation, so `C#` and `C` are never merged.

## 6. The `dq_flag` column

Every cleaned table has a last column `dq_flag`:

- `ok` means nothing suspicious was found for that row.
- Otherwise it lists one or more reasons separated by `|`, for example `outlier:duration` or `orphan_fk:gamesession_id|outlier:duration`.

Common flag codes: `outlier:<col>`, `negative:<col>`, `orphan_fk:<col>`, `dup_ignoring_id`, `dup_pk`, `fd_conflict:<col>`, `temporal:<a>_<b>`, `date_oor:<col>`, `ambiguous_date:<col>`, `nulled:<col>`.

```sql
-- Strict analysis (only rows with no known issue)
SELECT ... FROM clips WHERE dq_flag = 'ok';

-- As-is analysis (everything)
SELECT ... FROM clips;

-- How many rows have each issue?
SELECT dq_flag, COUNT(*) FROM clips GROUP BY 1 ORDER BY 2 DESC;
```

Note: rows where a value was set to NULL (`nulled:<col>`) are also flagged, so strict mode excludes them.

## 7. Importing into PostgreSQL with DBeaver (one table at a time)

### Step 1: Create the empty tables
1. In DBeaver, connect to your PostgreSQL database and open a new SQL editor (**SQL Editor > New SQL script**).
2. Open `cleaned/schema.sql` (File > Open File) or paste its contents.
3. Run the **whole** script with **Execute SQL Script** (`Alt + X`), not just the current statement (`Ctrl + Enter`).
4. In Database Navigator, right-click **Tables** and choose **Refresh** (`F5`). You should now see all tables, empty.

> `schema.sql` starts each table with `DROP TABLE IF EXISTS`. Make sure you are connected to the correct database, because an existing table with the same name will be replaced.

### Step 2: Import each CSV into its matching table
Repeat for every table (for example `clips`, `downloaded_clips`, `gamesession`, `premium`, `shared_clips`):

1. Right-click the table in Database Navigator and choose **Import Data**.
2. Choose **CSV** and click **Next**.
3. Select the file with the **same name** from the `cleaned/` folder (for table `clips` pick `clips.csv`). The wizard handles one table at a time, so Ctrl+A is not available.
4. On the **Importer settings** page:
   - Delimiter: `,`
   - Header: **top**
   - NULL value mark: **leave empty**
   - Enable **Set empty strings to NULL** if that option is shown (in these files an empty cell always means NULL).
   - Leave the timestamp format at its default. The files use `yyyy-MM-dd` and `yyyy-MM-dd HH:mm:ss`.
5. On the **Tables mapping** page, check that every CSV column is mapped to the column with the same name and the mapping says **existing**. The last column `dq_flag` must be mapped too.
6. Click **Next** through the remaining pages and **Proceed**.

Large tables (for example 550k rows) take a few minutes. Wait until the import finishes before starting the next table. The order of tables does not matter, because the schema has no foreign-key constraints.

### Step 3: Verify the import
Run this after all tables are loaded and compare with `cleaning_report/reconciliation.csv` (the `CleanedRows` column):

```sql
SELECT 'clips' AS t, COUNT(*) FROM clips
UNION ALL SELECT 'downloaded_clips', COUNT(*) FROM downloaded_clips
UNION ALL SELECT 'gamesession', COUNT(*) FROM gamesession
UNION ALL SELECT 'premium', COUNT(*) FROM premium
UNION ALL SELECT 'shared_clips', COUNT(*) FROM shared_clips;
```

Expected for the current dataset: `clips` 552,031; `downloaded_clips` 12,938; `gamesession` 37,579; `premium` 384; `shared_clips` 3,833. If a number is lower, the import was cut off. Truncate that table (`TRUNCATE "clips";`) and import it again.

### Alternative: import all tables at once with psql
Open a terminal inside the `cleaned/` folder. If the tables already exist, skip `schema.sql`.
```
psql -h localhost -p 5432 -U postgres -d postgres -f schema.sql
psql -h localhost -p 5432 -U postgres -d postgres -f load_postgres.sql
```

## 8. Detector: what it checks

| Layer | Checks |
|---|---|
| Column | Missing values and placeholder strings, constant columns, whitespace, category spelling variants, negative values, IQR outliers, sentinel values (0, -1, 9999), corrupt dates, out-of-range dates, mixed types |
| Table | Exact duplicates, duplicates except surrogate `id`, primary-key uniqueness, learned date order (for example `created_at` should be before `scheduled_at`), learned functional dependencies (for example the same `user_id` should always have the same `join_at`) |
| Cross-table | Foreign keys inferred from value overlap (not column names), orphan records, shared attributes that disagree across related tables |

"Learned" rules mean: if a rule holds for 90 to 99% of rows, the remaining rows are reported as anomalies.

## 9. Settings you can change

All settings are in the `CFG` dictionary at the top of each script, so no code editing is needed.

| Setting (cleaner) | Options | Default |
|---|---|---|
| `exact_duplicates` | `drop` (to quarantine) / `flag` | `drop` |
| `dup_ignoring_id` | `flag` / `drop` | `flag` |
| `orphan_fk` | `flag` / `null` / `drop` | `flag` |
| `fd_conflict` | `flag` / `majority` (overwrite with the most common value per key) | `flag` |
| `out_of_range_date` | `flag` / `null` | `flag` |
| `category_variants` | `merge` / `flag` | `merge` |
| `sql_quote` | `"` (PostgreSQL, Redshift) / `` ` `` (MySQL) | `"` |
| `strict_validation` | `True` / `False` | `True` |

Detector thresholds (`iqr_mult`, `order_learn_ratio`, `fk_min_overlap`, and others) are in the detector's `CFG`.

## 10. Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: universal_anomaly_detector` | Put both `.py` files in the same folder. |
| `Export validation FAILED` | Read the table printed in the error. It names the table and column whose values do not fit the declared type. Check `change_log_rows.csv` for that column. |
| DBeaver: date or timestamp conversion error | On the Importer settings page, check the timestamp format. Values look like `2023-06-01 12:00:21`. |
| DBeaver: value too long for `varchar(n)` | The data changed after `schema.sql` was generated. Re-run the cleaner and recreate the tables. |
| Row count after import is lower than expected | The import was cut off. Truncate the table and import it again. |
| Too many rows flagged as `outlier` | The distribution is heavily skewed. Raise `iqr_mult` in `CFG` (default 3.0). |

## 11. Limitations and trade-offs

- **Not a guarantee of correct business data.** The tools catch structural problems. Values that look plausible but are wrong for your business (for example a clip duration of 60 instead of 90) will not be found.
- **Rules are learned from the majority.** If a problem affects a large share of rows with the same pattern, it is treated as normal.
- **`schema.sql` is a snapshot of this batch.** Column types and `VARCHAR` lengths fit the current data. Loading a future batch into the same tables may need wider columns. After the first load, review and lock the schema manually.
- **Flag instead of drop.** Flagged rows stay in the table. Remember `WHERE dq_flag = 'ok'` when you want strict numbers.
- **Column names change.** Names are converted to `snake_case`, which can break old queries that used the original names.
- **Memory.** All files are loaded into RAM, which is fine for about 550k rows but not for tens of GB.
- **No time-series or multivariate checks yet** (volume spikes per day, distribution drift, Isolation Forest).
- Always read `change_summary.csv` after a run. A large `NULLED` or `DROPPED` count compared to the table size usually means a column type was inferred wrongly.
