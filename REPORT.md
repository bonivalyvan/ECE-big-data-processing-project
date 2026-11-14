# Data preparation

## Current status

The preparation pipeline and quality checks are implemented, but the official
IMDb TSV files are not present in the workspace. Therefore, full-dataset
missingness percentages, the five most-missing columns, and anomaly counts have
not yet been measured and are intentionally not reported as results.

Run instructions and output locations are documented in `PROJECT.md`. The
generated reports will include:

- Per-column row counts and percentages of actual null, IMDb `\N` missing
  markers, blank values, combined missingness, and invalid typed values.
- The five columns with the highest combined null/missing/blank percentage.
- Distinct orphan foreign-key reference counts and people whose recorded death
  year precedes their birth year.

## Methodological Explanation

Each source table is read by Spark with schema inference disabled so the
original values can be profiled before conversion. This preserves the
distinction between an actual null, IMDb's `\N` missing marker, and a blank
string. The top-five ranking uses their combined percentage because all three
represent an absent value; separate counts remain available to audit the
ranking. Invalid numeric and boolean values are counted before they are cast.

Spark then normalizes whitespace and missing markers, enforces the declared
table schemas, and writes Parquet outputs. Invalid typed values become null
without dropping their rows. Foreign-key checks compare distinct references to
distinct parent keys; comma-separated references are expanded before checking.
Chronology checks flag, but do not delete, people whose death year is earlier
than their birth year. Retaining flagged records and publishing issue counts
avoids irreversible assumptions while keeping downstream data usable.

This distributed approach is chosen to handle the full IMDb tables without
loading them into driver memory. Alternatives such as local Python/Pandas
profiling would be simpler for small samples but do not meet the Spark
processing requirement or scale as safely. Automatic deletion or imputation
was not selected: missing and inconsistent fields can be meaningful source
limitations, so the pipeline records them and applies only deterministic
normalization and type enforcement.

## Validation

Focused tests cover missing/blank/invalid values, typed conversion, distinct
orphan references, and death-before-birth detection. Full-dataset result tables
remain pending until the official source files are downloaded and the pipeline
is run.