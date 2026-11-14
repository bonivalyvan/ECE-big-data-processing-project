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

# Data exploration

## Current status

The Spark exploration queries and synthetic tests are implemented. The IMDb
Parquet tables are not present, so the real query outputs are pending. Once
Phase 2 has run, the CSV result folders will be written under
`data/reports/exploration/`.

The outputs cover runtime outliers, yearly genre popularity and its top-three
comparison, qualifying directors and television series, successful
multilingual actors, early-career peaks, and multi-role professionals. IMDb
`title.akas.language` is used as the available language indicator only when
`isOriginalTitle` is true; this is a proxy, not an authoritative source of a
film's original production language.

## Methodological Explanation

Feature-length films are defined as `titleType = movie` with a runtime of at
least 60 minutes. A runtime is flagged when it falls outside the genre's Tukey
fences (Q1 − 1.5 × IQR, Q3 + 1.5 × IQR); the output also reports the genre mean
and the film's difference from that mean. This robust outlier rule is less
sensitive to extreme runtimes than a standard-deviation cutoff, while a
z-score would be easier to interpret under a roughly normal distribution.

Yearly genre popularity uses the sum of IMDb votes for 1980–2025. Films with
multiple genres count once in each genre. The ranking is deterministic: vote
total descending, then genre name ascending. The comparison output groups the
top three genres against the remaining genres and reports genre-assigned film
counts, vote totals, and shares. Shares are calculated over all genre
assignments for that year, so a multi-genre film's votes contribute to each
genre. Vote totals were chosen over title counts because they better represent
the agreed popularity metric; title counts are included for context.

The director query requires exactly one rated movie at 8.0 or above, at least
two other rated movies below 8.0, and a mean rating of at most 6.0 across those
other movies. The television query requires at least three numbered seasons
with ten or more episodes each; among rated episodes, a minimum-rated episode
must occur in the highest-numbered season and a maximum-rated episode in season
one or two. Tied extreme episodes are all retained.

For actor language results, success uses the agreed IMDb threshold (rating at
least 7.0 and at least 1,000 votes), and only feature films with a language
tagged on an original-title AKA are counted. The IMDb dataset does not provide
a reliable canonical production-language field, so these results must be
interpreted as language-tagged original-title evidence rather than definitive
language attribution. Alternative data from a curated language database could
improve this, but would introduce a separate source and provenance burden.

Early-career analysis uses actors or actresses with at least 15 movies and a
first-to-last release-year span of at least 20 years. The first career third is
the interval where three times the elapsed years is no greater than the full
career span. A person qualifies when a highest-rated movie occurs in that
first third and the first-third average rating exceeds the average for the
remaining career. The multi-talented analysis consolidates actor, director, and
writer credits per movie, then compares mean movie ratings for each qualifying
professional's multi-role projects against their single-role projects. These
are descriptive comparisons, not causal estimates.

All joins, filtering, aggregation, ranking, and outputs use Spark. Local
Pandas-based analysis was rejected because it would not scale to the full IMDb
tables. Docker Compose is the recommended runtime on Windows because it runs
Spark on Linux and avoids the missing native Hadoop utility; the same Compose
stack will host Kafka for the streaming phase. The Docker CLI is available in
this workspace, but its engine is currently stopped, so the container build and
full-dataset run have not been verified here.

## Validation

Synthetic Spark data exercises each query's thresholds and expected result
shape. Full-dataset metrics and result rows remain pending until the IMDb
tables are available and the pipeline is run.