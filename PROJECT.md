# IMDb Big Data Project

## Project objective

Analyze the IMDb non-commercial datasets with Apache Spark, enrich a bounded
selection of films and principals from a human-authored open source, and process
relevant Wikipedia change events with Kafka and PySpark Structured Streaming.

The project follows the requirements in [project_instructions.md](./project_instructions.md).
This document will become the complete from-scratch execution and operations
guide as dependencies, data acquisition, and run commands are implemented.

## Agreed analysis definitions

These definitions were agreed during project setup and should be applied
consistently in code and in `REPORT.md`:

| Term | Definition |
| --- | --- |
| Feature-length film | Runtime of at least 60 minutes |
| Notably different runtime | Runtime outside the genre's Tukey fences: below Q1 − 1.5 × IQR or above Q3 + 1.5 × IQR |
| Genre popularity by year | Sum of IMDb votes for films in that genre and year; also report film counts and share for context |
| Successful film | IMDb rating of at least 7.0 and at least 1,000 votes |
| Early career | First third of the period between a person's first and last known film |

For films with multiple genres, the film contributes to each applicable genre.
The implementation must document any additional exclusions needed for missing,
invalid, or insufficient data.

## Planned data and processing

Expected IMDb tables include title basics, title ratings, title crew, title
episodes, title principals, name basics, and title akas. The seven source TSVs
are present locally under `data/raw/` in the current workspace and are excluded
from Git; a fresh clone must acquire them before processing.

Spark is the required engine for batch preparation, analysis, enrichment
transformations, and streaming analytics. Data gathering for enrichment may use
a separate collection step, but cleaning, schema enforcement, and joins must
use Spark. The streaming design will publish live Wikipedia change events to
Kafka and consume them with PySpark Structured Streaming.

## Repository layout

| Path | Purpose |
| --- | --- |
| `config/` | Non-secret project and pipeline configuration |
| `data/` | Local input and generated data; large or licensed data should not be committed |
| `src/` | Spark batch, enrichment, and streaming code |
| `tests/` | Focused tests for transformations and pipeline behavior |
| `PROJECT.md` | Setup, execution, and operations guide |
| `REPORT.md` | Final outputs, metrics, and methodology write-ups |

The directories above contain the Spark preparation and exploration pipelines,
their tests, and the Docker runner described below. IMDb source data is not
committed to the repository; local copies are kept under the ignored `data/`
directory.

## Phase 2: data preparation

### Prerequisites

- Python 3.13 (the project environment used during setup) and Java 17 or newer.
- The project virtual environment and dependencies:

  ```powershell
  .\.venv\Scripts\Activate.ps1
  python -m pip install -r requirements.txt
  ```

### Recommended Windows runtime: Docker Compose

Docker Desktop is installed in the current environment, but its Linux engine
must be started before containers can run. The Compose Spark service runs Python,
Java, and PySpark on Linux and mounts the repository at `/workspace`; this
avoids the native-Windows Hadoop `winutils.exe` requirement. Source, raw data,
prepared Parquet, and reports are all stored under the repository on the
primary disk and are visible on the host. Start Docker Desktop, then build the
image:

```powershell
docker compose build spark
```

For native Windows Spark without Docker, a compatible Hadoop distribution with
`winutils.exe` under `HADOOP_HOME\bin` is required. This workspace has no such
utility. The focused transformation tests run locally, while the output
integration test is skipped on Windows only when `HADOOP_HOME` is not configured.

### Acquire and prepare the IMDb files

The official IMDb TSVs are large and remain local; they are excluded from Git.
Check available disk space before downloading all seven files. To download the
complete set:

```powershell
docker compose run --rm spark -m src.download_imdb --confirm-large-download
```

To download only one table, omit the confirmation flag and select it explicitly:

```powershell
docker compose run --rm spark -m src.download_imdb --tables title.basics
```

Once all seven tables are present in `data/raw/`, run the Spark preparation:

```powershell
docker compose run --rm spark -m src.prepare_data
```

The input can contain compressed `.tsv.gz` files or uncompressed `.tsv` files.
The command writes typed Parquet tables to `data/processed/`, per-column quality
metrics and the top-five missingness ranking to `data/reports/preparation/`,
and foreign-key/chronology findings to the same report directory. Existing
output paths are protected by default; pass `--overwrite` to replace generated
outputs deliberately. The report's missingness ranking uses the combined
percentage of actual nulls, IMDb `\N` markers, and blank strings; each category
is also reported separately. Invalid values for typed columns are counted,
converted to null, and retained as rows rather than silently discarded.

The implementation profiles every column as a string before type conversion,
then cleans and enforces schemas with Spark. Orphan references are counted as
distinct missing keys; multi-valued IMDb references are split before checking.
These findings are reported rather than silently removed. Full-dataset
preparation and exploration results from the current local source snapshot are
summarized in `REPORT.md`; detailed CSV outputs remain in the ignored `data/`
directory.

The complete workflow requires `title.basics`, `title.akas`, `title.crew`,
`title.episode`, `title.principals`, `title.ratings`, and `name.basics`. Their
combined compressed download was about 2.0 GB on 2026-11-14; allow additional
space for Parquet outputs and temporary Spark processing. The files are
downloaded into the host's `data/raw/`, which the Spark container mounts at
`/workspace/data/raw/`; do not commit them.

## Phase 3: data exploration

Run this phase after Phase 2 has written all seven prepared Parquet tables:

```powershell
docker compose run --rm spark -m src.explore_data
```

The pipeline writes CSV report folders under `data/reports/exploration/`.
Use `--overwrite` to replace existing exploration outputs. It produces
per-genre runtime outliers, yearly genre popularity and top-three comparisons,
director and series results, multilingual and early-career actors, and
multi-talented project metrics. Without the Docker runner, the same command can
be run with the project's Python environment on Linux/macOS, or on native
Windows with the Hadoop utility configured.

## Phase 4: data enrichment

The enrichment phase selects feature-length movies (`runtimeMinutes >= 60`)
from 1980–2025. It chooses the ten highest-vote films per genre/year; ties are
ordered by rating descending and IMDb ID ascending. A film selected for more
than one genre is fetched once, with all qualifying genre/rank pairs retained.
For each film, the first three distinct principal IDs by IMDb credit ordering
are selected, without restricting their credit category.

First create the target manifests using Spark:

```powershell
docker compose run --rm spark -m src.enrich_data targets
```

Wikipedia collection uses the English Wikipedia Action API and only accepts
exact page-title matches to avoid attaching biographies or synopses to a
different person or film. The saved text is the human-authored lead extract,
not generated text. Every record retains its Wikipedia page URL and revision
ID; missing or ambiguous matches retain the API search URI and a status/note.
The output also records the CC BY-SA attribution notice. The collector makes
requests sequentially at no less than one per second, honors `Retry-After`,
retries transient errors, and appends records so an interrupted run can resume.

To check a small sample before collecting all targets:

```powershell
docker compose run --rm spark -m src.collect_wikipedia --max-entities 5
```

The full collection resumes from any sample records:

```powershell
docker compose run --rm spark -m src.collect_wikipedia
```

The current IMDb snapshot produces 7,151 film targets and 11,222 distinct
principal targets (18,373 Wikipedia lookups). At the enforced request interval,
the full API collection takes at least about five hours and may take longer
because of network latency and retries.
Run it only when you are ready to leave the collection active. Once it
finishes, Spark validates lineage, cleans and joins the records, and writes
Parquet outputs and a per-film principal-coverage report:

```powershell
docker compose run --rm spark -m src.enrich_data finalize
```

The final outputs are stored under `data/enrichment/processed/` and
`data/reports/enrichment/`. The finalization stage refuses to run while any
target lacks a collection record. Films with fewer than three distinct IMDb
principals, or fewer than three matching Wikipedia biographies, receive an
explicit coverage note. Successful text records name Wikipedia contributors
and retain the CC BY-SA 4.0 license and URL, alongside the source page and
revision. Enrichment inputs, API responses, and outputs are local and excluded
from Git. All target selection, schema enforcement, joins,
cleaning, and final writes use Spark; only Wikipedia HTTP acquisition runs
outside Spark.

## Phase 5: Wikipedia change streaming

This phase needs the finalized enrichment Parquet from Phase 4. The producer
reads Wikimedia's live EventStreams `recentchange` feed and publishes English
Wikipedia events to the local Kafka topic `wikipedia-recent-changes`. Spark
Structured Streaming accepts all English-Wikipedia main-namespace `edit` and
`new` events as candidates, including bot edits, so the 15–30 minute test
window processes many live events. It separately flags exact page-ID matches
against the enriched IMDb film and principal pages; unrelated article changes
remain in the output with `is_imdb_target = false` to make the broad candidate
filter and its target-specific match rate measurable.

Start the Kafka broker, producer, and Spark consumer with Docker Compose:

```powershell
docker compose --profile streaming up -d kafka
docker compose --profile streaming up -d wikipedia-producer spark-streaming
docker compose --profile streaming ps
docker compose --profile streaming logs -f --tail 20 spark-streaming
```

Keep the services running for 15–30 minutes. Spark prints a JSON metric line
per five-second micro-batch with candidate and exact IMDb target match counts.
The output events are appended to
`data/streaming/processed/changes/`; Spark's restart checkpoint is stored in
`data/streaming/checkpoints/wikipedia-changes-v4/`. Both paths are ignored by
Git. A fresh query starts at the latest Kafka offset; restarting with the
checkpoint resumes from its committed offset.
To stop the test without deleting Kafka data or Spark checkpoints:

```powershell
docker compose --profile streaming stop spark-streaming wikipedia-producer kafka
```

The stream uses the EventStreams event timestamp (`meta.dt`) as event time,
deduplicates by event ID, and applies a ten-minute watermark. Events arriving
within ten minutes of the current event-time watermark are processed; older
late events are dropped so the deduplication state remains bounded. Kafka
offsets and Spark's checkpoint let a stopped consumer resume without
reprocessing committed offsets. The Kafka topic retains up to 24 hours of
events for short local interruptions.

After the test, inspect the saved records and group by `is_imdb_target` to
report the number of candidate changes and exact IMDb-page matches. Include
the test duration, processed counts, matching counts, and any source
disconnects in `REPORT.md`.

## Phase 6: open question

The additional analysis asks whether feature-length films with alternate IMDb
titles recorded in more geographic regions also have more IMDb votes or
higher ratings, comparing films within the same genre and decade. It joins
`title.akas`, `title.basics`, and `title.ratings` with Spark. Region coverage is
the number of distinct non-empty region codes in a film's alternate-title
records; it is an availability proxy, not verified evidence of theatrical
distribution. Films are grouped as no recorded region, one region, two to
four regions, or five or more regions. The analysis uses rated feature films
from 1980–2025 with at least one vote, and reports medians to reduce the
influence of highly skewed IMDb vote counts.

Run the analysis after the Phase 2 preparation:

```powershell
docker compose run --rm spark -m src.open_question
```

The CSV outputs are written under `data/reports/open_question/`. The detailed
output is stratified by genre and release decade; the direct comparison
includes only genre-decade strata with at least ten films in both the
one-region and five-or-more-region groups. Use `--overwrite` to replace
existing outputs. Summarize the observed results and limitations in
`REPORT.md`.

## Git and AI-tool disclosure

Each implementation task must be developed on its own feature branch. Gemini is
disclosed as an AI/code-assistance tool used for this project; 