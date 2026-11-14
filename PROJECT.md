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
episodes, title principals, name basics, and title akas. The actual downloaded
files and their schemas must be inventoried before processing; no IMDb data is
currently present in the repository.

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

The directories above are present, but no source code, configuration, tests, or
data files have been added yet.

## Phase 2: data preparation

### Prerequisites

- Python 3.13 (the project environment used during setup) and Java 17 or newer.
- The project virtual environment and dependencies:

  ```powershell
  .\.venv\Scripts\Activate.ps1
  python -m pip install -r requirements.txt
  ```

### Acquire and prepare the IMDb files

The official IMDb TSVs are large and remain local; they are excluded from Git.
Check available disk space before downloading all seven files. To download the
complete set:

```powershell
python -m src.download_imdb --confirm-large-download
```

To download only one table, omit the confirmation flag and select it explicitly:

```powershell
python -m src.download_imdb --tables title.basics
```

Once all seven tables are present in `data/raw/`, run the Spark preparation:

```powershell
python -m src.prepare_data
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
These findings are reported rather than silently removed. The current report
does not yet contain full-dataset counts because the source data has not been
downloaded into this workspace.

## Git and AI-tool disclosure

Each implementation task must be developed on its own feature branch. Gemini is
to be disclosed as an AI/code-assistance tool in this project documentation; it
must not be added as a Git commit author or co-author. No commit has been
created during project setup.