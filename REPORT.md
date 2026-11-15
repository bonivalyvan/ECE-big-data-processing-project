# IMDb Data Engineering, Enrichment, and Streaming: Final Report

## Abstract

This project applies Apache Spark to seven IMDb non-commercial datasets for
data-quality assessment, exploratory analysis, Wikipedia-based enrichment,
and live change processing with Kafka and Spark Structured Streaming. The
preparation stage produced seven typed Parquet tables and identified the five
most incomplete columns. The exploration stage generated ten analytical
outputs. Enrichment covered 7,151 selected films and 21,401 film-principal
associations using human-authored English Wikipedia text with source lineage.
The live streaming test processed 1,591 candidate changes during a 17-minute
46-second window; none matched the enriched IMDb Wikipedia page IDs. An
additional analysis of 223,494 rated feature films found that IMDb alternate
title region coverage was associated with more votes, but not higher ratings,
even when comparing within genre and release decade. All quantitative results
reported here were generated from the local data pipelines; the data,
reproducibility procedures, and methodological limitations are documented
below.

# Data preparation

## Current status

The Spark preparation pipeline has completed successfully against the seven
local IMDb TSV files. It wrote all seven typed Parquet tables to
`data/processed/`, 39 per-column quality records, five top-missingness records,
and 11 data-quality issue records under `data/reports/preparation/`. The
top-five combined null/missing/blank percentages are:

| Rank | Column | Absent (%) |
| ---: | --- | ---: |
| 1 | `title.akas.attributes` | 99.47 |
| 2 | `title.basics.endYear` | 98.73 |
| 3 | `name.basics.deathYear` | 98.32 |
| 4 | `name.basics.birthYear` | 95.66 |
| 5 | `title.principals.job` | 80.66 |

Detailed column counts, orphan references, and chronology findings are in the
generated preparation reports. They contain:

- Per-column row counts and percentages of actual null, IMDb `\N` missing
  markers, blank values, combined missingness, and invalid typed values.
- The five columns with the highest combined null/missing/blank percentage.
- Distinct orphan foreign-key reference counts and people whose recorded death
  year precedes their birth year.

The 11 issue records comprise ten foreign-key checks and one chronology check.
Their aggregate findings are:

| Issue | Checks with findings | Affected references or people |
| --- | ---: | ---: |
| Orphan foreign-key references | 10 | 6,121 |
| Death year before birth year | 1 | 28 |

The orphan total is the sum across the ten foreign-key checks; it does not
represent 6,121 unique people or titles across all checks.

The raw TSVs and generated datasets are local and excluded from Git;
`PROJECT.md` documents how to reproduce the run.

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

Focused Docker tests cover missing/blank/invalid values, typed conversion,
literal quotes in titles, distinct orphan references, and death-before-birth
detection. All six preparation tests passed, and the full-dataset pipeline
completed with success markers for all seven Parquet tables and all three
preparation reports.

# Data exploration

## Current status

The Spark exploration completed successfully against the prepared IMDb tables.
All ten CSV result folders were written under `data/reports/exploration/`:

| Output folder | Rows |
| --- | ---: |
| `runtime_outliers_by_genre` | 32,341 |
| `genre_popularity_by_year` | 1,074 |
| `top_three_genres_by_year` | 138 |
| `top_three_vs_rest_by_year` | 92 |
| `directors_one_high_rating_and_low_others` | 1,568 |
| `series_episode_extremes` | 641 |
| `actors_multilingual` | 0 |
| `actors_3plus_languages` | 0 |
| `actors_early_career_peak` | 9,576 |
| `multitalented_project_performance` | 167,088 |

Row counts exclude CSV headers. The two language-based actor queries produced
no qualifying rows in this dataset; this is an observed result, not a pipeline
failure.

The outputs cover runtime outliers, yearly genre popularity and its top-three
comparison, qualifying directors and television series, successful
multilingual actors, early-career peaks, and multi-role professionals. IMDb
`title.akas.language` is used as the available language indicator only when
`isOriginalTitle` is true; this is a proxy, not an authoritative source of a
film's original production language.

The complete yearly top-three ranking is shown below. The final two columns
give the top three genres' share of yearly genre vote totals and genre-assigned
film counts, respectively. Multi-genre films contribute once to each genre.

| Year | Rank 1 | Rank 2 | Rank 3 | Top-three vote share | Top-three film share |
| ---: | --- | --- | --- | ---: | ---: |
| 1980 | Drama | Adventure | Horror | 44.3% | 36.9% |
| 1981 | Adventure | Drama | Horror | 46.5% | 36.1% |
| 1982 | Drama | Sci-Fi | Action | 43.7% | 39.1% |
| 1983 | Drama | Action | Adventure | 45.9% | 41.4% |
| 1984 | Action | Drama | Adventure | 44.1% | 39.8% |
| 1985 | Comedy | Adventure | Drama | 51.5% | 46.8% |
| 1986 | Drama | Adventure | Comedy | 46.2% | 47.4% |
| 1987 | Drama | Comedy | Action | 42.6% | 53.1% |
| 1988 | Drama | Comedy | Action | 46.8% | 51.8% |
| 1989 | Comedy | Drama | Adventure | 49.4% | 45.3% |
| 1990 | Drama | Comedy | Crime | 45.6% | 49.9% |
| 1991 | Drama | Crime | Comedy | 44.2% | 50.0% |
| 1992 | Drama | Comedy | Crime | 46.3% | 50.3% |
| 1993 | Drama | Comedy | Crime | 44.8% | 49.9% |
| 1994 | Drama | Crime | Comedy | 58.3% | 50.8% |
| 1995 | Drama | Crime | Comedy | 49.0% | 48.7% |
| 1996 | Drama | Comedy | Crime | 44.9% | 49.2% |
| 1997 | Drama | Action | Comedy | 44.1% | 52.6% |
| 1998 | Drama | Comedy | Crime | 51.5% | 50.6% |
| 1999 | Drama | Comedy | Crime | 43.6% | 51.1% |
| 2000 | Drama | Comedy | Adventure | 45.0% | 46.0% |
| 2001 | Drama | Comedy | Adventure | 42.4% | 46.8% |
| 2002 | Drama | Adventure | Action | 42.4% | 38.2% |
| 2003 | Drama | Action | Comedy | 45.5% | 51.1% |
| 2004 | Drama | Action | Comedy | 41.3% | 48.9% |
| 2005 | Drama | Comedy | Action | 40.4% | 48.4% |
| 2006 | Drama | Comedy | Thriller | 42.7% | 47.6% |
| 2007 | Drama | Comedy | Action | 38.3% | 45.8% |
| 2008 | Drama | Action | Comedy | 40.4% | 45.8% |
| 2009 | Drama | Comedy | Action | 41.3% | 45.2% |
| 2010 | Drama | Action | Adventure | 40.0% | 33.2% |
| 2011 | Drama | Action | Comedy | 46.1% | 43.0% |
| 2012 | Drama | Action | Comedy | 42.7% | 42.0% |
| 2013 | Drama | Action | Comedy | 41.5% | 41.3% |
| 2014 | Drama | Action | Adventure | 47.0% | 30.8% |
| 2015 | Drama | Action | Adventure | 46.6% | 31.9% |
| 2016 | Drama | Action | Adventure | 44.1% | 32.0% |
| 2017 | Drama | Action | Comedy | 45.0% | 41.8% |
| 2018 | Drama | Action | Adventure | 44.3% | 32.5% |
| 2019 | Drama | Action | Comedy | 43.6% | 41.9% |
| 2020 | Drama | Comedy | Action | 44.7% | 39.0% |
| 2021 | Drama | Action | Adventure | 46.4% | 32.4% |
| 2022 | Drama | Action | Comedy | 46.4% | 41.8% |
| 2023 | Drama | Action | Comedy | 44.9% | 42.8% |
| 2024 | Drama | Action | Comedy | 42.9% | 41.3% |
| 2025 | Drama | Action | Comedy | 39.3% | 41.5% |

Drama ranks first in most years; Comedy, Action, and Adventure frequently
occupy the other two positions. In 2025, the top-three genres represented
39.3% of genre vote totals and 41.5% of genre-assigned films, compared with
60.7% and 58.5% for all other genres.

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
stack will host Kafka for the streaming phase. The Spark image built
successfully, and all 12 repository tests passed in the Docker container. The
full-dataset query outputs listed above were generated successfully; their
methodological limitations still apply.

## Validation

Synthetic Spark data exercises each query's thresholds and expected result
shape. Full-dataset metrics and result rows are available in the local
exploration CSV folders; rerun the documented pipeline after updating the
source snapshot.

# Data enrichment

## Current status

Spark selected 7,151 distinct films and 21,401 film-principal associations for
enrichment. Those associations represent 11,222 distinct Wikipedia biography
targets, for 18,373 unique Wikipedia lookups overall. Of the 7,151 films, five
have no IMDb principal credit, ten have one, and seventeen have two; therefore,
32 films have fewer than three distinct principal credits. The final coverage
report documents this source limitation.

The complete collection and Spark finalization have finished. The collector
wrote one record for each of the 7,151 film and 11,222 principal targets. The
record statuses are 12,749 `found`, 4,561 `not_found`, 1,062 `ambiguous`, and
one `empty_extract`. The final outputs contain 7,151 enriched film rows and
21,401 film-principal association rows. Wikipedia text was available for 5,104
film summaries and 17,404 principal associations; 2,545 films have a coverage
note because fewer than three distinct IMDb credits or fewer than three
matching biographies were available. These counts reflect association rows
where principals are repeated across films.

## Methodological Explanation

The selection uses the ten largest IMDb vote counts per genre and release
year, restricted to feature films released in 1980–2025. This applies the
project's existing popularity measure while giving deterministic tie-breaking
by rating and IMDb ID. Multi-genre films retain each qualifying genre/rank but
are fetched once. The first three distinct principals by IMDb credit ordering
are used, regardless of category, because the requirement asks for principals
rather than actors only.

English Wikipedia's Action API is the single human-authored source. Search
results are accepted only when a page title matches the IMDb title or name
after case/punctuation normalization; ambiguous or nonmatching pages are not
guessed. The page lead extract is stored as the synopsis/biographical summary.
Each successful record retains its page URL, page/revision IDs, retrieval time,
the attribution "Wikipedia contributors," and the CC BY-SA 4.0 license and
URL. Missing records carry the search URI, status, and a note rather than
generated or inferred text.

The collector is sequential, enforces at least a one-second interval, honors
server `Retry-After` responses, retries transient HTTP failures, and appends
records for safe resumption. The collector also retries the transient
`cirrussearch-too-busy-error` response. A multilingual search could improve coverage but
would complicate source matching and attribution; a fuzzy title/name match
could increase recall but risks incorrect joins. Both alternatives were
rejected in favor of conservative, auditable matches. All selection,
cleaning, schema enforcement, joins, and final outputs are handled in Spark;
only the HTTP acquisition is outside Spark.

## Validation

All 12 repository tests passed in Docker before the full collection, and the
four focused Wikipedia collector tests passed after adding retry handling for
the temporary search-overload response. Spark finalization verified complete
target coverage and wrote the Parquet datasets to
`data/enrichment/processed/films/` and
`data/enrichment/processed/principals/`, plus the per-film CSV coverage report
under `data/reports/enrichment/film_coverage/`. The results are local and
ignored by Git. The tests include Spark tests for top-vote
selection, unique IMDb principals, lineage preservation, and coverage notes,
plus mocked Wikipedia API tests for exact matching and unavailable pages. The
live pilot verified the API integration. Collection and finalization are
complete for the current IMDb snapshot; the target manifests and API records
remain in the ignored local `data/enrichment/` directory.

# Wikipedia change streaming

## Current status

The local Kafka-to-Spark Structured Streaming test ran for 17 minutes 46
seconds, from 2026-11-15 10:51:10 to 11:08:56 UTC. Spark completed 144
micro-batches and emitted 1,591 candidate changes: 1,584 edits and 7 new-page
events. None matched an enriched IMDb film or principal page ID. This is a
zero-match result for this observation window, not a failure of the stream.
The producer log contains no EventStreams reconnect or disconnection messages,
and the Spark container had no restarts during the run.

The stream was limited to English Wikipedia main-namespace `edit` and `new`
events. This deliberately broad candidate set includes bot changes; exact
IMDb-page matches are measured separately by Wikipedia page ID. Keeping
nonmatching candidates in the output makes the candidate volume and exact
match rate auditable rather than implying every article change relates to IMDb.

## Methodological Explanation

The producer reads the Wikimedia EventStreams `recentchange` SSE feed and
publishes English Wikipedia events to Kafka. Spark parses `meta.dt` as event
time, filters the agreed candidate event types and namespace, then
deduplicates on the event ID with a ten-minute watermark. The watermark bounds
deduplication state: events arriving later than the allowed event-time delay
may be dropped. A page is marked as an IMDb target only when its Wikipedia
page ID exactly matches a page ID in the finalized enriched film or principal
tables; no title-based or fuzzy matching is applied in the stream.

Kafka retains events for up to 24 hours, while the named Kafka volume stores
the broker log directory. Spark writes candidate records as Parquet and uses a
separate checkpoint to resume committed offsets after a restart. The local
test's candidate and match counts come from the Spark micro-batch metrics and
were cross-checked against the saved Parquet output.

## Validation

The 17-minute-46-second live test processed 1,591 candidates across 144
micro-batches, with 0 exact IMDb-page matches and no logged EventStreams
reconnections. The saved output contains 1,591 records in the observation
window, including 1,584 edits and 7 new-page events. The output and checkpoint
remain local and are ignored by Git. Focused producer and Spark transformation
tests cover SSE parsing, English-only publishing, Kafka startup retry,
candidate filtering, and exact page-ID matching.

# Open question: alternate-title region coverage and film performance

## Question and Results

Among IMDb-rated feature films released from 1980 through 2025, are films with
alternate-title records in more regions associated with more IMDb votes or
higher ratings? This question is interesting because it compares a broad
measure of geographic title coverage with both audience attention and user
ratings, beyond the analyses above. No external data was added; the analysis
joins the IMDb `title.akas`, `title.basics`, and `title.ratings` tables.

The analysis included 223,494 rated feature films with at least one vote.
Overall medians show a strong increase in votes with more region-coded
alternate titles, while median ratings do not increase:

| Alternate-title regions | Films | Median IMDb votes | Median rating | Mean rating |
| --- | ---: | ---: | ---: | ---: |
| No recorded region | 258 | 17 | 6.6 | 6.37 |
| 1 region | 31,920 | 24 | 6.4 | 6.31 |
| 2–4 regions | 44,141 | 56 | 6.2 | 6.10 |
| 5+ regions | 147,175 | 332 | 6.1 | 6.01 |

To account for genre and release period, the comparison also matches
one-region and five-or-more-region films within the same genre and decade.
Among 106 genre-decade strata with at least ten films in each group, the
five-or-more-region group had a higher median vote count in all 106 strata.
The median of the per-stratum vote-count ratios was **15.98**. Median ratings
were lower for the five-or-more-region group in 68 strata, equal in 8, and
higher in 30; the median rating difference was **−0.20** points.

The observed association is therefore stronger for audience attention than
for rating: wider alternate-title region coverage tracks more votes, but does
not correspond to higher ratings. The overall and stratified CSV results are
under `data/reports/open_question/`; the detailed report contains 416
genre-decade-coverage rows and the paired report contains the 106 qualifying
strata.

## Methodological Explanation

For each IMDb title, geographic coverage is the count of distinct,
non-empty region codes in its `title.akas` records. The group labels are no
recorded region, one region, two to four regions, and five or more regions.
`title.basics` limits the population to movies of at least 60 minutes released
in 1980–2025 with a genre. The inner join to `title.ratings` retains titles
with a valid 0–10 rating and at least one vote. Multi-genre films contribute
once to each listed genre, as in the project's other genre analyses.

Spark reports the distinct film count, mean and median rating, and median vote
count by region-coverage tier. The detailed table groups by genre and release
decade. To compare the most distinct coverage groups directly, the paired
analysis keeps only genre-decade strata with at least ten films in both the
one-region and five-or-more-region groups; it compares their median-vote
ratio and median-rating difference. Medians were chosen because IMDb vote
counts are highly skewed. Stratification reduces, but does not eliminate,
differences in genre and release period.

An unstratified comparison alone would be simpler but could confound the
relationship with genre mix and film age. Mean votes would be more sensitive
to a few blockbuster titles, so medians are used for the principal
comparison. `title.akas.region` is an available, reproducible IMDb field, but
it records regions associated with alternate titles, not verified theatrical
distribution or audience availability. Thus region count is only a coverage
proxy; unmeasured factors may explain the association, and the results are
descriptive rather than causal. An external release/distribution dataset
could measure availability more directly, but would add source and
provenance requirements and is not needed for this bounded question.

## Conclusion

The project demonstrates a reproducible Spark workflow for profiling and
analyzing the full IMDb snapshot, a rate-limited enrichment process with
explicit lineage, and a live Kafka-to-Spark streaming pipeline. The analyses
also illustrate the importance of treating missing source data, ambiguous
matches, proxy measures, and observed zero-match streaming windows as
limitations to report rather than silently correcting or over-interpreting
them. Generated row-level outputs and large source data remain local; the
commands in `PROJECT.md` reproduce them from a fresh checkout.

## Final validation

The full project test suite passed in Docker: 18 tests, with no failures. The
streaming Compose configuration was validated, and the reported preparation,
exploration, enrichment, streaming, and open-question results were generated
from their corresponding pipelines. Large source data and generated output
files remain local and are excluded from Git; `PROJECT.md` describes how to
recreate them.

## References

1. IMDb. [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/).
2. MediaWiki. [Action API documentation](https://www.mediawiki.org/wiki/API:Main_page).
3. Wikimedia. [EventStreams documentation](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).
4. Creative Commons. [Attribution-ShareAlike 4.0 International](https://creativecommons.org/licenses/by-sa/4.0/).
