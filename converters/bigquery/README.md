<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Ossie BigQuery Converter

Offline conversion from an [Apache Ossie](https://github.com/apache/ossie)
semantic model to a BigQuery
[property graph with graph measures](https://docs.cloud.google.com/bigquery/docs/graph-measures).
No BigQuery connection required — the converter emits DDL text; it does not
create tables or deploy the graph.

- **Export** (`ossie-bigquery export`): Apache Ossie → a single
  `CREATE OR REPLACE PROPERTY GRAPH` statement.
- **Import** (`ossie-bigquery import`): not yet implemented (export-only for now,
  like the `snowflake` and `polaris` spokes).

## Mapping

| Apache Ossie | BigQuery property graph |
| --- | --- |
| `dataset` (`source` = `project.dataset.table`) | **NODE TABLE** (`KEY` from `primary_key`, `PROPERTIES` from `fields`) |
| `relationship` (`from`/`to`/`from_columns`/`to_columns`) | **EDGE TABLE** (`SOURCE KEY … REFERENCES` / `DESTINATION KEY … REFERENCES`) |
| `metric` (single-table aggregate) | `MEASURE(<agg>) AS <name>` in the owning node's `PROPERTIES` |
| `field` | graph property (bare column, or `<expr> AS <name>` for a computed field) |
| `description` + `ai_context.synonyms` | `OPTIONS(description="…")` (synonyms folded into the description) |

A BigQuery graph measure binds an aggregate to exactly one table's `KEY`, so the
result stays correct under fan-out joins; the cross-table rollup happens at query
time via `GRAPH_EXPAND(...) + AGG(...)`.

## Installation

```bash
pip install apache-ossie-bigquery        # once published to PyPI
# or, from a checkout of this directory:
pip install -e .
```

The only runtime dependency is `PyYAML`. Python 3.11+.

## Usage

```bash
ossie-bigquery export -i model.yaml -o graph.sql   # Apache Ossie -> property-graph DDL
ossie-bigquery export -i model.yaml                # -> stdout
```

With no `-o`, output goes to stdout. Conversions that drop information emit
warnings to stderr; any input that breaks a requirement raises a
`ConversionError`.

### Consuming the measures

A `MEASURE(...)` cannot be referenced directly in GQL `MATCH`/`RETURN`; query it
through `GRAPH_EXPAND` and wrap the measure column in `AGG(...)`, which performs
the aggregation exactly once per key:

```sql
SELECT
  store_sales_total_sales,             -- or AGG(...) with a GROUP BY dimension
FROM GRAPH_EXPAND("tpcds_retail_model");
```

## Requirements

- `version` must match the spec version this converter targets (`0.2.0.dev0`).
- Every dataset that becomes a node table (and every relationship's `from`
  dataset) must declare a non-empty `primary_key` — a graph node requires a
  `KEY`.
- Each dataset `source` must be a plain `project.dataset.table` identifier
  (a graph node table must be backed by a base table, not a subquery).
- Expressions use the `BIGQUERY` dialect, falling back to `ANSI_SQL`.

## Limitations

- **Export only.** BigQuery DDL → Apache Ossie import is not yet implemented.
- **Single-table measures only.** Only a metric whose aggregate references a
  single dataset becomes a `MEASURE()`. A metric that spans multiple datasets or
  references none (e.g. a cross-dataset ratio such as `SUM(a.x) / COUNT(DISTINCT
  b.y)`) **cannot** be a single measure and is **skipped with a warning** — the
  cross-table rollup is expected to be expressed at query time, or via native
  BigQuery measure features as they expand. A single-dataset metric whose body is
  not one of `SUM, AVG, COUNT, MIN, MAX` is emitted anyway (BigQuery validates it
  at deploy time).
- **Dropped-but-warned:** datasets without a `primary_key` (and any edge that
  references them), fields/metrics with no `BIGQUERY`/`ANSI_SQL` expression.
- BigQuery requires **exactly one root node table** (a node whose `KEY` is not
  referenced by any edge). The converter emits a warning if the graph does not
  have exactly one, but still produces the DDL.

## Development

```bash
uv sync
uv run pytest
```
