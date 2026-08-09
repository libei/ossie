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

Convert an [Apache Ossie](https://github.com/apache/ossie) semantic model into a
BigQuery [property graph with graph measures](https://docs.cloud.google.com/bigquery/docs/graph-measures).

The converter is an **offline text transform**: an Ossie model (YAML) goes in, a
single `CREATE OR REPLACE PROPERTY GRAPH` statement (SQL) comes out. It never
connects to BigQuery, reads no data, and creates or deploys nothing — you run the
emitted DDL yourself.

- **Export** (`ossie-bigquery export`): Ossie → property-graph DDL.
- **Import** (`ossie-bigquery import`): not yet implemented (export-only for now,
  like the `snowflake` and `polaris` spokes).

## Contents

- [Why a property graph](#why-a-property-graph)
- [Install](#install)
- [Quick start](#quick-start)
- [CLI reference](#cli-reference)
- [Python API](#python-api)
- [How the model maps](#how-the-model-maps)
- [Metrics and measures](#metrics-and-measures)
- [Deploying the DDL](#deploying-the-ddl)
- [Querying the graph and measures](#querying-the-graph-and-measures)
- [Requirements](#requirements)
- [Limitations](#limitations)
- [Warnings reference](#warnings-reference)
- [Development](#development)

## Why a property graph

An Ossie semantic model is a star/snowflake of datasets joined by foreign keys,
with model-level metrics. That maps directly onto BigQuery's native graph model:

| Apache Ossie | BigQuery property graph |
| --- | --- |
| `dataset` (`source` = `project.dataset.table`) | **NODE TABLE** — `KEY` from `primary_key`, `PROPERTIES` from `fields` |
| `relationship` (`from`/`to`/`from_columns`/`to_columns`) | **EDGE TABLE** — `SOURCE KEY … REFERENCES` / `DESTINATION KEY … REFERENCES` |
| single-table `metric` | `MEASURE(<agg>) AS <name>` in the owning node's `PROPERTIES` |
| `field` | graph property (bare column, or `<expr> AS <name>` for a computed field) |
| `description` + `ai_context.synonyms` | `OPTIONS(description="…")` |

A graph **measure** binds an aggregate to exactly one table's `KEY`. BigQuery then
keeps the aggregate correct even when a join fans the rows out — the aggregation
"locks" to the key and runs once per key, so a customer's revenue is not
double-counted because the customer has three orders. The cross-table rollup
happens at query time via `GRAPH_EXPAND` + `AGG` (see
[Querying the graph and measures](#querying-the-graph-and-measures)).

## Install

```bash
pip install apache-ossie-bigquery        # once published to PyPI
# or, from a checkout of this directory:
pip install -e .
```

The only runtime dependency is `PyYAML`. Python 3.11+.

## Quick start

Save this as `sales.yaml`:

```yaml
version: "0.2.0.dev0"

semantic_model:
  - name: sales_graph
    description: Minimal sales model
    datasets:
      - name: orders
        source: my_project.sales.orders
        primary_key: [order_id]
        fields:
          - name: order_id
            expression: {dialects: [{dialect: ANSI_SQL, expression: order_id}]}
          - name: amount
            expression: {dialects: [{dialect: ANSI_SQL, expression: amount}]}
            description: Order total in USD
      - name: customer
        source: my_project.sales.customer
        primary_key: [customer_id]
        fields:
          - name: customer_id
            expression: {dialects: [{dialect: ANSI_SQL, expression: customer_id}]}
          - name: country
            expression: {dialects: [{dialect: ANSI_SQL, expression: country}]}
    relationships:
      - name: orders_to_customer
        from: orders
        to: customer
        from_columns: [customer_id]
        to_columns: [customer_id]
    metrics:
      - name: total_revenue
        expression: {dialects: [{dialect: ANSI_SQL, expression: SUM(orders.amount)}]}
        description: Total order revenue
```

Convert it:

```bash
ossie-bigquery export -i sales.yaml -o sales_graph.sql
```

`sales_graph.sql` contains exactly:

```sql
CREATE OR REPLACE PROPERTY GRAPH sales_graph
  NODE TABLES (
    `my_project.sales.orders` AS orders
      KEY(order_id)
      PROPERTIES(
        order_id,
        amount OPTIONS(description="Order total in USD"),
        MEASURE(SUM(amount)) AS total_revenue OPTIONS(description="Total order revenue")
      ),
    `my_project.sales.customer` AS customer
      KEY(customer_id)
      PROPERTIES(
        customer_id,
        country
      )
  )
  EDGE TABLES (
    `my_project.sales.orders` AS orders_to_customer
      KEY(order_id)
      SOURCE KEY (order_id) REFERENCES orders (order_id)
      DESTINATION KEY (customer_id) REFERENCES customer (customer_id)
  )
  OPTIONS(description="Minimal sales model");
```

[Deploy it](#deploying-the-ddl), then get revenue per country without
double-counting fan-out (the graph is named `sales_graph`, matching the model
`name`; see [Where the graph lives](#deploying-the-ddl) to target a dataset):

```sql
SELECT
  customer_country,
  AGG(orders_total_revenue) AS revenue
FROM GRAPH_EXPAND("sales_graph")
GROUP BY customer_country;
```

## CLI reference

```
ossie-bigquery export -i <model.yaml> [-o <graph.sql>]
ossie-bigquery import -i <graph.sql> [-o <model.yaml>] [--name <model_name>]
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `-i`, `--input` | both | input file (required) |
| `-o`, `--output` | both | output file; **omit for stdout** |
| `--name` | `import` | model name to assign (import is not yet implemented) |

- With no `-o`, the result goes to **stdout**, so you can pipe it:
  ```bash
  ossie-bigquery export -i sales.yaml | bq query --use_legacy_sql=false --nouse_cache
  ```
- Conversions that drop information print **warnings to stderr** and keep going
  (see [Warnings reference](#warnings-reference)). Redirect them away with
  `2>/dev/null`, or capture them with `2> warnings.txt`.
- Any input that breaks a hard [requirement](#requirements) raises a
  `ConversionError`; the CLI prints `Error: …` to stderr and exits `1`.

## Python API

The conversion functions are pure `str -> str`; do file I/O yourself.

```python
import warnings
from ossie_bigquery import convert_ossie_to_bq_graph, ConversionError

with open("sales.yaml") as fh:
    model = fh.read()

try:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ddl = convert_ossie_to_bq_graph(model)
    for w in caught:
        print("dropped:", w.message)   # e.g. a cross-dataset metric that was skipped
except ConversionError as e:
    raise SystemExit(f"cannot convert: {e}")

with open("sales_graph.sql", "w") as fh:
    fh.write(ddl)
```

`convert_bq_graph_to_ossie(ddl_str, model_name=None)` exists as the import
entry point but currently raises `ConversionError` (not yet implemented).

## How the model maps

Each rule below shows the Ossie input and the emitted DDL, adapted from the
[`examples/tpcds_semantic_model.yaml`](../../examples/tpcds_semantic_model.yaml)
model that ships with Ossie (some fields trimmed for brevity). Each SQL block is
an **excerpt** of the single `CREATE OR REPLACE PROPERTY GRAPH` statement, shown
with the exact indentation the converter emits — following BigQuery's canonical
layout, `NODE TABLES` / `EDGE TABLES` nest under `CREATE`, each element table
nests under those, and its clauses nest again (so a node entry sits at four
spaces).

### Dataset → node table

The `source` (a `project.dataset.table` reference) backs the node table and is
backtick-quoted once; the node's label is the dataset `name`; `primary_key`
becomes the `KEY`. `fields` become properties, and `description` +
`ai_context.synonyms` fold into `OPTIONS(description=…)`.

```yaml
- name: customer
  source: tpcds.public.customer
  primary_key: [c_customer_sk]
  description: Customer dimension with demographic information
  ai_context:
    synonyms: [customers, shoppers, buyers]
  fields:
    - name: c_customer_sk
      expression: {dialects: [{dialect: ANSI_SQL, expression: c_customer_sk}]}
    - name: customer_full_name
      expression:
        dialects:
          - {dialect: ANSI_SQL, expression: "c_first_name || ' ' || c_last_name"}
      description: Customer full name (computed field)
```

```sql
    `tpcds.public.customer` AS customer
      KEY(c_customer_sk)
      OPTIONS(description="Customer dimension with demographic information\n\nSynonyms: customers, shoppers, buyers")
      PROPERTIES(
        c_customer_sk,
        c_first_name || ' ' || c_last_name AS customer_full_name OPTIONS(description="Customer full name (computed field)")
      )
```

- A field whose expression is just its own column name emits as a **bare
  column** (`c_customer_sk`). A field whose expression differs emits as
  `<expr> AS <name>` — a **computed property**.
- A composite `primary_key` keeps its column order: `primary_key: [ss_item_sk,
  ss_ticket_number]` → `KEY(ss_item_sk, ss_ticket_number)`.
- BigQuery graphs have no synonyms slot, so synonyms are appended to the
  description as `Synonyms: a, b, c`.

### Relationship → edge table

A `from`/`to` foreign-key relationship becomes an edge backed by the `from`
(many-side) table. Its `SOURCE KEY` is the `from` primary key; its `DESTINATION
KEY` is `from_columns`, referencing the `to` dataset's `to_columns`.

```yaml
- name: store_sales_to_customer
  from: store_sales
  to: customer
  from_columns: [ss_customer_sk]
  to_columns: [c_customer_sk]
```

```sql
    `tpcds.public.store_sales` AS store_sales_to_customer
      KEY(ss_item_sk, ss_ticket_number)
      SOURCE KEY (ss_item_sk, ss_ticket_number) REFERENCES store_sales (ss_item_sk, ss_ticket_number)
      DESTINATION KEY (ss_customer_sk) REFERENCES customer (c_customer_sk)
```

Composite join columns keep their order and are matched positionally:
`from_columns: [a, b]` with `to_columns: [x, y]` pairs `a→x`, `b→y`.

**Many-to-many** needs no special handling. Model the junction as an ordinary
dataset with two relationships (junction→A and junction→B); it emits as a node
with two edges — BigQuery's recommended "promote the junction to a node" shape.

### Metric → measure

See [Metrics and measures](#metrics-and-measures) below.

## Metrics and measures

A metric becomes a `MEASURE()` on the single node its aggregate references. The
converter finds that node by scanning the expression for a `<dataset>.` qualifier
(ignoring text inside string literals and not matching a name that is merely a
substring of a longer identifier), then strips that qualifier so the measure body
is table-local:

```yaml
metrics:
  - name: total_sales
    expression: {dialects: [{dialect: ANSI_SQL, expression: SUM(store_sales.ss_ext_sales_price)}]}
    description: Total sales revenue across all transactions
    ai_context:
      synonyms: [total revenue, gross sales, sales amount]
```

```sql
      PROPERTIES(
        ...
        MEASURE(SUM(ss_ext_sales_price)) AS total_sales OPTIONS(description="Total sales revenue across all transactions\n\nSynonyms: total revenue, gross sales, sales amount")
      )
```

BigQuery accepts these aggregates inside `MEASURE()`: **`SUM`, `AVG`, `COUNT`,
`COUNT(DISTINCT)`, `MIN`, `MAX`**.

A `MEASURE()` can only aggregate columns that the node exposes as properties, so
the converter **auto-exposes** any column a measure references that no `field`
already declares (added as a plain property before the measure). A metric like
`SUM(customer.credit_limit)` therefore works even when `credit_limit` is not
listed as a dimension `field` — it simply also becomes a queryable property.

Placement follows the number of datasets the expression references:

- **Exactly one dataset** → emitted as a `MEASURE()` on that node. If the body
  does not begin with a supported aggregate, it is still emitted (BigQuery
  validates it at deploy time) and a warning is printed.
- **Two or more datasets** (a cross-table ratio such as `SUM(store_sales.x) /
  COUNT(DISTINCT customer.y)`) → **skipped with a warning**. A single measure
  binds to one table's key, so a genuinely cross-table metric cannot be one
  measure. Compute it at query time from its component measures via `GRAPH_EXPAND`
  + `AGG` (below), or wait for native BigQuery measure features to cover it.
- **No dataset** (e.g. `COUNT(*)` with no qualifier) → **skipped with a
  warning**, because there is no node to attach it to.

This is a deliberate one-to-one mapping onto the native feature: the converter
does not decompose composite metrics or synthesize SQL views to fake them. The
skipped set shrinks as BigQuery's measure surface grows.

## Deploying the DDL

The emitted statement is standard GoogleSQL DDL. Run it any way you run SQL:

```bash
# from a file
bq query --use_legacy_sql=false --nouse_cache < sales_graph.sql

# or straight from the converter
ossie-bigquery export -i sales.yaml | bq query --use_legacy_sql=false --nouse_cache
```

or paste it into the BigQuery console, or send it through the BigQuery API /
client libraries.

**Where the graph lives.** The graph name is the model `name`. A bare name like
`sales_graph` is created in the job's default dataset. To pin it to a specific
dataset, either set a default dataset on the job, or make the model `name`
dataset-qualified — the converter backtick-quotes a dotted name as a path:

| model `name` | emitted graph name |
| --- | --- |
| `sales_graph` | `sales_graph` |
| `analytics.sales_graph` | `` `analytics.sales_graph` `` |
| `my_project.analytics.sales_graph` | `` `my_project.analytics.sales_graph` `` |

The `CREATE OR REPLACE` form makes re-deploys idempotent. Node/edge tables must
already exist — the converter references them, it does not create them.

## Querying the graph and measures

Once deployed, query the graph two ways.

**1. Graph pattern matching (`MATCH`/`RETURN`).** Traverse relationships as usual.
Note you **cannot** return a measure property here:

```sql
GRAPH sales_graph
MATCH (o:orders)-[]->(c:customer)
RETURN c.country, o.amount;
```

**2. Flatten with `GRAPH_EXPAND`, aggregate with `AGG`.** `GRAPH_EXPAND` returns
the whole graph as one flat table. Its columns are named
`<NodeLabel>_<property>` — so the `total_revenue` measure on the `orders` node
becomes the column `orders_total_revenue`, and `customer.country` becomes
`customer_country`.

Read a measure by wrapping its column in `AGG()`, which performs the fan-out-safe
aggregation exactly once per key. Group by any non-measure (dimension) columns:

```sql
SELECT
  customer_country,
  AGG(orders_total_revenue) AS revenue
FROM GRAPH_EXPAND("sales_graph")
GROUP BY customer_country;
```

The same pattern over the TPC-DS model — total sales and profit by brand, joining
`store_sales` measures to the `item` dimension through the graph:

```sql
SELECT
  item_i_brand,
  AGG(store_sales_total_sales)  AS total_sales,
  AGG(store_sales_total_profit) AS total_profit
FROM GRAPH_EXPAND("tpcds_retail_model")
GROUP BY item_i_brand
ORDER BY total_sales DESC;
```

Useful extras:

- **Recompute a skipped cross-table metric here.** A ratio the converter skipped
  (e.g. revenue per distinct customer) is just an expression over component
  measures at query time:
  ```sql
  SELECT
    AGG(store_sales_total_sales) / COUNT(DISTINCT customer_c_customer_sk) AS revenue_per_customer
  FROM GRAPH_EXPAND("tpcds_retail_model");
  ```
- **Inspect the flattened schema** without running the expansion:
  ```sql
  SELECT * FROM BQ.SHOW_GRAPH_EXPAND_SCHEMA("tpcds_retail_model");
  ```
- **Disable cached results** (`--nouse_cache`, or turn off cached results in the
  console). Editing the underlying tables does not invalidate a cached
  `GRAPH_EXPAND` result, so caching can return stale rows.

## Requirements

These are hard requirements — violating one raises a `ConversionError`:

- `version` must equal the spec version this converter targets (`0.2.0.dev0`).
- `semantic_model` must be a non-empty list. If it has more than one model, only
  the first is converted (with a warning).
- Every dataset needs a string `name`; names must be unique.
- Each relationship must name a `from`/`to` that exist, plus non-empty
  `from_columns` and `to_columns`.

## Limitations

- **Export only.** BigQuery DDL → Apache Ossie import is not implemented, by
  design (matching the `snowflake` and `polaris` spokes). Ossie is the authoring
  source of truth; the graph DDL is a generated, deliberately **lossy** deployment
  artifact, so it is not a faithful round-trip source. The export folds synonyms
  into free-text descriptions, drops keyless datasets, and skips cross-table
  metrics — none of which an importer could reconstruct back into the original
  model. Parsing `CREATE PROPERTY GRAPH` GoogleSQL is also a substantial effort of
  its own (no off-the-shelf parser handles it yet). The
  `convert_bq_graph_to_ossie` entry point and `import` subcommand are scaffolded —
  they raise `ConversionError` today — so the CLI and package shape stay stable
  for a future PR should a concrete import use case arise.
- **A node needs a key.** A dataset with no `primary_key` cannot be a graph node
  and is **skipped with a warning**; any relationship touching it is dropped too.
- **A node needs a base table.** Each `source` must be a plain
  `project.dataset.table` identifier — a graph node cannot be backed by a
  subquery. A non-identifier source is warned about.
- **Single-table measures only.** Cross-dataset or dataset-less metrics are
  skipped with a warning (see [Metrics and measures](#metrics-and-measures)).
- **Exactly one root.** BigQuery requires exactly one root node table — one whose
  `KEY` is referenced by no edge. The converter warns (but still emits) if the
  graph has zero or several roots.
- **Dialect.** Expressions use the `BIGQUERY` dialect, falling back to
  `ANSI_SQL`. A field/metric with neither is skipped with a warning.

## Warnings reference

Warnings go to stderr; the conversion still produces output. Each names the
element it concerns in brackets, e.g. `[customer_lifetime_value] metric spans …`.

| Warning contains | Meaning | What to do |
| --- | --- | --- |
| `no 'primary_key'; node table skipped` | dataset can't be a node | add a `primary_key`, or accept it (and its edges) being dropped |
| `references skipped dataset … edge omitted` | edge points at a dropped node | fix the node's `primary_key` |
| `spans multiple tables … cannot be a single MEASURE` | cross-dataset metric | recompute it at query time from component measures |
| `references no known dataset … cannot be a single MEASURE` | metric has no `<dataset>.` qualifier | qualify the columns, or compute at query time |
| `does not begin with a supported aggregate` | measure body isn't `SUM/AVG/COUNT/MIN/MAX` | fine if BigQuery accepts it; otherwise rewrite the metric |
| `has no BIGQUERY or ANSI_SQL expression` | no usable dialect for a field/metric | add a `BIGQUERY` or `ANSI_SQL` dialect |
| `not a plain project.dataset.table identifier` | `source` isn't a base table | point `source` at a table, not a subquery |
| `graph has no root node table` / `multiple root node tables` | not exactly one root | adjust relationships so one node has no incoming edge |
| `multiple semantic models found` | more than one model in the file | split them, or accept only the first being used |

## Development

```bash
uv sync
uv run pytest
```

The test suite includes a golden-file export (`tests/fixtures/tpcds_ossie.yaml` →
`tests/fixtures/tpcds_graph.sql`) plus unit tests for measure placement, edge
keys, root validation, dialect fallback, and description folding.
