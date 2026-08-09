# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Apache Ossie semantic model -> BigQuery Graph DDL.

Emits a single `CREATE OR REPLACE PROPERTY GRAPH` statement over the datasets'
existing base tables, with model-level metrics rendered as inline `MEASURE(...)`
properties:

  * each `dataset`      -> a NODE TABLE (`KEY` + `PROPERTIES`)
  * each `relationship` -> an EDGE TABLE (`SOURCE KEY ... REFERENCES` /
                           `DESTINATION KEY ... REFERENCES`)
  * each single-table `metric` -> a `MEASURE(<agg>) AS <name>` on its owning
    node

A measure binds an aggregate to exactly one table's KEY; the cross-table rollup
happens at query time via `GRAPH_EXPAND(...) + AGG(...)`. A metric whose
aggregate genuinely spans multiple datasets cannot be expressed as one MEASURE
and is skipped with a warning. The converter is a text transform; it references
the base tables, it does not create or deploy them.

Descriptions and synonyms map onto BigQuery's native metadata: a graph element
label and a property each take an `OPTIONS(description=..., synonyms=[...])`
clause, so an Ossie `description` and structured `ai_context.synonyms` are
carried through as first-class options rather than folded into free text.

Expression SQL is taken in whichever dialect the model provides: a BigQuery or
ANSI_SQL expression is used verbatim (BigQuery is an ANSI superset), and any
other SQL dialect (e.g. Snowflake, Databricks) is transpiled to BigQuery with
sqlglot.

The core `apache-ossie` package owns the model schema, so parsing and
structural validation are delegated to its pydantic models rather than
re-implemented here.

See: https://docs.cloud.google.com/bigquery/docs/graph-measures
"""

import re
import warnings

from ossie import OSIDocument
from ossie.models import OSIAIContextObject
from ossie.models import OSIDialect
from pydantic import ValidationError
import sqlglot
import sqlglot.expressions as sqlglot_exp
import yaml


class ConversionError(Exception):
  """Raised when an input cannot be converted."""


# Apache Ossie spec version this converter targets (see core-spec/). This is an
# exact-match check and must be bumped in lockstep with the `version` in
# core-spec/ whenever the spec version moves.
OSSIE_VERSION = "0.2.0.dev0"

# Aggregate functions BigQuery accepts inside MEASURE(...). See
# https://docs.cloud.google.com/bigquery/docs/graph-measures
SUPPORTED_AGGREGATES = ("SUM", "AVG", "COUNT", "MIN", "MAX")

# The dialect this converter emits, and the one every expression is normalized
# to before it is parsed or regenerated.
_BIGQUERY = "bigquery"

# Ossie SQL dialects mapped to their sqlglot names. BigQuery and ANSI_SQL are
# used verbatim; the rest are transpiled from these names to BigQuery. Non-SQL
# dialects (MDX, TABLEAU, MAQL) are absent -- they have no BigQuery rendering.
_SQLGLOT_DIALECT = {
    OSIDialect.SNOWFLAKE: "snowflake",
    OSIDialect.DATABRICKS: "databricks",
}

# Preference order when an expression offers several dialects. BigQuery and
# ANSI_SQL need no transpilation, so they win when present.
_DIALECT_PREFERENCE = (
    OSIDialect.BIGQUERY,
    OSIDialect.ANSI_SQL,
    OSIDialect.SNOWFLAKE,
    OSIDialect.DATABRICKS,
)

# All generated indentation flows through this single mechanism: one nesting
# level == one INDENT. Deriving every indent from `depth` (rather than
# hardcoded spaces) keeps the output indentation consistent by construction.
_INDENT = "  "

# A bare, unquoted identifier that needs no backticks.
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# One dotted component of a `project.dataset.table` reference. Unlike a bare
# identifier this also permits hyphens, since GCP project IDs commonly contain
# them (e.g. `sqlgen-testing`); such a reference is still a valid base table
# once the whole path is backtick-quoted.
_TABLE_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def convert_ossie_to_bq_graph(ossie_yaml_str):
  """Convert an Apache Ossie semantic model (YAML text) to BigQuery Graph DDL.

  Returns the `CREATE OR REPLACE PROPERTY GRAPH` statement as SQL text.
  Conversions that drop information emit warnings (to stderr).
  """
  raw = _load_yaml(ossie_yaml_str)
  if not isinstance(raw, dict):
    raise ConversionError(
        "Invalid Apache Ossie YAML: expected a mapping at the root"
    )
  version = str(raw.get("version", ""))
  if version != OSSIE_VERSION:
    raise ConversionError(
        f"Unsupported Apache Ossie version '{version}'. Supported:"
        f" {OSSIE_VERSION}"
    )
  document = _validate(raw)
  if not document.semantic_model:
    raise ConversionError("'semantic_model' must be a non-empty list")
  if len(document.semantic_model) > 1:
    _warn("model", "multiple semantic models found; converting only the first")
  return _convert_model(document.semantic_model[0])


def _load_yaml(text):
  """Parse YAML text, surfacing a syntax error as a ConversionError.

  Callers (and the CLI) then get a clean message rather than a raw traceback.
  """
  try:
    return yaml.safe_load(text)
  except yaml.YAMLError as e:
    raise ConversionError(f"Invalid YAML: {e}") from e


def _validate(raw):
  """Structurally validate the raw model against the core Ossie schema.

  Delegates to the shared `apache-ossie` pydantic models (`OSIDocument`), so
  required-field, type, and dialect-enum checks are owned by the core package
  rather than re-implemented here. A schema violation is re-raised as a
  ConversionError carrying pydantic's field-level report.
  """
  try:
    return OSIDocument.model_validate(raw)
  except ValidationError as e:
    raise ConversionError(f"Invalid Apache Ossie model:\n{e}") from e


def _warn(scope, msg):
  warnings.warn(f"[{scope}] {msg}", stacklevel=2)


def _convert_model(model):
  if not model.datasets:
    raise ConversionError(
        f"Model '{model.name}': 'datasets' must be a non-empty list"
    )
  datasets = {ds.name: ds for ds in model.datasets}
  if len(datasets) != len(model.datasets):
    # The schema does not enforce unique dataset names, but a graph needs
    # distinct node labels; a duplicate would collide on `AS <label>`.
    dupes = _duplicates(ds.name for ds in model.datasets)
    raise ConversionError(
        f"Model '{model.name}': duplicate dataset name"
        f" {', '.join(repr(d) for d in dupes)}"
    )

  # A graph node table requires a non-empty KEY. A dataset with no primary_key
  # cannot form a valid node, so skip it (and any edge that references it)
  # rather than emit an invalid `KEY()`.
  skipped = set()
  valid = []
  for ds in model.datasets:
    if ds.primary_key:
      valid.append(ds.name)
    else:
      _warn(
          ds.name,
          "dataset has no 'primary_key'; node table skipped "
          "(a graph node requires a KEY)",
      )
      skipped.add(ds.name)
  if not valid:
    _warn(
        "model",
        "every dataset was skipped (no primary_key); "
        "the generated graph would be empty and invalid",
    )

  # Metrics are model-level; place each on the single dataset its aggregate
  # references. Measures placed on a skipped dataset simply never render.
  measures_by_dataset = {}
  for metric in model.metrics or []:
    _place_metric(metric, datasets, measures_by_dataset)

  node_tables = [
      _render_node_table(datasets[name], measures_by_dataset.get(name, []))
      for name in valid
  ]

  edge_tables = []
  kept_edges = []
  for rel in model.relationships or []:
    frm, to = rel.from_dataset, rel.to
    if frm not in datasets or to not in datasets:
      raise ConversionError(
          f"Model '{model.name}': relationship '{rel.name}' references an"
          " unknown dataset"
      )
    dangling = [n for n in (frm, to) if n in skipped]
    if dangling:
      _warn(
          rel.name,
          f"references skipped dataset {', '.join(repr(n) for n in dangling)};"
          " edge omitted",
      )
      continue
    edge_tables.append(_render_edge_table(datasets[frm], rel))
    kept_edges.append((frm, to))

  _validate_single_root(valid, kept_edges)

  blocks = [
      f"CREATE OR REPLACE PROPERTY GRAPH {_qualify_graph(model.name)}",
      _table_group("NODE TABLES", node_tables),
  ]
  if edge_tables:
    blocks.append(_table_group("EDGE TABLES", edge_tables))
  graph_opts = _options_clause(_description_of(model), _synonyms_of(model))
  if graph_opts:
    blocks.append(_line(1, graph_opts))
  return "\n".join(blocks) + ";\n"


def _duplicates(names):
  """Return the names that appear more than once, in first-seen order."""
  seen = set()
  dupes = []
  for name in names:
    if name in seen and name not in dupes:
      dupes.append(name)
    seen.add(name)
  return dupes


def _place_metric(metric, datasets, measures_by_dataset):
  expr = _pick_expression(metric.expression, f"metric '{metric.name}'")
  if expr is None:
    _warn(
        metric.name,
        "metric has no BigQuery-convertible SQL expression; skipped",
    )
    return

  referenced = _referenced_datasets(expr, datasets.keys())
  if len(referenced) != 1:
    detail = (
        "references no known dataset"
        if not referenced
        else f"spans multiple tables ({', '.join(referenced)})"
    )
    _warn(
        metric.name,
        f"metric {detail}; skipped (cannot be a single MEASURE)",
    )
    return

  dataset = referenced[0]
  body = _strip_qualifier(expr, dataset).strip()
  if not _starts_with_supported_aggregate(body):
    _warn(
        metric.name,
        f"metric expression '{body}' does not begin with a supported aggregate"
        f" ({', '.join(SUPPORTED_AGGREGATES)}); emitting anyway",
    )

  measure = f"MEASURE({body}) AS {metric.name}"
  opts = _options_clause(_description_of(metric), _synonyms_of(metric))
  measures_by_dataset.setdefault(dataset, []).append({
      "ddl": f"{measure} {opts}" if opts else measure,
      "columns": _referenced_columns(body),
  })


def _render_node_table(ds, measures):
  table = _qualify_table(ds.source, ds.name)

  properties = []
  exposed = set()
  for field in ds.fields or []:
    rendered = _render_field_property(ds.name, field)
    if rendered is None:
      continue
    properties.append(rendered)
    exposed.add(field.name)

  # A MEASURE can only aggregate columns that are exposed as properties. Expose
  # any column a measure references that no field already declares, so e.g.
  # MEASURE(SUM(credit_limit)) works even when credit_limit is not itself
  # listed as a dimension field.
  for measure in measures:
    for col in measure["columns"]:
      if col not in exposed:
        properties.append(col)
        exposed.add(col)
  properties.extend(measure["ddl"] for measure in measures)

  lines = [
      _line(2, f"{table} AS {ds.name}"),
      _line(3, f"KEY({', '.join(ds.primary_key)})"),
  ]
  label = _label_options(ds)
  if label:
    lines.append(_line(3, label))
  if properties:
    lines.append(_properties_block(properties))
  return "\n".join(lines)


def _render_field_property(dataset, field):
  expr = _pick_expression(
      field.expression, f"dataset '{dataset}': field '{field.name}'"
  )
  if expr is None:
    _warn(
        field.name,
        f"field on '{dataset}' has no BigQuery-convertible SQL expression;"
        " skipped",
    )
    return None
  # A bare column when the expression is just the column, else `<expr> AS
  # <name>`.
  local = _strip_qualifier(expr, dataset).strip()
  prop = field.name if local == field.name else f"{local} AS {field.name}"
  opts = _options_clause(_description_of(field), _synonyms_of(field))
  return f"{prop} {opts}" if opts else prop


def _render_edge_table(from_ds, rel):
  from_cols = _require_columns(rel.from_columns, rel.name, "from_columns")
  to_cols = _require_columns(rel.to_columns, rel.name, "to_columns")

  backing = _qualify_table(from_ds.source, from_ds.name)
  # Direct foreign-key edge: backed by the `from` (many) dataset's base table,
  # which holds the FK columns. It is its own source node; its primary key is
  # both the edge key and the SOURCE KEY.
  source_key = from_ds.primary_key

  lines = [
      _line(2, f"{backing} AS {rel.name}"),
      _line(3, f"KEY({', '.join(source_key)})"),
      _line(
          3,
          f"SOURCE KEY ({', '.join(source_key)}) REFERENCES"
          f" {rel.from_dataset} ({', '.join(source_key)})",
      ),
      _line(
          3,
          f"DESTINATION KEY ({', '.join(from_cols)}) REFERENCES"
          f" {rel.to} ({', '.join(to_cols)})",
      ),
  ]
  label = _label_options(rel)
  if label:
    lines.append(_line(3, label))
  return "\n".join(lines)


def _require_columns(cols, rel_name, key):
  if not cols:
    raise ConversionError(
        f"relationship '{rel_name}': '{key}' must be a non-empty list of"
        " column names"
    )
  return cols


def _validate_single_root(valid, edges):
  """Warn unless exactly one node table is a root (referenced by no edge).

  BigQuery requires exactly one root node table -- for an FK graph, the dataset
  that is never an edge destination. This is a query-time constraint, so it is
  a warning rather than an error.
  """
  if not valid:
    return
  destinations = {to for _, to in edges}
  roots = [name for name in valid if name not in destinations]
  if len(roots) != 1:
    detail = (
        "no root node table"
        if not roots
        else f"multiple root node tables ({', '.join(roots)})"
    )
    _warn(
        "model",
        f"graph has {detail}; BigQuery requires exactly one "
        "(a node whose KEY is not referenced by any edge)",
    )


# --- expression analysis (sqlglot) ------------------------------------------
#
# A metric becomes a MEASURE by reading three facts off the parsed expression's
# column nodes -- which datasets it references, the expression rewritten
# table-local, and the columns it aggregates -- so string literals, function
# names, keywords, and type names are handled by the grammar, not by scanning
# text. Every expression here is already BigQuery SQL (see `_pick_expression`).


def _pick_expression(expression, what):
  """Return BigQuery SQL for an Ossie expression, or None if it has none.

  A BigQuery or ANSI_SQL dialect is used verbatim (BigQuery is an ANSI
  superset); any other SQL dialect is transpiled to BigQuery with sqlglot.
  Non-SQL dialects (MDX, TABLEAU, MAQL) have no BigQuery rendering and yield
  None, leaving the caller to warn and skip.
  """
  by_dialect = {d.dialect: d.expression for d in expression.dialects}
  for dialect in _DIALECT_PREFERENCE:
    sql = by_dialect.get(dialect)
    if sql is None:
      continue
    if dialect in (OSIDialect.BIGQUERY, OSIDialect.ANSI_SQL):
      return sql
    return _transpile(sql, _SQLGLOT_DIALECT[dialect], what)
  return None


def _transpile(sql, read_dialect, what):
  """Transpile `sql` from `read_dialect` to BigQuery.

  Rewrites dialect-specific constructs BigQuery does not share (e.g. Snowflake
  `IFF`/`NVL`) to their BigQuery form. If sqlglot cannot parse the expression,
  it is passed through unchanged with a warning rather than dropped.
  """
  try:
    return sqlglot.transpile(sql, read=read_dialect, write=_BIGQUERY)[0]
  except sqlglot.errors.SqlglotError:
    _warn(
        what,
        f"could not transpile expression to BigQuery: {sql!r}; "
        "emitting unchanged",
    )
    return sql


def _parse(expression):
  """Parse a BigQuery SQL expression into a sqlglot tree, or None."""
  try:
    return sqlglot.parse_one(expression, dialect=_BIGQUERY)
  except sqlglot.errors.SqlglotError:
    return None


def _referenced_datasets(expression, dataset_names):
  """Return the datasets whose columns `expression` references, in order.

  Only names in `dataset_names` are returned, de-duplicated. A measure binds to
  one table, so a metric referencing zero or several datasets cannot become a
  single MEASURE.
  """
  tree = _parse(expression)
  if tree is None:
    return []
  allowed = set(dataset_names)
  found = []
  for column in tree.find_all(sqlglot_exp.Column):
    table = column.table
    if table in allowed and table not in found:
      found.append(table)
  return found


def _strip_qualifier(expression, dataset):
  """Return `expression` with `<dataset>.` column qualifiers removed.

  Rewrites e.g. `SUM(orders.amount)` to `SUM(amount)` so the expression is
  local to its owning node table. Columns qualified by any other name are left
  intact; an unparseable expression is returned unchanged.
  """
  tree = _parse(expression)
  if tree is None:
    return expression
  for column in tree.find_all(sqlglot_exp.Column):
    if column.table == dataset:
      column.set("table", None)
  return tree.sql(dialect=_BIGQUERY)


def _referenced_columns(expression):
  """Return the bare column names `expression` references, in order."""
  tree = _parse(expression)
  if tree is None:
    return []
  names = []
  for column in tree.find_all(sqlglot_exp.Column):
    if column.name not in names:
      names.append(column.name)
  return names


def _starts_with_supported_aggregate(body):
  m = re.match(r"^([A-Za-z_]+)\s*\(", body.lstrip())
  return bool(m) and m.group(1).upper() in SUPPORTED_AGGREGATES


# --- identifiers, metadata, and layout --------------------------------------


def _qualify_graph(name):
  return name if _SIMPLE_IDENT_RE.match(name) else f"`{name}`"


def _qualify_table(source, context):
  """Backtick-quote a `project.dataset.table` reference.

  Apache Ossie sources are already fully qualified dotted identifiers, so the
  whole reference is wrapped once. Warns on a source that is not a plain dotted
  identifier (e.g. a subquery), which cannot back a graph node table.
  """
  s = source.strip()
  parts = s.split(".")
  if not all(_TABLE_PART_RE.match(p.strip("`")) for p in parts):
    _warn(
        context,
        f"source '{source}' is not a plain project.dataset.table "
        "identifier; a graph node table requires a base table",
    )
    return s
  return "`" + ".".join(p.strip("`") for p in parts) + "`"


def _line(depth, text):
  return _INDENT * depth + text


def _table_group(keyword, entries):
  """Render a `NODE TABLES (...)` / `EDGE TABLES (...)` clause and its entries.

  The clause keyword is indented one level under the CREATE statement and each
  element table one level under that -- BigQuery's canonical graph layout.
  """
  inner = ",\n".join(entries)
  return f"{_line(1, keyword + ' (')}\n{inner}\n{_line(1, ')')}"


def _properties_block(properties):
  body = ",\n".join(_line(4, p) for p in properties)
  return f"{_line(3, 'PROPERTIES(')}\n{body}\n{_line(3, ')')}"


def _string_literal(value):
  """Render a Python string as a BigQuery double-quoted string literal.

  Escapes the backslash, the quote, and control characters so a multi-line
  description stays a valid literal.
  """
  escaped = (
      value.replace("\\", "\\\\")
      .replace('"', '\\"')
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  )
  return f'"{escaped}"'


def _options_clause(description, synonyms):
  """Render an `OPTIONS(...)` clause from a description and/or synonyms.

  BigQuery Graph exposes both as first-class label and property options
  (`description = <string>`, `synonyms = <array of strings>`), so each maps to
  its own key. Returns None when there is no metadata to emit.
  """
  parts = []
  if description:
    parts.append(f"description={_string_literal(description)}")
  if synonyms:
    rendered = ", ".join(_string_literal(s) for s in synonyms)
    parts.append(f"synonyms=[{rendered}]")
  return f"OPTIONS({', '.join(parts)})" if parts else None


def _label_options(obj):
  """Return the `DEFAULT LABEL OPTIONS(...)` clause for a node or edge, or None.

  BigQuery attaches an element's description and synonyms to its default label,
  between the KEY clause and PROPERTIES.
  """
  opts = _options_clause(_description_of(obj), _synonyms_of(obj))
  return f"DEFAULT LABEL {opts}" if opts else None


def _description_of(obj):
  """Return a trimmed description for an Apache Ossie object, or None.

  A string-form `ai_context` (the schema allows a string or a structured
  object) has no options key of its own, so it is folded into the description.
  """
  parts = []
  desc = getattr(obj, "description", None)
  if isinstance(desc, str) and desc.strip():
    parts.append(desc.strip())
  ai = getattr(obj, "ai_context", None)
  if isinstance(ai, str) and ai.strip():
    parts.append(ai.strip())
  return "\n".join(parts) if parts else None


def _synonyms_of(obj):
  """Return the synonyms from an object's structured `ai_context`, else []."""
  ai = getattr(obj, "ai_context", None)
  if isinstance(ai, OSIAIContextObject):
    return list(ai.synonyms or [])
  return []
