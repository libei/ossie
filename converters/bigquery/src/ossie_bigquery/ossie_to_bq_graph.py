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

"""Apache Ossie semantic model -> BigQuery property-graph DDL.

Emits a single `CREATE OR REPLACE PROPERTY GRAPH` statement over the datasets'
existing base tables, with model-level metrics rendered as inline `MEASURE(...)`
properties:

  * each `dataset`      -> a NODE TABLE (`KEY` + `PROPERTIES`)
  * each `relationship` -> an EDGE TABLE (`SOURCE KEY ... REFERENCES` /
                           `DESTINATION KEY ... REFERENCES`)
  * each single-table `metric` -> a `MEASURE(<agg>) AS <name>` on its owning
  node

A BigQuery graph measure binds an aggregate to exactly one table's KEY; the
cross-table rollup happens at query time via `GRAPH_EXPAND(...) + AGG(...)`. A
metric whose aggregate genuinely spans multiple datasets cannot be expressed as
one MEASURE and is skipped with a warning. The converter is a text transform; it
references the base tables, it does not create or deploy them.

See: https://docs.cloud.google.com/bigquery/docs/graph-measures
"""

import re
import warnings

from ._common import (
    ConversionError,
    OSSIE_VERSION,
    SUPPORTED_AGGREGATES,
    description_of,
    load_yaml,
    pick_expression,
    require,
    require_str,
    synonyms_of,
)
from ._expr import referenced_columns, referenced_datasets, strip_qualifier

# All generated indentation flows through this single mechanism: one nesting
# level == one INDENT. Keeping every indent derived from `depth` (rather than
# hardcoded spaces) makes the output indentation consistent by construction.
_INDENT = "  "

# A bare, unquoted identifier that needs no backticks.
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# One dotted component of a `project.dataset.table` reference. Unlike a bare
# identifier this also permits hyphens, since GCP project IDs commonly contain
# them (e.g. `sqlgen-testing`); such a reference is still a valid base table once
# the whole path is backtick-quoted.
_TABLE_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]*$")


def convert_ossie_to_bq_graph(ossie_yaml_str):
  """Convert an Apache Ossie semantic model (YAML text) to BigQuery property-graph

  DDL (SQL text). Conversions that drop information emit warnings (stderr).
  """
  root = load_yaml(ossie_yaml_str)
  if not isinstance(root, dict):
    raise ConversionError(
        "Invalid Apache Ossie YAML: expected a mapping at the root"
    )
  version = str(root.get("version", ""))
  if version != OSSIE_VERSION:
    raise ConversionError(
        f"Unsupported Apache Ossie version '{version}'. Supported:"
        f" {OSSIE_VERSION}"
    )
  models = root.get("semantic_model")
  if not isinstance(models, list) or not models:
    raise ConversionError("'semantic_model' must be a non-empty list")
  if len(models) > 1:
    _warn("model", "multiple semantic models found; converting only the first")
  return _convert_model(models[0])


def _warn(scope, msg):
  warnings.warn(f"[{scope}] {msg}", stacklevel=2)


def _convert_model(model):
  model_name = require_str(model, "name", "semantic model")

  dataset_list = model.get("datasets") or []
  if not isinstance(dataset_list, list) or not dataset_list:
    raise ConversionError(
        f"Model '{model_name}': 'datasets' must be a non-empty list"
    )

  datasets = {}
  for ds in dataset_list:
    name = require_str(ds, "name", f"Model '{model_name}': dataset")
    if name in datasets:
      raise ConversionError(
          f"Model '{model_name}': duplicate dataset name '{name}'"
      )
    datasets[name] = ds

  relationships = model.get("relationships") or []
  metrics = model.get("metrics") or []

  # A graph node table requires a non-empty KEY. A dataset with no primary_key
  # cannot form a valid node, so skip it (and any edge that references it) rather
  # than emit an invalid `KEY()`.
  skipped = set()
  valid = []
  for name, ds in datasets.items():
    pk = ds.get("primary_key")
    if isinstance(pk, list) and pk:
      valid.append(name)
    else:
      _warn(
          name,
          "dataset has no 'primary_key'; node table skipped "
          "(a graph node requires a KEY)",
      )
      skipped.add(name)
  if datasets and not valid:
    _warn(
        "model",
        "every dataset was skipped (no primary_key); "
        "the generated graph would be empty and invalid",
    )

  # Metrics are model-level; place each on the single dataset its aggregate
  # references. Measures placed on a skipped dataset simply never render.
  measures_by_dataset = {}
  for metric in metrics:
    _place_metric(model_name, metric, datasets, measures_by_dataset)

  node_tables = [
      _render_node_table(
          name, datasets[name], measures_by_dataset.get(name, [])
      )
      for name in valid
  ]

  edge_tables = []
  kept_edges = []
  for rel in relationships:
    rel_name = require_str(rel, "name", f"Model '{model_name}': relationship")
    frm = require_str(
        rel, "from", f"Model '{model_name}': relationship '{rel_name}'"
    )
    to = require_str(
        rel, "to", f"Model '{model_name}': relationship '{rel_name}'"
    )
    if frm not in datasets or to not in datasets:
      raise ConversionError(
          f"Model '{model_name}': relationship '{rel_name}' references an"
          " unknown dataset"
      )
    dangling = [n for n in (frm, to) if n in skipped]
    if dangling:
      _warn(
          rel_name,
          f"references skipped dataset {', '.join(repr(n) for n in dangling)}; "
          "edge omitted",
      )
      continue
    edge_tables.append(_render_edge_table(model_name, rel, datasets))
    kept_edges.append((frm, to))

  _validate_single_root(valid, kept_edges)

  blocks = [
      f"CREATE OR REPLACE PROPERTY GRAPH {_qualify_graph(model_name)}",
      _table_group("NODE TABLES", node_tables),
  ]
  if edge_tables:
    blocks.append(_table_group("EDGE TABLES", edge_tables))
  graph_opts = _options_clause(
      description_of(model), synonyms_of(model.get("ai_context"))
  )
  if graph_opts:
    blocks.append(_line(1, graph_opts))
  return "\n".join(blocks) + ";\n"


def _place_metric(model_name, metric, datasets, measures_by_dataset):
  name = require_str(metric, "name", f"Model '{model_name}': metric")
  expr = pick_expression(metric.get("expression"), f"metric '{name}'")
  if expr is None:
    _warn(name, "metric has no BIGQUERY or ANSI_SQL expression; skipped")
    return

  referenced = referenced_datasets(expr, list(datasets))
  if len(referenced) != 1:
    detail = (
        "references no known dataset"
        if not referenced
        else f"spans multiple tables ({', '.join(referenced)})"
    )
    _warn(name, f"metric {detail}; skipped (cannot be a single MEASURE)")
    return

  dataset = referenced[0]
  body = strip_qualifier(expr, dataset).strip()
  if not _starts_with_supported_aggregate(body):
    _warn(
        name,
        f"metric expression '{body}' does not begin with a supported aggregate "
        f"({', '.join(SUPPORTED_AGGREGATES)}); emitting anyway",
    )

  measure = f"MEASURE({body}) AS {name}"
  opts = _options_clause(
      description_of(metric), synonyms_of(metric.get("ai_context"))
  )
  measures_by_dataset.setdefault(dataset, []).append({
      "ddl": f"{measure} {opts}" if opts else measure,
      "columns": referenced_columns(body),
  })


def _starts_with_supported_aggregate(body):
  m = re.match(r"^([A-Za-z_]+)\s*\(", body.lstrip())
  return bool(m) and m.group(1).upper() in SUPPORTED_AGGREGATES


def _render_node_table(name, ds, measures):
  table = _qualify_table(require_str(ds, "source", f"dataset '{name}'"), name)

  properties = []
  exposed = set()
  for field in ds.get("fields") or []:
    rendered = _render_field_property(name, field)
    if rendered is None:
      continue
    properties.append(rendered)
    exposed.add(field["name"])

  # A BigQuery graph MEASURE can only aggregate columns that are exposed as
  # properties. Expose any column a measure references that no field already
  # declares, so e.g. MEASURE(SUM(credit_limit)) works even when credit_limit
  # is not itself listed as a dimension field.
  for measure in measures:
    for col in measure["columns"]:
      if col not in exposed:
        properties.append(col)
        exposed.add(col)
  properties.extend(measure["ddl"] for measure in measures)

  lines = [
      _line(2, f"{table} AS {name}"),
      _line(3, f"KEY({', '.join(ds['primary_key'])})"),
  ]
  # Element-table description attaches to the DEFAULT LABEL: after the KEY
  # clause, before PROPERTIES (grammar: element_table_definition).
  label_opts = _options_clause(
      description_of(ds), synonyms_of(ds.get("ai_context"))
  )
  if label_opts:
    lines.append(_line(3, label_opts))
  if properties:
    lines.append(_properties_block(properties))
  return "\n".join(lines)


def _render_field_property(dataset, field):
  fname = require_str(field, "name", f"dataset '{dataset}': field")
  expr = pick_expression(
      field.get("expression"), f"dataset '{dataset}': field '{fname}'"
  )
  if expr is None:
    _warn(
        fname,
        f"field on '{dataset}' has no BIGQUERY or ANSI_SQL expression; skipped",
    )
    return None
  # A bare column when the expression is just the column, else `<expr> AS <name>`.
  local = strip_qualifier(expr, dataset).strip()
  prop = fname if local == fname else f"{local} AS {fname}"
  opts = _options_clause(
      description_of(field), synonyms_of(field.get("ai_context"))
  )
  return f"{prop} {opts}" if opts else prop


def _render_edge_table(model_name, rel, datasets):
  rel_name = rel["name"]
  frm, to = rel["from"], rel["to"]
  scope = f"Model '{model_name}': relationship '{rel_name}'"
  from_cols = _require_columns(rel, "from_columns", scope)
  to_cols = _require_columns(rel, "to_columns", scope)

  from_ds = datasets[frm]
  backing = _qualify_table(
      require_str(from_ds, "source", f"dataset '{frm}'"), frm
  )
  # Direct foreign-key edge: backed by the `from` (many) dataset's base table,
  # which holds the FK columns. It is its own source node; its primary key is
  # both the edge key and the SOURCE KEY.
  source_key = from_ds["primary_key"]

  lines = [
      _line(2, f"{backing} AS {rel_name}"),
      _line(3, f"KEY({', '.join(source_key)})"),
      _line(
          3,
          f"SOURCE KEY ({', '.join(source_key)}) REFERENCES"
          f" {frm} ({', '.join(source_key)})",
      ),
      _line(
          3,
          f"DESTINATION KEY ({', '.join(from_cols)}) REFERENCES"
          f" {to} ({', '.join(to_cols)})",
      ),
  ]
  label_opts = _options_clause(
      description_of(rel), synonyms_of(rel.get("ai_context"))
  )
  if label_opts:
    lines.append(_line(3, label_opts))
  return "\n".join(lines)


def _require_columns(rel, key, scope):
  cols = require(rel, key, scope)
  if (
      not isinstance(cols, list)
      or not cols
      or not all(isinstance(c, str) for c in cols)
  ):
    raise ConversionError(
        f"{scope}: '{key}' must be a non-empty list of column names"
    )
  return cols


def _validate_single_root(valid, edges):
  """BigQuery requires exactly one root node table (its KEY appears in no other

  table). For an FK graph that is the dataset that is never an edge destination.
  Warn -- not error -- since it is a query-time constraint.
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


# --- string / layout helpers ------------------------------------------------


def _line(depth, text):
  return _INDENT * depth + text


def _table_group(keyword, entries):
  """Render a graph-level `NODE TABLES (...)` / `EDGE TABLES (...)` clause and its

  entries, indented one level under the CREATE statement (BigQuery's canonical
  layout: the clause keyword nests under CREATE, each element table under that).
  """
  inner = ",\n".join(entries)
  return f"{_line(1, keyword + ' (')}\n{inner}\n{_line(1, ')')}"


def _properties_block(properties):
  body = ",\n".join(_line(4, p) for p in properties)
  return f"{_line(3, 'PROPERTIES(')}\n{body}\n{_line(3, ')')}"


def _quote(s):
  """Render a value as a BigQuery double-quoted string literal, escaping backslash,

  the quote, and control characters so a multi-line description stays a valid
  literal.
  """
  escaped = (
      s.replace("\\", "\\\\")
      .replace('"', '\\"')
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  )
  return f'"{escaped}"'


def _describe(description, synonyms):
  """Combine a description and synonyms into one metadata string.

  BigQuery graphs have no dedicated synonyms slot, so synonyms are folded into
  the description -- the only metadata sink the graph DDL exposes.
  """
  parts = []
  if description and description.strip():
    parts.append(description.strip())
  if synonyms:
    parts.append("Synonyms: " + ", ".join(synonyms))
  return "\n\n".join(parts) if parts else None


def _options_clause(description, synonyms):
  text = _describe(description, synonyms)
  return f"OPTIONS(description={_quote(text)})" if text else None
