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
                           `DESTINATION KEY ... REFERENCES`), sugar for a
                           many-to-one foreign-key edge backed by the `from`
                           table
  * each first-class `edge` -> an EDGE TABLE modeled like a dataset (its own
                           `source`, `primary_key`, `fields`, plus a
                           `source_key`/`destination_key` endpoint); this is how
                           many-to-many edges (backed by a junction table) are
                           expressed
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
other SQL dialect is transpiled to BigQuery with sqlglot. The set of
transpilable dialects is whatever sqlglot recognizes, so a SQL dialect added to
the core spec later needs no change here.

The core `apache-ossie` package owns the model schema, so parsing and
structural validation are delegated to its pydantic models rather than
re-implemented here.

See: https://docs.cloud.google.com/bigquery/docs/graph-measures
"""

import json
import re
import warnings

from ossie import OSIDocument
from ossie.models import OSIAIContextObject
from ossie.models import OSIDialect
from ossie.models import OSIField
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

# Dialects whose SQL is already valid BigQuery and so is used verbatim, in
# preference order. BigQuery is an ANSI superset, so an ANSI_SQL expression
# needs no transpilation either. Any other SQL dialect is transpiled (see
# `_sqlglot_dialect`); these two are preferred because they carry no transpile
# risk.
_VERBATIM_DIALECTS = (OSIDialect.BIGQUERY, OSIDialect.ANSI_SQL)

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
  document = _parse_document(raw)
  if not document.semantic_model:
    raise ConversionError("'semantic_model' must be a non-empty list")
  if len(document.semantic_model) > 1:
    _warn("model", "multiple semantic models found; converting only the first")
  return _render_property_graph(document.semantic_model[0])


def _load_yaml(text):
  """Parse YAML text into a Python object, or raise ConversionError.

  Wraps a YAML syntax error so callers (and the CLI) get a clean message rather
  than a raw traceback.
  """
  try:
    return yaml.safe_load(text)
  except yaml.YAMLError as e:
    raise ConversionError(f"Invalid YAML: {e}") from e


def _parse_document(raw):
  """Validate a raw model mapping and return it as an `OSIDocument`.

  Delegates to the shared `apache-ossie` pydantic models, so required-field,
  type, and dialect-enum checks are owned by the core package rather than
  re-implemented here. A schema violation is re-raised as a ConversionError
  carrying pydantic's field-level report.
  """
  try:
    return OSIDocument.model_validate(raw)
  except ValidationError as e:
    raise ConversionError(f"Invalid Apache Ossie model:\n{e}") from e


def _warn(scope, message):
  """Emit a stderr warning tagged with the element `scope` it concerns."""
  warnings.warn(f"[{scope}] {message}", stacklevel=2)


def _render_property_graph(model):
  """Render one semantic model as a `CREATE OR REPLACE PROPERTY GRAPH` string.

  Builds the NODE TABLES clause (one entry per dataset with a primary key), the
  EDGE TABLES clause (one entry per relationship between kept datasets), and a
  graph-level OPTIONS clause, then joins them into the final statement.
  """
  if not model.datasets:
    raise ConversionError(
        f"Model '{model.name}': 'datasets' must be a non-empty list"
    )
  datasets = {ds.name: ds for ds in model.datasets}
  if len(datasets) != len(model.datasets):
    # The schema does not enforce unique dataset names, but a graph needs
    # distinct node labels; a duplicate would collide on `AS <label>`.
    dupes = _duplicate_names(ds.name for ds in model.datasets)
    raise ConversionError(
        f"Model '{model.name}': duplicate dataset name"
        f" {', '.join(repr(d) for d in dupes)}"
    )

  # A graph node table requires a non-empty KEY. A dataset with no primary_key
  # cannot form a valid node, so skip it (and any edge that references it)
  # rather than emit an invalid `KEY()`.
  skipped = set()
  node_names = []
  for ds in model.datasets:
    if ds.primary_key:
      node_names.append(ds.name)
    else:
      _warn(
          ds.name,
          "dataset has no 'primary_key'; node table skipped "
          "(a graph node requires a KEY)",
      )
      skipped.add(ds.name)
  if not node_names:
    _warn(
        "model",
        "every dataset was skipped (no primary_key); "
        "the generated graph would be empty and invalid",
    )

  # Metrics are model-level; place each on the single dataset its aggregate
  # references. Measures placed on a skipped dataset simply never render.
  measures_by_dataset = {}
  for metric in model.metrics or []:
    _register_metric_measure(metric, datasets, measures_by_dataset)

  node_tables = [
      _render_node_table(datasets[name], measures_by_dataset.get(name, []))
      for name in node_names
  ]

  # Edges come from two sources, normalized to one internal shape and rendered
  # by one path: the core-spec `relationships` (sugar for a many-to-one FK edge
  # backed by the `from` table) and first-class `edges` (an edge modeled like a
  # dataset; see `_first_class_edges`). Relationships render first, then edges.
  edges = []
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
    edges.append(_normalize_relationship_edge(rel, datasets))
  for raw in _first_class_edges(model):
    edge = _normalize_first_class_edge(raw, model, datasets, skipped)
    if edge is not None:
      edges.append(edge)

  edge_tables = [_render_edge(edge) for edge in edges]

  # The single-root check is a GRAPH_EXPAND concern: that flattening walks a
  # foreign-key hierarchy and needs one root. A many-to-many edge (backed by a
  # junction table, not by either endpoint) is ignored by GRAPH_EXPAND and
  # references both endpoints, so root-ness no longer maps to a real requirement
  # -- skip the check rather than warn about a valid many-to-many graph.
  fk_edges = [
      (edge["src_node"], edge["dst_node"])
      for edge in edges
      if not edge["many_to_many"]
  ]
  if not any(edge["many_to_many"] for edge in edges):
    _warn_unless_single_root(node_names, fk_edges)

  blocks = [
      f"CREATE OR REPLACE PROPERTY GRAPH {_qualify_graph_name(model.name)}",
      _render_tables_clause("NODE TABLES", node_tables),
  ]
  if edge_tables:
    blocks.append(_render_tables_clause("EDGE TABLES", edge_tables))
  graph_opts = _render_options_clause(
      _element_description(model),
      _clean_synonyms(_element_synonyms(model), None, model.name),
  )
  if graph_opts:
    blocks.append(_indented_line(1, graph_opts))
  return "\n".join(blocks) + ";\n"


def _duplicate_names(names):
  """Return the names that appear more than once, in first-seen order."""
  seen = set()
  dupes = []
  for name in names:
    if name in seen and name not in dupes:
      dupes.append(name)
    seen.add(name)
  return dupes


def _register_metric_measure(metric, datasets, measures_by_dataset):
  """Record a metric as a MEASURE under the single dataset it aggregates.

  Resolves the metric's SQL, finds the one dataset its columns reference, and
  appends a rendered MEASURE (plus the columns it needs exposed) to
  `measures_by_dataset[dataset]`. A metric that resolves to no SQL, or whose
  aggregate spans zero or several datasets, cannot be a single MEASURE and is
  skipped with a warning.
  """
  expr = _pick_expression(metric.expression, f"metric '{metric.name}'")
  if expr is None:
    _warn(
        metric.name,
        "metric has no BigQuery-convertible SQL expression; skipped",
    )
    return

  referenced = _find_referenced_datasets(expr, datasets.keys())
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
  body = _strip_dataset_qualifier(expr, dataset).strip()
  if not _starts_with_supported_aggregate(body):
    _warn(
        metric.name,
        f"metric expression '{body}' does not begin with a supported aggregate"
        f" ({', '.join(SUPPORTED_AGGREGATES)}); emitting anyway",
    )

  measure = f"MEASURE({body}) AS {metric.name}"
  opts = _render_options_clause(
      _element_description(metric),
      _clean_synonyms(_element_synonyms(metric), metric.name, metric.name),
  )
  measures_by_dataset.setdefault(dataset, []).append({
      "ddl": f"{measure} {opts}" if opts else measure,
      "columns": _find_referenced_columns(body),
  })


def _render_node_table(ds, measures):
  """Render one dataset as a NODE TABLE entry: `<table> AS <label>` plus its

  KEY, optional label OPTIONS, and PROPERTIES clause.

  `measures` are the rendered MEASURE entries already assigned to this dataset
  (see `_register_metric_measure`); each is added to the PROPERTIES clause.
  """
  table = _qualify_table_name(ds.source, ds.name)

  properties = []
  exposed = set()
  for field in ds.fields or []:
    rendered = _render_property_from_field(ds.name, field)
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
      _indented_line(2, f"{table} AS {ds.name}"),
      _indented_line(3, f"KEY({', '.join(ds.primary_key)})"),
  ]
  label = _render_default_label_clause(ds)
  if label:
    lines.append(_indented_line(3, label))
  if properties:
    lines.append(_render_properties_clause(properties))
  return "\n".join(lines)


def _render_property_from_field(dataset, field):
  """Render one Ossie field as an entry of a node table's PROPERTIES clause.

  A BigQuery graph property is emitted as a bare column when the field's
  expression is just that column, otherwise as `<expr> AS <name>`, with an
  optional trailing OPTIONS clause. Returns None (and warns) when the field has
  no BigQuery-convertible expression, so the caller drops it.
  """
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
  local = _strip_dataset_qualifier(expr, dataset).strip()
  prop = field.name if local == field.name else f"{local} AS {field.name}"
  opts = _render_options_clause(
      _element_description(field),
      _clean_synonyms(_element_synonyms(field), field.name, field.name),
  )
  return f"{prop} {opts}" if opts else prop


def _render_edge(edge):
  """Render one normalized edge as an EDGE TABLE entry.

  `edge` is the internal shape produced by `_normalize_relationship_edge` (a
  core-spec relationship) or `_normalize_first_class_edge` (a first-class edge);
  both render identically here. An EDGE TABLE is a NODE TABLE plus two
  endpoints:
  `<backing> AS <name>`, a `KEY`, a `SOURCE KEY (...) REFERENCES <node> (...)`,
  a
  matching `DESTINATION KEY`, an optional label, and a PROPERTIES clause built
  from the edge's fields exactly as a node's fields are.
  """
  lines = [
      _indented_line(2, f"{edge['backing']} AS {edge['name']}"),
      _indented_line(3, f"KEY({', '.join(edge['key'])})"),
      _indented_line(
          3,
          f"SOURCE KEY ({', '.join(edge['src_columns'])}) REFERENCES"
          f" {edge['src_node']} ({', '.join(edge['src_references'])})",
      ),
      _indented_line(
          3,
          f"DESTINATION KEY ({', '.join(edge['dst_columns'])}) REFERENCES"
          f" {edge['dst_node']} ({', '.join(edge['dst_references'])})",
      ),
  ]
  opts = _render_options_clause(
      edge["description"],
      _clean_synonyms(edge["synonyms"], edge["name"], edge["name"]),
  )
  if opts:
    lines.append(_indented_line(3, f"DEFAULT LABEL {opts}"))

  properties = []
  for field in edge["fields"]:
    rendered = _render_property_from_field(edge["strip_context"], field)
    if rendered is not None:
      properties.append(rendered)
  if properties:
    lines.append(_render_properties_clause(properties))
  return "\n".join(lines)


def _normalize_relationship_edge(rel, datasets):
  """Normalize a core-spec `relationship` into the internal edge shape.

  A relationship is sugar for a many-to-one foreign-key edge: it is backed by
  the `from` (many-side) table, which holds the FK columns. That table is its
  own
  source node, so its primary key is both the edge KEY and the SOURCE KEY
  (referencing itself), and the DESTINATION KEY is `from_columns` referencing
  the
  `to` node's `to_columns`. Edge properties, if any, ride in the relationship's
  Google-owned extension (see `_edge_property_fields`).
  """
  from_ds = datasets[rel.from_dataset]
  from_cols = _require_columns(rel.from_columns, rel.name, "from_columns")
  to_cols = _require_columns(rel.to_columns, rel.name, "to_columns")
  return {
      "name": rel.name,
      "backing": _qualify_table_name(from_ds.source, from_ds.name),
      "key": from_ds.primary_key,
      "src_node": rel.from_dataset,
      "src_columns": from_ds.primary_key,
      "src_references": from_ds.primary_key,
      "dst_node": rel.to,
      "dst_columns": from_cols,
      "dst_references": to_cols,
      "fields": _edge_property_fields(rel),
      "description": _element_description(rel),
      "synonyms": _element_synonyms(rel),
      # Direct-FK properties are columns of the `from` table, qualified by its
      # node name; strip that qualifier so `orders.order_date` renders as
      # `order_date`.
      "strip_context": from_ds.name,
      "many_to_many": False,
  }


# Vendor tag of the custom-extension entry that carries edge properties. This
# is BigQuery's converter, so the extension is owned by Google rather than the
# vendor-neutral COMMON namespace -- claiming COMMON for a convention that is
# not yet standardized would overclaim. Edge properties are read only from this
# vendor's extension; extensions owned by any other vendor are left untouched.
_EDGE_FIELDS_VENDOR = "GOOGLE"

# Key, inside that extension's JSON payload, of the `fields` list holding the
# edge properties. Edge properties have no home in the core spec yet, so the
# payload holds a `fields` list of the exact same shape as a dataset's `fields`
# -- deliberately the form a future spec-native `relationships[].fields` would
# take, so promoting it into the core spec later needs no change to already-
# authored models.
_EDGE_FIELDS_KEY = "fields"

# Key, inside a *model-level* Google-owned extension's JSON payload, of the
# `edges` list holding first-class edges (see `_first_class_edges`). A BigQuery
# EDGE TABLE is structurally a NODE TABLE plus two endpoints, so an edge is
# modeled exactly like a dataset -- its own `source`, `primary_key`, and
# `fields`, plus a `source_key`/`destination_key` naming where it connects. The
# core spec has no first-class edge slot yet, so the list rides in the
# extension, shaped as a future spec-native `edges:` (a sibling of `datasets:`).
_EDGE_LIST_KEY = "edges"


def _edge_property_fields(rel):
  """Return a relationship's declared edge-property fields as `OSIField`s.

  Reads the relationship's Google-owned custom-extension entry (see
  `_EDGE_FIELDS_VENDOR`) whose JSON payload carries a `fields` list (see
  `_EDGE_FIELDS_KEY`) and validates each entry with the shared `OSIField` model
  -- the same validation a node field gets -- so a malformed edge field is
  reported the same way and both share one rendering path. Extensions owned by
  other vendors are left untouched. A relationship with no such extension yields
  an empty list; a non-JSON payload on this vendor's extension is ignored with a
  warning.
  """
  fields = []
  for ext in rel.custom_extensions or []:
    if ext.vendor_name != _EDGE_FIELDS_VENDOR:
      continue
    try:
      payload = json.loads(ext.data)
    except (json.JSONDecodeError, TypeError):
      _warn(
          rel.name,
          f"custom extension {ext.vendor_name!r} is not valid JSON; ignored",
      )
      continue
    if not isinstance(payload, dict) or _EDGE_FIELDS_KEY not in payload:
      continue
    for raw in payload.get(_EDGE_FIELDS_KEY) or []:
      try:
        fields.append(OSIField.model_validate(raw))
      except ValidationError as e:
        raise ConversionError(
            f"relationship '{rel.name}': invalid edge property:\n{e}"
        ) from e
  return fields


def _first_class_edges(model):
  """Return the raw first-class edge dicts declared on a model, in order.

  Reads every model-level Google-owned custom-extension entry (see
  `_EDGE_FIELDS_VENDOR`) whose JSON payload carries an `edges` list (see
  `_EDGE_LIST_KEY`) and returns those entries unvalidated, for
  `_normalize_first_class_edge` to check. Extensions owned by other vendors, and
  payloads without an `edges` key, are ignored; a non-JSON payload on this
  vendor's extension is skipped with a warning, and a non-list `edges` raises.
  """
  edges = []
  for ext in model.custom_extensions or []:
    if ext.vendor_name != _EDGE_FIELDS_VENDOR:
      continue
    try:
      payload = json.loads(ext.data)
    except (json.JSONDecodeError, TypeError):
      _warn(
          model.name,
          f"custom extension {ext.vendor_name!r} is not valid JSON; ignored",
      )
      continue
    if not isinstance(payload, dict) or _EDGE_LIST_KEY not in payload:
      continue
    raw_edges = payload.get(_EDGE_LIST_KEY)
    if not isinstance(raw_edges, list):
      raise ConversionError(
          f"model '{model.name}': '{_EDGE_LIST_KEY}' must be a list of edges"
      )
    edges.extend(raw_edges)
  return edges


def _normalize_first_class_edge(raw, model, datasets, skipped):
  """Normalize one first-class edge dict into the internal edge shape, or None.

  A first-class edge is modeled like a dataset: its own `source` (backing
  table),
  `primary_key` (the edge KEY), and `fields` (edge properties), plus a
  `source_key`/`destination_key` endpoint. Each endpoint is `{columns, node,
  references}`: `columns` are the edge's own key columns, which REFERENCE `node`
  (`references`); `references` defaults to that node's `primary_key`. The edge
  `primary_key` defaults to the two endpoints' columns combined.

  Returns the normalized edge, or None when it references a skipped dataset (no
  valid node to attach to -- warned and dropped, as a dangling relationship is).
  Raises ConversionError on a structurally invalid edge (missing name/source,
  unknown node, malformed endpoint, or a columns/references arity mismatch).
  """
  if not isinstance(raw, dict):
    raise ConversionError(f"model '{model.name}': each edge must be a mapping")
  name = raw.get("name")
  if not (isinstance(name, str) and name.strip()):
    raise ConversionError(f"model '{model.name}': an edge is missing a 'name'")
  ident = f"edge '{name}'"
  source = raw.get("source")
  if not (isinstance(source, str) and source.strip()):
    raise ConversionError(
        f"{ident}: 'source' is required and must be a project.dataset.table"
        " string"
    )

  src = _parse_endpoint(
      raw.get("source_key"), ident, "source_key", datasets, skipped
  )
  dst = _parse_endpoint(
      raw.get("destination_key"), ident, "destination_key", datasets, skipped
  )
  dangling = [ep["node"] for ep in (src, dst) if ep["dangling"]]
  if dangling:
    _warn(
        name,
        f"references skipped dataset {', '.join(repr(n) for n in dangling)};"
        " edge omitted",
    )
    return None

  key = raw.get("primary_key")
  if key is None:
    key = _dedup(src["columns"] + dst["columns"])
  else:
    key = _require_str_list(key, ident, "primary_key")

  fields = []
  for raw_field in raw.get(_EDGE_FIELDS_KEY) or []:
    try:
      fields.append(OSIField.model_validate(raw_field))
    except ValidationError as e:
      raise ConversionError(f"{ident}: invalid edge property:\n{e}") from e

  # A junction edge is backed by its own table, distinct from either endpoint's
  # node table; GRAPH_EXPAND ignores it, so it is excluded from the single-root
  # check. A first-class edge backed by its own source node is a plain FK edge.
  many_to_many = source.strip() != datasets[src["node"]].source.strip()

  return {
      "name": name,
      "backing": _qualify_table_name(source, name),
      "key": key,
      "src_node": src["node"],
      "src_columns": src["columns"],
      "src_references": src["references"],
      "dst_node": dst["node"],
      "dst_columns": dst["columns"],
      "dst_references": dst["references"],
      "fields": fields,
      "description": _raw_description(raw),
      "synonyms": _raw_synonyms(raw),
      # A first-class edge's properties are columns of its own backing table,
      # carrying no node qualifier; pass the edge name, which will not match a
      # bare column, so nothing is stripped.
      "strip_context": name,
      "many_to_many": many_to_many,
  }


def _parse_endpoint(raw_ep, ident, side, datasets, skipped):
  """Validate one edge endpoint (`source_key`/`destination_key`) block.

  Returns `{node, columns, references, dangling}`: `columns` are the edge's own
  key columns, `node` is the dataset they REFERENCE, and `references` are that
  node's key columns -- defaulting to the node's `primary_key` when omitted.
  When
  `node` is a skipped dataset, returns early with `dangling=True` (the caller
  drops the edge) rather than validating columns. Raises ConversionError on a
  missing/malformed block, an unknown node, or a columns/references arity
  mismatch.
  """
  if not isinstance(raw_ep, dict):
    raise ConversionError(
        f"{ident}: '{side}' is required and must be a mapping with 'columns'"
        " and 'node'"
    )
  node = raw_ep.get("node")
  if not (isinstance(node, str) and node.strip()):
    raise ConversionError(f"{ident}: '{side}.node' is required")
  if node not in datasets:
    raise ConversionError(
        f"{ident}: '{side}.node' references unknown dataset {node!r}"
    )
  if node in skipped:
    return {"node": node, "columns": [], "references": [], "dangling": True}
  columns = _require_str_list(raw_ep.get("columns"), ident, f"{side}.columns")
  references = raw_ep.get("references")
  if references is None:
    # A non-skipped node always has a primary_key (that is what skipping tests).
    references = list(datasets[node].primary_key)
  else:
    references = _require_str_list(references, ident, f"{side}.references")
  if len(columns) != len(references):
    raise ConversionError(
        f"{ident}: '{side}' has {len(columns)} column(s) but"
        f" {len(references)} reference(s); they must match one to one"
    )
  return {
      "node": node,
      "columns": columns,
      "references": references,
      "dangling": False,
  }


def _raw_synonyms(raw):
  """Return the synonyms from a raw edge dict's structured `ai_context`, else

  an empty list -- the raw-dict counterpart of `_element_synonyms`.
  """
  ai = raw.get("ai_context")
  if isinstance(ai, dict):
    syn = ai.get("synonyms")
    if isinstance(syn, list):
      return [s for s in syn if isinstance(s, str)]
  return []


def _raw_description(raw):
  """Return the description text for a raw edge dict, or None.

  The raw-dict counterpart of `_element_description`: a `description` string
  plus
  a string-form `ai_context` folded in (a structured `ai_context` carries
  synonyms, handled by `_raw_synonyms`).
  """
  parts = []
  desc = raw.get("description")
  if isinstance(desc, str) and desc.strip():
    parts.append(desc.strip())
  ai = raw.get("ai_context")
  if isinstance(ai, str) and ai.strip():
    parts.append(ai.strip())
  return "\n".join(parts) if parts else None


def _dedup(items):
  """Return `items` with duplicates removed, keeping first-seen order."""
  seen = set()
  out = []
  for item in items:
    if item not in seen:
      seen.add(item)
      out.append(item)
  return out


def _require_str_list(value, ident, field_name):
  """Return `value` if it is a non-empty list of non-empty strings, else raise.

  Used for the raw JSON column lists on a first-class `edge` (its `primary_key`
  and each endpoint's `columns`/`references`), which -- unlike the core-spec
  `from_columns`/`to_columns` -- pydantic has not already validated. `ident`
  names the element (e.g. "edge 'enrolled_in'") for the error message.
  """
  if not (
      isinstance(value, list)
      and value
      and all(isinstance(v, str) and v.strip() for v in value)
  ):
    raise ConversionError(
        f"{ident}: '{field_name}' must be a non-empty list of column names"
    )
  return value


def _require_columns(cols, rel_name, field_name):
  """Return `cols`, or raise ConversionError if it is empty.

  `field_name` names the relationship field (e.g. "from_columns") for the error
  message.
  """
  if not cols:
    raise ConversionError(
        f"relationship '{rel_name}': '{field_name}' must be a non-empty list of"
        " column names"
    )
  return cols


def _warn_unless_single_root(node_names, edges):
  """Warn unless exactly one node table is a root (referenced by no edge).

  BigQuery requires exactly one root node table -- for an FK graph, the dataset
  that is never an edge destination. This is a query-time constraint, so it is
  a warning rather than an error.
  """
  if not node_names:
    return
  destinations = {to for _, to in edges}
  roots = [name for name in node_names if name not in destinations]
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


def _pick_expression(expression, context_label):
  """Return BigQuery SQL for an Ossie expression, or None if it has none.

  A BigQuery or ANSI_SQL dialect is used verbatim (BigQuery is an ANSI
  superset); any other SQL dialect is transpiled to BigQuery with sqlglot.
  Non-SQL dialects (e.g. MDX, MAQL) have no BigQuery rendering and yield None,
  leaving the caller to warn and skip. `context_label` names the element being
  converted and is used only in any transpile warning.
  """
  by_dialect = {d.dialect: d.expression for d in expression.dialects}
  for dialect in _VERBATIM_DIALECTS:
    sql = by_dialect.get(dialect)
    if sql is not None:
      return sql
  # Otherwise transpile the first dialect sqlglot recognizes, in the order the
  # model declares them. Resolving the sqlglot dialect by name (rather than a
  # hardcoded table) means a SQL dialect added to the core spec later is picked
  # up automatically, and a non-SQL dialect is simply not recognized and skipped
  # -- neither case needs a change here.
  for d in expression.dialects:
    if d.dialect in _VERBATIM_DIALECTS:
      continue
    name = _sqlglot_dialect(d.dialect)
    if name is not None:
      return _transpile_to_bigquery(d.expression, name, context_label)
  return None


def _sqlglot_dialect(dialect):
  """Return the sqlglot dialect name for an Ossie dialect, or None.

  The name is derived from the dialect value and checked against sqlglot's
  registry rather than looked up in a fixed table, so a SQL dialect newly added
  to the core spec transpiles automatically once sqlglot supports it. A dialect
  sqlglot does not know -- a non-SQL one such as MDX or MAQL -- yields None.
  """
  name = dialect.value.lower()
  try:
    sqlglot.Dialect.get_or_raise(name)
  except ValueError:
    return None
  return name


def _transpile_to_bigquery(sql, read_dialect, context_label):
  """Transpile `sql` from `read_dialect` to BigQuery.

  Rewrites dialect-specific constructs BigQuery does not share (conditional and
  null-handling functions, quoting, and the like) into their BigQuery form. If
  sqlglot cannot parse the expression, it is passed through unchanged with a
  warning rather than dropped. `context_label` names the element being converted
  and is used only in that warning.
  """
  try:
    return sqlglot.transpile(sql, read=read_dialect, write=_BIGQUERY)[0]
  except sqlglot.errors.SqlglotError:
    _warn(
        context_label,
        f"could not transpile expression to BigQuery: {sql!r}; "
        "emitting unchanged",
    )
    return sql


def _parse_sql(expression):
  """Parse a BigQuery SQL expression into a sqlglot syntax tree, or None."""
  try:
    return sqlglot.parse_one(expression, dialect=_BIGQUERY)
  except sqlglot.errors.SqlglotError:
    return None


def _find_referenced_datasets(expression, dataset_names):
  """Return the datasets whose columns `expression` references, in order.

  Only names in `dataset_names` are returned, de-duplicated. A measure binds to
  one table, so a metric referencing zero or several datasets cannot become a
  single MEASURE.
  """
  tree = _parse_sql(expression)
  if tree is None:
    return []
  allowed = set(dataset_names)
  found = []
  for column in tree.find_all(sqlglot_exp.Column):
    table = column.table
    if table in allowed and table not in found:
      found.append(table)
  return found


def _strip_dataset_qualifier(expression, dataset):
  """Return `expression` with `<dataset>.` column qualifiers removed.

  Rewrites e.g. `SUM(orders.amount)` to `SUM(amount)` so the expression is
  local to its owning node table. Columns qualified by any other name are left
  intact; an unparseable expression is returned unchanged.
  """
  tree = _parse_sql(expression)
  if tree is None:
    return expression
  for column in tree.find_all(sqlglot_exp.Column):
    if column.table == dataset:
      column.set("table", None)
  return tree.sql(dialect=_BIGQUERY)


def _find_referenced_columns(expression):
  """Return the bare column names `expression` references, in order."""
  tree = _parse_sql(expression)
  if tree is None:
    return []
  names = []
  for column in tree.find_all(sqlglot_exp.Column):
    if column.name not in names:
      names.append(column.name)
  return names


def _starts_with_supported_aggregate(body):
  """Return True if `body` begins with a call to a SUPPORTED_AGGREGATES func."""
  m = re.match(r"^([A-Za-z_]+)\s*\(", body.lstrip())
  return bool(m) and m.group(1).upper() in SUPPORTED_AGGREGATES


# --- identifiers, metadata, and layout --------------------------------------


def _qualify_graph_name(name):
  """Return the graph name, backtick-quoted only if it is not a bare

  identifier.
  """
  return name if _SIMPLE_IDENT_RE.match(name) else f"`{name}`"


def _qualify_table_name(source, context_label):
  """Backtick-quote a `project.dataset.table` reference.

  Apache Ossie sources are already fully qualified dotted identifiers, so the
  whole reference is wrapped once. Warns (tagged with `context_label`, the
  dataset name) on a source that is not a plain dotted identifier (e.g. a
  subquery), which cannot back a graph node table.
  """
  s = source.strip()
  parts = s.split(".")
  if not all(_TABLE_PART_RE.match(p.strip("`")) for p in parts):
    _warn(
        context_label,
        f"source '{source}' is not a plain project.dataset.table "
        "identifier; a graph node table requires a base table",
    )
    return s
  return "`" + ".".join(p.strip("`") for p in parts) + "`"


def _indented_line(depth, text):
  """Return `text` prefixed with `depth` levels of indentation."""
  return _INDENT * depth + text


def _render_tables_clause(keyword, entries):
  """Render a `NODE TABLES (...)` or `EDGE TABLES (...)` clause.

  `keyword` is `"NODE TABLES"` or `"EDGE TABLES"` and `entries` are the rendered
  element tables that go inside its parentheses. The keyword is indented one
  level under the CREATE statement and each element table one level under that
  -- BigQuery's canonical graph layout.
  """
  inner = ",\n".join(entries)
  return (
      f"{_indented_line(1, keyword + ' (')}\n{inner}\n{_indented_line(1, ')')}"
  )


def _render_properties_clause(properties):
  """Render a node table's `PROPERTIES(...)` clause from rendered entries."""
  body = ",\n".join(_indented_line(4, p) for p in properties)
  return f"{_indented_line(3, 'PROPERTIES(')}\n{body}\n{_indented_line(3, ')')}"


def _render_string_literal(value):
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


def _render_options_clause(description, synonyms):
  """Render an `OPTIONS(...)` clause from a description and/or synonyms.

  BigQuery Graph exposes both as first-class label and property options
  (`description = <string>`, `synonyms = <array of strings>`), so each maps to
  its own key. Returns None when there is no metadata to emit.
  """
  parts = []
  if description:
    parts.append(f"description={_render_string_literal(description)}")
  if synonyms:
    rendered = ", ".join(_render_string_literal(s) for s in synonyms)
    parts.append(f"synonyms=[{rendered}]")
  return f"OPTIONS({', '.join(parts)})" if parts else None


def _render_default_label_clause(element):
  """Return the `DEFAULT LABEL OPTIONS(...)` clause for a node or edge, or None.

  BigQuery attaches an element's description and synonyms to its default label,
  between the KEY clause and PROPERTIES. `element` is the Ossie dataset or
  relationship being rendered.
  """
  opts = _render_options_clause(
      _element_description(element),
      _clean_synonyms(
          _element_synonyms(element),
          getattr(element, "name", None),
          getattr(element, "name", None),
      ),
  )
  return f"DEFAULT LABEL {opts}" if opts else None


def _element_description(element):
  """Return the description text for an Ossie model element, or None.

  `element` is any Ossie object that may carry `description`/`ai_context` (a
  model, dataset, field, relationship, or metric). A string-form `ai_context`
  (the schema allows a string or a structured object) has no options key of its
  own, so it is folded into the description.
  """
  parts = []
  desc = getattr(element, "description", None)
  if isinstance(desc, str) and desc.strip():
    parts.append(desc.strip())
  ai = getattr(element, "ai_context", None)
  if isinstance(ai, str) and ai.strip():
    parts.append(ai.strip())
  return "\n".join(parts) if parts else None


def _element_synonyms(element):
  """Return the synonyms from an Ossie element's structured `ai_context`, else

  an empty list. `element` is any Ossie object that may carry `ai_context`.
  """
  ai = getattr(element, "ai_context", None)
  if isinstance(ai, OSIAIContextObject):
    return list(ai.synonyms or [])
  return []


def _clean_synonyms(synonyms, own_name, scope):
  """Drop synonyms BigQuery would reject as duplicates, warning on each.

  BigQuery treats a graph label or property name as an implicit synonym of
  itself, and rejects a `CREATE PROPERTY GRAPH` whose synonym list repeats that
  name or lists the same synonym twice -- both compared case-insensitively.
  Keep the first spelling of each distinct synonym and drop the rest so the
  emitted DDL is always accepted. `own_name` is the label/property name the
  synonyms hang off (its implicit synonym), or None where there is none (the
  graph itself); `scope` tags any warning.
  """
  own = own_name.casefold() if isinstance(own_name, str) else None
  kept = []
  seen = set()
  for syn in synonyms:
    key = syn.casefold()
    if key == own:
      _warn(scope, f"synonym {syn!r} duplicates the name it labels; dropped")
      continue
    if key in seen:
      _warn(scope, f"duplicate synonym {syn!r} dropped")
      continue
    seen.add(key)
    kept.append(syn)
  return kept
