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

"""Tests for the Apache Ossie -> BigQuery property-graph exporter."""

import re
import warnings

from _util import load_fixture
from ossie_bigquery import ConversionError
from ossie_bigquery import converter as exporter
import pytest
import yaml

V = exporter.OSSIE_VERSION


# --- helpers ---------------------------------------------------------------


def _expr(sql, dialect="ANSI_SQL"):
  return {"dialects": [{"dialect": dialect, "expression": sql}]}


def _field(name, sql=None, dialect="ANSI_SQL", **extra):
  f = {
      "name": name,
      "expression": _expr(sql if sql is not None else name, dialect),
  }
  f.update(extra)
  return f


def _model(datasets, relationships=None, metrics=None, name="m", **extra):
  model = {"name": name, "datasets": datasets}
  if relationships is not None:
    model["relationships"] = relationships
  if metrics is not None:
    model["metrics"] = metrics
  model.update(extra)
  return yaml.safe_dump({"version": V, "semantic_model": [model]})


def _convert(*args, **kwargs):
  """Convert while suppressing lossy-transform warnings (tested separately)."""
  with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    return exporter.convert_ossie_to_bq_graph(*args, **kwargs)


def _warnings_for(ossie):
  with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    exporter.convert_ossie_to_bq_graph(ossie)
  return [str(w.message) for w in caught]


# --- golden file -----------------------------------------------------------


def test_tpcds_export_matches_golden():
  out = _convert(load_fixture("tpcds_ossie.yaml"))
  assert out == load_fixture("tpcds_graph.sql")


def test_tpcds_export_warns_only_on_cross_dataset_metric():
  msgs = _warnings_for(load_fixture("tpcds_ossie.yaml"))
  # The only lossy transform in the golden fixture is the cross-dataset metric.
  assert len(msgs) == 1
  assert "customer_lifetime_value" in msgs[0]
  assert "cannot be a single MEASURE" in msgs[0]


# --- top-level validation --------------------------------------------------


def test_unsupported_version_rejected():
  ossie = (
      "version: '9.9.9'\nsemantic_model:\n  - name: m\n    datasets:\n      -"
      " {name: d, source: c.s.t, primary_key: [k]}\n"
  )
  with pytest.raises(ConversionError, match="Unsupported Apache Ossie version"):
    exporter.convert_ossie_to_bq_graph(ossie)


def test_non_mapping_root_rejected():
  with pytest.raises(ConversionError, match="expected a mapping"):
    exporter.convert_ossie_to_bq_graph("- just\n- a\n- list\n")


def test_empty_semantic_model_rejected():
  with pytest.raises(ConversionError, match="non-empty list"):
    exporter.convert_ossie_to_bq_graph(f"version: {V}\nsemantic_model: []\n")


def test_invalid_yaml_raises_conversion_error():
  with pytest.raises(ConversionError, match="Invalid YAML"):
    exporter.convert_ossie_to_bq_graph("semantic_model: [oops\n")


def test_multiple_models_warns_and_uses_first():
  ossie = yaml.safe_dump({
      "version": V,
      "semantic_model": [
          {
              "name": "first",
              "datasets": [
                  {"name": "a", "source": "c.s.a", "primary_key": ["k"]}
              ],
          },
          {
              "name": "second",
              "datasets": [
                  {"name": "b", "source": "c.s.b", "primary_key": ["k"]}
              ],
          },
      ],
  })
  with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    out = exporter.convert_ossie_to_bq_graph(ossie)
  assert "PROPERTY GRAPH first" in out
  assert "second" not in out
  assert any("multiple semantic models" in str(w.message) for w in caught)


def test_missing_dataset_name_raises_clean_error():
  ossie = yaml.safe_dump({
      "version": V,
      "semantic_model": [
          {"name": "m", "datasets": [{"source": "c.s.t", "primary_key": ["k"]}]}
      ],
  })
  with pytest.raises(ConversionError, match="missing required 'name'"):
    exporter.convert_ossie_to_bq_graph(ossie)


def test_duplicate_dataset_name_rejected():
  ossie = _model([
      {"name": "d", "source": "c.s.a", "primary_key": ["k"]},
      {"name": "d", "source": "c.s.b", "primary_key": ["k"]},
  ])
  with pytest.raises(ConversionError, match="duplicate dataset name"):
    exporter.convert_ossie_to_bq_graph(ossie)


# --- node tables & keys ----------------------------------------------------


def test_composite_key_preserves_column_order():
  ossie = _model(
      [{"name": "f", "source": "c.s.f", "primary_key": ["b_sk", "a_sk"]}]
  )
  out = _convert(ossie)
  assert "KEY(b_sk, a_sk)" in out


def test_dataset_without_primary_key_is_skipped_with_warning():
  ossie = _model([
      {"name": "f", "source": "c.s.f", "primary_key": ["k"]},
      {"name": "nokey", "source": "c.s.nokey"},
  ])
  out = _convert(ossie)
  assert "AS nokey" not in out
  assert "AS f" in out
  assert any(
      "nokey" in m and "no 'primary_key'" in m for m in _warnings_for(ossie)
  )


def test_source_must_be_dotted_identifier_warns():
  ossie = _model(
      [{"name": "f", "source": "SELECT * FROM t", "primary_key": ["k"]}]
  )
  msgs = _warnings_for(ossie)
  assert any("not a plain project.dataset.table" in m for m in msgs)


def test_hyphenated_project_id_is_backtick_wrapped_without_warning():
  # GCP project IDs commonly contain hyphens (e.g. `my-proj`); such a source is
  # a valid base table and must be quoted, not rejected as a non-identifier.
  ossie = _model(
      [{"name": "f", "source": "my-proj.ds.tbl", "primary_key": ["k"]}]
  )
  out = _convert(ossie)
  assert "`my-proj.ds.tbl` AS f" in out
  assert not any(
      "not a plain project.dataset.table" in m for m in _warnings_for(ossie)
  )


# --- field properties ------------------------------------------------------


def test_simple_field_is_bare_column():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "fields": [_field("amount")],
  }])
  out = _convert(ossie)
  # bare column, not `amount AS amount`
  assert re.search(r"^\s+amount\s*$", out, re.MULTILINE)
  assert "amount AS amount" not in out


def test_computed_field_uses_expr_as_name():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "fields": [_field("full", "a || b")],
  }])
  out = _convert(ossie)
  assert "a || b AS full" in out


def test_field_without_usable_dialect_is_dropped_with_warning():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "fields": [
          {"name": "keep", "expression": _expr("keep")},
          {"name": "drop", "expression": _expr("d", dialect="SNOWFLAKE")},
      ],
  }])
  out = _convert(ossie)
  assert "keep" in out
  assert "AS drop" not in out
  assert any(
      "drop" in m and "no BIGQUERY or ANSI_SQL" in m
      for m in _warnings_for(ossie)
  )


def test_bigquery_dialect_preferred_over_ansi():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "fields": [
          {
              "name": "d",
              "expression": {
                  "dialects": [
                      {"dialect": "ANSI_SQL", "expression": "ansi_expr"},
                      {"dialect": "BIGQUERY", "expression": "bq_expr"},
                  ]
              },
          },
      ],
  }])
  out = _convert(ossie)
  assert "bq_expr AS d" in out
  assert "ansi_expr" not in out


# --- metric -> MEASURE placement -------------------------------------------


def _one_fact(metric_expr, name="rev"):
  return _model(
      [{"name": "orders", "source": "c.s.orders", "primary_key": ["k"]}],
      metrics=[{"name": name, "expression": _expr(metric_expr)}],
  )


def test_single_table_metric_becomes_measure():
  out = _convert(_one_fact("SUM(orders.amount)"))
  assert "MEASURE(SUM(amount)) AS rev" in out


def test_measure_column_is_auto_exposed_as_property():
  # BigQuery rejects a MEASURE that aggregates a column not exposed as a
  # property, so a measure column no field declares must be added as one.
  out = _convert(_one_fact("SUM(orders.amount)"))
  assert re.search(r"^\s+amount,\s*$", out, re.MULTILINE)  # bare property
  # ordering: the exposed column precedes the MEASURE that needs it
  assert out.index("\n        amount,") < out.index("MEASURE(SUM(amount))")


def test_measure_reusing_a_declared_field_adds_no_duplicate_property():
  ossie = _model(
      [{
          "name": "orders",
          "source": "c.s.orders",
          "primary_key": ["k"],
          "fields": [_field("amount")],
      }],
      metrics=[{"name": "rev", "expression": _expr("SUM(orders.amount)")}],
  )
  out = _convert(ossie)
  # `amount` is declared once; the measure reuses it, no second property line
  assert out.count("\n        amount,") == 1


def test_measure_over_multiple_columns_exposes_each():
  out = _convert(_one_fact("SUM(orders.qty * orders.price)"))
  assert re.search(r"^\s+qty,\s*$", out, re.MULTILINE)
  assert re.search(r"^\s+price,\s*$", out, re.MULTILINE)


def test_measure_strips_only_the_owning_qualifier():
  out = _convert(_one_fact("SUM(orders.amount)"))
  assert "orders.amount" not in out  # qualifier stripped


def test_lookalike_qualifier_not_stripped():
  """A table whose name merely ends with the owning dataset name is left intact."""
  ossie = _model(
      [{"name": "orders", "source": "c.s.orders", "primary_key": ["k"]}],
      metrics=[
          {"name": "rev", "expression": _expr("SUM(store_orders.amount)")}
      ],
  )
  # store_orders is not a known dataset -> metric references no known dataset -> skipped
  assert "MEASURE" not in _convert(ossie)


def test_cross_dataset_metric_is_skipped_with_warning():
  ossie = _model(
      [
          {"name": "orders", "source": "c.s.orders", "primary_key": ["k"]},
          {"name": "customer", "source": "c.s.customer", "primary_key": ["c"]},
      ],
      relationships=[{
          "name": "r",
          "from": "orders",
          "to": "customer",
          "from_columns": ["cid"],
          "to_columns": ["c"],
      }],
      metrics=[{
          "name": "ratio",
          "expression": _expr(
              "SUM(orders.amount) / COUNT(DISTINCT customer.c)"
          ),
      }],
  )
  assert "MEASURE" not in _convert(ossie)
  assert any("spans multiple tables" in m for m in _warnings_for(ossie))


def test_metric_referencing_no_dataset_is_skipped_with_warning():
  ossie = _model(
      [{"name": "orders", "source": "c.s.orders", "primary_key": ["k"]}],
      metrics=[{"name": "c", "expression": _expr("COUNT(*)")}],
  )
  assert "MEASURE" not in _convert(ossie)
  assert any("references no known dataset" in m for m in _warnings_for(ossie))


def test_unsupported_aggregate_single_table_emitted_with_warning():
  out = _convert(_one_fact("MEDIAN(orders.amount)"))
  assert "MEASURE(MEDIAN(amount)) AS rev" in out
  assert any(
      "does not begin with a supported aggregate" in m
      for m in _warnings_for(_one_fact("MEDIAN(orders.amount)"))
  )


def test_metric_without_usable_dialect_is_skipped_with_warning():
  ossie = _model(
      [{"name": "orders", "source": "c.s.orders", "primary_key": ["k"]}],
      metrics=[{
          "name": "m",
          "expression": _expr("SUM(orders.x)", dialect="SNOWFLAKE"),
      }],
  )
  assert "MEASURE" not in _convert(ossie)
  assert any("no BIGQUERY or ANSI_SQL" in m for m in _warnings_for(ossie))


# --- edge tables -----------------------------------------------------------


def _two_ds(rel_extra=None):
  rel = {
      "name": "o_to_c",
      "from": "orders",
      "to": "customer",
      "from_columns": ["cust_id"],
      "to_columns": ["c_id"],
  }
  if rel_extra:
    rel.update(rel_extra)
  return _model(
      [
          {
              "name": "orders",
              "source": "c.s.orders",
              "primary_key": ["order_id"],
          },
          {
              "name": "customer",
              "source": "c.s.customer",
              "primary_key": ["c_id"],
          },
      ],
      relationships=[rel],
  )


def test_edge_source_and_destination_keys():
  out = _convert(_two_ds())
  assert "`c.s.orders` AS o_to_c" in out
  assert "SOURCE KEY (order_id) REFERENCES orders (order_id)" in out
  assert "DESTINATION KEY (cust_id) REFERENCES customer (c_id)" in out


def test_edge_key_is_from_primary_key():
  out = _convert(_two_ds())
  # edge KEY reuses the from-side PK
  assert re.search(r"AS o_to_c\n\s+KEY\(order_id\)", out)


def test_composite_edge_columns_preserve_order():
  ossie = _model(
      [
          {
              "name": "orders",
              "source": "c.s.orders",
              "primary_key": ["o1", "o2"],
          },
          {
              "name": "customer",
              "source": "c.s.customer",
              "primary_key": ["c1", "c2"],
          },
      ],
      relationships=[{
          "name": "r",
          "from": "orders",
          "to": "customer",
          "from_columns": ["fa", "fb"],
          "to_columns": ["c1", "c2"],
      }],
  )
  out = _convert(ossie)
  assert "SOURCE KEY (o1, o2) REFERENCES orders (o1, o2)" in out
  assert "DESTINATION KEY (fa, fb) REFERENCES customer (c1, c2)" in out


def test_relationship_to_unknown_dataset_raises():
  ossie = _model(
      [{"name": "orders", "source": "c.s.orders", "primary_key": ["k"]}],
      relationships=[{
          "name": "r",
          "from": "orders",
          "to": "ghost",
          "from_columns": ["x"],
          "to_columns": ["y"],
      }],
  )
  with pytest.raises(ConversionError, match="unknown dataset"):
    exporter.convert_ossie_to_bq_graph(ossie)


def test_edge_to_keyless_dataset_is_dropped():
  ossie = _model(
      [
          {"name": "orders", "source": "c.s.orders", "primary_key": ["k"]},
          {
              "name": "customer",
              "source": "c.s.customer",
          },  # no PK -> skipped node
      ],
      relationships=[{
          "name": "r",
          "from": "orders",
          "to": "customer",
          "from_columns": ["cid"],
          "to_columns": ["c"],
      }],
  )
  out = _convert(ossie)
  assert "EDGE TABLES" not in out
  assert any("edge omitted" in m for m in _warnings_for(ossie))


def test_missing_join_columns_raise():
  ossie = _model(
      [
          {"name": "orders", "source": "c.s.orders", "primary_key": ["k"]},
          {"name": "customer", "source": "c.s.customer", "primary_key": ["c"]},
      ],
      relationships=[
          {"name": "r", "from": "orders", "to": "customer", "to_columns": ["c"]}
      ],  # from_columns missing
  )
  with pytest.raises(ConversionError, match="from_columns"):
    exporter.convert_ossie_to_bq_graph(ossie)


# --- root-node validation --------------------------------------------------


def test_single_root_emits_no_warning():
  msgs = _warnings_for(_two_ds())
  assert not any("root node table" in m for m in msgs)


def test_multiple_roots_warns():
  # Two facts, no edges -> two roots.
  ossie = _model([
      {"name": "a", "source": "c.s.a", "primary_key": ["k"]},
      {"name": "b", "source": "c.s.b", "primary_key": ["k"]},
  ])
  assert any("multiple root node tables" in m for m in _warnings_for(ossie))


def test_no_root_warns_on_cycle():
  # a -> b and b -> a : every node is an edge destination, so no root.
  ossie = _model(
      [
          {"name": "a", "source": "c.s.a", "primary_key": ["ak"]},
          {"name": "b", "source": "c.s.b", "primary_key": ["bk"]},
      ],
      relationships=[
          {
              "name": "r1",
              "from": "a",
              "to": "b",
              "from_columns": ["x"],
              "to_columns": ["bk"],
          },
          {
              "name": "r2",
              "from": "b",
              "to": "a",
              "from_columns": ["y"],
              "to_columns": ["ak"],
          },
      ],
  )
  assert any("no root node table" in m for m in _warnings_for(ossie))


# --- options / description folding -----------------------------------------


def test_synonyms_are_folded_into_description():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "description": "A fact",
      "ai_context": {"synonyms": ["events", "log"]},
  }])
  out = _convert(ossie)
  assert 'OPTIONS(description="A fact\\n\\nSynonyms: events, log")' in out


def test_string_ai_context_folded_into_description():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "ai_context": "free text note",
  }])
  out = _convert(ossie)
  assert 'OPTIONS(description="free text note")' in out


def test_description_quoting_escapes_specials():
  ossie = _model([{
      "name": "f",
      "source": "c.s.f",
      "primary_key": ["k"],
      "description": 'has "quote" and\ttab',
  }])
  out = _convert(ossie)
  assert r"has \"quote\" and\ttab" in out


# --- consumption shape (GRAPH_EXPAND + AGG) --------------------------------


def test_emitted_measures_are_consumable_via_graph_expand():
  """A MEASURE(...) cannot be selected directly in GQL; it is read through

  GRAPH_EXPAND with AGG(...). This asserts the emitted measure names compose
  into
  the documented consumption query shape (test-only; not a shipped artifact).
  """
  out = _convert(load_fixture("tpcds_ossie.yaml"))
  measures = re.findall(r"MEASURE\(.+?\) AS (\w+)", out)
  assert measures == ["total_sales", "total_profit"]
  graph = re.search(r"PROPERTY GRAPH (\w+)", out).group(1)
  query = (
      "SELECT " + ", ".join(f"AGG({m})" for m in measures) + "\n"
      f'FROM GRAPH_EXPAND("{graph}");'
  )
  assert 'FROM GRAPH_EXPAND("tpcds_retail_model")' in query
  assert "AGG(total_sales)" in query and "AGG(total_profit)" in query
