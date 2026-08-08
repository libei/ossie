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

"""Shared helpers for the Apache Ossie -> BigQuery property-graph converter.

Export is a pure offline transform: an Apache Ossie semantic model (YAML) in, a
single `CREATE OR REPLACE PROPERTY GRAPH` statement (SQL text) out. The only
cross-cutting concerns live here: version + dialect constants, YAML loading, and
Apache Ossie expression/ai_context accessors.
"""

import re

import yaml

# Apache Ossie semantic model spec version this converter targets (see core-spec).
#
# NOTE: this is an exact-match check (see convert_ossie_to_bq_graph). Like the
# databricks spoke, this converter intentionally has no `apache-ossie` package
# dependency, so nothing updates this automatically -- it MUST be bumped in
# lockstep with the `version` in `core-spec/` whenever the spec version moves.
OSSIE_VERSION = "0.2.0.dev0"

# Vendor id used for dialect selection.
VENDOR = "BIGQUERY"

# Expression dialects this converter understands, in preference order.
DIALECT_BIGQUERY = "BIGQUERY"
DIALECT_ANSI = "ANSI_SQL"

# Aggregate functions BigQuery accepts inside MEASURE(...). See
# https://docs.cloud.google.com/bigquery/docs/graph-measures
SUPPORTED_AGGREGATES = ("SUM", "AVG", "COUNT", "MIN", "MAX")

# A bare SQL identifier (single column reference), e.g. `c_name`.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConversionError(Exception):
  """Raised when an input cannot be converted."""


def require(obj, key, what):
  """Return `obj[key]`, or raise a clean ConversionError if it's missing/empty -- so

  malformed input surfaces as an error message rather than a raw KeyError
  traceback.

  Presence is tested by key (not truthiness), so a legitimately falsy value such
  as
  `0` or `False` is returned; a missing key, a null, or an empty/whitespace
  string is
  rejected.
  """
  if not isinstance(obj, dict) or key not in obj or obj[key] is None:
    raise ConversionError(f"{what} is missing required '{key}'")
  value = obj[key]
  if isinstance(value, str) and not value.strip():
    raise ConversionError(f"{what} has an empty '{key}'")
  return value


def require_str(obj, key, what):
  """Like require(), but also enforce the value is a string -- so a non-string scalar

  (e.g. a YAML number for a name or expression) raises a clean ConversionError
  instead
  of crashing later in a string operation.
  """
  value = require(obj, key, what)
  if not isinstance(value, str):
    raise ConversionError(
        f"{what}: '{key}' must be a string, got {type(value).__name__}"
    )
  return value


def load_yaml(text):
  """Parse YAML, surfacing a syntax error as a ConversionError so callers (and the

  CLI) get a clean message rather than a raw traceback.

  Plain SafeLoader is fine here: unlike the Metric View spoke, the output is SQL
  text (not YAML), so there is no `on:`-key round-trip hazard to guard against.
  """
  try:
    return yaml.safe_load(text)
  except yaml.YAMLError as e:
    raise ConversionError(f"Invalid YAML: {e}") from e


def is_simple_identifier(expr):
  """True if `expr` is a single bare column reference (no operators/functions).

  A non-string input is simply not an identifier (returns False) rather than
  raising.
  """
  return isinstance(expr, str) and bool(_IDENTIFIER_RE.match(expr.strip()))


def pick_expression(ossie_expression, what):
  """Choose the SQL string for an Apache Ossie expression: BIGQUERY, else ANSI_SQL.

  Returns None if neither dialect is present (the caller warns and skips). Does
  not warn about other dialects here -- only the absence of a usable one
  matters.
  """
  dialects = {
      d.get("dialect"): d.get("expression")
      for d in (ossie_expression or {}).get("dialects") or []
  }
  expr = dialects.get(DIALECT_BIGQUERY) or dialects.get(DIALECT_ANSI)
  if expr is not None and not isinstance(expr, str):
    raise ConversionError(
        f"{what}: expression must be a string, got {type(expr).__name__}"
    )
  return expr


def synonyms_of(ai_context):
  """Extract the synonyms list from an Apache Ossie ai_context (object form only)."""
  if isinstance(ai_context, dict):
    return list(ai_context.get("synonyms") or [])
  return []


def description_of(obj):
  """Return a trimmed `description` string for an Apache Ossie object, or None.

  A string-form `ai_context` (the schema allows string or object) has no
  graph-DDL
  home of its own, so it is folded into the description here.
  """
  parts = []
  desc = obj.get("description")
  if isinstance(desc, str) and desc.strip():
    parts.append(desc.strip())
  ai = obj.get("ai_context")
  if isinstance(ai, str) and ai.strip():
    parts.append(ai.strip())
  return "\n".join(parts) if parts else None
