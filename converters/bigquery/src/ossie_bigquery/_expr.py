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

"""Literal-aware helpers for the dataset-qualified SQL expressions in a model.

Apache Ossie metrics reference columns as `<dataset>.<column>`. Detecting and
stripping those qualifiers must ignore text inside string literals, so a value
such as 'orders.note' is treated as data, not as a reference to the `orders`
dataset. A BigQuery graph measure binds to exactly one table, so which dataset a
metric expression references decides where the MEASURE lands -- getting this
right matters.
"""

import re

# Matches a single- or double-quoted SQL string literal, honoring backslash
# escapes. (Triple-quoted / raw literals are uncommon in these expressions and
# are treated as ordinary text.)
_STRING_LITERAL = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"")


def _blank_string_literals(expression):
  """Replace string-literal contents with blanks of equal length, so scanning sees

  literal-free text without shifting any offsets.
  """
  return _STRING_LITERAL.sub(lambda m: " " * len(m.group(0)), expression)


def _map_outside_string_literals(expression, fn):
  """Apply `fn` only to the parts of `expression` outside string literals, leaving

  each literal verbatim.
  """
  out = []
  last = 0
  for m in _STRING_LITERAL.finditer(expression):
    out.append(fn(expression[last : m.start()]))
    out.append(m.group(0))
    last = m.end()
  out.append(fn(expression[last:]))
  return "".join(out)


def _entity_qualifier(name, flags=0):
  """Regex matching a `<name>.` qualifier, including the BigQuery backtick-quoted

  form (`` `name`. ``). A negative lookbehind keeps the name from matching
  inside a
  larger identifier (e.g. `customer_orders.` when `name` is `orders`), and the
  optional backticks let it match whether or not the identifier is quoted.
  """
  return re.compile(r"(?<![\w`])`?" + re.escape(name) + r"`?\.", flags)


def referenced_datasets(expression, dataset_names):
  """Return the dataset names whose `<name>.` qualifier appears in an expression, in

  the order they first appear, ignoring text inside string literals.
  """
  scannable = _blank_string_literals(expression)
  hits = []
  for name in dataset_names:
    m = _entity_qualifier(name).search(scannable)
    if m:
      hits.append((m.start(), name))
  hits.sort(key=lambda h: h[0])
  return [name for _, name in hits]


def strip_qualifier(expression, dataset):
  """Remove the `<dataset>.` qualifier (bare or backtick-quoted) so an expression

  references table-local columns, without touching text inside string literals.
  """
  pattern = _entity_qualifier(dataset)
  return _map_outside_string_literals(
      expression, lambda seg: pattern.sub("", seg)
  )


# Identifiers that can appear in an aggregate expression without being column
# references: SQL keywords and scalar type names. Used to tell a real column
# apart from syntax when deciding which columns a MEASURE needs exposed as
# graph properties.
_NON_COLUMN_WORDS = frozenset({
    "DISTINCT",
    "ALL",
    "AS",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "IN",
    "IS",
    "LIKE",
    "BETWEEN",
    "CAST",
    "SAFE_CAST",
    "INTERVAL",
    "OVER",
    # scalar type names (e.g. inside CAST(x AS INT64))
    "INT64",
    "FLOAT64",
    "NUMERIC",
    "BIGNUMERIC",
    "STRING",
    "BOOL",
    "BOOLEAN",
    "BYTES",
    "DATE",
    "DATETIME",
    "TIME",
    "TIMESTAMP",
    "GEOGRAPHY",
    "JSON",
    "ARRAY",
    "STRUCT",
})

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def referenced_columns(expression):
  """Return the column identifiers referenced in a (table-local) SQL expression,

  in first-appearance order. Ignores text inside string literals, function-name
  identifiers (one immediately followed by `(`), and SQL keywords / type names.
  A heuristic, not a full parser -- enough to know which columns a MEASURE needs
  exposed as graph properties.
  """
  scannable = _blank_string_literals(expression)
  out = []
  for m in _IDENT_RE.finditer(scannable):
    name = m.group(0)
    if scannable[m.end() :].lstrip().startswith("("):
      continue  # function call, not a column reference
    if name.upper() in _NON_COLUMN_WORDS:
      continue
    if name not in out:
      out.append(name)
  return out
