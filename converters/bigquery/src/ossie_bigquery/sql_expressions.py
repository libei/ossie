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

"""Dataset-qualifier analysis for the SQL expressions in a semantic model.

Apache Ossie fields and metrics reference columns as `<dataset>.<column>`.
Turning a metric into a MEASURE needs three facts about its expression: which
datasets it references (a MEASURE binds to exactly one table), the same
expression rewritten table-local (with the owning `<dataset>.` qualifier
dropped), and the columns it aggregates (which must be exposed as graph
properties).

Each fact is read from the parsed expression's column nodes via sqlglot, not by
scanning text -- so string literals, function names, keywords, and type names
are handled by the grammar rather than by special cases.
"""

import sqlglot
import sqlglot.expressions as exp

# Expressions are BigQuery SQL by the time they are emitted, so parse and
# regenerate in that dialect for faithful rendering.
_DIALECT = "bigquery"


def _parse(expression):
  """Parse a SQL expression into a sqlglot tree, or None if it will not parse.

  An unparseable expression is treated as referencing nothing and is left
  unchanged, matching how the converter warns-and-skips other lossy input.
  """
  try:
    return sqlglot.parse_one(expression, dialect=_DIALECT)
  except sqlglot.errors.SqlglotError:
    return None


def referenced_datasets(expression, dataset_names):
  """Return the datasets whose columns `expression` references.

  Only names in `dataset_names` are returned, de-duplicated and in the order
  they are encountered. A measure binds to one table, so a metric referencing
  zero or several datasets cannot become a single MEASURE.
  """
  tree = _parse(expression)
  if tree is None:
    return []
  allowed = set(dataset_names)
  found = []
  for column in tree.find_all(exp.Column):
    table = column.table
    if table in allowed and table not in found:
      found.append(table)
  return found


def strip_qualifier(expression, dataset):
  """Return `expression` with `<dataset>.` column qualifiers removed.

  Rewrites e.g. `SUM(orders.amount)` to `SUM(amount)` so the expression is
  local to its owning node table. Columns qualified by any other name are left
  intact; an unparseable expression is returned unchanged.
  """
  tree = _parse(expression)
  if tree is None:
    return expression
  for column in tree.find_all(exp.Column):
    if column.table == dataset:
      column.set("table", None)
  return tree.sql(dialect=_DIALECT)


def referenced_columns(expression):
  """Return the bare column names `expression` references, in order.

  Used to expose the columns a MEASURE aggregates as graph properties. Because
  the names come from the parsed tree's column nodes, keywords, function names,
  and type names are excluded structurally rather than by a maintained word
  list.
  """
  tree = _parse(expression)
  if tree is None:
    return []
  names = []
  for column in tree.find_all(exp.Column):
    if column.name not in names:
      names.append(column.name)
  return names
