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

"""BigQuery property-graph DDL -> Apache Ossie semantic model.

Deferred. This converter ships export-only for now (like the snowflake and
polaris spokes): parsing `CREATE PROPERTY GRAPH` DDL back into an Apache Ossie
model is a separate, larger effort. The entry point is defined so the CLI shape
and package layout are stable for a later PR.
"""

from ._common import ConversionError


def convert_bq_graph_to_ossie(ddl_str, model_name=None):
  """Convert BigQuery property-graph DDL to an Apache Ossie semantic model.

  Not yet implemented -- raises ConversionError.
  """
  raise ConversionError(
      "BigQuery -> Apache Ossie import is not yet implemented; "
      "this converter currently supports export only"
  )
