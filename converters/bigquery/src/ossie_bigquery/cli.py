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

"""Command-line interface for the Apache Ossie <-> BigQuery property-graph converter.

    ossie-bigquery export -i model.yaml [-o graph.sql]
    ossie-bigquery import -i graph.sql  [-o model.yaml] [--name my_model]

`export` converts an Apache Ossie semantic model to a BigQuery
`CREATE OR REPLACE PROPERTY GRAPH` statement (with inline `MEASURE(...)`
properties). `import` (the reverse) is not yet implemented. With no `-o`, the
result is written to stdout. Conversions that drop information emit warnings to
stderr.
"""

import argparse
import sys

from ._common import ConversionError
from .bq_graph_to_ossie import convert_bq_graph_to_ossie
from .ossie_to_bq_graph import convert_ossie_to_bq_graph


def _build_parser():
  parser = argparse.ArgumentParser(
      prog="ossie-bigquery",
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  sub = parser.add_subparsers(dest="command")
  sub.required = True  # set as attribute (the add_subparsers kwarg is 3.7+)

  exp = sub.add_parser(
      "export",
      help="Apache Ossie semantic model -> BigQuery property-graph DDL",
  )
  exp.add_argument(
      "-i", "--input", required=True, help="Apache Ossie YAML file"
  )
  exp.add_argument("-o", "--output", help="output .sql file (default: stdout)")

  imp = sub.add_parser(
      "import",
      help="BigQuery property-graph DDL -> Apache Ossie (not yet implemented)",
  )
  imp.add_argument("-i", "--input", required=True, help="BigQuery DDL file")
  imp.add_argument(
      "-o", "--output", help="output Apache Ossie YAML (default: stdout)"
  )
  imp.add_argument("--name", help="Apache Ossie model name")
  return parser


def main(argv=None):
  args = _build_parser().parse_args(argv)
  try:
    with open(args.input) as fh:
      text = fh.read()
    if args.command == "export":
      out = convert_ossie_to_bq_graph(text)
    else:
      out = convert_bq_graph_to_ossie(text, model_name=args.name)
  except (ConversionError, OSError) as e:
    print(f"Error: {e}", file=sys.stderr)
    return 1

  if args.output:
    with open(args.output, "w") as fh:
      fh.write(out)
  else:
    sys.stdout.write(out)
  return 0


if __name__ == "__main__":
  sys.exit(main())
