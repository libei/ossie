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

"""Shared test helpers: fixture loading."""

import pathlib

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures"


def load_fixture(name):
  with open(FIXTURES / name) as fh:
    return fh.read()


def sql_body(text):
  """Strip a leading block of `--` license/comment lines (and the blank lines

  around it) so a golden `.sql` fixture can carry the ASF header while the
  comparison sees only the emitted DDL.
  """
  lines = text.splitlines(keepends=True)
  i = 0
  while i < len(lines):
    stripped = lines[i].strip()
    if stripped == "" or stripped.startswith("--"):
      i += 1
      continue
    break
  return "".join(lines[i:])
