#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#

"""
Basic utilities for handling avro schemas
"""
import sys

from avro import schema


def parse_schema_from_string(schema_str):
    """Parse a schema given a schema string"""
    if sys.version_info[0] < 3:
        return schema.parse(schema_str)
    else:
        return schema.Parse(schema_str)


def parse_schema_from_file(schema_path):
    """Parse a schema from a file path"""
    with open(schema_path) as f:
        return parse_schema_from_string(f.read())
