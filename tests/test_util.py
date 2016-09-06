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

import sys

if sys.version_info[0] < 3:
    import data_gen
    import unittest
else:
    from tests import data_gen
    import unittest2 as unittest
from avro import schema

from confluent_kafka.avro.serializer import Util


class TestUtil(unittest.TestCase):
    def test_schema_from_string(self):
        parsed = Util.parse_schema_from_string(data_gen.BASIC_SCHEMA)
        self.assertTrue(isinstance(parsed, schema.Schema))

    def test_schema_from_file(self):
        parsed = Util.parse_schema_from_file(data_gen.get_schema_path('adv_schema.avsc'))
        self.assertTrue(isinstance(parsed, schema.Schema))


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestUtil)
