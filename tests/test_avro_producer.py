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

from confluent_kafka.avro.serializer import util

if sys.version_info[0] < 3:
    import unittest
else:
    import unittest2 as unittest
from confluent_kafka.avro.avro_producer import AvroProducer

from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer


class TestAvroProducer(unittest.TestCase):
    def setUp(self):
        self.client = CachedSchemaRegistryClient('http://127.0.0.1:9002')
        self.ms = MessageSerializer(self.client)

    def test_instantiation(self):
        obj = AvroProducer(None, self.ms)
        self.assertTrue(isinstance(obj, AvroProducer))
        self.assertNotEqual(obj, None)

    def test_Produce(self):
        producer = AvroProducer(None, self.ms)
        valueSchema = util.parse_schema_from_file("basic_schema.avsc")
        try:
            producer.produce('test', {"name": 'abc"'}, valueSchema, 'mykey')
            self.fail("Should expect value_schema")
        except Exception as e:
            pass


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestAvroProducer)
