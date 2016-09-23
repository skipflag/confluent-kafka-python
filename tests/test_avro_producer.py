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


class TestAvroProducer(unittest.TestCase):
    def setUp(self):
        pass

    def test_instantiation(self):
        obj = AvroProducer(None, 'http://127.0.0.1:9002')
        self.assertTrue(isinstance(obj, AvroProducer))
        self.assertNotEqual(obj, None)

    def test_Produce(self):
        producer = AvroProducer(None, 'http://127.0.0.1:9002')
        valueSchema = util.parse_schema_from_file("basic_schema.avsc")
        try:
            producer.produce(topic='test', value={"name": 'abc"'}, value_schema=valueSchema, key='mykey')
            self.fail("Should expect value_schema")
        except Exception as e:
            pass

    def test_Produce_arguments(self):
        value_schema = util.parse_schema_from_file("basic_schema.avsc")
        producer = AvroProducer(None, 'http://127.0.0.1:9002', default_value_schema=value_schema)

        try:
            producer.produce(topic='test', value={"name": 'abc"'})
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            if exc_type.__name__ == 'SerializerError':
                self.fail()

    def test_Produce_arguments(self):
        producer = AvroProducer(None, 'http://127.0.0.1:9002')
        try:
            producer.produce(topic='test', value={"name": 'abc"'}, key='mykey')
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            if exc_type.__name__ == 'SerializerError':
                pass


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestAvroProducer)
