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


import logging

from confluent_kafka.avro.serializer import SerializerError


class AvroProducer(object):
    '''
        Kafka Producer client which does avro schema encoding to messages.
        Handles schema registration, Message serialization.

        Constructor takes below parameters

        @:param: producer: confluent_kafka.Producer object
        @:param: message_serializer: Message Serializer object
    '''
    def __init__(self, producer, message_serializer):  # real signature unknown; restored from __doc__
        self.producer = producer
        self.serializer = message_serializer
        self.log = logging.getLogger(__name__)

    def produce(self, topic, value=None, value_avro_schema=None, key=None, key_avro_schema=None, **kwargs):
        '''
            Sends message to kafka by encoding with specified avro schema
            @:param: topic: topic name
            @:param: value: A dictionary object
            @:param: value_avro_schema : Avro schema for value
            @:param: key: A dictionary object
            @:param: key_avro_schema : Avro schema for key
            @:exception: SerializerError
        '''
        if value is not None:
            if value_avro_schema is not None:
                value = self.serializer.encode_record_with_schema(topic, value_avro_schema, value)
            else:
                self.log.error("Schema required for value serialization")
                raise SerializerError("Avro schema required for value")

        if key is not None:
            if key_avro_schema is not None:
                key = self.serializer.encode_record_with_schema(topic, key_avro_schema, key, True)
            else:
                self.log.error("Schema required for key serialization")
                raise SerializerError("Avro schema required for key")

        self.producer.produce(topic, value, key, **kwargs)