#!/usr/bin/env python

import os
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from imagery import Imagery, imagery_schema_str, imagery_to_dict

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--out', dest='output_topic', required=True)
    parser.add_argument('--path', dest='path', required=True)
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    sr_config = dict(config_parser['schema_registry'])
    # print(config)

    # Create Producer instance
    producer = Producer(config)
    sr_client = SchemaRegistryClient(sr_config)
    
    # Create serializers
    string_serializer = StringSerializer()
    json_serializer = JSONSerializer(schema_str=imagery_schema_str, schema_registry_client=sr_client, to_dict=imagery_to_dict)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Create an Imagery object and publish to Kafka
    def publish_imagery(title, path):
        print("Publishing image: {}".format(path))
        imagery = Imagery(title, path)
        imagery.set_image(path)
        producer.produce(topic=topic,
                         key=string_serializer(title),
                         value=json_serializer(imagery, SerializationContext(topic, MessageField.VALUE)),
                         callback=delivery_callback)

    topic = args.output_topic
    # publish images from the passed directory
    if os.path.isfile(args.path):
        fname = os.path.basename(args.path)
        if fname.endswith('.jpg'):
            publish_imagery(fname, args.path)
    elif os.path.isdir(args.path):
        for entry in os.scandir(args.path):
            if entry.is_file() and entry.name.endswith('.jpg'):
                publish_imagery(entry.name, entry.path)
    else:
        raise Exception('unsupport path type')    

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()