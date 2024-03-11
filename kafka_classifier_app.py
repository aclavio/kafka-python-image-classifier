#!/usr/bin/env python

import io
import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, OFFSET_BEGINNING
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from imagery import Imagery, imagery_schema_str, dict_to_imagery, imagery_to_dict
from classifier import Classifer

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--in', dest='input_topic', required=True)
    parser.add_argument('--out', dest='output_topic', required=True)
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    sr_config = dict(config_parser['schema_registry'])

    # Create Client instances
    consumer = Consumer(config)
    producer = Producer(config)
    sr_client = SchemaRegistryClient(sr_config)

    # Create serdes
    string_serializer = StringSerializer()
    json_serializer = JSONSerializer(schema_str=imagery_schema_str, schema_registry_client=sr_client, to_dict=imagery_to_dict)
    json_deserializer = JSONDeserializer(imagery_schema_str, from_dict=dict_to_imagery)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Callback to handle producer acks
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')[0:40]))

    # Initialize Classifier
    classifier = Classifer()

    # Subscribe to topic
    out_topic = args.output_topic
    in_topic = args.input_topic
    consumer.subscribe([in_topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("Consumed event from topic {topic}[{offset}]: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), offset=msg.offset(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')[0:40]))
                
                imagery = json_deserializer(msg.value(), SerializationContext(msg.topic, MessageField.VALUE))
                print("Got Imagery: {}".format(imagery.title))

                # Classify the image
                predictions = classifier.predict(imagery.image_base64)
                print("Got predictions: {}".format(predictions))
                for p in predictions:
                    imagery.add_classification(p[1], p[2])
                    print("prediction: {name}={weight}".format(name=p[1], weight=p[2]))
                
                # Publish the Classified image
                producer.produce(topic=out_topic,
                         key=string_serializer(imagery.title),
                         value=json_serializer(imagery, SerializationContext(out_topic, MessageField.VALUE)),
                         callback=delivery_callback)
                producer.poll(1)

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()