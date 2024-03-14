# Kafka + Python "Image Classifier"

This application demonstrates an integration between Apache Kafka, and a Python based machine-learning image classifier.

## Getting Started

First install dependencies:

```
pip install -r requirements.txt
```

Configure your connection to Kafka using the `client.ini` file:

```
[default]
bootstrap.servers=<bootstrap-host>:<bootstrap-port>
security.protocol=<PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL | etc>
; if using SASL 
sasl.mechanisms=<PLAIN | GSSAPI | SCRAM-SHA-256>
sasl.username=<sasl username>
sasl.password=<sasl password>

[schema_registry]
url=http://<sr-host>:<sr-port>
```

Load your "source" topic with imagery:

```
python producer.py client.ini --out <topic-name> --path imagery/
```

The producer application will scan the directory specified as `--path` for JPEG files under 1MB in size.  Each file will be published to the `--out` topic as a Kafka event with the image content as base64 encoded string.  

*NOTE: embedding binary data in Kafka events is only recommended for "smallish" files.  Larger files should probably utilize an object store and pass URIs for retrieval by the classifier app.*

Run the classifier application:

```
python kafka_classifier_app.py client.ini --in <source-topic> --out <destination-topic>
```

The classifier will pull the source event data, decode the images, send them through a machine-learning based image classification model (this demo uses the ResNet50 model embedded with TensorFlow and Keras).  The model's predictions will then be added to the event message and republished to the destination Kafka topic.

Cheers! 🍻