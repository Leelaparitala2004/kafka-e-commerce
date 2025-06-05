import json
from kafka import KafkaProducer

# Define folder containing SSL certificates
folder_name = "./"  # Update if your certs are in another path

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka-1511b031-leelanandanparitala33-8240.j.aivencloud.com:20584",
    security_protocol="SSL",
    ssl_cafile=folder_name + "ca.pem",
    ssl_certfile=folder_name + "service.cert",
    ssl_keyfile=folder_name + "service.key",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)
