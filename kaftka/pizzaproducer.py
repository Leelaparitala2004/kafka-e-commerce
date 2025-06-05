from faker import Faker
from pizzaproducer import PizzaProvider
import json
from kafka import KafkaProducer

# Setup
fake = Faker()
fake.add_provider(PizzaProvider)

folder_name = "./"

producer = KafkaProducer(
    bootstrap_servers="kafka-1511b031-leelanandanparitala33-8240.j.aivencloud.com:20584",
    security_protocol="SSL",
    ssl_cafile=folder_name + "ca.pem",
    ssl_certfile=folder_name + "service.cert",
    ssl_keyfile=folder_name + "service.key",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce 10 messages
for i in range(10):
    message = {
        "order_id": i + 1,
        "customer_name": fake.name(),
        "address": fake.address(),
        "phone": fake.phone_number(),
        "pizza": fake.pizza_name()
    }

    producer.send(
        "test-topic",
        key={"order": i + 1},
        value=message
    )
    print(f"Sent: {message}")

producer.flush()
