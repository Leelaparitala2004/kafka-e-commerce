from kafka import KafkaProducer
from faker import Faker
import json
import time

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'ecommerce-orders'

while True:
    order = {
        'order_id': fake.uuid4(),
        'customer': fake.name(),
        'product': fake.word(ext_word_list=['pizza', 'burger', 'pasta', 'sushi']),
        'quantity': fake.random_int(min=1, max=5),
        'timestamp': fake.iso8601()
    }
    producer.send(topic, order)
    print(f"Sent: {order}")
    time.sleep(1)
