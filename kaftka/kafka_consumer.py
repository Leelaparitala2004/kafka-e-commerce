from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'ecommerce-orders',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='order-consumer-group'
)

print("Listening for messages...")
for message in consumer:
    order = message.value
    print(f"Received: {order}")
