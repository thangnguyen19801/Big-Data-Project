from kafka import KafkaConsumer
import json

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
        'tiki',
        bootstrap_servers='localhost:9092',
        max_poll_records = 100,
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        key_deserializer=lambda m: json.loads(m.decode('ascii')),
        auto_offset_reset='earliest'#,'smallest'
    )

for message in consumer:
    print(message.key)
    print(message.value)