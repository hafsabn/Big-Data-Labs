from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:29092'])

# Send JSON messages with a "gt" field
messages = [
    '{"gt": "A"}',
    '{"gt": "B"}',
    '{"gt": "A"}',
    '{"gt": "C"}'
]

for msg in messages:
    producer.send('topic-example', msg.encode('utf-8'))
    print(f"Sent: {msg}")

producer.flush()