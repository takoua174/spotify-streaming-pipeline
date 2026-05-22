import json
from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'song-metadata-consumer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['genre-signals'])

messages = []

print("Reading from 'genre-signals' topic... (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("Reached end of partition")
            else:
                print(f"Error: {msg.error()}")
            continue

        value = json.loads(msg.value().decode('utf-8'))
        messages.append(value)
        print(f"Consumed: {value}")

except KeyboardInterrupt:
    print("\nStopped.")

finally:
    consumer.close()
    with open('song-metadata.json', 'w') as f:
        json.dump(messages, f, indent=2)
    print(f"Saved {len(messages)} messages to song-metadata.json")