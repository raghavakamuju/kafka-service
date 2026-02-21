from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['orders'])
print('Consumer started. Waiting for messages on topic "orders"... (Ctrl+C to exit)')

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print('Error: {}'.format(msg.error()))
        continue
    if msg.topic() is not None:
        print('Message received from topic: {}'.format(msg.topic()))
        print('Message key: {}'.format(msg.key()))
        print('Message value: {}'.format(msg.value().decode('utf-8')))
        print('Message partition: {}'.format(msg.partition()))
        print('Message offset: {}'.format(msg.offset()))
        print('Message timestamp: {}'.format(msg.timestamp()))
        print('Message headers: {}'.format(msg.headers()))
        print('Message timestamp type: {}'.format(msg.timestamp_type()))    
        