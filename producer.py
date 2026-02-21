from confluent_kafka import Producer
import json
from datetime import datetime
producer = Producer({'bootstrap.servers': 'localhost:9092'})
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} partition [{}] offset [{}]'.format(msg.topic(), msg.partition(), msg.offset()))
        print(msg.value().decode('utf-8'), 'this is the value')


value={
    "order_id": 1,
    "customer_id": 1,
    "product_id": 1,
    "quantity": 1,
    "price": 100,
    "status": "pending",
    "created_at": datetime.now().isoformat(),
}
producer.produce(topic='orders', key='test', value=json.dumps(value).encode('utf-8'), callback=delivery_report)

producer.flush()
print('Message sent successfully')