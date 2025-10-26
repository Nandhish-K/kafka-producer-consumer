import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

order1 = {
    "order_id": str(uuid.uuid4()),
    "user": "lara",
    "item": "frozen yogurt",
    "quantity": 10
}
order2 = {
    "order_id": str(uuid.uuid4()),
    "user": "Nandhish",
    "item": "Biriyani",
    "quantity": 5
}


value1 = json.dumps(order1).encode("utf-8")
value2 = json.dumps(order2).encode("utf-8")

producer.produce(
    topic="orders",
    value=value1,
    callback=delivery_report
)
producer.produce(
    topic="orders",
    value=value2,
    callback=delivery_report
)

producer.flush()