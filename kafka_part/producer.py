import random
import time
import json

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'
MESSAGE_TOPIC = 'input_topic'

FEEDBACK_ID2FEEDBACK_MAP = {1: "like", 2: "seen"}

def producer_job(producer):
    print("Producing messages")
    for i in range(100000):
        user_id = random.randint(1, 10)
        object_id = random.randint(1, 10)
        feedback_id = random.randint(1, 2)
        feedback = FEEDBACK_ID2FEEDBACK_MAP[feedback_id]
        timespent_ms = random.randint(1, int(10e6)) if feedback == "seen" else 0
        message = {
            "user_id": user_id, "object_id": object_id, "timestamp": i,
            "feedback": feedback, "timespent_ms": timespent_ms
        }
        producer.send(MESSAGE_TOPIC, value=message)
        print("Message sent:", message)
        time.sleep(0.1)
    producer.close()
    print("producer closed:")



if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, value_serializer=lambda x: json.dumps(x).encode('utf-8'), acks=1)
    producer_job(producer)