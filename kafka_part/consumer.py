import json

from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'
MESSAGE_TOPIC = 'object_mean_seen_ms_topic'

def consume():
    consumer = KafkaConsumer(MESSAGE_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVER, auto_offset_reset='earliest', group_id="group1")

    for message in consumer:
        # print("Message:", json.loads(message.value.decode('utf-8').replace("'", '"')))
        print(message.value)
if __name__ == '__main__':
    consume()