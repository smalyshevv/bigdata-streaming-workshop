import json
from typing import Dict

import redis
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from redis import Redis

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"


def write_to_redis(redis_client: Redis, key: int, value: str):
    try:
        redis_client.set(key, value)
        print(f"Data stored in Redis: {key} -> {value}")
    except Exception as e:
        print(f"Error writing to Redis: {e}")


def read_from_redis(redis_client: Redis, key: int):
    try:
        value: str = redis_client.get(key)
        return value
    except Exception as e:
        print(f"Error writing to Redis: {e}")


def get_values_from_redis_from_current_message_value(key, redis_client: Redis):
    read_value: str = read_from_redis(redis_client, key)
    if not read_value:
        msg_value = 0
        msg_count = 0
    else:
        msg = read_value.split(',')
        msg_value = int(msg[0])
        msg_count = int(msg[1])
    return msg_value, msg_count


class RedisMapperTs(MapFunction):
    def __init__(self):
        self.redis_client = None

    def map(self, value):
        if self.redis_client is None:
            self.redis_client = redis.StrictRedis(host='redis', port=6379, db=1, decode_responses=True)
        value_dict: Dict[str, int] = json.loads(value.replace("'", '"'))
        key: int = value_dict['object_id']
        msg_value, msg_count = get_values_from_redis_from_current_message_value(key, self.redis_client)
        msg_new_value = msg_value + value_dict['timespent_ms']
        value_to_store = f'{msg_new_value},{msg_count + 1}'
        write_to_redis(self.redis_client, key, value_to_store)


class RedisMapperCtr(MapFunction):
    def __init__(self):
        self.redis_client = None

    def map(self, value):
        if self.redis_client is None:
            self.redis_client = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
        try:
            value_dict = json.loads(value.replace("'", '"'))
            key = value_dict['object_id']
            read_value = read_from_redis(self.redis_client, key)
            if not read_value:
                msg_value = 0
                msg_count = 0
            else:
                msg = read_value.split(',')
                msg_value = int(msg[0])
                msg_count = int(msg[1])
            like_count = 1 if value_dict['feedback'] == 'like' else 0
            msg_new_value = msg_value + like_count
            value_to_store = f'{msg_new_value},{msg_count + 1}'
            write_to_redis(self.redis_client, key, value_to_store)
        except KeyError:
            pass


def process():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source_ctr = KafkaSource.builder().set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER).set_topics(
        "object_ctr_topic").set_group_id("object_ctr").set_starting_offsets(
        KafkaOffsetsInitializer.earliest()).set_value_only_deserializer(SimpleStringSchema()).build()

    source_ts = KafkaSource.builder().set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER).set_topics(
        "object_mean_seen_ms_topic").set_group_id("object_ts").set_starting_offsets(
        KafkaOffsetsInitializer.earliest()).set_value_only_deserializer(SimpleStringSchema()).build()

    # process

    datastream_ctr: DataStream = env.from_source(source_ctr, WatermarkStrategy.no_watermarks(), "source_ctr")
    datastream_ts: DataStream = env.from_source(source_ts, WatermarkStrategy.no_watermarks(), "source_ts")

    datastream_ctr.map(RedisMapperCtr())
    datastream_ts.map(RedisMapperTs())

    env.execute()


if __name__ == '__main__':
    process()
