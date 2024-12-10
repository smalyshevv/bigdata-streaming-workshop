from pyflink.common import SimpleStringSchema, Types, WatermarkStrategy
from pyflink.common.typeinfo import RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, DataStream, MapFunction, FilterFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, \
    KafkaSink
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

KAFKA_BOOTSTRAP_SERVER = "kafka:9092"

# class MyFilter(FilterFunction):
#
#     def filter(self, value):
#         value['feedback'] == 'seen'


class EqFunction(MapFunction):
    def map(self, value):
        user_id, object_id, timestamp, feedback, timespant_ms = value
        return str({"user_id": user_id, "object_id": object_id, "timestamp": timestamp, "feedback": feedback, "timespent_ms": timespant_ms})

def process():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # source
    type_info: RowTypeInfo = Types.ROW_NAMED(
        ['user_id', 'object_id', 'timestamp', 'feedback', 'timespent_ms'],
        [Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT()]
    )

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_topics("input_topic") \
        .set_group_id("group_id1") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    # sink
    string_serializer_ctr = KafkaRecordSerializationSchema \
        .builder() \
        .set_topic("object_ctr_topic") \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    sink_ctr = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_record_serializer(string_serializer_ctr) \
        .build()

    string_serializer_mean_seen_ms = KafkaRecordSerializationSchema \
        .builder() \
        .set_topic("object_mean_seen_ms_topic") \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    sink_mean_seen_ms = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_record_serializer(string_serializer_mean_seen_ms) \
        .build()

    # process

    datastream: DataStream = env.from_source(source, WatermarkStrategy.no_watermarks(), "input_source")

    datastream.map(EqFunction(), Types.STRING()).sink_to(sink_ctr)
    datastream.filter(lambda x: x['feedback'] == 'seen').map(EqFunction(), Types.STRING()).sink_to(sink_mean_seen_ms)

    env.execute()

if __name__ == '__main__':
    process()