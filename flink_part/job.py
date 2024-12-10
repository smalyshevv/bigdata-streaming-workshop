from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import RowTypeInfo, Types
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'

def process():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    type_info: RowTypeInfo = Types.ROW_NAMED(
        ['user_id', 'object_id', 'timestamp', 'feedback', 'timespent_ms'],
        [Types.INT(), Types.INT(), Types.INT(), Types.STRING(), Types.INT()]
    )

    #source
    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_topics("input_topic") \
        .set_group_id("group_id1") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    string_serializer = KafkaRecordSerializationSchema\
        .builder()\
        .set_topic("output_topic")\
        .set_value_serialization_schema(SimpleStringSchema())\
        .build()

    #sink
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_record_serializer(string_serializer) \
        .build()

    #processing

    datastream: DataStream = env.from_source(source, WatermarkStrategy.no_watermarks(), "input_source1")

    datastream.map(lambda x: str(x), Types.STRING()).sink_to(sink)

    env.execute()


if __name__ == '__main__':
    process()