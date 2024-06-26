from pyspark.sql import SparkSession
from pyspark.sql import functions as f, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, LongType, TimestampType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = \
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'subscribe': 'persist_topic',
}


def spark_init() -> SparkSession:
    return (SparkSession.builder.
            master("local").
            appName("SKh-s8-3-4-2").
            config("spark.jars.packages", kafka_lib_id).
            getOrCreate())


def load_df(spark: SparkSession) -> DataFrame:
    return (spark.read.format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .option('kafka.security.protocol', 'SASL_SSL', )
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512', )
            .option('kafka.sasl.jaas.config',
                    'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";', )
            .option('subscribe', 'persist_topic')
            .load())


def transform(df: DataFrame) -> DataFrame:
    incomming_message_schema = StructType([
        StructField("subscription_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True)
    ])
    return (df.select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value"))
            .select(col("parsed_key_value.subscription_id"),
                    col("parsed_key_value.name"),
                    col("parsed_key_value.description"),
                    col("parsed_key_value.price"),
                    col("parsed_key_value.currency"),
                    col("parsed_key_value.key"),
                    col("parsed_key_value.value"),
                    col("parsed_key_value.topic"),
                    col("parsed_key_value.partition"),
                    col("parsed_key_value.offset"),
                    col("parsed_key_value.timestamp"),
                    col("parsed_key_value.timestampType"),
                    ))


spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)

df.printSchema()
df.show(truncate=False)
