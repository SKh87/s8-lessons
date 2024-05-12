from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
kafka_lib_id = \
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    return (SparkSession.builder.
            master("local").
            appName("SKh-s8-3-6-1").
            config("spark.jars.packages", kafka_lib_id).
            getOrCreate())


def load_df(spark: SparkSession) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .option('kafka.security.protocol', 'SASL_SSL', )
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512', )
            .option('kafka.sasl.jaas.config',
                    'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";', )
            .option('subscribe', 'student.topic.cohort22.pepper8')
            .load())


def transform(df: DataFrame) -> DataFrame:
    incomming_message_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ])
    df.printSchema()
    df = df.select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_value"))
    df.printSchema()
    return df.select(
        col("parsed_value.client_id").alias("client_id"),
        col("parsed_value.timestamp").alias("timestamp"),
        col("parsed_value.lat").alias("lat"),
        col("parsed_value.lon").alias("lon"),
    ).dropDuplicates(["client_id", "timestamp"]).withWatermark("timestamp", "10 minutes")



spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()

query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())
try:
    query.awaitTermination()
finally:
    query.stop()