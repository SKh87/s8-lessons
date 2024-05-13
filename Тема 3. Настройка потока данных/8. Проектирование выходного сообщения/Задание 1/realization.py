from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType


def spark_init(test_name) -> SparkSession:
    return (SparkSession.builder
            .appName(test_name)
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0",)
            .getOrCreate())


# postgresql_settings = {
#     'user': 'master',
#     'password': 'de-master-password'
# }

postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}

def read_marketing(spark: SparkSession) -> DataFrame:
    df = (spark.read.format("jdbc")
          .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
          .option("user", "student")
          .option("password", "de-student")
          .option("schema", "public")
          .option("dbtable", "marketing_companies")
          .option("driver", "org.postgresql.Driver")
          .load())
    # df.cache()
    df.show()
    return df.select(
        col("id").alias("adv_campaign_id"),
        col("name").alias("adv_campaign_name"),
        col("description").alias("adv_campaign_description"),
        col("start_time").alias("adv_campaign_start_time"),
        col("end_time").alias("adv_campaign_end_time"),
        col("point_lat").alias("adv_campaign_point_lat"),
        col("point_lon").alias("adv_campaign_point_lon")
    )


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    df = (spark.readStream.format("kafka")
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
          .option('kafka.security.protocol', 'SASL_SSL', )
          .option('kafka.sasl.mechanism', 'SCRAM-SHA-512', )
          .option('kafka.sasl.jaas.config',
                  'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";', )
          .option('subscribe', 'student.topic.cohort22.Skh')
          .load())
    incomming_message_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ])
    df = df.select(col("timestamp").alias("created_at"), col("offset"), from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_value"))
    # df.printSchema()
    df = df.select(
        col("parsed_value.client_id").alias("client_id"),
        col("parsed_value.timestamp").alias("timestamp"),
        col("parsed_value.lat").alias("lat"),
        col("parsed_value.lon").alias("lon"),
        col("created_at").alias("created_at"),
        col("offset").alias("offset"),
    ).dropDuplicates(["client_id", "timestamp"]).withWatermark("timestamp", "10 minutes")
    return df


def join(user_df, marketing_df) -> DataFrame:
    return user_df.crossJoin(marketing_df).select(
        col("client_id"),
        col("adv_campaign_id"),
        col("adv_campaign_name"),
        col("adv_campaign_description"),
        col("adv_campaign_start_time"),
        col("adv_campaign_end_time"),
        col("adv_campaign_point_lat"),
        col("adv_campaign_point_lon"),
        col("created_at"),
        col("offset")
    )


if __name__ == "__main__":
    spark = spark_init("SKh-s8-3-8-1")
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()
