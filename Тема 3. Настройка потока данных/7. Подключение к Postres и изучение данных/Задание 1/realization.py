from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("SKh-s8-3-7-1")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
         .getOrCreate()
         )

df = (spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
      # .option("url", "rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net")
      # .option("port", 6432)
      .option("user", "student")
      .option("password", "de-student")
      # .option("database", "de")
      .option("schema", "public")
      .option("dbtable", "marketing_companies")
      .option("driver", "org.postgresql.Driver")
      .load())
df.printSchema()
df.count()
