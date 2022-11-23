from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

conf = SparkConf().setAppName('LoL')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

df = spark.read.option("header", "true").csv("../Datasets/LOL/match.csv").drop('gameCreation', 'gameDuration', 'gameId', 'gameType', 'gameVersion', 
                                    'mapId', 'participantIdentities', 'platformId', 'queueId', 'seasonId', 'status.message', 'status.status_code')


schema = StructType([
      StructField("participantId",IntegerType(),True),
      StructField("game",IntegerType(),True),
  ])


df = df.select(col("_c0"), from_json(col("participant"), schema))

#json_schema = spark.read.json(df.rdd.map(lambda row: row)).schema

#df = df.withColumn("participant", Functions.from_json(Functions.col("participant"), json_schema))

#df.rdd.saveAsTextFile("output")

df.printSchema()

df.show()
