from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import from_json

conf = SparkConf().setAppName('CSGO')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

df = spark.read.option("header", True).csv("../Datasets/LOL/match.csv")

schema = spark.read.json(df.rdd.map(lambda row: row.participants)).schema
dfCSVJSON = df.select("id", from_json("participants", schema).alias("jsonData")).select("id","jsonData.*")
dfCSVJSON.show()
