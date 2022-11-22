from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession

conf = SparkConf().setAppName('LoL')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

# Lee el csv eliminando las columnas que no interesan
priceDF = spark.read.option("header", "true").csv("lol_matches.csv").drop('gameCreation', 'gameDuration', 'gameId', 'gameType', 'gameVersion', 'mapId', 'participantIdentities', 
                                        'platformId', 'queueId', 'seasonId', 'status.message', 'status.status_code')

priceDF.rdd.saveAsTextFile("output")

priceDF.show()