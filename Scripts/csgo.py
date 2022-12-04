from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, lit

conf = SparkConf().setAppName('CSGO')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

df = spark.read.option("header", True).csv("../Datasets/CSGO/csgo_games.csv")

df = df.withColumn('t1_world_rank', df['t1_world_rank'].cast("Int"))
df = df.withColumn('t2_world_rank', df['t2_world_rank'].cast("Int"))
df = df.withColumn('winner', df['winner'].cast("String"))


winnerDF = df.select('winner', 't1_world_rank', 't2_world_rank')


winnerDF = winnerDF.withColumn("higher_rank_is_winner", \
   when((winnerDF.t1_world_rank < winnerDF.t2_world_rank) & (winnerDF.winner == "t1"), lit(True)) \
       .when((winnerDF.t1_world_rank > winnerDF.t2_world_rank) & (winnerDF.winner == "t2"), lit(True)) \
    .otherwise(lit(False)))

countDF = winnerDF.groupBy("higher_rank_is_winner").count()

# Get row count
rows = df.count()

# # Get columns count
# cols = len(df.columns)
# print(f"DataFrame Columns count : {cols}")

countDF = countDF.withColumn("count", countDF["count"] / rows)

winnerDF.show()
countDF.show()



