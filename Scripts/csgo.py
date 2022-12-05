from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, lit

conf = SparkConf().setAppName('CSGO')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

def leer_csv():
    "Devuelve un DataFrame con los datos del csv"
    df = spark.read.option("header", True).csv("../Datasets/CSGO/csgo_games.csv")

    # Conversion columnas a datos usados
    df = df.withColumn('t1_world_rank', df['t1_world_rank'].cast("Int"))
    df = df.withColumn('t2_world_rank', df['t2_world_rank'].cast("Int"))
    df = df.withColumn('winner', df['winner'].cast("String"))

    return df

def main ():
    # Datos CSV
    df = leer_csv()
    rows = df.count() #Numero de filas ==> Partidas guardadas
    # # Get columns count
    # cols = len(df.columns)
    # print(f"DataFrame Columns count : {cols}")

    # Datos que queremos evaluar
    winnerDF = df.select('winner', 't1_world_rank', 't2_world_rank')

    # Evaluacion Datos ==> Gano el que tenia mejor world rank?
    winnerDF = winnerDF.withColumn("higher_rank_is_winner", \
    when((winnerDF.t1_world_rank < winnerDF.t2_world_rank) & (winnerDF.winner == "t1"), lit(True)) \
        .when((winnerDF.t1_world_rank > winnerDF.t2_world_rank) & (winnerDF.winner == "t2"), lit(True)) \
        .otherwise(lit(False)))

    # Resultado: Cuantos son True/False?
    countDF = winnerDF.groupBy("higher_rank_is_winner").count() 
    # Calculamos %
    countDF = countDF.withColumn("count", countDF["count"] / rows)

    winnerDF.show()
    countDF.show()

main()



