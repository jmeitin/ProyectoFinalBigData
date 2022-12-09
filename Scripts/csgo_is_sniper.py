from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, lit, greatest, coalesce, col

conf = SparkConf().setAppName('CSGO Is Sniper')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

def leer_csv():
    "Devuelve un DataFrame con los datos del csv"
    df = spark.read.option("header", True).csv("csgo_games2GB.csv")

    # Conversion columnas a datos usados
    for player in range(5): # [0, 4]
        nombre = 't1_player' + str(player + 1) + '_is_sniper' #'t1_player1_is_sniper'
        df = df.withColumn(nombre, df[nombre].cast("Boolean"))
        df = df.withColumn(nombre, df[nombre].cast("Int"))
    
    for player in range(5): # [0, 4]
        nombre = 't2_player' + str(player + 1) + '_is_sniper' #'t1_player1_is_sniper'
        df = df.withColumn(nombre, df[nombre].cast("Boolean"))
        df = df.withColumn(nombre, df[nombre].cast("Int"))

    df = df.withColumn('winner', df['winner'].cast("String"))

    return df

def num_sniper(dataframe, team):

    player1 = dataframe["t" + team + "_player1_is_sniper"]
    player2 = dataframe["t" + team + "_player2_is_sniper"]
    player3 = dataframe["t" + team + "_player3_is_sniper"]
    player4 = dataframe["t" + team + "_player4_is_sniper"]
    player5 = dataframe["t" + team + "_player5_is_sniper"]

    return player1 + player2 + player3 + player4 + player5

def main ():
    # Datos CSV
    df = leer_csv()
    rows = df.count() #Numero de filas ==> Partidas guardadas

    # Datos que queremos evaluar
    is_sniperDF = df.select('winner', 't1_player1_is_sniper', 't1_player2_is_sniper', 't1_player3_is_sniper', 't1_player4_is_sniper',
    't1_player5_is_sniper', 't2_player1_is_sniper', 't2_player2_is_sniper', 't2_player3_is_sniper', 't2_player4_is_sniper', 't2_player5_is_sniper')

    is_sniperDF = is_sniperDF.withColumn("n_snipers_t1", num_sniper(is_sniperDF, "1"))
    is_sniperDF = is_sniperDF.withColumn("n_snipers_t2", num_sniper(is_sniperDF, "2"))

    is_sniperDF = is_sniperDF.withColumn("more_snipers_in_t1", \
    when((is_sniperDF.n_snipers_t1 > is_sniperDF.n_snipers_t2), lit("Hay mas")) \
        .when((is_sniperDF.n_snipers_t1 < is_sniperDF.n_snipers_t2), lit("Hay menos")) \
        .otherwise(lit("Hay los mismos")))

    is_sniperDF = is_sniperDF.drop('t1_player1_is_sniper', 't1_player2_is_sniper', 't1_player3_is_sniper', 't1_player4_is_sniper',
    't1_player5_is_sniper', 't2_player1_is_sniper', 't2_player2_is_sniper', 't2_player3_is_sniper', 't2_player4_is_sniper', 't2_player5_is_sniper')

    # Evaluacion Datos ==> Gano el que tenia al MVP?
    winnerDF = is_sniperDF.withColumn("more_snipers_is_winner", \
    when((is_sniperDF.more_snipers_in_t1 == "Hay mas") & (is_sniperDF.winner == "t1"), lit("Hay mas")) \
        .when((is_sniperDF.more_snipers_in_t1 == "Hay menos") & (is_sniperDF.winner == "t2"), lit("Hay mas")) \
        .when(is_sniperDF.more_snipers_in_t1 == "Hay los mismos" , lit("Hay los mismos")) \
        .otherwise(lit("Hay menos")))

    # Resultado: Cuantos son True/False?
    countDF = winnerDF.groupBy("more_snipers_is_winner").count() 

    # Calculamos %
    countDF = countDF.withColumn("count", countDF["count"] / rows)

    winnerDF.show()
    countDF.show() # 55.66%

main()

