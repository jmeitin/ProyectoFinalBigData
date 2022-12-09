from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, lit, greatest, coalesce, col

conf = SparkConf().setAppName('CSGO MVP')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

def leer_csv():
    "Devuelve un DataFrame con los datos del csv"
    df = spark.read.option("header", True).csv("../Datasets/csgo_games2GB.csv")

    # Conversion columnas a datos usados
    for player in range(5): # [0, 4]
        nombre = 't1_player' + str(player + 1) + '_rating' #'t1_player1_rating'
        df = df.withColumn(nombre, df[nombre].cast("Float"))
    
    for player in range(5): # [0, 4]
        nombre = 't2_player' + str(player + 1) + '_rating' #'t1_player1_rating'
        df = df.withColumn(nombre, df[nombre].cast("Float"))

    df = df.withColumn('winner', df['winner'].cast("String"))

    return df

def getMaxInRows(*cols):
    return float(max(x for x in cols))

def mvpRating():
    minf = lit(float("-inf"))

    rowmax = greatest(*[coalesce(col(x), minf) for x in ['t1_player1_rating','t1_player2_rating', 't1_player3_rating', 
    't1_player4_rating', 't1_player5_rating', 't2_player1_rating', 't2_player2_rating', 't2_player3_rating',
    't2_player4_rating', 't2_player5_rating']])

    return rowmax

def main ():
    # Datos CSV
    df = leer_csv()
    rows = df.count() #Numero de filas ==> Partidas guardadas

    # Datos que queremos evaluar
    ratingDF = df.select('winner', 't1_player1_rating', 't1_player2_rating', 't1_player3_rating', 't1_player4_rating',
    't1_player5_rating', 't2_player1_rating', 't2_player2_rating', 't2_player3_rating', 't2_player4_rating', 't2_player5_rating')

    ratingDF = ratingDF.withColumn("mvpRating", mvpRating())

    ratingDF = ratingDF.withColumn("mvpInTeam1", when((ratingDF.t1_player1_rating == ratingDF.mvpRating) | 
    (ratingDF.t1_player2_rating == ratingDF.mvpRating) |
    (ratingDF.t1_player3_rating == ratingDF.mvpRating) | 
    (ratingDF.t1_player4_rating == ratingDF.mvpRating) |
    (ratingDF.t1_player5_rating == ratingDF.mvpRating), lit(True)) \
        .otherwise(lit(False)))

    ratingDF = ratingDF.drop('t1_player1_rating', 't1_player2_rating', 't1_player3_rating', 't1_player4_rating',
    't1_player5_rating', 't2_player1_rating', 't2_player2_rating', 't2_player3_rating', 't2_player4_rating', 't2_player5_rating')

    # Evaluacion Datos ==> Gano el que tenia al MVP?
    winnerDF = ratingDF.withColumn("mpv_in_team_is_winner", \
    when((ratingDF.mvpInTeam1 == True) & (ratingDF.winner == "t1"), lit(True)) \
        .when((ratingDF.mvpInTeam1 == False) & (ratingDF.winner == "t2"), lit(True)) \
        .otherwise(lit(False)))

    # Resultado: Cuantos son True/False?
    countDF = winnerDF.groupBy("mpv_in_team_is_winner").count() 

    # Calculamos %
    countDF = countDF.withColumn("count", countDF["count"] / rows)

    winnerDF.show()
    countDF.show() # 55.66%

main()

