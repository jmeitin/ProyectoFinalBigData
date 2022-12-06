from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, lit, pow

conf = SparkConf().setAppName('CSGO Team Rating')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

def leer_csv():
    "Devuelve un DataFrame con los datos del csv"
    df = spark.read.option("header", True).csv("../Datasets/CSGO/csgo_games.csv")

    # Conversion columnas a datos usados
    for player in range(5): # [0, 4]
        nombre = 't1_player' + str(player + 1) + '_rating' #'t1_player1_rating'
        df = df.withColumn(nombre, df[nombre].cast("Float")) #df.withColumn('t1_player2_rating', df['t1_player2_rating'].cast("Float"))
    
    for player in range(5): # [0, 4]
        nombre = 't2_player' + str(player + 1) + '_rating' #'t1_player1_rating'
        df = df.withColumn(nombre, df[nombre].cast("Float"))

    df = df.withColumn('winner', df['winner'].cast("String"))

    return df

def mean(dataframe, team):
    player1 = dataframe["t" + team + "_player1_rating"]
    player2 = dataframe["t" + team + "_player2_rating"]
    player3 = dataframe["t" + team + "_player3_rating"]
    player4 = dataframe["t" + team + "_player4_rating"]
    player5 = dataframe["t" + team + "_player5_rating"]

    return (player1 + player2 + player3 + player4 + player5) / 5

def team_variance(dataframe, team):
    player1 = dataframe["t" + team + "_player1_rating"]
    player2 = dataframe["t" + team + "_player2_rating"]
    player3 = dataframe["t" + team + "_player3_rating"]
    player4 = dataframe["t" + team + "_player4_rating"]
    player5 = dataframe["t" + team + "_player5_rating"]
    
    media = (player1 + player2 + player3 + player4 + player5) / 5

    variance = (pow(player1 - media, 2) + pow(player2 - media, 2) + pow(player3 - media, 2) +
    pow(player4 - media, 2) + pow(player5 - media, 2)) / 5

    return variance

def main ():
    # Datos CSV
    df = leer_csv()
    rows = df.count() #Numero de filas ==> Partidas guardadas

    # # Get columns count
    # cols = len(df.columns)
    # print(f"DataFrame Columns count : {cols}")

    # Datos que queremos evaluar
    ratingDF = df.select('winner', 't1_player1_rating', 't1_player2_rating', 't1_player3_rating', 't1_player4_rating',
    't1_player5_rating', 't2_player1_rating', 't2_player2_rating', 't2_player3_rating', 't2_player4_rating', 't2_player5_rating')

    ratingDF = ratingDF.withColumn("team1_rating", mean(ratingDF, "1"))
    ratingDF = ratingDF.withColumn("team2_rating", mean(ratingDF, "2"))

    ratingDF = ratingDF.withColumn("team1_rating_variance", team_variance(ratingDF, "1"))
    ratingDF = ratingDF.withColumn("team2_rating_variance", team_variance(ratingDF, "2"))

    ratingDF = ratingDF.drop('t1_player1_rating', 't1_player2_rating', 't1_player3_rating', 't1_player4_rating',
    't1_player5_rating', 't2_player1_rating', 't2_player2_rating', 't2_player3_rating', 't2_player4_rating', 't2_player5_rating')

    # Evaluacion Datos ==> Gano el que tenia mejor RATING PROMEDIO?
    ratingDF = ratingDF.withColumn("higher_team_rating_is_winner", \
    when((ratingDF.team1_rating > ratingDF.team2_rating) & (ratingDF.winner == "t1"), lit(True)) \
        .when((ratingDF.team2_rating > ratingDF.team1_rating) & (ratingDF.winner == "t2"), lit(True)) \
        .otherwise(lit(False)))

    # Evaluacion Datos ==> Gano el que tenia menor VARIANZA en el RATING?
    ratingDF = ratingDF.withColumn("lower_team_rating_variance_is_winner", \
    when((ratingDF.team1_rating_variance < ratingDF.team2_rating_variance) & (ratingDF.winner == "t1"), lit(True)) \
        .when((ratingDF.team2_rating_variance < ratingDF.team1_rating_variance) & (ratingDF.winner == "t2"), lit(True)) \
        .otherwise(lit(False)))

    # Resultado: Cuantos son True/False?
    meanDF = ratingDF.groupBy("higher_team_rating_is_winner").count()
    varianceDF = ratingDF.groupBy("lower_team_rating_variance_is_winner").count()

    # Calculamos %
    meanDF = meanDF.withColumn("count", meanDF["count"] / rows)
    varianceDF = varianceDF.withColumn("count", varianceDF["count"] / rows)

    ratingDF.show()
    meanDF.show() # 57.35%
    varianceDF.show() # 49.74%

main()



