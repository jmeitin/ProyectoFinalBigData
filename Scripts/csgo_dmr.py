from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import when, lit, pow

conf = SparkConf().setAppName('CSGO Dmr')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

def leer_csv():
    "Devuelve un DataFrame con los datos del csv"
    df = spark.read.option("header", True).csv("csgo_games2GB.csv")

    # Conversion columnas a datos usados
    for player in range(5): # [0, 4]
        nombre = 't1_player' + str(player + 1) + '_dmr' #'t1_player1_dmr'
        df = df.withColumn(nombre, df[nombre].cast("Float"))
    
    for player in range(5): # [0, 4]
        nombre = 't2_player' + str(player + 1) + '_dmr' #'t1_player1_dmr'
        df = df.withColumn(nombre, df[nombre].cast("Float"))

    df = df.withColumn('winner', df['winner'].cast("String"))

    return df

def mean(dataframe, team):
    player1 = dataframe["t" + team + "_player1_dmr"]
    player2 = dataframe["t" + team + "_player2_dmr"]
    player3 = dataframe["t" + team + "_player3_dmr"]
    player4 = dataframe["t" + team + "_player4_dmr"]
    player5 = dataframe["t" + team + "_player5_dmr"]

    return (player1 + player2 + player3 + player4 + player5) / 5

def team_variance(dataframe, team):
    player1 = dataframe["t" + team + "_player1_dmr"]
    player2 = dataframe["t" + team + "_player2_dmr"]
    player3 = dataframe["t" + team + "_player3_dmr"]
    player4 = dataframe["t" + team + "_player4_dmr"]
    player5 = dataframe["t" + team + "_player5_dmr"]
    
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
    dmrDF = df.select('winner', 't1_player1_dmr', 't1_player2_dmr', 't1_player3_dmr', 't1_player4_dmr',
    't1_player5_dmr', 't2_player1_dmr', 't2_player2_dmr', 't2_player3_dmr', 't2_player4_dmr', 't2_player5_dmr')

    dmrDF = dmrDF.withColumn("team1_dmr", mean(dmrDF, "1"))
    dmrDF = dmrDF.withColumn("team2_dmr", mean(dmrDF, "2"))

    dmrDF = dmrDF.withColumn("team1_dmr_variance", team_variance(dmrDF, "1"))
    dmrDF = dmrDF.withColumn("team2_dmr_variance", team_variance(dmrDF, "2"))

    dmrDF = dmrDF.drop('t1_player1_dmr', 't1_player2_dmr', 't1_player3_dmr', 't1_player4_dmr',
    't1_player5_dmr', 't2_player1_dmr', 't2_player2_dmr', 't2_player3_dmr', 't2_player4_dmr', 't2_player5_dmr')

    # Evaluacion Datos ==> Gano el que tenia mejor dmr PROMEDIO?
    dmrDF = dmrDF.withColumn("higher_team_dmr_is_winner", \
    when((dmrDF.team1_dmr > dmrDF.team2_dmr) & (dmrDF.winner == "t1"), lit(True)) \
        .when((dmrDF.team2_dmr > dmrDF.team1_dmr) & (dmrDF.winner == "t2"), lit(True)) \
        .otherwise(lit(False)))

    # Evaluacion Datos ==> Gano el que tenia menor VARIANZA en el dmr?
    dmrDF = dmrDF.withColumn("lower_team_dmr_variance_is_winner", \
    when((dmrDF.team1_dmr_variance < dmrDF.team2_dmr_variance) & (dmrDF.winner == "t1"), lit(True)) \
        .when((dmrDF.team2_dmr_variance < dmrDF.team1_dmr_variance) & (dmrDF.winner == "t2"), lit(True)) \
        .otherwise(lit(False)))

    # Resultado: Cuantos son True/False?
    meanDF = dmrDF.groupBy("higher_team_dmr_is_winner").count()
    varianceDF = dmrDF.groupBy("lower_team_dmr_variance_is_winner").count()

    # Calculamos %
    meanDF = meanDF.withColumn("count", meanDF["count"] / rows)
    varianceDF = varianceDF.withColumn("count", varianceDF["count"] / rows)

    dmrDF.show()
    meanDF.show() # 58.01%
    varianceDF.show() # 50.54%

main()



