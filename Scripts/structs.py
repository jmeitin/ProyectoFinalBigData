from pyspark.sql.types import *
from pyspark.sql.types import NumericType

def stats_struct():
    struct = StructType([
    StructField("participanId", NumericType(), True),
    StructField("win", BooleanType(), True),
    StructField("kills", NumericType(), True),
    StructField("deaths", NumericType(), True),
    StructField("assits", NumericType(), True),
    StructField("largestKillingSpree", NumericType(), True),
    StructField("largestMultiKill", NumericType(), True),
    StructField("killingSprees", NumericType(), True),
    StructField("longestTimeSpentLiving", NumericType(), True),
    StructField("doubleKills", NumericType(), True),
    StructField("tripleKills", NumericType(), True),
    StructField("quadraKills", NumericType(), True),
    StructField("pentaKills", NumericType(), True),
    StructField("unrealKills", NumericType(), True),
    StructField("totalDamageDealt", NumericType(), True),
    StructField("magicDamageDealt", NumericType(), True),
    StructField("physicalDamageDealt", NumericType(), True),
    StructField("trueDamageDealt", NumericType(), True),
    StructField("largestCriticalStrike", NumericType(), True),
    StructField("totalDamageDealtToChampions", NumericType(), True),
    StructField("magicDamageDealtToChampions", NumericType(), True),
    StructField("physicalDamageDealtToChampions", NumericType(), True),
    StructField("trueDamageDealtToChampions", NumericType(), True),
    StructField("totalHeal", NumericType(), True),
    StructField("totalUnitsHealed", NumericType(), True),
    StructField("damageSelfMitigated", NumericType(), True),
    StructField("damageDealtToObjectives", NumericType(), True),
    StructField("damageDealtToTurrets", NumericType(), True),
    StructField("visionScore", NumericType(), True),
    StructField("timeCCingOthers", NumericType(), True),
    StructField("totalDamageTaken", NumericType(), True),
    StructField("magicalDamageTaken", NumericType(), True),
    StructField("physicalDamageTaken", NumericType(), True),
    StructField("trueDamageTaken", NumericType(), True),
    StructField("goldEarned", NumericType(), True),
    StructField("goldSpent", NumericType(), True),
    StructField("turretKills", NumericType(), True),
    StructField("inhibitorKills", NumericType(), True),
    StructField("totalMinionsKilled", NumericType(), True),
    StructField("neutralMinionsKilled", NumericType(), True),
    StructField("neutralMinionsKilledTeamJungle", NumericType(), True),
    StructField("neutralMinionsKilledEnemyJungle",NumericType(), True),
    StructField("visionWardsBoughtInGame", NumericType(), True),
    StructField("sightWardsBoughtInGame", NumericType(), True),
    StructField("wardsPlaced", NumericType(), True),
    StructField("wardsKilled", NumericType(), True),
    StructField("firstBloodKill", BooleanType(), True),
    StructField("firstBloodAssist", BooleanType(), True),
    StructField("firstTowerKill", BooleanType(), True),
    StructField("firstTowerAssist", BooleanType(), True),
    StructField("firstInhibitorKill", BooleanType(), True),
    StructField("firstInhibitorAssist", BooleanType(), True),
    StructField("combatPlayerScore", NumericType(), True),
    StructField("objectivePlayerScore", NumericType(), True),
    StructField("totalPlayerScore", NumericType(), True),
    StructField("totalScoreRank", NumericType(), True), ])

    return struct

def main_struct():
    struct = StructType([
    StructField("participanId", NumericType(), True),
    StructField("teamId", NumericType(), True), ])
    
    return struct
