import pyspark
import pandas as pd
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql import types
import argparse

# Create Spark Session
spark = SparkSession.builder.master("local[*]").appName("Spark").getOrCreate()

# Set up parameters
parser=argparse.ArgumentParser()

parser.add_argument('--year', required=True)
parser.add_argument('--season_type', required=True)
args=parser.parse_args()

year=args.year
season_type=args.season_type

#===============================================================
#Submit to bigquery
BUCKET = 'gs://nfl-data-lake_nfl-project-de'
temp_bucket = "dataproc-temp-europe-west6-447072446412-ooqjzme9"
spark.conf.set('temporaryGcsBucket', temp_bucket)

#===============================================================
# Transform TEAM DETAILS (once only)

df_teams = spark.read.parquet(f"{BUCKET}/nfl_parquets/teams/{year}/{season_type}").cache()

# Selecting only the columns that we need
columns = [
    "id",
    "location",
    "name",
    "displayName",
    "isActive",
    "logo",
]

print(f"Cleaning the data for the teams_df started")

df_teams_clean = (
    df_teams.select(columns)
    .selectExpr(
        "CAST(id AS int) as teamId",
        "CAST(name AS string) as teamName",
        "CAST(displayName AS string) as teamDisplayName",
        "CAST(location AS string) as teamLocation",
        "CAST(isActive AS boolean) as isActive",
        "CAST(logo AS string) as teamLogo",
    )
    .withColumn("year", F.lit(year).cast("integer"))
    .drop("id")
    .orderBy("teamId")
)

print(f"Cleaning the data for the teams_df is successful!")

#===============================================================
# Transform TEAM STATS

df_teams_stats = spark.read.parquet(f"{BUCKET}/nfl_parquets/teams_stats/{year}/{season_type}").cache()

df_teams_stats_clean = (
    df_teams_stats.drop(
        "displayValue",
        "perGameDisplayValue",
        "rankDisplayValue",
        "displayName",
        "shortDisplayName",
    )
    .fillna(0)
    .withColumn("year", F.lit(year).cast("integer"))
    .withColumn("seasonType", F.lit(season_type).cast("integer"))
    .selectExpr(
        "CAST(name as string) as teamStatName",
        "CAST(description as string) as teamStatDescription",
        "CAST(abbreviation as string) as teamStatAbbreviation",
        "CAST(value as double) as teamStatValue",
        "CAST(perGameValue as double) as teamStatPerGameValue",
        "CAST(rank as int) as teamRank",
        "CAST(`categories.displayName` as string) as teamStatCategory",
        "CAST(teamId as int) as teamId",
        "CAST(year as int) as year",
        "CAST(seasonType as int) as seasonType",
    )
    .orderBy("teamId")
)

df_teams_stats_clean = df_teams_stats_clean.withColumn('teamStatCategory', 
    F.when(df_teams_stats_clean.teamStatName == 'interceptionTouchdowns', 'Scoring')
    .otherwise(df_teams_stats_clean.teamStatCategory)
)

df_teams_stats_clean = df_teams_stats_clean.withColumn('teamStatCategory',
    F.when(df_teams_stats_clean.teamStatCategory == 'Defensive Interceptions', 'Defensive')
    .otherwise(df_teams_stats_clean.teamStatCategory)
)
#===============================================================
# Transform ATHLETES

df_athletes = spark.read.parquet(f"{BUCKET}/nfl_parquets/athletes/{year}/{season_type}").cache()

# Positions have been made earlier, make sure to download it 
df_positions = spark.read.csv(f"{BUCKET}/nfl_csv/positions_csv.csv", header=True)


columns_select = [
    "id",
    "firstName",
    "lastName",
    "fullName",
    "shortName",
    "weight",
    "height",
    "displayHeight",
    "age",
    "dateOfBirth",
    "debutYear",
    "`birthPlace.city`",
    "`birthPlace.state`",
    "`birthPlace.country`",
    "`position.id`",
    "`position.name`",
    "`experience.years`",
    "`status.name`",
    "teamId",
    "`headshot.href`",
]

df_athletes_clean = (
    df_athletes.select(*columns_select)
    .selectExpr(
        "CAST(id AS int) as athleteId",
        "CAST(firstName AS string) as firstName",
        "CAST(lastName AS string) as lastName",
        "CAST(fullName AS string) as fullName",
        "CAST(shortName AS string) as shortName",
        "CAST(weight AS double) as weightLbs",
        "CAST(height AS double) as heightInches",
        "CAST(age AS int) as age",
        "CAST(dateOfBirth AS date) as dateOfBirth",
        "CAST(debutYear AS int) as debutYear",
        "CAST(`birthPlace.city` AS string) as birthPlaceCity",
        "CAST(`birthPlace.state` AS string) as birthPlaceState",
        "CAST(`birthPlace.country` AS string) as birthPlaceCountry",
        "CAST(`position.id` AS int) as positionId",
        "CAST(`position.name` AS string) as positionName",
        "CAST(`experience.years` AS int) as experienceYears",
        "CAST(`status.name` AS string) as statusName",
        "CAST(teamId AS int) as teamId",
        "CAST(`headshot.href` AS string) as headshot",
    )
    .na.fill(0)
    .withColumn("year", F.lit(year).cast("integer"))
    # .where(F.col("statusName") == "Active")
    # # fix the .(dot) notation before using select to prevent struct type expected str returned
    # # .withColumn("headshot",
    .orderBy(F.col("teamId"), F.col("positionId"))
)

# Join athletes and positions
df_athletes_clean = (
    df_athletes_clean.
    join( df_positions, df_athletes_clean["positionName"] == df_positions["positionName2"], how="inner")
    .drop("_c0", "positionName2", "statusId")
)

#===============================================================
# Transform ATHLETES STATS

df_athletes_stats = spark.read.parquet(f"{BUCKET}/nfl_parquets/athletes_stats/{year}/{season_type}").cache()

df_athletes_stats_clean = (
    df_athletes_stats
    #
    .drop("perGameDisplayValue", "rankDisplayValue", "abbreviation", "displayValue")
    .selectExpr(
        "CAST(name AS string) as athleteStatName",
        "CAST(displayName AS string) as athleteStatDisplayName",
        "CAST(shortDisplayName AS string) as athleteStatShortDisplayName",
        "CAST(description AS string) as athleteStatDescription",
        "CAST(value AS double) as athleteStatValue",
        "CAST(rank AS int) as athleteStatRank",
        "CAST(perGameValue AS double) as athletePerGameValue",
        "CAST(`split.categories.displayName` AS string) as athleteStatCategory",
        "CAST(athleteId AS int) as athleteId",
    )
    .withColumn("year", F.lit(year).cast("integer"))
    .withColumn("seasonType", F.lit(season_type).cast("integer"))
    .na.fill(0)
    .orderBy(F.col("athleteId"))
)

#===============================================================
# Tranform LEADERS

df_leaders = spark.read.parquet(f"{BUCKET}/nfl_parquets/leaders/{year}/{season_type}").cache()

df_leaders_clean = (
    df_leaders.withColumnRenamed("athlete.$ref", "athleteref")
    .withColumn("athleteId", F.regexp_extract(F.col("athleteref"), r"\/(\w*)\?", 1))  #1 : 1 word
    .selectExpr(
        "CAST(athleteId AS int) as athleteId",
        "CAST(displayValue AS string) as leaderDisplayValue",
        "CAST(value AS double) as leaderValue",
        "CAST(name AS string) as statName",
        "CAST(shortDisplayName AS string) as leaderShortDisplayName",
    )
    .withColumn("year", F.lit(year).cast("integer"))
    .withColumn("seasonType", F.lit(season_type).cast("integer"))
    .drop("rel", "statistics.$ref", "team.$ref", "athleteref", "__index_level_0__")
)

df_athletes_clean.createOrReplaceTempView("athletes")
df_athletes_stats_clean.createOrReplaceTempView("athletes_stats")
df_leaders_clean.createOrReplaceTempView("leaders")

#==============================================================
#Transform TEAM DEFENSE STATS

df_defense = (
    spark.read.format("parquet")
    .load(f"{BUCKET}/nfl_parquets/teams_defense_stats/{year}/{season_type}")
    .cache()
)

df_defense_clean = (df_defense
    .withColumn("gamesPlayed", F.col("gamesPlayed").cast("double"))
    .withColumn("totalYDS", F.col("totalYDS").cast("double"))
    .withColumn("totalYDSG", F.col("totalYDSG").cast("double"))
    .withColumn("passingYDS", F.col("passingYDS").cast("double"))
    .withColumn("passingYDSG", F.col("passingYDSG").cast("double"))
    .withColumn("rushingYDS", F.col("rushingYDS").cast("double"))
    .withColumn("rushingYDSG", F.col("rushingYDSG").cast("double"))
    .withColumn("points", F.col("points").cast("double"))
    .withColumn("pointsPerGame", F.col("pointsPerGame").cast("double"))
    .withColumn("statCategory", F.lit("Defensive").cast("string"))
    .drop("__index_level_0__")
)

df_defense_stack = (
    df_defense_clean.select(
        "teamName",F.expr(
            "stack(9,'gamesPlayed', gamesPlayed, 'totalYDS', totalYDS, 'totalYDSG', totalYDSG, 'passingYDS', passingYDS, 'passingYDSG', passingYDSG, 'rushingYDS', rushingYDS, 'rushingYDSG', rushingYDSG, 'points', points, 'pointsPerGame', pointsPerGame) AS (statName, statValue)"
        ))
    .withColumn("year", F.lit(year).cast("int"))
    .withColumn("seasonType", F.lit(season_type).cast("int"))
    .withColumn("statCategory", F.lit("Defensive").cast("string"))
)
#================================================================
# Loading all to bigquery
# log: start
print(f"Loading for nfl data to parquet started")

df_list=[
    df_teams_clean,
    df_leaders_clean,
    df_defense_stack
]

filenames=[
    'teams',
    'leaders',
    'teams_defense_stats'
]

BQ_DATASET="nfl_data_all"
for df, filename in zip(df_list, filenames):
    df.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.{year}_{season_type}_{filename}") \
    .save()

    # Within each partition, data is ordered by clusters, what will be your cluster? category
df_w_partition=[ 
    df_teams_stats_clean,
    df_athletes_clean,
    df_athletes_stats_clean
]

filenames_w_partition=[
    'teams_stats',
    'athletes',
    'athletes_stats'
]

for df, filename in zip(df_w_partition, filenames_w_partition):
    if 'teamId' in df.columns:
        df.write.format('bigquery') \
        .mode('overwrite') \
        .option('table', f"{BQ_DATASET}.{year}_{season_type}_{filename}_PARTITIONED") \
        .partitionBy('teamId') \
        .save()

    elif 'athleteId' in df.columns:
        df.write.format('bigquery') \
        .mode('overwrite') \
        .option('table', f"{BQ_DATASET}.{year}_{season_type}_{filename}_PARTITIONED") \
        .partitionBy('athleteId') \
        .save()

# log: end
print(f"Loading for nfl data to parquet ended")

#===============================================================

# ## SQL TRANSFORMATIONS FOR DASHBOARD

# # Dashboard 1 : Who's your extraordinary teammate

# df_athletes_clean.createOrReplaceTempView("athletes")
# df_athletes_stats_clean.createOrReplaceTempView("athletes_stats")
# df_leaders_clean.createOrReplaceTempView("leaders")

# # Get the top 1 leader in each metric
# leaders = spark.sql(
#     """
#     WITH stats AS (
#     SELECT l.name as Forte, l.value as ValueForte, a.teamId, l.athleteId, a.shortName as athleteName, a.positionParent, a.positionName
#     FROM leaders l
#     JOIN athletes a ON a.athleteId=l.athleteId
#     WHERE value IN (SELECT max(value) FROM leaders GROUP BY name)
#     ORDER BY Forte DESC, teamId
#     )
    
#     SELECT athleteName, Forte, ValueForte, athleteId, teamId, positionParent, positionName
#     FROM stats
#     ORDER BY teamId, athleteId
#     """
# )

# leaders.createOrReplaceTempView("top")

# # Get stats of the top (1) leaders
# leaders_stats = spark.sql(
#     """
#     SELECT DISTINCT a.*, t.athleteName, t.teamId, t.positionParent, t.positionName
#     FROM top as t
#     JOIN athletes_stats as a ON a.athleteId=t.athleteId
#     """
# )
# leaders_stats.createOrReplaceTempView("leaders_stats")

# # Get the teammates and their stats

# teammates_all = spark.sql(
#     """
#     -- from athletes table, get the name and other info
#     WITH leader_teammates AS
#     (SELECT a.shortName as athleteName, a.positionParent, a.positionName, a.athleteId, a.teamId, a.headshot
#     FROM athletes a),
    
#     -- table for the athletes in the leaders' team
    
#     teammates AS
#     (SELECT DISTINCT l1.*
#     FROM leader_teammates l1
#     JOIN leaders_stats l2 ON l1.teamId = l2.teamId),
    
#     -- getting the stats for each athlete
    
#     teammates_stats AS
#     (SELECT t.teamId, t.athleteName, t.positionParent, t.positionName, t.headshot, as.*
#     FROM teammates AS t
#     JOIN athletes_stats AS as ON as.athleteId=t.athleteId
#     WHERE as.name IN (SELECT Forte FROM top)
#     ORDER BY teamId),
    
#     -- getting the average value for each metric
    
#     metric_average AS
#     (SELECT ROUND(AVG(value),4) as averageValue, name as metricName, category
#     FROM athletes_stats
#     WHERE name IN (SELECT Forte FROM top) and value != 0
#     GROUP BY name, category)
    
#     -- main query 
    
#     SELECT ma.averageValue, ts.*
#     FROM metric_average ma
#     JOIN teammates_stats ts ON (ts.name=ma.metricName and ts.category=ma.category)
    
#     EXCEPT 
    
#     SELECT ma.averageValue, ts.*
#     FROM metric_average ma
#     JOIN teammates_stats ts ON ts.name=ma.metricName
#     WHERE ts.category='Passing' and ts.name IN ('sacks','interceptions')
#     """
# )
# # Write to csv --> make this big query

# # teammates_all.write.option("header", True).mode("overwrite").csv(
# #     os.path.join(f"./dashboards/{year}/{season_type}/", "dashboard1_scatter")


# BQ_DATASET="nfl_data_all"
# teammates_all.write.format('bigquery') \
#     .mode('overwrite') \
#     .option('table', f"{BQ_DATASET}.leaders_teammates_{year}_{season_type}") \
#     .save()

#===================================================================================
# Dashboard 2: Radar chart - weakness/strength

df_teams_stats_clean.createOrReplaceTempView("teams_stats")
df_teams_clean.createOrReplaceTempView("teams")

stats = spark.sql(
    """
    SELECT ts.teamStatName, ts.teamStatAbbreviation, ts.teamStatValue, ts.teamStatCategory, ts.teamId, ts.teamRank, PERCENT_RANK() OVER(
                        PARTITION BY ts.teamStatName
                        ORDER BY ts.teamRank DESC)*100 AS percentileRank, 
                        t.teamLogo 
    FROM teams_stats ts
    JOIN teams t ON t.teamId=ts.teamId
    WHERE ts.teamStatCategory IN ('General','Passing','Rushing','Receiving', 'Kicking') and ts.teamStatName IN ("fumbles",
    "fumblesRecovered",
    "fumblesTouchdowns",
    "fumblesForced",
    "fumblesLost", 
    "completionPct", 
    "interceptionPct",
    "passingYards",
    "passingTouchdownPct",
    "longPassing",
    "rushingAttempts",
    "rushingYards",
    "rushingTouchdowns",
    "longRushing",
    "yardsPerRushAttempt",
    "longReception",
    "yardsPerReception",
    "receivingYardsPerGame",
    "receivingTargets",
    "receivingTouchdowns",
    "extraPointPct",
    "fieldGoalPct",
    "longFieldGoalMade",
    "longKickoff",
    "totalKickingPoints")
    
    UNION ALL
    SELECT ts.teamStatName, ts.teamStatAbbreviation, ts.teamStatValue, ts.teamStatCategory, ts.teamId, ts.teamRank, PERCENT_RANK() OVER(
                    PARTITION BY ts.teamStatName
                    ORDER BY ts.teamRank DESC)*100 AS percentileRank, 
                    t.teamLogo
    FROM teams_stats ts
    JOIN teams t ON t.teamId=ts.teamId
    WHERE ts.teamStatCategory IN ('Defensive','Scoring') and ts.teamStatName IN (
    "totalTackles",
    "avgSackYards", 
    "avgStuffYards",
    "avgInterceptionYards",
    "interceptions",
    "passingTouchdowns",
    "returnTouchdowns",
    "rushingTouchdowns",
    "interceptionTouchdowns",
    "totalPointsPerGame")
    
    ORDER BY teamStatCategory, teamStatName, percentileRank DESC
    """
)

# Write to big query
# stats.write.mode("overwrite").option("header", True).csv(
#     os.path.join(f"./dashboards/{year}/{season_type}", "dashboard2_radar")
# )

BQ_DATASET="nfl_data_all"
stats.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.{year}_{season_type}_radar_stats") \
    .save()

#=======================================================================

# View for defense stats
df_defense_stack.createOrReplaceTempView("addtl_defense_stats")

defense_stats = spark.sql(
    """
    SELECT t.teamId,ads.*
    FROM addtl_defense_stats ads
    JOIN teams t ON ads.teamName=t.teamDisplayName
    """
)

defense_stats.createOrReplaceTempView("defense_stats")

# Making dense rank
dense_rank = spark.sql(
    """
    SELECT teamId, teamStatCategory, teamStatName, teamStatValue,
            DENSE_RANK() OVER(
                PARTITION BY teamStatName
                ORDER BY teamStatValue DESC) as statValuesDenseRank
    FROM teams_stats
    WHERE teamStatCategory IN ('Passing', 'Rushing', 'Receiving') AND teamStatName IN (
    'netTotalYards',
    'yardsPerGame',
    'completionPct',
    'netpassingYards',
    'netpassingYardsPerGame',
    'rushingYardsPerGame',
    'rushingYards',
    'totalPoints',
    'totalPointsPerGame'
    'longPassing',
    'longRushing',
    'passingTouchdowns',
    'receivingTouchdowns',
    'rushingTouchdowns'
    )
    UNION ALL
    SELECT teamId, teamstatCategory, teamStatName, teamStatValue,
            DENSE_RANK() OVER(
                PARTITION BY teamStatName
                ORDER BY teamStatValue DESC) as statValuesDenseRank
    FROM teams_stats
    WHERE teamStatCategory IN ('General','Defensive', 'Defensive Interception') and teamStatName IN ('fumblesForced','fumblesTouchdowns','passesDefended',
    'avgInterceptionYards',
    'avgSackYards',
    'avgStuffYards',
    'tacklesForLoss',
    'defensiveTouchdowns'
    )
    UNION ALL
    SELECT teamId, statCategory as teamStatCategory, statName as teamStatName, statValue as teamStatValue,
        DENSE_RANK() OVER(
                PARTITION BY statName
                ORDER BY statValue) as statValuesDenseRank
    FROM defense_stats
    """
)

dense_rank.createOrReplaceTempView("dense_rank")

# Making the offense and defense rank
classify = spark.sql(
    """
    WITH defense AS
    (SELECT dr.teamId,
            AVG(dr.statValuesDenseRank) as defenseAvgRank
    FROM dense_rank dr
    WHERE teamStatCategory IN ('General','Defensive', 'Defensive Interception')
    GROUP BY dr.teamId
    ),
    
    offense AS
    (
    SELECT dr.teamId,
            AVG(dr.statValuesDenseRank) as offenseAvgRank
    FROM dense_rank dr
    WHERE teamStatCategory IN ('Passing', 'Rushing', 'Receiving', 'Scoring')
    GROUP BY dr.teamId
    )
    
    SELECT o.teamId,t.teamDisplayName, t.teamLogo, 
            DENSE_RANK() OVER(
            ORDER BY defenseAvgRank) as defRank,
            DENSE_RANK() OVER(
            ORDER BY offenseAvgRank) as offRank
    FROM teams t
    JOIN defense d on t.teamId=d.teamId
    JOIN offense o on o.teamId=t.teamId
    ORDER BY offenseAvgRank
    """
)

BQ_DATASET="nfl_data_all"
classify.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.{year}_{season_type}_best_worst_teams") \
    .save()