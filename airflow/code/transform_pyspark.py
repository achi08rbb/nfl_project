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
BUCKET = 'gs://nfl-data-lake_nfl-de-project'
temp_bucket = "dataproc-temp-europe-west6-834475897757-zf5hpybi"
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
    "shortDisplayName",
    "isActive",
    "isAllStar",
    "logo",
]

# log: start
print(f"Cleaning the data for the teams_df started")

df_teams_clean = (
    df_teams.select(columns)
    .withColumn("teamId", F.col("id").cast("Integer"))
    .drop("id")
    .orderBy("teamId")
)

# log: end
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
        "__index_level_0__"
    )
    .fillna(0)
    .withColumnRenamed("categories.displayName", "category")
    .withColumn("teamId", F.col("teamId").cast("integer"))
    .withColumn("year", F.lit(year))
    .withColumn("seasonType", F.lit(season_type))
    .orderBy("teamId")
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
    .withColumn("teamId", F.col("teamId").cast("integer"))
    .withColumn("id", F.col("id").cast("integer"))
    .withColumn("dateOfBirth", F.col("dateOfBirth").cast("date"))
    .withColumn("year", F.lit(year).cast("int"))
    .withColumn("seasonType", F.lit(season_type).cast("int"))
    .withColumnRenamed("id", "athleteId")
    .withColumnRenamed("weight", "weight_lbs")
    .withColumnRenamed("height", "height_inches")
    .withColumnRenamed("birthPlace.city", "birthPlaceCity")
    .withColumnRenamed("birthPlace.state", "birthPlaceState")
    .withColumnRenamed("birthPlace.country", "birthPlaceCountry")
    .withColumnRenamed("position.id", "positionId")
    .withColumnRenamed("position.name", "positionName")
    .withColumnRenamed("experience.years", "experienceYears")
    .withColumn("experienceYears", F.col("experienceYears").cast("integer"))
    .withColumnRenamed("status.name", "statusName")
    .withColumnRenamed("position.parent.name", "positionParentName")
    .withColumnRenamed("position.parent.abbreviation", "positionParentAbbreviation")
    .withColumnRenamed("headshot.href", "headshot")
    .withColumn("headshot", F.regexp_replace(F.col("headshot"), "a.espncdn", "www.espn"))
    .na.fill(0)
    # .where(F.col("statusName") == "Active")
    # fix the .(dot) notation before using select to prevent struct type expected str returned
    # .withColumn("headshot",
    .orderBy(F.col("teamId"), F.col("positionId"))
)

# Join athletes and positions
df_athletes_clean = (
    df_athletes_clean.
    join( df_positions, df_athletes_clean["positionName"] == df_positions["positionName2"], how="inner",)
    .drop("_c0", "positionName2", "statusId")
)


#===============================================================
# Transform ATHLETES STATS

df_athletes_stats = spark.read.parquet(f"{BUCKET}/nfl_parquets/athletes_stats/{year}/{season_type}").cache()

df_athletes_stats_clean = (
    df_athletes_stats
    .withColumnRenamed("split.categories.displayName", "category")
    .withColumn("athleteId", F.col("athleteId").cast("integer"))
    .withColumn("year", F.lit(year))
    .withColumn("seasonType", F.lit(season_type))
    .drop("perGameDisplayValue", "rankDisplayValue", "__index_level_0__")
    .na.fill(0)
    .orderBy(F.col("athleteId"))
)

#===============================================================
# Tranform LEADERS

df_leaders = spark.read.parquet(f"{BUCKET}/nfl_parquets/leaders/{year}/{season_type}").cache()

df_leaders_clean = (
    df_leaders.withColumnRenamed("athlete.$ref", "athleteref")
    .withColumn("athleteId", F.regexp_extract(F.col("athleteref"), r"\/(\w*)\?", 1))  #1 : 1 word
    .withColumn("year", F.lit(year))
    .withColumn("seasonType", F.lit(season_type))
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

df_defense_clean = (
    df_defense.withColumnRenamed("Team", "team")
    .withColumnRenamed("('Unnamed: 0_level_0', 'GP')", "gamesPlayed")
    .withColumnRenamed("('Total', 'YDS')", "totalYDS")
    .withColumnRenamed("('Total', 'YDS/G')", "totalYDSG")
    .withColumnRenamed("('Passing', 'YDS')", "passingYDS")
    .withColumnRenamed("('Passing', 'YDS/G')", "passingYDSG")
    .withColumnRenamed("('Rushing', 'YDS')", "rushingYDS")
    .withColumnRenamed("('Rushing', 'YDS/G')", "rushingYDSG")
    .withColumnRenamed("('Points', 'PTS')", "points")
    .withColumnRenamed("('Points', 'PTS/G')", "pointsPerGame")
    .withColumn("gamesPlayed", F.col("gamesPlayed").cast("double"))
    .withColumn("totalYDS", F.col("totalYDS").cast("double"))
    .withColumn("totalYDSG", F.col("totalYDSG").cast("double"))
    .withColumn("passingYDS", F.col("passingYDS").cast("double"))
    .withColumn("passingYDSG", F.col("passingYDSG").cast("double"))
    .withColumn("rushingYDS", F.col("rushingYDS").cast("double"))
    .withColumn("rushingYDSG", F.col("rushingYDSG").cast("double"))
    .withColumn("points", F.col("points").cast("double"))
    .withColumn("pointsPerGame", F.col("pointsPerGame").cast("double"))
    .drop("__index_level_0__")
)

df_defense_stack= (df_defense_clean.select(
    "team",
    F.expr(
        "stack(9,'gamesPlayed', gamesPlayed, 'totalYDS', totalYDS, 'totalYDSG', totalYDSG, 'passingYDS', passingYDS, 'passingYDSG', passingYDSG, 'rushingYDS', rushingYDS, 'rushingYDSG', rushingYDSG, 'points', points, 'pointsPerGame', pointsPerGame) AS (name, value)"
    ))
    .withColumn("year", F.lit(year))
    .withColumn("seasonType", F.lit(season_type))
    .withColumn("category", F.lit("Defensive")))
#================================================================
#Loading all to bigquery
# log: start
print(f"Loading for nfl data to parquet started")

df_list=[
    df_teams_clean,
    df_teams_stats_clean,
    df_athletes_clean,
    df_athletes_stats_clean,
    df_leaders_clean,
    df_defense_stack
]

filenames=[
    'teams',
    'teams_stats',
    'athletes',
    'athletes_stats',
    'leaders',
    'teams_defense_stats'
]

BQ_DATASET="nfl_data_all"
for df, filename in zip(df_list, filenames):
    df.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.{filename}_{year}_{season_type}") \
    .save()

# log: end
print(f"Loading for nfl data to parquet ended")

#================================================================
#================================================================

## SQL TRANSFORMATIONS FOR DASHBOARD

# Dashboard 1 : Who's your extraordinary teammate

df_athletes_clean.createOrReplaceTempView("athletes")
df_athletes_stats_clean.createOrReplaceTempView("athletes_stats")
df_leaders_clean.createOrReplaceTempView("leaders")

# Get the top 1 leader in each metric
leaders = spark.sql(
    """
    WITH stats AS (
    SELECT l.name as Forte, l.value as ValueForte, a.teamId, l.athleteId, a.shortName as athleteName, a.positionParent, a.positionName
    FROM leaders l
    JOIN athletes a ON a.athleteId=l.athleteId
    WHERE value IN (SELECT max(value) FROM leaders GROUP BY name)
    ORDER BY Forte DESC, teamId
    )
    
    SELECT athleteName, Forte, ValueForte, athleteId, teamId, positionParent, positionName
    FROM stats
    ORDER BY teamId, athleteId
    """
)

leaders.createOrReplaceTempView("top")

# Get stats of the top (1) leaders
leaders_stats = spark.sql(
    """
    SELECT DISTINCT a.*, t.athleteName, t.teamId, t.positionParent, t.positionName
    FROM top as t
    JOIN athletes_stats as a ON a.athleteId=t.athleteId
    """
)
leaders_stats.createOrReplaceTempView("leaders_stats")

# Get the teammates and their stats

teammates_all = spark.sql(
    """
    -- from athletes table, get the name and other info
    WITH leader_teammates AS
    (SELECT a.shortName as athleteName, a.positionParent, a.positionName, a.athleteId, a.teamId, a.headshot
    FROM athletes a),
    
    -- table for the athletes in the leaders' team
    
    teammates AS
    (SELECT DISTINCT l1.*
    FROM leader_teammates l1
    JOIN leaders_stats l2 ON l1.teamId = l2.teamId),
    
    -- getting the stats for each athlete
    
    teammates_stats AS
    (SELECT t.teamId, t.athleteName, t.positionParent, t.positionName, t.headshot, as.*
    FROM teammates AS t
    JOIN athletes_stats AS as ON as.athleteId=t.athleteId
    WHERE as.name IN (SELECT Forte FROM top)
    ORDER BY teamId),
    
    -- getting the average value for each metric
    
    metric_average AS
    (SELECT ROUND(AVG(value),4) as averageValue, name as metricName, category
    FROM athletes_stats
    WHERE name IN (SELECT Forte FROM top) and value != 0
    GROUP BY name, category)
    
    -- main query 
    
    SELECT ma.averageValue, ts.*
    FROM metric_average ma
    JOIN teammates_stats ts ON (ts.name=ma.metricName and ts.category=ma.category)
    
    EXCEPT 
    
    SELECT ma.averageValue, ts.*
    FROM metric_average ma
    JOIN teammates_stats ts ON ts.name=ma.metricName
    WHERE ts.category='Passing' and ts.name IN ('sacks','interceptions')
    """
)
# Write to csv --> make this big query

# teammates_all.write.option("header", True).mode("overwrite").csv(
#     os.path.join(f"./dashboards/{year}/{season_type}/", "dashboard1_scatter")


BQ_DATASET="nfl_data_all"
teammates_all.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.leaders_teammates_{year}_{season_type}") \
    .save()

#===================================================================================



# Dashboard 2: Radar chart - weakness/strength

df_teams_stats_clean.createOrReplaceTempView("teams_stats")
df_teams_clean.createOrReplaceTempView("teams")

stats = spark.sql(
    """
    SELECT ts.name, ts.abbreviation, ts.value, ts.category, ts.teamId, t.displayName, ts.rank, PERCENT_RANK() OVER(
                        PARTITION BY ts.name
                        ORDER BY rank DESC)*100 AS percentileRank, 
                        t.logo 
    FROM teams_stats ts
    JOIN teams t ON t.teamId=ts.teamId
    WHERE ts.category IN ('General','Passing','Rushing','Receiving', 'Kicking') and ts.name IN ("fumbles",
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
    
    SELECT ts.name, ts.abbreviation, ts.value, ts.category, ts.teamId, t.displayName, ts.rank, PERCENT_RANK() OVER(
                        PARTITION BY ts.name
                        ORDER BY rank DESC)*100 AS percentileRank, 
                        t.logo 
    FROM teams_stats ts
    JOIN teams t ON t.teamId=ts.teamId
    WHERE ts. category IN ('Defensive', 'Defensive Interceptions', 'Scoring') and ts.name IN (
    "totalTackles",
    "avgSackYards", 
    "avgStuffYards",
    "avgInterceptionYards",
    "interceptions",
    "passingTouchdowns",
    "receivingTouchdowns",
    "returnTouchdowns",
    "rushingTouchdowns",
    "interceptionTouchdowns",
    "totalPointsPerGame")
    
    ORDER BY category, name, percentileRank DESC
    """
)

# Write to big query
# stats.write.mode("overwrite").option("header", True).csv(
#     os.path.join(f"./dashboards/{year}/{season_type}", "dashboard2_radar")
# )

BQ_DATASET="nfl_data_all"
stats.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.radar_stats_{year}_{season_type}") \
    .save()

#=======================================================================

# View for defense stats
df_defense_stack.createOrReplaceTempView("addtl_defense_stats")

defense_stats = spark.sql(
    """
    SELECT t.teamId,ads.*
    FROM addtl_defense_stats ads
    JOIN teams t ON ads.team=t.displayName
    """
)

defense_stats.createOrReplaceTempView("defense_stats")

# Making dense rank
dense_rank = spark.sql(
    """
    SELECT teamId, category, name, value,
            DENSE_RANK() OVER(
                PARTITION BY name
                ORDER BY value DESC) as valuesDenseRank
    FROM teams_stats
    WHERE category IN ('Passing', 'Rushing', 'Receiving') AND name IN (
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
    SELECT teamId, category, name, value,
            DENSE_RANK() OVER(
                PARTITION BY name
                ORDER BY value DESC) as valuesDenseRank
    FROM teams_stats
    WHERE category IN ('General','Defensive', 'Defensive Interception') and name IN ('fumblesForced','fumblesTouchdowns','passesDefended',
    'avgInterceptionYards',
    'avgSackYards',
    'avgStuffYards',
    'tacklesForLoss',
    'defensiveTouchdowns'
    )
    UNION ALL
    SELECT teamId, category, name, value,
        DENSE_RANK() OVER(
                PARTITION BY name
                ORDER BY value) as valuesDenseRank
    FROM defense_stats
    """
)

dense_rank.createOrReplaceTempView("dense_rank")

# Making the offense and defense rank
classify = spark.sql(
    """
    WITH defense AS
    (SELECT dr.teamId,
            AVG(dr.valuesDenseRank) as defenseAvgRank
    FROM dense_rank dr
    WHERE category IN ('General','Defensive', 'Defensive Interception')
    GROUP BY dr.teamId
    ),
    
    offense AS
    (
    SELECT dr.teamId,
            AVG(dr.valuesDenseRank) as offenseAvgRank
    FROM dense_rank dr
    WHERE category IN ('Passing', 'Rushing', 'Receiving', 'Scoring')
    GROUP BY dr.teamId
    )
    
    SELECT o.teamId,t.displayName, t.logo, 
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

# Writing to csv
# classify.write.mode("overwrite").option("header", True).csv(
#     os.path.join(f"./dashboards/{year}/{season_type}/", "dashboard3_window")
# )

BQ_DATASET="nfl_data_all"
classify.write.format('bigquery') \
    .mode('overwrite') \
    .option('table', f"{BQ_DATASET}.best_worst_teams_{year}_{season_type}") \
    .save()

