Team
not needed

"CAST(location AS string) as teamLocation",
"CAST(name AS string) as teamName",
"CAST(displayName AS string) as teamDisplayName",
"CAST(shortDisplayName AS string) as teamShortDisplayName",
"CAST(isActive AS boolean) as isActive",
"CAST(logos AS string) as teamLogo",
"CAST(teamId AS int) as teamId",
"CAST(year AS int) as year"

Team Stats


types.StructType([
    types.StructField('name', types.StringType(), True), 
    types.StructField('description', types.StringType(), True), 
    types.StructField('abbreviation', types.StringType(), True), 
    types.StructField('value', types.DoubleType(), False), 
    types.StructField('perGameValue', types.DoubleType(), False), 
    types.StructField('rank', types.IntegerType(), False), 
    types.StructField('category', types.StringType(), True), 
    types.StructField('teamId', types.IntegerType(), True), 
    types.StructField('year', types.IntegerType(), False), 
    types.StructField('seasonType', types.IntegerType(), False)
    ])

"CAST(name as string) as name" ,
"CAST(description as string) as description" ,
"CAST(abbreviation as string) as abbreviation" ,
"CAST(value as double) as value" ,
"CAST(perGameValue as double) as perGameValue" ,
"CAST(rank as int) as rank" ,
"CAST(category as string) as category" ,
"CAST(teamId as int) as teamId" ,
"CAST(year as int) as year" ,
seasonType int

"CAST(name as STRING) as name" ,
"CAST(description as STRING) as description" ,
"CAST(abbreviation as string) as abbreviation" ,
"CAST(value as double) as value" ,
"CAST(perGameValue as double) as perGameValue" ,
"CAST(rank as int) as rank" ,
"CAST(category as string) as category" ,
"CAST(teamId as int) as teamId" ,
"CAST(year as int) as year" ,
"CAST(seasonType as int) as seasonType"

Athletes

athleteId: int,
firstName: string,
lastName: string,
fullName: string,
shortName: string,
weight: double,
height: double,
displayHeight: string,
age: double,
dateOfBirth: date,
debutYear: int,
birthPlaceCity: string,
birthPlaceState: string,
birthPlaceCountry: string,
positionId: string,
positionName: string,
experienceYears: int,
statusName: string, 
teamId: int, 
headshot: string,
positionParent: string

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
"CAST(headshot AS string) as headshot"

Athletes Stats

"CAST(name AS string) as statName",
"CAST(displayName AS string) as statDisplayName",
"CAST(shortDisplayName AS string) as statShortDisplayName",
"CAST(description AS string) as statDescription",
"CAST(value AS double) as statValue",
"CAST(rank AS int) as statRank",
"CAST(perGameValue AS double) as perGameValue",
"CAST(`split.categories.displayName` AS string) as statCategory",
"CAST(athleteId AS int) as athleteId"

Leaders

"CAST(displayValue AS string) as leaderDisplayValue",
"CAST(value AS double) as leaderValue",
"CAST(name AS string) as leaderName",
"CAST(shortDisplayName AS string) as leaderShortDisplayName",
"CAST(athleteId AS int) as athleteId"

Defense

"CAST(Team AS string) AS teamName",
"CAST(`('Unnamed: 0_level_0', 'GP')` AS double) AS gamesPlayed",
"CAST(`('Total', 'YDS')` AS double) AS totalYDS",
"CAST(`('Total', 'YDS/G')` AS double) AS totalYDSG",
"CAST(`('Passing', 'YDS')` AS double) AS passingYDS",
"CAST(`('Passing', 'YDS/G')` AS double) AS passingYDSG",
"CAST(`('Rushing', 'YDS')` AS double) AS rushingYDS",
"CAST(`('Rushing', 'YDS/G')` AS double) AS rushingYDSG",
"CAST(`('Points', 'PTS')` AS double) AS points",
"CAST(`('Points', 'PTS/G')` AS double) AS pointsPerGame"


mapping = {
    'teamName','gamesPlaye 'totalYDS',: 'totalYDSG: 'passingYDS',: 'passingYDSG: 'rushingYDS',: 'rushingYDSG: 'points',: 'pointsPerGame',
}


# Dashboard 1 : Who's your extraordinary teammate

df_athletes_clean.createOrReplaceTempView("athletes")
df_athletes_stats_clean.createOrReplaceTempView("athletes_stats")
df_leaders_clean.createOrReplaceTempView("leaders")

# Get the top 1 leader in each metric
leaders = spark.sql(
    """
    WITH stats AS (
    SELECT l.name as Forte, l.leaderValue as ValueForte, a.teamId, l.athleteId, a.shortName as athleteName, a.positionParent, a.positionName
    FROM leaders l
    JOIN athletes a ON a.athleteId=l.athleteId
    WHERE value IN (SELECT max(leaderValue) FROM leaders GROUP BY name)
    ORDER BY Forte DESC, teamId
    )
    
    SELECT athleteName, Forte, leaderValueForte, athleteId, teamId, positionParent, positionName
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
    (SELECT ROUND(AVG(value),4) as averageValue, name as metricName, athleteStatCategory
    FROM athletes_stats
    WHERE name IN (SELECT Forte FROM top) and value != 0
    GROUP BY name, athleteStatCategory)
    
    -- main query 
    
    SELECT ma.averageValue, ts.*
    FROM metric_average ma
    JOIN teammates_stats ts ON (ts.name=ma.metricName and ts.teamStatCategory=ma.athleteStatCategory)
    
    EXCEPT 
    
    SELECT ma.averageValue, ts.*
    FROM metric_average ma
    JOIN teammates_stats ts ON ts.name=ma.metricName
    WHERE ts.teamStatCategory='Passing' and ts.name IN ('sacks','interceptions')
    """
)
# Write to csv --> make this big query

# teammates_all.write.option("header", True).mode("overwrite").csv(
#     os.path.join(f"./dashboards/{year}/{season_type}/", "dashboard1_scatter")