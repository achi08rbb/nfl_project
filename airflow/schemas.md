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
