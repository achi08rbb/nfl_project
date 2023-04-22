Team
not needed

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

"CAST(name AS string) as name",
"CAST(displayName AS string) as displayName",
"CAST(shortDisplayName AS string) as shortDisplayName",
"CAST(description AS string) as description",
"CAST(abbreviation AS string) as abbreviation",
"CAST(value AS double) as value",
"CAST(displayValue AS string) as displayValue",
"CAST(rank AS double) as rank",
"CAST(rankDisplayValue AS string) as rankDisplayValue",
"CAST(perGameValue AS double) as perGameValue",
"CAST(perGameDisplayValue AS string) as perGameDisplayValue",
"CAST(`split.categories.displayName` AS string) as statCategory",
"CAST(athleteId AS string) as athleteId"