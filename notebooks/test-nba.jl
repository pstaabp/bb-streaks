using Test, DataFrames
include("NBA.jl")

@testset "NBA Teams                           " begin
  @test isa(NBA.getTeams(1967),Vector{String}) 
  @test length(NBA.getTeams(1967)) == 10
  @test NBA.getTeams(1967)==["Baltimore Bullets", "Boston Celtics", "Chicago Bulls", "Cincinnati Royals", "Detroit Pistons", "Los Angeles Lakers", "New York Knicks", "Philadelphia 76ers", "San Francisco Warriors", "St. Louis Hawks"]
  @test isa(NBA.getTeams(1967,show_only_names = false),DataFrame)
  @test size(NBA.getTeams(1967, show_only_names = false))==(10,2)
end

@testset "NBA Regular Seasons                 " begin
  @test nrow(NBA.getSeason("Miami Heat",2020)) == 73
  @test nrow(NBA.getSeason("Golden State Warriors",2020)) == 65
  @test nrow(NBA.getSeason("Boston Celtics",2019)) == 82
  @test nrow(NBA.getSeason("Detroit Pistons",2012)) == 66
  @test nrow(NBA.getSeason("Atlanta Hawks",1999)) == 50
  @test nrow(NBA.getSeason("Kansas City Kings",1980)) == 82
  @test nrow(NBA.getSeason("Phoenix Suns",1978)) == 82
  @test nrow(NBA.getSeason("Philadelphia 76ers",1966)) == 80
  @test nrow(NBA.getSeason("Minneapolis Lakers",1960)) == 75
end

@testset "NBA Full Seasons                    " begin
  @test nrow(NBA.getSeason(1957)) == 288
  @test nrow(NBA.getSeason(1966)) == 360
  @test nrow(NBA.getSeason("Washington Wizards",2005, include_playoffs = true)) == 92
  @test nrow(NBA.getSeason("Miami Heat",2020, include_playoffs = true)) == 94
end

@testset "NBA Seasons that don't exist        " begin
  @test_throws ArgumentError NBA.getSeason(1945)
  @test_throws ArgumentError NBA.getSeason(2021)
  @test_throws ArgumentError NBA.getSeason(35,1960)
  @test_throws ArgumentError NBA.getSeason("Toronto Raptors",1970)
end

# Check a record for each year.  This helps ensure that the loaded data is correct.

@testset "NBA Records                         " begin
  @test NBA.getRegularSeasonRecord("Minneapolis Lakers",     1955) == (W=40,L=32)
  @test NBA.getRegularSeasonRecord("Fort Wayne Pistons",     1956) == (W=37,L=35)
  @test NBA.getRegularSeasonRecord("Rochester Royals",       1957) == (W=31,L=41)
  @test NBA.getRegularSeasonRecord("Cincinnati Royals",      1958) == (W=33,L=39)
  @test NBA.getRegularSeasonRecord("Syracuse Nationals",     1959) == (W=35,L=37)
  
  @test NBA.getRegularSeasonRecord("New York Knicks",        1960) == (W=27,L=48)
  @test NBA.getRegularSeasonRecord("St. Louis Hawks",        1961) == (W=51,L=28)
  @test NBA.getRegularSeasonRecord("Philadelphia Warriors",  1962) == (W=49,L=31)
  @test NBA.getRegularSeasonRecord("Chicago Zephyrs",        1963) == (W=25,L=55)
  @test NBA.getRegularSeasonRecord("San Francisco Warriors", 1964) == (W=48,L=32)
  @test NBA.getRegularSeasonRecord("Baltimore Bullets",      1965) == (W=37,L=43)
  @test NBA.getRegularSeasonRecord("Detroit Pistons",        1966) == (W=22,L=58)
  @test NBA.getRegularSeasonRecord("Chicago Bulls",          1967) == (W=33,L=48)
  @test NBA.getRegularSeasonRecord("San Diego Rockets",      1968) == (W=15,L=67)
  @test NBA.getRegularSeasonRecord("Seattle SuperSonics",    1969) == (W=30,L=52)
  
  @test NBA.getRegularSeasonRecord("Milwaukee Bucks",        1970) == (W=56,L=26)
  @test NBA.getRegularSeasonRecord("Buffalo Braves",         1971) == (W=22,L=60)
  @test NBA.getRegularSeasonRecord("Portland Trail Blazers", 1972) == (W=18,L=64)
  @test NBA.getRegularSeasonRecord("Cleveland Cavaliers",    1973) == (W=32,L=50)
  @test NBA.getRegularSeasonRecord("Phoenix Suns",           1974) == (W=30,L=52)
  @test NBA.getRegularSeasonRecord("Kansas City-Omaha Kings",1975) == (W=44,L=38)
  @test NBA.getRegularSeasonRecord("Los Angeles Lakers",     1976) == (W=40,L=42)
  @test NBA.getRegularSeasonRecord("New Orleans Jazz",       1977) == (W=35,L=47)
  @test NBA.getRegularSeasonRecord("San Antonio Spurs",      1978) == (W=52,L=30)
  @test NBA.getRegularSeasonRecord("San Diego Clippers",     1979) == (W=43,L=39)
  
  @test NBA.getRegularSeasonRecord("Denver Nuggets",         1980) == (W=30,L=52)
  @test NBA.getRegularSeasonRecord("Philadelphia 76ers",     1981) == (W=62,L=20)
  @test NBA.getRegularSeasonRecord("New Jersey Nets",        1982) == (W=44,L=38)
  @test NBA.getRegularSeasonRecord("Atlanta Hawks",          1983) == (W=43,L=39)
  @test NBA.getRegularSeasonRecord("Chicago Bulls",          1984) == (W=27,L=55)
  @test NBA.getRegularSeasonRecord("Golden State Warriors",  1985) == (W=22,L=60)
  @test NBA.getRegularSeasonRecord("Utah Jazz",              1986) == (W=42,L=40)
  @test NBA.getRegularSeasonRecord("Houston Rockets",        1987) == (W=42,L=40)
  @test NBA.getRegularSeasonRecord("Indiana Pacers",         1988) == (W=38,L=44)
  @test NBA.getRegularSeasonRecord("Phoenix Suns",           1989) == (W=55,L=27)
  
  @test NBA.getRegularSeasonRecord("Minnesota Timberwolves", 1990) == (W=22,L=60)
  @test NBA.getRegularSeasonRecord("Orlando Magic",          1991) == (W=31,L=51)
  @test NBA.getRegularSeasonRecord("New York Knicks",        1992) == (W=51,L=31)
  @test NBA.getRegularSeasonRecord("Dallas Mavericks",       1993) == (W=11,L=71)
  @test NBA.getRegularSeasonRecord("Charlotte Hornets",      1994) == (W=41,L=41)
  @test NBA.getRegularSeasonRecord("Boston Celtics",         1995) == (W=35,L=47)
  @test NBA.getRegularSeasonRecord("Chicago Bulls",          1996) == (W=72,L=10)
  @test NBA.getRegularSeasonRecord("Toronto Raptors",        1997) == (W=30,L=52)
  @test NBA.getRegularSeasonRecord("Vancouver Grizzlies",    1998) == (W=19,L=63)
  @test NBA.getRegularSeasonRecord("Washington Wizards",     1999) == (W=18,L=32)
  
  @test NBA.getRegularSeasonRecord("Sacramento Kings",       2000) == (W=44,L=38)
  @test NBA.getRegularSeasonRecord("San Antonio Spurs",      2001) == (W=58,L=24)
  @test NBA.getRegularSeasonRecord("Detroit Pistons",        2002) == (W=50,L=32)
  @test NBA.getRegularSeasonRecord("Cleveland Cavaliers",    2003) == (W=17,L=65)
  @test NBA.getRegularSeasonRecord("New Orleans Hornets",    2004) == (W=41,L=41)
  @test NBA.getRegularSeasonRecord("Los Angeles Lakers",     2005) == (W=34,L=48)
  @test NBA.getRegularSeasonRecord("Charlotte Bobcats",      2006) == (W=26,L=56)
  @test NBA.getRegularSeasonRecord("Milwaukee Bucks",        2007) == (W=28,L=54)
  @test NBA.getRegularSeasonRecord("Miami Heat",             2008) == (W=15,L=67)
  @test NBA.getRegularSeasonRecord("Los Angeles Clippers",   2009) == (W=19,L=63)
  
  @test NBA.getRegularSeasonRecord("Denver Nuggets"         ,2010) == (W=53,L=29)
  @test NBA.getRegularSeasonRecord("Portland Trail Blazers" ,2011) == (W=48,L=34)
  @test NBA.getRegularSeasonRecord("Houston Rockets"        ,2012) == (W=34,L=32)
  @test NBA.getRegularSeasonRecord("Minnesota Timberwolves", 2013) == (W=31,L=51)
  @test NBA.getRegularSeasonRecord("Oklahoma City Thunder",  2014) == (W=59,L=23)
  @test NBA.getRegularSeasonRecord("Brooklyn Nets",          2015) == (W=38,L=44)
  @test NBA.getRegularSeasonRecord("Golden State Warriors",  2016) == (W=73,L=9)
  @test NBA.getRegularSeasonRecord("Utah Jazz",              2017) == (W=51,L=31)
  @test NBA.getRegularSeasonRecord("Boston Celtics",         2018) == (W=55,L=27)
  @test NBA.getRegularSeasonRecord("New Orleans Pelicans",   2019) == (W=33,L=49)
  @test NBA.getRegularSeasonRecord("Washington Wizards",     2020) == (W=25,L=47)
end

# Check to make sure that the score streak functions picks up all cases.

# this has a single score streak with the case the home team is the same.
sched1 = DataFrame(
  DATE_ID = 1:10,
  SEASON = [1 for i=1:10],
  VISITOR_ID = [1,1,2,3,1,4,2,1,3,1],
  VISITOR_SCORE = [95,95,98,98,110,93,93,118,87,123],
  HOME_ID = [2,4,1,1,3,1,1,3,1,4],
  HOME_SCORE = [94,108,105,105,104,120,100,93,97,105]
)

# this has a single score streak with the case the visiting team is the same.
sched2 = DataFrame(
  DATE_ID = 1:10,
  SEASON = [1 for i=1:10],
  VISITOR_ID = [1,1,2,3,1,4,2,1,3,1],
  VISITOR_SCORE = [95,95,98,121,110,93,93,118,87,123],
  HOME_ID = [2,4,1,1,3,1,1,3,1,4],
  HOME_SCORE = [108,108,105,90,104,120,100,93,97,105]
)

# this is a case where the team is a visitor then a home team in a streak
sched3 = DataFrame(
  DATE_ID = 1:10,
  SEASON = [1 for i=1:10],
  VISITOR_ID = [1,1,2,3,1,4,2,1,3,1],
  VISITOR_SCORE = [95,105,98,121,110,93,93,118,87,123],
  HOME_ID = [2,4,1,1,3,1,1,3,1,4],
  HOME_SCORE = [108,98,105,90,104,120,100,93,97,105]
)

# this is a case where the team is a home team then a visiting team in a streak
sched4 = DataFrame(
  DATE_ID = 1:10,
  SEASON = [1 for i=1:10],
  VISITOR_ID    = [1,    1,   2,   3,   1,   4,   2,   1,  3,   1],
  VISITOR_SCORE = [95, 105,  98, 121, 110,  93,  93, 100, 87, 123],
  HOME_ID       = [2,    4,   1,   1,   3,   1,   1,   3,  1,   4],
  HOME_SCORE    = [108, 88, 105,  90, 104, 120, 100,  93, 97, 105]
)

@testset "Checking that the score streak works" begin
  @test nrow(subset(NBA.buildPairs(sched1),NBA.filterOrder2())) == 1
  @test nrow(subset(NBA.buildPairs(sched2),NBA.filterOrder2())) == 1
  @test nrow(subset(NBA.buildPairs(sched3),NBA.filterOrder2())) == 1
  @test nrow(subset(NBA.buildPairs(sched4),NBA.filterOrder2())) == 1
end

@testset "Score Streaks for the NBA           " begin
  @test nrow(NBA.order2Streaks("Phoenix Suns", 1969))==1
  @test nrow(NBA.order2StreaksHistoric(1990)) == 3
  @test nrow(NBA.getSeason("Los Angeles Lakers",2000, include_playoffs = true)) == 105
end