module NBA

using Dates, DataFrames, SQLite, Chain

import StatsBase: weights, std
import Statistics.std
import Statistics.mean
# import Optim: NelderMead, optimize
# import QuadGK: quadgk
# import SpecialFunctions: erf

export num_games_per_year, num_teams_per_year


db = SQLite.DB( joinpath(@__DIR__,"..","nba.sqlite"))



## a dictionary of all teams
team_name_df = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_TEAM_NAMES;"));

date_and_ids = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_GAME_DATES"));


function getNumGames()
  DataFrame(DBInterface.execute(db,"SELECT COUNT(*) FROM NBA_SCORES"))[1,1]
end

# a sql statement that changes _ids to names and dates
joined_stmt = """SELECT DATE, NBA_GAME_DATES.SEASON, VTN.NAME AS VISITOR_TEAM, VISITOR_SCORE, HTN.NAME AS HOME_TEAM, HOME_SCORE, OTS, PLAYOFF FROM NBA_SCORES 
      JOIN NBA_GAME_DATES ON NBA_SCORES.DATE_ID = NBA_GAME_DATES.DATE_ID 
      JOIN NBA_TEAM_NAMES as VTN ON NBA_SCORES.VISITOR_ID = VTN.TEAM_ID
      JOIN NBA_TEAM_NAMES AS HTN ON NBA_SCORES.HOME_ID = HTN.TEAM_ID"""

std_stmt = "SELECT * FROM NBA_SCORES JOIN NBA_GAME_DATES ON NBA_SCORES.DATE_ID = NBA_GAME_DATES.DATE_ID"

"""
    getSeason(year)

Gets the season for the year `year`.  The resulting DataFrame containing every game in the season.  If the year is not in the database an error is returned. 

There are two options for the function, if `prettify` is true (default), then the team names and dates are human readable.  If false, then
the internal ids are used for teams and dates. Note: this is used for score-streaks. 

The boolean option `include_playoffs` is used to include playoffs in the season or not (default).  

# Examples
```julia-repl
julia> NBA.getSeason(2018)

julia> NBA.getSeason(2018, prettify = false)

julia> NBA.getSeason(2018, include_playoffs = true)
```
"""
function getSeason(year::Integer; prettify=true, include_playoffs = false)
  1955<=year<=2020 || throw(ArgumentError("The season $year is not in the database."))
  
  # either join the date and team databases for dates and named teams or else use ids for everything
  local stmt = (prettify ? joined_stmt : std_stmt) * " WHERE NBA_GAME_DATES.SEASON=$year" *
    (include_playoffs ? "" : " AND PLAYOFF='N'")

  df = DataFrame(DBInterface.execute(db,stmt))
 
  # for the 'pretty' version, turn the date into an Date not a string.
  prettify ? transform!(df,:DATE => (d-> Date.(d, "yyyy-mm-dd")) => :DATE) : select(df,Not(:DATE_ID_1))
end

"""
    `getSeason(team_id, year)`

Gets the season for the given `team_id` in the year `year`.  Returns the season results 
as a DataFrame.  If the year is not in the database an error is returned.  If the team is not in the database, an empty dataframe
is returned.  

# Examples
```julia-repl
julia> NBA.getSeason(60,2020)
```
"""
function getSeason(team::Int,year::Integer; prettify = true, include_playoffs = false)
  1955<=year<=2020 || throw(ArgumentError("The season $year is not in the database."))
  local stmt = (prettify ? joined_stmt : std_stmt) * " WHERE NBA_GAME_DATES.SEASON=$year AND (VISITOR_ID = $team OR HOME_ID = $team)" *
    (include_playoffs ? "" : " AND PLAYOFF='N'")
    
  df = DataFrame(DBInterface.execute(db,stmt))
  nrow(df) > 0 || throw(ArgumentError("The team with id $team is not in the season $year"))
  prettify ? transform!(df,:DATE => (d-> Date.(d, "yyyy-mm-dd")) => :DATE) : select(df,Not(:DATE_ID_1))
end

"""
    `getSeason(team_name, year)`

Gets the season for the given `team_name` in the year `year`.  Returns the season results 
as a DataFrame.  If the year is not in the database an error is returned.  If the team is not in the database,
an error is thrown.

# Examples
```julia-repl
julia> NBA.getSeason("Washington Wizards",2020)
```
"""
function getSeason(team_name::String,year::Integer; prettify = true, include_playoffs = false)
  team = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_TEAM_NAMES WHERE NAME='$team_name' AND SEASON=$year"))
  nrow(team)==1 || throw(ArgumentError("The '$team_name' did not play during the $year season"))
  getSeason(team[1,1],year, prettify = prettify, include_playoffs = include_playoffs)
end



"""
    getRegularSeasonRecord(team_id::Int, year::Int)

Returns the record of `team_id` in the year `year` as a named tuple as (W=wins,L=losses) 

# Examples
```julia-repl
julia> NBA.getRegularSeasonRecord(20,1995)
(77,67)

julia> NBA.getRegularSeasonRecord(35,1920)
(72, 81, 1)

```
"""
function getRegularSeasonRecord(team_id::Int,season::Int)
  local reg_season = getSeason(team_id,season,include_playoffs=false,prettify = false)
  num_wins = @chain reg_season begin
    transform([:VISITOR_ID,:VISITOR_SCORE,:HOME_ID,:HOME_SCORE] => 
      ByRow((visitor_id, visitor_score, home_id, home_score) -> (visitor_score > home_score ? visitor_id : home_id)) => :WINNING_TEAM_ID)
    subset(:WINNING_TEAM_ID => w-> w .== team_id)
    nrow
  end
  (W=num_wins, L = nrow(reg_season)-num_wins)
end


"""
    getRegularSeasonRecord(team_name::String, _year::Int)

Returns the record of `team_name` in the year `_year` as a named tuple as (W=wins,L=losses)

# Examples
```julia-repl
julia> NBA.getRegularSeasonRecord("Colorado Rockies",1995)
(77,67)

julia> NBA.getRegularSeasonRecord("Boston Red Sox",1920)
(72, 81, 1)

```
"""
function getRegularSeasonRecord(team_name::String,year::Int)
  team = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_TEAM_NAMES WHERE NAME='$team_name' AND SEASON=$year"))
  nrow(team)==1 || throw(ArgumentError("The '$team_name' did not play during the $year season"))
  getRegularSeasonRecord(team[1,1],year)
end



"""
  getTeams(year::Int)

returns an array of the names of the teams in the given year.
```julia-repl
julia> NBA.getTeams(2020)
```
"""
function getTeams(year::Int; show_only_names = true)
  1950 <= year <= 2020 || throw(ArgumentError("The season $year is not in the database."))
  team_df = DataFrame(DBInterface.execute(db,"SELECT TEAM_ID, NAME FROM NBA_TEAM_NAMES WHERE SEASON=$year"))

  show_only_names ? sort(team_df.NAME) : team_df
end

"""
  getSeasonsForTeam(team::String)

returns an array of seasons for which the `team` played. 
```julia-repl
julia> NBA.getSeasonsForTeam("New Orleans Jazz")

5-element Vector{Int64}:
 1975
 1976
 1977
 1978
 1979
```
"""
function getSeasonsForTeam(team::String)
  df = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_TEAM_NAMES WHERE NAME='$team'"))
  df.SEASON
end

"""
  getRegularSeasonSchedule(team_name,season)

returns the Season Schedule as a DataFrame as is commonly seen for a given team. 
```julia-repl
julia> getRegularSeasonSchedule("Boston Celtics",2010)
```
"""
function getRegularSeasonSchedule(team_name::String, season::Int)
  local team_id = team_name_to_id[team_name]
  sched = @chain getSeason(team_id,season, prettify = false) begin
    select(
      :DATE_ID,
      [:VISITOR_ID,:HOME_ID,:VISITOR_SCORE,:HOME_SCORE] => ByRow(
        (v_id,h_id,v_score,h_score)->(visitor_id=v_id,home_id=h_id,visitor_score=v_score,home_score=h_score)) => :game,
      :OTS
    )
    select(
      :DATE_ID => (g_id -> gameIDtoDate.(g_id)) => :Date,
      :game => ByRow(g -> g.visitor_id == team_id ? g.home_id : g.visitor_id )=> :opp_id,
      :game => ByRow(g -> g.visitor_id == team_id ? "Away" : "Home") => :home_away,
      :game => ByRow(g -> g.visitor_id == team_id ? g.visitor_score : g.home_score ) => :team_score,
      :game => ByRow(g -> g.visitor_id == team_id ? g.home_score : g.visitor_score ) => :opp_score,
      :game => ByRow(g -> 
        g.visitor_id == team_id && g.visitor_score > g.home_score ||
        g.home_id == team_id && g.home_score > g.visitor_score
        ? "W" : "L") => :W_L
      )
  end
  sched.wins = map(n-> count(x-> x=="W",sched.W_L[1:n]),1:nrow(sched));
  sched.losses = map(n-> count(x-> x=="L",sched.W_L[1:n]),1:nrow(sched));
  sched
end


"""
  buildPairs(season::DataFrame)

this takes a `DataFrame` and builds a new dataframe to be able to detect order-2 streaks. 

Note: the `season` must have the columns `GAME_ID, VISITOR_ID, VISITOR_SCORE, HOME_ID, HOME_SCORE`.
"""
function buildPairs(season::DataFrame)
  s=copy(season)
  s[!,:game_id1]=0:(nrow(s)-1)
  s[!,:game_id2]=1:nrow(s)
  @chain s begin
    innerjoin(s,on=:game_id2 => :game_id1, makeunique=true)
    select( :SEASON,
      :DATE_ID => :DATE_ID1, :HOME_ID => :HOME_ID1, :HOME_SCORE => :HOME_SCORE1, :VISITOR_ID => :VISITOR_ID1, :VISITOR_SCORE => :VISITOR_SCORE1, 
      :DATE_ID_1 => :DATE_ID2, :HOME_ID_1 => :HOME_ID2, :HOME_SCORE_1 => :HOME_SCORE2, :VISITOR_ID_1 => :VISITOR_ID2, :VISITOR_SCORE_1 => :VISITOR_SCORE2, 
    )
  end
end

function filterOrder2()
  [:VISITOR_ID1, :VISITOR_SCORE1, :HOME_ID1, :HOME_SCORE1, :VISITOR_ID2, :VISITOR_SCORE2, :HOME_ID2, :HOME_SCORE2] => 
    (vid1,vsc1,hid1,hsc1,vid2,vsc2,hid2,hsc2)-> 
      (vsc1 .== vsc2 .&& hsc1 .== hsc2 .&& (vid1 .== vid2 .|| hid1 .== hid2)) .||
      (vsc1 .== hsc2 .&& hsc1 .== vsc2 .&& (vid1 .== hid2 .|| vid2 .== hid1))
end




"""
  order2StreaksSeason(seasons)

Is a general way to return all order2 streaks for the seasons passed in.  
Generally the seasons are per-team seasons and can be historic or simulated. 

"""
function order2StreaksSeason(seasons::Vector{DataFrame})
  df = subset(buildPairs(seasons[1]),filterOrder2())
  for i=2:length(seasons)
    append!(df,subset(buildPairs(seasons[i]),filterOrder2()))
  end
  df
end

"""
  order2Streak(team_id::Int,year::Int)

returns a DataFrame of order-2 streaks for the team_id in the given year.
"""
function order2Streaks(team_id::Int,year::Int; prettify = false)
  o2 = @chain getSeason(team_id,year, prettify = false) begin
    buildPairs()
    subset(filterOrder2())
  end
  prettify ? prettifyOrder2(o2) : o2
end


"""
  order2Streak(team_name::String,year::Int)

returns a DataFrame of order-2 streaks for the given team in the given year.
"""
function order2Streaks(team_name::String,year::Int; prettify = false)
  team = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_TEAM_NAMES WHERE NAME='$team_name' AND SEASON=$year"))
  nrow(team)==1 || throw(ArgumentError("The '$team_name' did not play during the $year season"))

  order2Streaks(team[1,1],year, prettify = prettify)
end




  
"""
  order2Streaks(year)

returns a DataFrame of all order-2 streaks in the given year. 
"""
function order2Streaks(year::Int)
  team_ids = getTeams(year, show_only_names = false).TEAM_ID
  order2StreaksSeason(map(id -> getSeason(id,year, prettify = false),team_ids))
end

"""
  allOrder2StreaksHistoric(starting_year::Int)

  returns a DataFrame with all order-2 streaks starting with the `starting_year` season
"""
function allOrder2StreaksHistoric(starting_year::Int)
  1955 <= starting_year <=  2020 || throw(ArgumentError("The starting year must be between 1955 and 2020"))
  order2streaks = @chain order2StreaksHistoric(starting_year) begin
    transform([] => (() -> starting_year) => :season)
  end
  for year=(starting_year+1):2020
    o2 = @chain order2StreaksHistoric(year) begin
      transform([] => (() -> starting_year) => :season)
    end
    if nrow(o2)>0
      append!(order2streaks,o2)
    end
  end
  prettifyOrder2(order2streaks)
end
    

"""
  allOrder2StreaksHistoric()

  returns a DataFrame with all order-2 streaks starting with the 1950 season
"""
# function allOrder2StreaksHistoric()
#   allOrder2StreaksHistoric(1950)
# end




# function gameIDtoDate(x::DataValues.DataValue{Int64})
#   gameIDtoDate(getindex(x))
# end


function gameIDtoDate(id::Int)
  DBInterface.execute(db,"SELECT DATE FROM NBA_GAME_DATES WHERE DATE_ID = $id;") |> DataFrame |> df->df[1,1]
end

"""
  prettifyOrder2(df::DataFrame)

changes the DataFrame with team ids and game ids over to team names and game dates. 
"""
function prettifyOrder2(df::DataFrame)
  team_names = select(team_name_df,Not(:SEASON))
  date_df = select(date_and_ids,Not(:SEASON))
  @chain df begin
    innerjoin(team_names, on=:VISITOR_ID1 => :TEAM_ID, makeunique = true)
    rename(:NAME => :VISITOR_TEAM1)
    innerjoin(team_names, on=:HOME_ID1 => :TEAM_ID)
    rename(:NAME => :HOME_TEAM1)
    innerjoin(team_names, on=:HOME_ID2 => :TEAM_ID)
    rename(:NAME => :HOME_TEAM2)
    innerjoin(team_names, on=:VISITOR_ID2 => :TEAM_ID)
    rename(:NAME => :VISITOR_TEAM2)
    innerjoin(date_df, on= :DATE_ID1 => :DATE_ID, makeunique = true)
    rename(:DATE => :GAME_DATE1)
    innerjoin(date_df, on= :DATE_ID2 => :DATE_ID)
    rename(:DATE => :GAME_DATE2)
    select(:SEASON,:GAME_DATE1, :HOME_TEAM1, :HOME_SCORE1, :VISITOR_TEAM1, :VISITOR_SCORE1, :GAME_DATE2, :HOME_TEAM2, :HOME_SCORE2, :VISITOR_TEAM2, :VISITOR_SCORE2) 
  end
end


"""
  buildOrder3(season::DataFrame)

this takes a `DataFrame` and builds a new dataframe to be able to detect order-3 streaks. 

Note: the `season` must have the columns `GAME_ID, a1, away1, h1, home1`.
"""
# function buildOrder3(season::DataFrame)
#   s = copy(season)
#   s[!,:gid3] = -1:nrow(s)-2
#   buildOrder2(s) |>
#     @join(s,_.gid1,_.gid3,{_.GAME_ID1,_.gid1,_.a1,_.away1,_.h1,_.home1,
#                             _.GAME_ID2,_.gid2,_.a2,_.away2,_.h2,_.home2,
#                             GAME_ID3 = __.GAME_ID,__.gid3,a3=__.a1,away3=__.away1,h3=__.h1,home3=__.home1}) |>
#     DataFrame
# end


### this is needed for the order3 filter

o3_A = Array{Symbol}(undef,8,3)
o3_B = Array{Symbol}(undef,8,3)
o3_C = Array{Symbol}(undef,8,3)
for i=0:7
  o3_A[i+1,:] = map((x,y)-> Symbol(string(x,y)),map(x->x==0 ? "a" : "h",digits(i,base=2,pad=3)),[1,2,3])
  o3_B[i+1,:] = map((x,y)-> Symbol(string(x,y)),map(x->x==0 ? "away" : "home", digits(i,base=2,pad=3)),[1,2,3])
  o3_C[i+1,:] = map((x,y)-> Symbol(string(x,y)),map(x->x==0 ? "home" : "away", digits(i,base=2,pad=3)),[1,2,3])
end

function order3(r)
  for i=1:8
    a = map(x->getfield(r,x),o3_A[i,:])
    b = map(x->getfield(r,x),o3_B[i,:])
    c = map(x->getfield(r,x),o3_C[i,:])
    if all(x->x == a[1],a[2:3]) && all(x->x == b[1],b[2:3]) && all(x->x == c[1],c[2:3]) 
      return true
    end
  end
  false
end

"""
  order3StreaksSeason(seasons)

Is a general way to return all order3 streaks for the seasons passed in.  
Generally the seasons are per-team seasons and can be historic or simulated. 

"""
# function order3StreaksSeason(seasons::Array{DataFrame,1})
#   df = buildOrder3(seasons[1]) |> @filter(order3(_)) |> DataFrame
#   for i=2:length(seasons)
#     append!(df,buildOrder3(seasons[i]) |> @filter(order3(_)) |> DataFrame)
#   end
#   df
# end



"""
  order3StreaksHistoric(year::Int)

returns a dataframe with information about all order-3 streaks in the given year. 
"""
function order3StreaksHistoric(year::Int)
  teams = getTeamIDs(year)
  order3StreaksSeason(map(team_id->getSeason(team_id,year),teams))
#   streaks = buildOrder3(getSeason(teams[1],year)) |> @filter(order3(_)) |> DataFrame
#   for i=2:length(teams)
#     append!(streaks,MLB.buildOrder3(getSeason(teams[i],year)) |> @filter(order3(_)) |> DataFrame)
#   end
#   streaks
end

"""
  allOrder3StreaksHistoric()

  returns a DataFrame with all order-3 streaks
"""
function allOrder3StreaksHistoric()
  df = order3StreaksHistoric(1901)
  for year=1902:2019
    append!(df,order3StreaksHistoric(year))
  end
  df
end


# function prettifyOrder3(df::DataFrame)
#   df |> @mutate(at1 = team_short_name[_.a1],ht1 = team_short_name[_.h1],
#                 at2 = team_short_name[_.a2],ht2 = team_short_name[_.h2],
#                 at3 = team_short_name[_.a3],ht2 = team_short_name[_.h3],
#                 date1 = Date(gameIDtoDate(_.GAME_ID1),"y-m-d"),
#                 date2 = Date(gameIDtoDate(_.GAME_ID2),"y-m-d"),
#                 date3 = Date(gameIDtoDate(_.GAME_ID3),"y-m-d")) |> 
#    @select(:date1,:at1,:away1,:ht1,:home1,:date2,:at2,:away2,:ht2,:home2,:date3,:at3,:away3,:ht3,:home3,-:a1) |>
  
#   DataFrame
# end


### ORDER 4 

ord4_A = Array{Symbol}(undef,2^4,4)
ord4_B = Array{Symbol}(undef,2^4,4)
ord4_C = Array{Symbol}(undef,2^4,4)
for i=0:15
  ord4_A[i+1,:] = map((x,y)-> Symbol(string(x,y)),map(x->x==0 ? "a" : "h",digits(i,base=2,pad=4)),[1,2,3,4])
  ord4_B[i+1,:] = map((x,y)-> Symbol(string(x,y)),map(x->x==0 ? "away" : "home", digits(i,base=2,pad=4)),[1,2,3,4])
  ord4_C[i+1,:] = map((x,y)-> Symbol(string(x,y)),map(x->x==0 ? "home" : "away", digits(i,base=2,pad=4)),[1,2,3,4])
end

function order4(r)
  for i=1:2^4
    a = map(x->getfield(r,x),ord4_A[i,:])
    b = map(x->getfield(r,x),ord4_B[i,:])
    c = map(x->getfield(r,x),ord4_C[i,:])
    if all(x->x == a[1],a[2:4]) && all(x->x == b[1],b[2:4]) && all(x->x == c[1],c[2:4]) 
      return true
    end
  end
  false
end

"""
  buildOrder4(season::DataFrame)

this takes a `DataFrame` and builds a new dataframe to be able to detect order-3 streaks. 

Note: the `season` must have the columns `GAME_ID, a1, away1, h1, home1`.
"""
# function buildOrder4(season::DataFrame)
#   s = copy(season)
#   s[!,:gid3] = -1:nrow(s)-2
#   s[!,:gid4] = -2:nrow(s)-3
#   s2 = buildOrder2(s) |>
#     @join(s,_.gid1,_.gid3,{_.GAME_ID1,_.gid1,_.a1,_.away1,_.h1,_.home1,
#                             _.GAME_ID2,_.gid2,_.a2,_.away2,_.h2,_.home2,
#                             GAME_ID3 = __.GAME_ID,__.gid3,a3=__.a1,away3=__.away1,h3=__.h1,home3=__.home1}) |>
#     DataFrame
#   s2 |> @join(s,_.gid1,_.gid4,{_.GAME_ID1,_.gid1,_.a1,_.away1,_.h1,_.home1,
#                                _.GAME_ID2,_.gid2,_.a2,_.away2,_.h2,_.home2,
#                                _.GAME_ID3,_.gid3,_.a3,_.away3,_.h3,_.home3,
#                                 GAME_ID4 = __.GAME_ID,__.gid4,a4=__.a1,away4=__.away1,h4=__.h1,home4=__.home1}) |>
#     DataFrame
# end

"""
  order4StreaksSeason(seasons)

Is a general way to return all order4 streaks for the seasons passed in.  
Generally the seasons are per-team seasons and can be historic or simulated. 

"""
# function order4StreaksSeason(seasons::Array{DataFrame,1})
#   df = buildOrder4(seasons[1]) |> @filter(order4(_)) |> DataFrame
#   for i=2:length(seasons)
#     append!(df,buildOrder4(seasons[i]) |> @filter(order4(_)) |> DataFrame)
#   end
#   df
# end


function get_all_scores()
    DataFrame(DBInterface.execute(db, "SELECT H_SCORE,V_SCORE FROM NBA_SCORES"))
end

function run_total(str::String, num::Int)
  DataFrame(DBInterface.execute(db, "SELECT COUNT($str) FROM NBA_SCORES WHERE $str = $num"))[1,1]
end


### Develop distributions

# function get_run_distribution_per_year()
#   run_dist_per_year = []
#   for i=1901:2019
#     season = MLB.get_season(i) |> @map({score = (_.V_SCORE,_.H_SCORE)}) |> DataFrame
#     run_dist_matrix = [count(a->a==(i,j),season[:score]) for i=0:30,j=0:30];
#     run_dist_vector = reshape(run_dist_matrix,961);
#     push!(run_dist_per_year,Categorical(run_dist_vector/sum(run_dist_vector)))
#   end
#   run_dist_per_year
# end

"""
  loadOrder2Streaks()

returns all order-2 same score streaks as a `DataFrame`.  This loads the results from the sqlite database.
"""
# function loadOrder2Streaks()
#     DBInterface.execute(db,"SELECT * FROM ORDER2") |> 
#     DataFrame |> 
#     @rename(:A1=>:a1,:AWAY1=>:away1,:H1=>:h1,:HOME1=>:home1,
#             :A2=>:a2,:AWAY2=>:away2,:H2=>:h2,:HOME2=>:home2) |>
#     DataFrame |>
#     prettifyOrder2 |>
#     DataFrame
# end

"""
  loadOrder3Streaks()

returns all order-3 same score streaks as a `DataFrame`.  This loads the results from the sqlite database.
"""
# function loadOrder3Streaks()
#     DBInterface.execute(db,"SELECT * FROM ORDER3") |> 
#     DataFrame |> 
#     @rename(:A1=>:a1,:AWAY1=>:away1,:H1=>:h1,:HOME1=>:home1,
#             :A2=>:a2,:AWAY2=>:away2,:H2=>:h2,:HOME2=>:home2,
#             :A3=>:a3,:AWAY3=>:away3,:H3=>:h3,:HOME3=>:home3) |>
#     DataFrame |>
#     prettifyOrder3 |>
#     DataFrame
# end 


#### Fits scoring distribution to normals and skew normals

"""
GamePoint(x::Int,num::Int) store the distribution as pair of values x (the number of points scores)
and `num`, the number of games that occured with that value. 

"""
struct GamePoint
  x::Int
  num::Int
end


xvals(gpts::Vector{GamePoint}) = map(gp->gp.x,gpts)
numgames(gpts::Vector{GamePoint}) = map(gp->gp.num,gpts)
frgames(gpts::Vector{GamePoint}) = map(gp->gp.num,gpts)/sum(map(gp->gp.num,gpts))
mean(gpts::Vector{GamePoint}) = mean(xvals(gpts),weights(numgames(gpts)))
std(gpts::Vector{GamePoint}) = std(xvals(gpts),weights(numgames(gpts)))

# function numGameArray(all_seasons::DataFrame, diff::Int)
#   d10 = all_seasons |> 
#     @filter(_.diff==diff) |>
#     @groupby(_.adj_home) |>
#     @map({score=key(_),num=length(_)}) |>
#     @orderby(_.score) |>
#     DataFrame
#   df = append!(DataFrame([(score=i,num=0) for i=1:200]),d10) |>
#     @orderby(_.score) |> 
#     @groupby(_.score) |>
#     @map({score=key(_),num=sum(_.num)}) |>
#     DataFrame;
#   sc = df[!,:score]
#   nums = df[!,:num]
#   [GamePoint(sc[i],nums[i]) for i=1:length(sc)]
# end

### Change mean/standard deviation of the distribution

ϕ(x)=exp(-x^2/2)/sqrt(2*pi)
Φ(x)=0.5*(1+erf(x/sqrt(2)))
skew_normal(x::Real;μ::Real=0,σ::Real=1,α::Real=0) = (2/σ)*ϕ((x-μ)/σ)*Φ(α*(x-μ)/σ)
skew_norm_cdf(x::Real;μ::Real=0,σ::Real=1,α::Real=0) = quadgk(x->skew_normal(x,μ=μ,σ=σ,α=α),x-0.5,x+0.5)[1]
dis_skew_normal(x::Vector{T};μ::Real=0,σ::Real=1,α::Real=0) where {T <: Real} = skew_norm_cdf.(x,μ=μ,σ=σ,α=α)

# params_db = SQLite.DB("params.sqlite")

"""
this gets the parameters of each of skew normal for each slices through the fitted distribution
"""
function getParams(diff::Int)
  DBInterface.execute(SQLite.DB("params.sqlite"),"SELECT * from params WHERE diff=$diff;") |> 
    DataFrame |> 
    df -> (fract=df[1,:fract],μ = df[1,:mean], σ = df[1,:std], α = df[1,:skew])
end


"""
this returns a distribution (as a Categorical Distribution) for a given mean and standard deviation

"""
# function simulatedDistribution(mean::Real,sd_scale::Real)
  
#   # get the fractions of each of the slices
#   fr= append!([getParams(k).fract for k=-45:-1],[0],[getParams(k).fract for k=1:45])
  
#   # sum of squares with the fractions around 0 removed
#   hsumsq(x::Vector{T}) where {T <: Real } =   sum(abs2,append!(fr[1:44],fr[48:end])-dis_skew_normal(collect(-42:45),μ=x[1],σ=x[2],α=x[3]))
  
#   # fit a skew normal to the 
#   res = optimize(hsumsq,[0,10.0,1.0],NelderMead())
  

#   fr2 = dis_skew_normal(collect(-50:50),μ=res.minimizer[1]-mean+100,σ=sd_scale*res.minimizer[2],α=res.minimizer[3])
#   ## inject the dip near diff=0 
#   fr3 = append!(fr2[1:49],[0.8*fr2[49],0,0.8*fr2[50]],fr2[50:end])
#   fr4 = fr3/(sum(fr3));
#   fr_df = [(diff=k, fract = fr4[k+51]) for k=-50:50] |> DataFrame;
  
#   @show "calculating new distribution"
#   new_dist = DataFrame(home=Int[],visitor=Int[],prob=Float64[])
#   for diff in (-45:45 |> @filter( _ != 0) |> collect)
#     p = getParams(diff)
#     if diff % 5 == 0 @show diff end
# #     fr = fr_df |> @filter(_.diff==diff) |> DataFrame |> df->df[1,:fract]
#     append!(new_dist, DataFrame(home=1:200,visitor=1-diff:200-diff,prob=p.fract*dis_skew_normal(collect(1:200),μ=p.μ,σ=sd_scale*p.σ,α=p.α)))
#   end
  
#   @show "developing the probability distribution"
#   prob_dist = zeros(Float64,200,200)
#   for i=1:200
#     for j=1:200
#       pro = new_dist |> @filter(_.home==j && _.visitor==i) |> DataFrame
#       if nrow(pro) == 1
#         prob_dist[i,j] = pro[1,:prob]
#       end
#     end
#   end
#   v4 = reshape(prob_dist,200^2)
#   Categorical(v4/sum(v4))
# end


end # module 