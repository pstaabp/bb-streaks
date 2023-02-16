# This is a command line script to parse the NBA scores stored in HTML files

using ArgParse, SQLite, YAML, Gumbo, Cascadia, Dates, DataFrames, Downloads, CSV

function parse_commandline()
    s = ArgParseSettings()

    @add_arg_table! s begin
        "--verbose"
            help = "turn on verbose mode"
            action = :store_true
        "--fetch-data" 
            help = "fetch the data from basketball-reference.com"
            action = :store_true
        "--rebuild-db"
            help = "rebuild the sqlite database.  Note: also you need to specific a database file."
            action = :store_true
        "--database-name"
            help = "specify the database name/path"
            arg_type = String
            default = "nba.sqlite"
        "--create-csv"
            help = "(re)create csv files"
            action = :store_true
        "--csv-dir"
            help = "specific the directory where csv files will be stored."
            arg_type = String
            default = "nba-csv"
        "--years"
            help = "year or year range for fetching/parsing.  Examples are '1950..1959' or '2020'"
            arg_type = String
            default = "2020"
        "--html-dir"
            help = "specific the directory where html files will be stored or read from."
            arg_type = String
            default = "nba-raw-html"
            required = true
    end

    return parse_args(s)
end

function main()
  local parsed_args = parse_commandline()
  local verbose = parsed_args["verbose"]
  local years
  local db
  verbose && println("Parsed args:")
  for (arg,val) in parsed_args
    verbose && println("  $arg  =>  $val")
  end
  dbfile = parsed_args["database-name"]
  if parsed_args["rebuild-db"]
    db = rebuildDB(dbfile,verbose)
  end
  m1 = match(r"^(?<year1>\d{4})$",parsed_args["years"])
  m2 = match(r"^(?<year1>\d{4})\.\.(?<year2>\d{4})$",parsed_args["years"])

  if m1 != nothing
    years = [parse(Int,m1["year1"])]
  elseif m2 != nothing
    years = collect(parse(Int,m2["year1"]):parse(Int,m2["year2"]))
  else
    error("The format of the years argument must be a 4-digit year or a range like '1950..1959'")
  end
  if parsed_args["fetch-data"]
    fetchData(parsed_args["html-dir"],years,verbose)
  end
  if parsed_args["rebuild-db"]
    parseData(db,parsed_args["html-dir"],years,verbose)
  end
  if parsed_args["create-csv"]
    createCSVfiles(parsed_args["html-dir"],parsed_args["csv-dir"],years,verbose)
  end
end

function rebuildDB(dbfile::String,verbose::Bool)
  db = SQLite.DB(dbfile)
  verbose && println("Rebuilding the database at $dbfile")
  DBInterface.execute(db,"DROP TABLE IF EXISTS NBA_SCORES")
  DBInterface.execute(db,"DROP TABLE IF EXISTS NBA_TEAM_NAMES")
  DBInterface.execute(db,"DROP TABLE IF EXISTS NBA_GAME_DATES")
  
  verbose && println("Creating table NBA_SCORES")
  DBInterface.execute(db,"""CREATE TABLE IF NOT EXISTS NBA_SCORES
     (GAME_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        DATE_ID          INTEGER     NOT NULL,
        VISITOR_ID       INTEGER     NOT NULL,
        VISITOR_SCORE    INTEGER     NOT NULL,
        HOME_ID          INTEGER     NOT NULL,
        HOME_SCORE       INTEGER     NOT NULL,
        OTS              INTEGER     NOT NULL,
        PLAYOFF          TEXT        NOT NULL
        );""")
  
  verbose && println("Creating table NBA_TEAM_NAMES")
  DBInterface.execute(db,"""CREATE TABLE IF NOT EXISTS NBA_TEAM_NAMES
     (TEAM_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        NAME         TEXT      NOT NULL,
        SEASON       INTEGER   NOT NULL)""")
  DBInterface.execute(db,"CREATE UNIQUE INDEX idx_name_season ON NBA_TEAM_NAMES(NAME,SEASON)")
  
  verbose && println("Creating table NBA_GAME_DATES")
  DBInterface.execute(db,"""CREATE TABLE IF NOT EXISTS NBA_GAME_DATES
     (DATE_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        DATE         TEXT      NOT NULL,
        SEASON       INTEGER   NOT NULL)""")
  DBInterface.execute(db,"CREATE UNIQUE INDEX idx_date_season ON NBA_GAME_DATES(DATE,SEASON)")
  db
end

function writeToDB(db::SQLite.DB,row)
  if row == nothing
    return
  end
  local date = row["date"]
  local season = row["season"]
  local visitor_team = row["vname"]
  local visitor_team_score = row["vpts"]
  local home_team = row["hname"]
  local home_team_score = row["hpts"]
  local ots = row["OTS"]
  local playoffs = row["playoffs"]
  
  
  DBInterface.execute(db,"INSERT OR IGNORE INTO NBA_GAME_DATES (DATE,SEASON) VALUES ('$date', $season)")
  local result = DataFrame(DBInterface.execute(db,"SELECT * FROM NBA_GAME_DATES WHERE DATE = '$date' AND SEASON=$season"))
  local date_id = result[1,1]

  DBInterface.execute(db,"INSERT OR IGNORE INTO NBA_TEAM_NAMES (NAME,SEASON) VALUES ('$visitor_team',$season)")
  result = DataFrame(DBInterface.execute(db, "SELECT * FROM NBA_TEAM_NAMES WHERE NAME = '$visitor_team' AND SEASON=$season"))
  local visitor_team_id = result[1,1]
  
  result = DataFrame(DBInterface.execute(db,"INSERT OR IGNORE INTO NBA_TEAM_NAMES (NAME,SEASON) VALUES ('$home_team',$season)"))
  result = DataFrame(DBInterface.execute(db, "SELECT * FROM NBA_TEAM_NAMES WHERE NAME = '$home_team' AND SEASON=$season"))
  local home_team_id = result[1,1]
  
  DBInterface.execute(db,"""INSERT INTO NBA_SCORES
         (DATE_ID,VISITOR_ID,VISITOR_SCORE,HOME_ID,HOME_SCORE,OTS,PLAYOFF) VALUES
         ('$date_id',$visitor_team_id,$visitor_team_score,$home_team_id,$home_team_score,'$ots','$playoffs');""")

end

# This parses a row in an html table and returns the row as a Dictionary.
function parseTableRow(row, year::Int, playoff_start::Date)
  date_str = eachmatch(sel"th a",row)
  length(date_str)==0 && return
  date = Date(children(date_str[1])[1].text,"e, u d, y")
  vname = eachmatch(sel"td[data-stat='visitor_team_name'] a",row)
  vpts = eachmatch(sel"td[data-stat='visitor_pts']",row)
  hname = eachmatch(sel"td[data-stat='home_team_name'] a",row)
  hpts = eachmatch(sel"td[data-stat='home_pts']",row)
  otstring = children(eachmatch(sel"td[data-stat='overtimes']", row)[1])
  ots = length(otstring) == 0 ? 0 : otstring[1].text == "OT" ? 1 : parse(Int,match(r"(?<n>\d)OT",otstring[1].text)["n"])

  Dict(
    "date" => Dates.format(date,"yyyy-mm-dd"),
    "vpts" => parse(Int,children(vpts[1])[1].text),
    "vname" => children(vname[1])[1].text,
    "hpts" => parse(Int,children(hpts[1])[1].text),
    "hname" => children(hname[1])[1].text,
    "OTS" => ots,
    "season" => year,
    "playoffs" => date >= playoff_start ? "Y" : "N"
  )
end

# This function parses a html file to determine the children htmlfiles for each month of games.

function getGameFiles(year::Int,html_dir::String)
  main_html = parsehtml(read("$html_dir/$year.html",String))
  m = eachmatch(sel".filter", main_html.root)
  m2 = eachmatch(sel"a",m[1])
  map(link-> "$year/" * split(link.attributes["href"],"/")[3], m2)
end

function getPlayoffDates(verbose::Bool)
  verbose && println("Loading the playoff dates from playoffs.yaml")
  try 
  return YAML.load_file("playoffs.yml")
  catch e
    error("The file 'playoffs.yml' must be in the same directory as this file.")
  end
end

function createCSVfiles(html_dir::String,csv_dir::String,years::Vector{Int},verbose::Bool)
  local playoff_dates = getPlayoffDates(verbose)
  if !isdir("$csv_dir")
    mkdir("$csv_dir")
  end
  
  for year in years
    verbose && println("Creating the csv file for year $year")
    df = DataFrame(DATE=[],SEASON=[],VISITOR_TEAM=[],VISITOR_SCORE=[],HOME_TEAM=[],HOME_SCORE=[],OTS=[],PLAYOFF=[])
    playoff_date = playoff_dates[findfirst(x->x["year"]==year,playoff_dates)]["date"]
    verbose && println("The playoff start date in the $year season is $playoff_date")
    verbose && println("Parsing the $year season")
    months = getGameFiles(year,html_dir)
    for month in months
      file = parsehtml(read("$html_dir/$month",String))
      for row in eachmatch(sel"table#schedule tbody tr", file.root)
        row = parseTableRow(row, year, Date(playoff_date,"yyyy-mm-dd"))
        if row != nothing
          append!(df,DataFrame(DATE=row["date"],SEASON=row["season"],VISITOR_TEAM=row["vname"],VISITOR_SCORE=row["vpts"],
            HOME_TEAM=row["hname"],HOME_SCORE=row["hpts"],OTS=row["OTS"],PLAYOFF=row["playoffs"]))
        end
      end
    end
    CSV.write("$csv_dir/$year.csv",df)
  end
end

function parseData(db::SQLite.DB,html_dir::String,years::Vector{Int},verbose::Bool)
  local playoff_dates = getPlayoffDates(verbose)
  for year in years
    playoff_date = playoff_dates[findfirst(x->x["year"]==year,playoff_dates)]["date"]
    verbose && println("The playoff start date in the $year season is $playoff_date")
    verbose && println("Parsing the $year season")
    months = getGameFiles(year,html_dir)
    for month in months
      file = parsehtml(read("$html_dir/$month",String))
      for row in eachmatch(sel"table#schedule tbody tr", file.root)
        writeToDB(db,parseTableRow(row, year, Date(playoff_date,"yyyy-mm-dd")))
      end
    end
  end
end

function fetchData(html_dir::String,years::Vector{Int},verbose::Bool)
  url = "https://www.basketball-reference.com/leagues"
  for year in years
    verbose && println("Fetching the data for season $year")
    # check if HTML file already exists else download the file for the year
    if !isfile("$html_dir/NBA_$(year)_games.html")
      try 
        Downloads.download("$url/NBA_$(year)_games.html","$html_dir/$(year).html")
      catch e
        if isa(e,RequestError)
          error("You have had too many http requests.  Please wait a while and try again.")
        else
          error(e)
        end
      end
    end
    
    # check if the directory exists.  If not create it.
    if !isdir("$html_dir/$year")
      mkdir("$html_dir/$year")
    end
    # Then download each individual months
    months = getGameFiles(year,html_dir)
    for month in months
      if !isfile(month)
        try 
          Downloads.download("$url/" * split(month,"/")[2],"$html_dir/$month")
        catch e
          @show e
        end
      end
    end
  end
end

main()