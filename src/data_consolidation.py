import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)


def create_consolidate_tables():
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_city_data():

    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_data():

    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    station_data_df = pd.DataFrame()
# ID, CODE, NAME, CITY_NAME, CITY_CODE, ADDRESS, LONGITUDE, LATITUDE, STATUS, CREATED_DATE, CAPACITTY
# pour on genere un id unique 

    for index, row in raw_data_df.iterrows():
            station_data_df.at[index, "id"] = f"{row['code_insee_commune']}_{row['stationcode']}"

    station_data_df["code"] = raw_data_df["stationcode"].astype(str)
    station_data_df["name"] = raw_data_df["name"]
    station_data_df["city_name"] = raw_data_df["nom_arrondissement_communes"]
    station_data_df["city_code"] = raw_data_df["code_insee_commune"]
    station_data_df["address"] = None
    station_data_df["longitude"] = raw_data_df["coordonnees_geo.lon"]
    station_data_df["latitude"] = raw_data_df["coordonnees_geo.lat"]
    station_data_df["status"] = raw_data_df["is_installed"]
    station_data_df["created_date"] = date.today()
    station_data_df["capacitty"] = raw_data_df["capacity"]
    station_data_df.drop_duplicates(subset=["id"], inplace=True)
    print(station_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")

# fonction pour consolider les donnees de disponibilite des stations ( statsion_id , bicycle_docks_available, bicycle_available, last_statement_date, created_date)
def consolidate_station_statement_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    station_statement_data_df = pd.DataFrame()

    for index, row in raw_data_df.iterrows():
            station_statement_data_df.at[index, "station_id"] = f"{row['code_insee_commune']}_{row['stationcode']}"

    station_statement_data_df["bicycle_docks_available"] = raw_data_df["numdocksavailable"]
    station_statement_data_df["bicycle_available"] = raw_data_df["numbikesavailable"]
    station_statement_data_df["last_statement_date"] = pd.to_datetime(raw_data_df["duedate"], errors='coerce')
    station_statement_data_df["created_date"] = datetime.now()
    print(station_statement_data_df)
    con.execute("INSERT INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM station_statement_data_df;")

