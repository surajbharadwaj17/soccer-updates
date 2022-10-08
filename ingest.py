from dataclasses import dataclass
from database import DBManager, DBConfig
import requests

@dataclass
class DataSourceConfig:
    url:str
    uri:str
    headers:dict

class DataCollector:
    def __init__(self,type) -> None:
        self.source = self._get_source(type)
        
    def collect(self):
        return requests.get(f"{self.source.url}/{self.source.uri}", headers=self.source.headers)

    def _get_source(self, type):
        if type == "area":
            uri = "areas"
        elif type == "teams":
            uri = "teams"
        elif type == "competitions":
            uri = "competitions"

        return DataSourceConfig(url="https://api.football-data.org/v4",
            uri=f"{uri}",
            headers= { 'X-Auth-Token': '7b12df0d8cf74ca29cc2926ebb4b00e1' })

class DataProcessor:
    def __init__(self, type) -> None:
        self.type = type

    def process(self, data):
        if self.type == "area":
            self.proc_data = self._process_area_data(data)
        elif self.type == "teams":
            self.proc_data = self._process_teams_data(data)
        elif self.type == "competitions":
            self.proc_data = self._process_competitions_data(data)
        return self.proc_data

    def _process_area_data(self, data):
        self.proc_data = []
        for area in data["areas"]:
            self.proc_data.append({
                "id" : area["id"],
                "name" : area["name"],
                "country_code" : area["countryCode"],
                "region" : area["parentArea"]
            })
        return self.proc_data

    def _process_teams_data(self, data):
        pass

    def _process_competitions_data(self, data):
        self.proc_data = []
        for comp in data["competitions"]:
            self.proc_data.append({
                "id": comp["id"],
                "name" : comp["name"],
                "area_id" : comp["area"]["id"],
                "code" : comp["code"],
                "type" : comp["type"]
            })
        return self.proc_data


class DataManager:
    def __init__(self, data_type) -> None:
        self.type = data_type
        self.db = self._get_db()
        self.collector = DataCollector(type=data_type)
        self.processor = DataProcessor(type=data_type)

    def _get_db(self):
        return DBManager(
            dbConfig=DBConfig(
                user="postgres",
                pwd="postgres",
                host="localhost",
                port="2345",
                name="football",
                schema="football_updates"
            )
        ) 

    def _get_data(self) -> dict:
        return self.collector.collect().json()

    def _process_data(self, data):
        return self.processor.process(data)

    def _ingest_data(self, proc_data, table):
        sql = self.db.insert(table=table, data=proc_data)
        self.db.execute(sql)


manager = DataManager(data_type="competitions")
data = manager._get_data()
proc_data = manager._process_data(data)
#manager._ingest_data(proc_data, "t_competitions")
print(manager.db.metadata)

# from sqlalchemy_schemadisplay import create_schema_graph
# graph = create_schema_graph(
#     metadata=manager.db.metadata,
#     show_datatypes=False,
#     show_indexes=False,
#     rankdir="LR",
#     concentrate=False
# )
# graph.write('schema.jpg')

from eralchemy import render_er
filename = "football-schema.png"
render_er(manager.db.metadata, filename)