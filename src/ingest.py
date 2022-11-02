from dataclasses import dataclass
from typing import Any, List
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import *
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
        print(f"{self.source.url}/{self.source.uri}")
        return requests.get(f"{self.source.url}/{self.source.uri}", headers=self.source.headers)

    def _get_source(self, type, team_id='90'):
        if type == "area":
            uri = "areas"
        elif type == "competitions":
            uri = "competitions"
        elif type == "teams":
            uri = "competitions/CL/teams"
        elif type == "person/team":
            uri = f"teams/{team_id}"
        elif type == "standings":
            uri = "competitions/CL/standings"
        else:
            uri = "/"

        return DataSourceConfig(url="https://api.football-data.org/v4",
                uri=f"{uri}",
                headers= { 'X-Auth-Token': '7b12df0d8cf74ca29cc2926ebb4b00e1' })

class DataProcessor:
    def __init__(self, type) -> None:
        self.type = type
        # Creating a db connection data validations and decisions
        self.db = DBManager(
            dbConfig=DBConfig(
                user="postgres",
                pwd="postgres",
                host="db",
                port="5432",
                name="football",
                schema="football_updates"
            )
        ) 

    def process(self, data):
        if self.type == "area":
            self.proc_data = self._process_area_data(data)
        elif self.type == "teams":
            self.proc_data = self._process_teams_data(data)
        elif self.type == "competitions":
            self.proc_data = self._process_competitions_data(data)
        elif self.type == "persons":
            self.proc_data = self._process_persons_data(data)
        elif self.type == "standings":
            self.proc_data = self._process_standings_data(data)
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

    def _manage_squad_data(self, squad) -> List:
        """
        Check for unseen players and managers into persons and return the list of person ids to insert
        """
        df_person = self.db.select(
            table="t_person"
        ) 
        insert_list = []
        for person in squad:
            if person["id"] not in df_person["id"].values:
                data = {
                    "id" : person["id"],
                    "name" : person["name"],
                    "nationality" : person["nationality"],
                    "position" : person["position"]}
                
                # Insert new found person data
                sql = self.db.insert(table="t_person", data=data)
                self.db.execute(sql=sql)

            insert_list.append(person["id"])

        return insert_list


    def _manage_coach_data(self, data) -> int:
        """
        Check if coach data already present , return id : int
        """
        if data["id"]:
            df_coach = self.db.select(
                table="t_person"
            )
            if data["id"] not in df_coach["id"].values:
                    person = {
                        "id" : data["id"],
                        "name" : data["name"],
                        "nationality" : data["nationality"],
                        "position" : "Coach"}

                    # Insert new found person data
                    sql = self.db.insert(table="t_person", data=person)
                    self.db.execute(sql=sql)
        
            return data["id"]
        return 50157


    def _process_teams_data(self, data):
        self.proc_data = []
        for team in data["teams"]:
            # try:
            self.proc_data.append({
                "id" : team["id"],
                "name" : team["name"],
                "area_id" : team["area"]["id"],
                "running_comps" : [ comp["id"] for comp in team["runningCompetitions"]],
                'squad' : self._manage_squad_data(team["squad"]),
                "coach" : self._manage_coach_data(team["coach"]) 
                 })
            # except Exception as e:
            #     print(str(e))
        return self.proc_data

    def _process_standings_data(self, data):
        self.proc_data = []

        for stage in data["standings"]:
            for table in stage["table"]:
                self.proc_data.append({
                    "position" : table["position"],
                    "stage" : stage["stage"],
                    "group" : stage["group"],
                    "team_id" : table["team"]["id"],
                    "team_name" : table["team"]["name"],
                    "comp_id" : 2001
                })

        return self.proc_data
            

    def _process_persons_data(self, data:List):
        """ List input [List of different squads, Coach data]"""
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
    def __init__(self, data_type, schema:str="football_updates") -> None:
        self.type = data_type
        self.db = DBManager(
            dbConfig=DBConfig(
                user="postgres",
                pwd="postgres",
                host="db",
                port="5432",
                name="football",
                schema="football_updates"
            )
        ) 
        self.collector = DataCollector(type=data_type)
        self.processor = DataProcessor(type=data_type)

    def get_data(self) -> dict:
        if self.type == "persons":
            return self._get_persons_data()
        elif self.type == "teams":
            return self._get_teams_data()
        elif type == "standings":
            return self._get_standings_data()
        else:
            return self.collector.collect().json()

    def _get_standings_data(self):
        self.collector.source = self.collector._get_source(type="standings")
        return self.collector.collect().json()

    def _get_persons_data(self) -> List:
        """
        Requires collection from different sub-resources and manipulation
        """
        self.collector.source = self.collector._get_source(type="teams")
        teams_data = self.collector.collect().json()["teams"][0]
        squad_data, coach_data = [], []
        for team in teams_data:
            self.collector.source = self.collector._get_source(type="person/team", team_id=teams_data["id"])
            response = self.collector.collect().json()
            try:
                squad_data.append(response["squad"])
                coach_data.append(response["coach"])
            except Exception as e:
                print(response.keys())
            # Hitting max api calls - BLOCKED -> Change approach
        return {
            "squads" : squad_data,
            "coaches" : coach_data
        }

    def _get_teams_data(self):
        self.collector.source = self.collector._get_source(type="teams")
        return self.collector.collect().json()

    def process_data(self, data):
        return self.processor.process(data)

    def ingest_data(self, proc_data, table):
        sql = self.db.insert(table=table, data=proc_data)
        #sql = self._onupdate(sql, table)
        self.db.execute(sql)


