from typing import List
from sqlalchemy import MetaData, engine
import sqlalchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import MetaData, Table, Integer, Column, ForeignKey, String, ARRAY, event
from sqlalchemy.dialects import *
from sqlalchemy.schema import CreateSchema


class Tables:
    def __init__(self, schema:str, engine) -> None:
        self.schema = schema
        self.engine = engine
        self.metadata = MetaData(schema=self.schema, bind=self.engine)
        self.base = declarative_base(metadata=self.metadata)
    
    def area(self):
        return Table(
            "t_area",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String, nullable=False),
            Column("country_code", String, nullable=False),
            Column("region", String)
        )

    
    def competitions(self):
        return Table(
            "t_competitions",
            self.metadata,
            Column("id", Integer, primary_key=True, nullable=False),
            Column("name", String, nullable=False),
            Column("area_id", Integer, ForeignKey("t_area.id")),
            Column("code", String, nullable=False),
            Column("type", String)
        )

    
    def person(self):
        return Table(
            "t_person",
            self.metadata,
            Column("id", Integer, primary_key=True, nullable=False),
            Column("name", String, nullable=False),
            Column("nationality", String),
            Column("position", String)
            # Column("jersey_number", String),
            # Column("current_team", Integer, ForeignKey("t_teams.id"))
        )

    
    def teams(self):
        return Table(
            "t_teams",
            self.metadata,
            Column("id", Integer, primary_key=True, nullable=False),
            Column("name", String, nullable=False),
            Column("area_id", Integer, ForeignKey("t_area.id")),
            Column("running_comps", ARRAY(Integer)),
            Column("coach", Integer, ForeignKey("t_person.id")),
            Column("squad", ARRAY(Integer))
        )
    
    
    def standings(self):
        return Table(
            "t_standings",
            self.metadata,
            Column("position", Integer, primary_key=True, nullable=False),
            Column("stage", String, primary_key=True),
            Column("group", String, primary_key=True),
            Column("team_id", Integer, nullable=False),
            Column("comp_id", Integer, nullable=False)
        )

    def _create(self):
        tables = [getattr(Tables,str(key))(self) for key in Tables.__dict__ if not key.startswith("_")]
        
        # Check if schema exists
        inspector = sqlalchemy.inspect(self.engine)
        if self.schema not in inspector.get_schema_names():
            self.engine.execute(CreateSchema(self.schema))

        self.metadata.create_all(self.engine)
        return self.metadata

# engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost:2345/ny_taxi", echo=True)
# con = engine.connect()
# tables = Tables(schema="football_updates", engine=engine) 
# tables._create()
    

