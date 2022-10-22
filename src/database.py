from schema import Tables
from sqlalchemy import and_, create_engine, insert, select, update
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.inspection import inspect
from dataclasses import dataclass
import pandas as pd
@dataclass
class DBConfig:
    user : str
    pwd : str
    host : str
    port : int
    name: str
    schema: str
    
    @property
    def connection_string(self):
        return f"postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.name}"

class DBManager:
    def __init__(self, dbConfig:DBConfig) -> None:
        self.config = dbConfig
        self.engine = self._init_engine()
        self.metadata = self._init_tables()
        

    def _init_engine(self):
        return create_engine(self.config.connection_string)

    def _init_tables(self):
        tables = Tables(schema=self.config.schema, engine=self.engine)
        try:
            metadata = tables._create()
            return metadata
        except Exception as e:
            print(f"Error creating tables\n{str(e)}")

    def _table(self, table_name:str):
        return self.metadata.tables[f"{self.config.schema}.{table_name}"]

    def _onconflict(self, sql, table_name):
        table = self._table(table_name)
        primary_keys = [key.name for key in inspect(table).primary_key]

        update_dict = {
        c.name: c for c in sql.excluded
        }
        sql = sql.on_conflict_do_update(
        index_elements=primary_keys,
        set_=update_dict,
        )
        return sql


    def _filter(self, sql, filters):
        for k,v in filters.items():
            and_()
            sql = sql.filter()
        return sql

    def insert(self, table, data, onconflict=True):
        sql = insert(self._table(table_name=table)).values(data)
        if onconflict:
            sql = self._onconflict(sql, table)
        return sql

    def select(self, table, filters:dict=None) -> pd.DataFrame:
        sql = select(self._table(table_name=table))
        if filters:
            sql = self._filter(sql, filters)
            sql = select(self._table(table_name=table)).where(filters)

        with self.engine.connect() as con:
            df = pd.read_sql(sql=sql, con=con)
        return df

    def update(self, table, filters:dict, data):
        sql = update(self._table(table_name=table)).where(filters)

    def delete(self, table, filters:dict):
        pass

    def execute(self, sql):
        with self.engine.connect() as con:
            con.execute(sql)