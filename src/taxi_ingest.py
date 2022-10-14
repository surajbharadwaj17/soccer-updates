from numpy import append
from database import DBManager, DBConfig
import pandas as pd

class TaxiIngest:
    def __init__(self) -> None:
        self.dbConfig = DBConfig(user="postgres",
                pwd="postgres",
                host="localhost",
                port="2345",
                name="football",
                schema="ny_taxi")
        self.db = DBManager(self.dbConfig)


    def ingest(self):

        # Read parquet
        df = pd.read_parquet("./taxi/yellow_tripdata_2022-01.parquet", engine="pyarrow")

        engine = self.db.engine
        df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append", chunksize=100000)
        print("Taxi data ingested.")

