from typing import List
import time
from ingest import DataManager
from datetime import datetime
from argparse import ArgumentParser

def ingest(types):

    config = {
        # Type : Tablename
        "area" : "t_area",
        "competitions" : "t_competitions",
        "teams" : "t_teams",
        "standings" : "t_standings"
    }

    # Adding sleep 10 for db to start
    time.sleep(10)

    for data_type in (types[0]).split(","):
        if data_type in config:
            print(f"Ingesting {data_type} data ...")
            start_time = datetime.now()

            manager = DataManager(data_type=data_type)
            data = manager._get_data()
            proc_data = manager._process_data(data)
            manager._ingest_data(proc_data, config[data_type])

            end_time = datetime.now()
            delta = end_time-start_time
            print(f"Completed {data_type} ingestion in {delta.total_seconds()} seconds or {delta.total_seconds()/60} mins ")



if __name__ == "__main__":

    # Get args from args
    parser = ArgumentParser(description="Enter types as a list")
    parser.add_argument("--types", nargs="*", type=str, help="List of data types for ingestion..")
    args = parser.parse_args()

    ingest(args.types)


