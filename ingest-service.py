from ingest import DataManager
from datetime import datetime

def ingest(types):

    config = {
        # Type : Tablename
        "area" : "t_area",
        "competitions" : "t_competitions",
        "teams" : "t_teams",
        "standings" : "t_standings"
    }

    for key,val in config.items():
        if key in types:
            print(f"Ingesting {key} data ...")
            start_time = datetime.now()

            manager = DataManager(data_type=key)
            data = manager._get_data()
            proc_data = manager._process_data(data)
            manager._ingest_data(proc_data, val)

            end_time = datetime.now()
            delta = end_time-start_time
            print(f"Completed {key} ingestion in {delta.total_seconds()} seconds or {delta.total_seconds()/60} mins ")



if __name__ == "__main__":

    # Get args from args

    types = ["area", "competitions", "teams", "standings"]
    ingest(types)


