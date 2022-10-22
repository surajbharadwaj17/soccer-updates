FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 requests pyarrow

WORKDIR /app
COPY src/ /app/src
COPY src/taxi /app/src/taxi
# COPY src/ingest.py /app/ingest.py
# COPY ingest_service.py /app/ingest_service.py
# COPY database.py /app/database.py
# COPY schema.py /app/schema.py

ENTRYPOINT ["python", "src/ingest_service.py", "--types=area,competitions,teams,standings"]

# python ingest_service.py --types=['area', 'competitions', 'teams', 'standings']
