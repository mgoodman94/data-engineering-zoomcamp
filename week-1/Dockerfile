FROM python:3.10.8

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

WORKDIR /app/src

ENTRYPOINT ["python", "ny_taxi_etl.py", "--trip_file", "yellow_head.csv", "--zone_file", "taxi+_zone_lookup.csv"]