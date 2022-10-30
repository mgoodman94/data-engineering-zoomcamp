# Dockerized NYC Taxi ETL

Spin up Postgres and PGAdmin containers and load NYC taxi data into a table.


## Usage

Create the Postgres and PGAdmin containers
```bash
docker compose up -d 
```

Build the project image
```bash
docker build -t week-1-image .
```

Run the ETL process in a container on the same network as Postgres
```bash
docker container run -d --network pg-database week-1-image
```

PGAdmin listens on locahost:8080. Go to that in your browser and log in using the credentials in `docker-compose.yml` to query the data that was loaded. 

## Acknowledgments
DataTalksClub