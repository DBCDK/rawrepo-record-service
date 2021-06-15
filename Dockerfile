FROM docker.dbc.dk/payara5-micro:latest

LABEL RAWREPO_URL="Full connection string for the rawrepo database. Format is 'username:pass@dbserver/dbname'. (Required)"
LABEL HOLDINGS_URL="Full connection string for the holdings items database. Format is 'username:pass@dbserver/dbname'. (Required)"
LABEL VIPCORE_ENDPOINT="URL to VipCore endpoint"
LABEL VIPCORE_CACHE_AGE="Time in hours to cache results from VipCore. Default 8"
LABEL DUMP_THREAD_COUNT="Number of threads to use for dumping agencies. Default 8"
LABEL DUMP_FETCH_SIZE="How many rows should be fetched as a time. Default 50"

COPY target/rawrepo-record-service-1.0-SNAPSHOT.war rawrepo-record-service.json deployments/

EXPOSE 8080