FROM docker.dbc.dk/payara-micro:latest

ADD docker/config.d/* config.d
ADD target/*.war wars

ENV LOGBACK_FILE file:///data/logback-include-stdout.xml

LABEL RAWREPO_URL="Full connection string for the rawrepo database. Format is 'connection-name username:pass@dbserver/dbname'. (Required)"