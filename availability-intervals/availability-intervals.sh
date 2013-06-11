#!/bin/bash

source ~/etc/bdm_db.conf

# Lifted from http://stackoverflow.com/a/246128
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PGHOST=$BDM_PG_HOST \
    PGPORT=$BDM_PG_PORT \
    PGDATABASE=$BDM_PG_MGMT_DBNAME \
    PGUSER=$BDM_PG_USER \
    PGPASSWORD=$BDM_PG_PASSWORD \
    PGSSLMODE=$BDM_PG_SSLMODE \
    $DIR/availability-intervals $@
