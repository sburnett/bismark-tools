#!/bin/bash

source ~/etc/bdm_db.conf

# Lifted from http://stackoverflow.com/a/246128
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# This script is in $GOROOT/src/github.com/sburnett/bismark-tools/scripts, but
# the executable is in $GOROOT/bin/availability-intervals
EXE_PATH=$DIR/../../../../../bin/availability-intervals

PGHOST=$BDM_PG_HOST \
    PGPORT=$BDM_PG_PORT \
    PGDATABASE=$BDM_PG_MGMT_DBNAME \
    PGUSER=$BDM_PG_USER \
    PGPASSWORD=$BDM_PG_PASSWORD \
    PGSSLMODE=$BDM_PG_SSLMODE \
    $EXE_PATH $@
