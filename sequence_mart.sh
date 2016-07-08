#!/bin/sh
PROD_CMD=$1
DIVISION=$2
ENS_VERSION=$3
EG_VERSION=$4
STAG_CMD=$5

if [ -z "$PROD_CMD" ] || [ -z "$DIVISION" ] || [ -z "$ENS_VERSION" ] || [ -z "$EG_VERSION" ]; then
    echo "Usage: $0 PROD_CMD DIVISION ENS_VERSION EG_VERSION [STAG_CMD]"
    echo "Example: $0 mysql-prod-1-ensrw metazoa 76 23 mysql-staging-1-ensrw"
    exit 1
fi

# Create a sequence mart.
# Run on an interactive cluster node with plenty of memory.

DB_TYPE=core
MART_DBNAME=${DIVISION}_sequence_mart_${EG_VERSION}
BIG_MEM=1

# Set variables required by dna_chunks.pl.
eval $($PROD_CMD details env_ENSMART)
export ENSMARTHOST=$ENSMARTHOST
export ENSMARTPORT=$ENSMARTPORT
export ENSMARTUSER=$ENSMARTUSER
export ENSMARTPWD=$ENSMARTPASS
export ENSMARTDRIVER=mysql

# Sequence mart pipeline.
$PROD_CMD <<< "CREATE DATABASE IF NOT EXISTS $MART_DBNAME;"

cd $(dirname $0)

perl division_species.pl \
  $($PROD_CMD details script) \
  -release $ENS_VERSION \
  -division $DIVISION \
> /tmp/dbs.tmp

while read SPECIES; do
  perl dna_chunks.pl \
    $SPECIES \
    $DB_TYPE \
    ${ENS_VERSION}_${EG_VERSION} \
    $MART_DBNAME \
    "$SPECIES" \
    $BIG_MEM
done < /tmp/dbs.tmp

# Get species as a comma-separated list and remove trailing comma
SPECIES_LIST=$(tr '\n' ',' < /tmp/dbs.tmp)
SPECIES_LIST=${SPECIES_LIST/%,/}

rm /tmp/dbs.tmp

cd ../scripts

perl generate_sequence_template.pl \
  $($PROD_CMD details script) \
  -seq_mart $MART_DBNAME \
  -e_release $ENS_VERSION \
  -release $EG_VERSION \
  -species $SPECIES_LIST

# Copy to staging server.
if [ -n "$STAG_CMD" ]; then
  $STAG_CMD <<< "CREATE DATABASE IF NOT EXISTS $MART_DBNAME;"
  $PROD_CMD mysqldump $MART_DBNAME | $STAG_CMD $MART_DBNAME
fi
