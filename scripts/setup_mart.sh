#!/bin/bash --

srv=$1
mart=$2

base_dir=$(dirname $0)

echo "Creating mart $mart on $srv"
$srv -e "DROP DATABASE IF EXISTS $mart; CREATE DATABASE $mart"

echo "Creating master schemata"

ENSEMBL_VERSION=$(perl -e "use Bio::EnsEMBL::ApiVersion qw/software_version/; print software_version()")
 
CORE=master_schema_$ENSEMBL_VERSION
FUNCGEN=master_schema_funcgen_$ENSEMBL_VERSION
VARIATION=master_schema_variation_$ENSEMBL_VERSION
$srv -e "CREATE DATABASE IF NOT EXISTS $CORE;"
$srv -e "CREATE DATABASE IF NOT EXISTS $FUNCGEN;"
$srv -e "CREATE DATABASE IF NOT EXISTS $VARIATION;"

$srv $CORE < $ENSEMBL_CVS_ROOT_DIR/ensembl/sql/table.sql
$srv $FUNCGEN < $ENSEMBL_CVS_ROOT_DIR/ensembl-funcgen/sql/table.sql
$srv $VARIATION < $ENSEMBL_CVS_ROOT_DIR/ensembl-variation/sql/table.sql
$srv $FUNCGEN < $HELPER_SQL

echo "Creating funcgen helpers"
HELPER_SQL=$base_dir/probestuff_helper.sql
$srv -e "show databases" | grep funcgen | while read db; do
    cnt=$($srv --column-names=false $db -e "show tables like \"MTMP_probestuff_helper\"" | wc -l)
    if [ $cnt -eq 0 ]; then 
        $srv $db < $HELPER_SQL
    fi
done

echo "Creating variation helper for all dbs"
cd ${ENSEMBL_CVS_ROOT_DIR}/ensembl-variation
perl -I modules -I scripts/import \
    scripts/misc/mart_variation_effect.pl \
    $($srv details naga) \
    -tmpdir /tmp -tmpfile mtmp.txt \
    -table transcript_variation
cd -

echo "Completed setup of $mart on $srv"
