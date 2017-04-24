#!/bin/bash --


srv=$1
mart=$2

echo "Creating mart $mart on $srv"
$srv -e "DROP DATABASE IF EXISTS $mart; CREATE DATABASE $mart"

echo "Creating master schemata"

ENSEMBL_VERSION=$(perl -e "use Bio::EnsEMBL::ApiVersion qw/software_version/; print software_version()")
 
CORE=master_schema_$ENSEMBL_VERSION
FUNCGEN=master_schema_funcgen_$ENSEMBL_VERSION
VARIATION=master_schema_variation_$ENSEMBL_VERSION
HELPER_SQL=$BASE_DIR/ensembl-biomart/scripts/probestuff_helper.sql

function count_database {
    $srv --column-names=false -e "show databases" | grep $1 | wc -l
}

if [ $(count_database $CORE) -eq 0 ]; then
    echo "Creating $CORE"
    $srv -e "CREATE DATABASE $CORE"
    $srv $CORE < $BASE_DIR/ensembl/sql/table.sql
fi
    
if [ $(count_database $FUNCGEN) -eq 0 ]; then
    echo "Creating $FUNCGEN"
    $srv -e "CREATE DATABASE $FUNCGEN"
    $srv $FUNCGEN < $BASE_DIR/ensembl-funcgen/sql/table.sql
fi

if [ $(count_database $VARIATION) -eq 0 ]; then
    echo "Creating $VARIATION"
    $srv -e "CREATE DATABASE $VARIATION;"
    $srv $VARIATION < $BASE_DIR/ensembl-variation/sql/table.sql
fi

$srv -e "show databases" | grep funcgen | while read db; do
	echo "Creating funcgen helper for $db"
        $srv $db < $HELPER_SQL
done

$srv -e "show databases" | grep variation | while read db; do
    cnt=$($srv --column-names=false $db -e "show tables like \"MTMP_transcript_variation\"" | wc -l)
    if [ $cnt -eq 0 ]; then 
    echo "Creating variation transcript_variation MTMP table for $db"
    cd ${BASE_DIR}/ensembl-variation
    perl -I modules -I scripts/import \
        scripts/misc/mart_variation_effect.pl \
        $($srv details script) \
        -db $db \
        -tmpdir /tmp -tmpfile mtmp_${db}.txt \
        -table transcript_variation
    fi
done

$srv -e "show databases" | grep variation | while read db; do
	echo "Creating variation phenotype MTMP table for $db"
	cd ${BASE_DIR}/ensembl-variation
	perl -I modules -I scripts/import \
	    scripts/misc/mart_phenotypes.pl \
	    $($srv details script) \
	    -db $db \
	    -tmpdir /tmp -tmpfile phenotype_${db}.txt
        cd -
done
	
echo "Completed setup of $mart on $srv"
