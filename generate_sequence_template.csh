#!/bin/csh

if ( $# != 2 ) then
	echo "Usage: csh generate_sequence_template.csh [Ensembl Release] [EG RELEASE]"
	echo "e.g.: csh generate_sequence_template.csh 58 5"
	exit 1
endif

set RELEASE = $1
set EG_RELEASE = $2

if ( -d "/nfs/panda/production/ensembl_genomes/ensembl/code/ensembl-${RELEASE}" ) then
	echo "Using ensembl-${RELEASE}"
	setenv PERL5LIB /nfs/panda/production/ensembl_genomes/ensembl/code/ensembl-${RELEASE}/modules:$PERL5LIB
else
	echo "Using ensembl-head"
	setenv PERL5LIB /nfs/panda/production/ensembl_genomes/ensembl/code/ensembl-head/modules:$PERL5LIB
endif

#set DB_HOST = mysql-eg-staging-1
#set DB_PORT = 4160
#set DB_USER = admin
#set DB_PASS = RilfI941

set DB_HOST = mysql-cluster-eg-prod-1.ebi.ac.uk
set DB_PORT = 4238
set DB_USER = ensrw
set DB_PASS = writ3rp1

# perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart bacterial_sequence_mart_${EG_RELEASE} -release $RELEASE

echo "Running perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart protist_sequence_mart_${EG_RELEASE} -release $RELEASE"

perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart protist_sequence_mart_${EG_RELEASE} -release $RELEASE

# perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart fungal_sequence_mart_${EG_RELEASE} -release $RELEASE

# perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart plant_sequence_mart_${EG_RELEASE} -release $RELEASE

# perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart metazoa_sequence_mart_${EG_RELEASE} -release $RELEASE

