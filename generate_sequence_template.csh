#!/bin/csh

if ( $# != 3 ) then
	echo "Usage: csh generate_sequence_template.csh [Ensembl Release] [EG RELEASE] [DIVISION:Bacteria|Fungi|Protist|Plants|Metazoa]"
	echo "e.g.: csh generate_sequence_template.csh 58 5 Fungi"
	exit 1
endif

set RELEASE = $1
set EG_RELEASE = $2
set EG_DIVISION = $3

setenv PATH /nfs/panda/ensemblgenomes/perl/perlbrew/perls/5.14.2/bin:$PATH

setenv PERL5LIB /nfs/panda/ensemblgenomes/apis/bioperl/ensembl-stable

if ( -d "/nfs/panda/ensemblgenomes/apis/ensembl/${RELEASE}" ) then
	echo "Using ensembl-${RELEASE}"
	setenv PERL5LIB /nfs/panda/ensemblgenomes/apis/ensembl/${RELEASE}/ensembl/modules:$PERL5LIB
else
	echo "Using ensembl-head"
	setenv PERL5LIB /nfs/panda/ensemblgenomes/production/mart/sequence_mart/ensembl-head/modules:$PERL5LIB
endif

set DB_HOST = mysql-cluster-eg-prod-1.ebi.ac.uk
set DB_PORT = 4238
set DB_USER = ensrw
set DB_PASS = writ3rp1

if ( "$EG_DIVISION" == "Bacteria" ) then
	echo "Running perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart bacteria_sequence_mart_${EG_RELEASE} -release $RELEASE"
	perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart bacteria_sequence_mart_${EG_RELEASE} -release $RELEASE
else if ( "$EG_DIVISION" == "Protist" ) then
	echo "Running perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart protists_sequence_mart_${EG_RELEASE} -release $RELEASE"
	perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart protists_sequence_mart_${EG_RELEASE} -release $RELEASE
else if ( "$EG_DIVISION" == "Fungi" ) then
	echo "Running perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart fungi_sequence_mart_${EG_RELEASE} -release $RELEASE"
	perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart fungi_sequence_mart_${EG_RELEASE} -release $RELEASE
else if ( "$EG_DIVISION" == "Plants" ) then
	echo "Running perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart plants_sequence_mart_${EG_RELEASE} -release $RELEASE"
	perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart plants_sequence_mart_${EG_RELEASE} -release $RELEASE
else if ( "$EG_DIVISION" == "Metazoa" ) then
	echo "Running perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart metazoa_sequence_mart_${EG_RELEASE} -release $RELEASE"
	perl generate_sequence_template.pl -h $DB_HOST -port $DB_PORT -u $DB_USER -pwd $DB_PASS -seq_mart metazoa_sequence_mart_${EG_RELEASE} -release $RELEASE
else
	echo "DIVISION is unknown, $EG_DIVISION"
	exit 1
endif
endif
endif
endif
endif
