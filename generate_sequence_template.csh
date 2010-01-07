#!/bin/csh

set RELEASE = 56
setenv PERL5LIB /nfs/panda/production/ensembl_genomes/ensembl/code/ensembl-${RELEASE}/modules:$PERL5LIB

perl generate_sequence_template.pl -h mysql-eg-staging-1 -port 4160 -u admin -pwd RilfI941 -seq_mart bacterial_sequence_mart_4 -release $RELEASE

perl generate_sequence_template.pl -h mysql-eg-staging-1 -port 4160 -u admin -pwd RilfI941 -seq_mart protist_sequence_mart_4 -release $RELEASE

perl generate_sequence_template.pl -h mysql-eg-staging-1 -port 4160 -u admin -pwd RilfI941 -seq_mart fungal_sequence_mart_4 -release $RELEASE

perl generate_sequence_template.pl -h mysql-eg-staging-1 -port 4160 -u admin -pwd RilfI941 -seq_mart plant_sequence_mart_4 -release $RELEASE

perl generate_sequence_template.pl -h mysql-eg-staging-1 -port 4160 -u admin -pwd RilfI941 -seq_mart metazoa_sequence_mart_4 -release $RELEASE
