#!/usr/bin/env perl

# Copyright [2009-2014] EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# $Source$
# $Revision$
# $Date$
# $Author$
#
# Script for uncompressing the individual genotypes from table 'compressed_genotype_var'
# Generate a SQL file
# e.g. perl uncompress_genotype_var.pl zea_mays | gzip -c > zea_mays.sql.gz
# Then just load the sql file into the database

use strict;
use warnings;

use Bio::EnsEMBL::Registry;

my $species = shift;

if (!defined $species) {
   die "No species specified!\n";
}

warn "Loading registry\n";
Bio::EnsEMBL::Registry->
  load_registry_from_db( -host => 'mysql-eg-prod-1.ebi.ac.uk',
                         -user => 'ensro',
                         -port => '4238',
                       );

## Get adaptor (we just use it to get a variation dbc!)
warn "Getting adaptor\n";
my $ga = Bio::EnsEMBL::Registry->
  get_adaptor($species, "variation", "individualgenotype")
  or die("ERROR: Failed to adaptor");
warn "$ga\n";

warn "Preparing SQL\n";
my $sth = $ga->dbc->
  prepare(qq{
    SELECT
      variation_id, subsnp_id, genotypes
    FROM
      compressed_genotype_var
    })
  or die;

warn "Preparing SQL2\n";
my $sth2 = $ga->dbc->
  prepare(qq{
    SELECT
      genotype_code_id,
      MAX(IF(haplotype_id=1, allele, 0)) AS allele_1,
      MAX(IF(haplotype_id=2, allele, 0)) AS allele_2
    FROM
      genotype_code
    INNER JOIN
      allele_code
    USING
      (allele_code_id)
    GROUP BY
      genotype_code_id
    })
  or die;
  
warn "Executing SQL2\n";
$sth2->execute();
my ($gt_code_id, $allele_1, $allele_2);
$sth2->bind_columns(\$gt_code_id, \$allele_1, \$allele_2);
my $gt_href= {};
while($sth2->fetch()){
    $gt_href->{$gt_code_id} = [$allele_1,$allele_2];
}
$sth2->finish();

warn "Executing SQL\n";
$sth->execute
  or die;

print "/*!40000 ALTER TABLE `tmp_individual_genotype_single_bp` DISABLE KEYS */;\n";
print "INSERT INTO tmp_individual_genotype_single_bp VALUES ";
my $index = 0;
while(my ($variation_id, $subsnp_id, $compressed_genotypes) = $sth->fetchrow_array()) {
    my @genotypes = unpack("(ww)*", $compressed_genotypes);
    
    while(@genotypes) {
        my $individual_id = shift @genotypes;
        my $gt_code_id    = shift @genotypes;
        my $alleles_aref = $gt_href->{$gt_code_id};
	my $allele_1 = $alleles_aref->[0];
	my $allele_2 = $alleles_aref->[1];
	if (!defined $subsnp_id) {
	    $subsnp_id = 'NULL';
	}
	
	if ($index != 0) {
	    print ",";
	}
	
	print "($variation_id,$subsnp_id,'$allele_1','$allele_2',$individual_id)";
	
	$index++;
    }
}
$sth->finish();

print ";\n";
print "/*!40000 ALTER TABLE `tmp_individual_genotype_single_bp` ENABLE KEYS */;";

warn "OK\n";
