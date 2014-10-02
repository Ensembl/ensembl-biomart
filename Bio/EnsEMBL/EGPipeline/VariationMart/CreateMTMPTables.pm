=head1 LICENSE

Copyright [2009-2014] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

package Bio::EnsEMBL::EGPipeline::VariationMart::CreateMTMPTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return {
    'sv_exists' => 0,
  };
}

sub run {
  my ($self) = @_;
  
  my $dbh = $self->get_DBAdaptor('variation')->dbc()->db_handle;
  
  if ($self->param('sv_exists')) {
    # Apparently, the variation set MTMP table is only required for human. If you
    # run this script for any other species, the table doesn't get created. So
    # don't bother for now, but I'll leave it here in case we need it back...
    #$self->run_vs_script('--sv 1');
    
    $self->supporting_structural_variation($dbh);
  }
  
  # Always need transcript_variation table; currently don't have regulation
  # data in EG, but if we have it in the future it will automatically be
  # detected and these options switched on by the preceding pipeline module.
  $self->run_vf_script('transcript_variation');
  if ($self->param('motif_features')) {
    $self->run_vf_script('motif_features');
  }
  if ($self->param('regulatory_features')) {
    $self->run_vf_script('regulatory_features');
  }
  
  # Apparently, the variation set MTMP table is only required for human. If you
  # run this script for any other species, the table doesn't get created. So
  # don't bother for now, but I'll leave it here in case we need it back...
  #$self->run_vs_script();
  
  # Reconstitute old tables and views that are still needed by biomart.
  $self->evidence($dbh);
  $self->population_genotype($dbh);
  $self->variation_annotation($dbh);
}

sub run_vf_script {
  my ($self, $table) = @_;
  my $variation_import_lib = $self->param_required('variation_import_lib');
  my $variation_feature_script = $self->param_required('variation_feature_script');
  
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $dbh = $dbc->db_handle();
  
  my $drop_sql = "DROP TABLE IF EXISTS MTMP_$table;";
  $dbh->do($drop_sql) or $self->throw($dbh->errstr);
  
  my $cmd = "perl -I$variation_import_lib $variation_feature_script ".
    " --host ".$dbc->host.
    " --port ".$dbc->port.
    " --user ".$dbc->username.
    " --pass ".$dbc->password.
    " --db ".$dbc->dbname.
    " --table $table".
    " --tmpdir /tmp ".
    " --tmpfile mtmp.txt";
  
  if (system($cmd)) {
    $self->throw("Loading failed when running $cmd");
  }
}

sub run_vs_script {
  my ($self, $options) = @_;
  $options ||= '';
  my $variation_import_lib = $self->param_required('variation_import_lib');
  my $variation_set_script = $self->param_required('variation_set_script');
  
  # The script deals with any existing data, so doesn't need to be checked here.
  
  my $cmd = "perl -I$variation_import_lib $variation_set_script ".
    " --registry_file ".$self->param_required('registry').
    " --species ".$self->param_required('species').
    " --tmpdir /tmp ".
    " --tmpfile mtmp.txt".
    " $options";
  
  if (system($cmd)) {
    $self->throw("Loading failed when running $cmd");
  }
}

sub evidence {
  my ($self, $dbh) = @_;
  
  my $drop_sql = 'DROP TABLE IF EXISTS MTMP_evidence;';
  
  my $create_sql =
  'CREATE TABLE MTMP_evidence ( '.
    '`variation_id` int(10) unsigned NOT NULL, '.
    '`evidence` SET("Multiple_observations","Frequency","HapMap","1000Genomes","Cited","ESP"), '.
    'PRIMARY KEY (`variation_id`))'.
  'ENGINE=MyISAM DEFAULT CHARSET=latin1;';
  
  my $insert_sql =
  'INSERT INTO MTMP_evidence '.
  'SELECT '.
    'v.variation_id, '.
    'GROUP_CONCAT(a.value) '.
  'FROM '.
    'variation v INNER JOIN '.
    'attrib a ON FIND_IN_SET(attrib_id, evidence_attribs) '.
  'GROUP BY v.variation_id;';
  
  $dbh->do($drop_sql) or $self->throw($dbh->errstr);
  $dbh->do($create_sql) or $self->throw($dbh->errstr);
  $dbh->do($insert_sql) or $self->throw($dbh->errstr);
}

sub population_genotype {
  my ($self, $dbh) = @_;
  
  my $drop_sql = 'DROP TABLE IF EXISTS MTMP_population_genotype;';
  
  my $create_sql =
  'CREATE TABLE MTMP_population_genotype ( '.
    '`population_genotype_id` int(10) unsigned NOT NULL AUTO_INCREMENT, '.
    '`variation_id` int(10) unsigned NOT NULL, '.
    '`subsnp_id` int(15) unsigned DEFAULT NULL, '.
    '`allele_1` varchar(25000) DEFAULT NULL, '.
    '`allele_2` varchar(25000) DEFAULT NULL, '.
    '`frequency` float DEFAULT NULL, '.
    '`population_id` int(10) unsigned DEFAULT NULL,'.
    '`count` int(10) unsigned DEFAULT NULL, '.
    'PRIMARY KEY (`population_genotype_id`),'.
    'UNIQUE KEY `pop_genotype_idx` (`variation_id`,`subsnp_id`,`frequency`,`population_id`,`allele_1`(5),`allele_2`(5)), '.
    'KEY `variation_idx` (`variation_id`), '.
    'KEY `subsnp_idx` (`subsnp_id`), '.
    'KEY `population_idx` (`population_id`))'.
  'ENGINE=MyISAM DEFAULT CHARSET=latin1;';
  
  my $insert_sql =
  'INSERT INTO MTMP_population_genotype '.
  'SELECT '.
    'p.population_genotype_id, '.
    'p.variation_id, '.
    'p.subsnp_id, '.
    'ac1.allele, '.
    'ac2.allele, '.
    'p.frequency, '.
    'p.population_id, '.
    'p.count '.
  'FROM '.
    'population_genotype p INNER JOIN '.
    'genotype_code gc1 ON p.genotype_code_id = gc1.genotype_code_id INNER JOIN '.
    'genotype_code gc2 ON p.genotype_code_id = gc2.genotype_code_id INNER JOIN '.
    'allele_code ac1 ON gc1.allele_code_id = ac1.allele_code_id INNER JOIN '.
    'allele_code ac2 ON gc2.allele_code_id = ac2.allele_code_id '.
  'WHERE gc1.haplotype_id = 1 AND gc2.haplotype_id = 2;';
  
  $dbh->do($drop_sql) or $self->throw($dbh->errstr);
  $dbh->do($create_sql) or $self->throw($dbh->errstr);
  $dbh->do($insert_sql) or $self->throw($dbh->errstr);
}

sub variation_annotation {
  my ($self, $dbh) = @_;
  
  my $drop_sql = 'DROP VIEW IF EXISTS MTMP_variation_annotation;';
  
  my $create_sql =
  'CREATE VIEW MTMP_variation_annotation AS '.
  'SELECT '.
    'v.variation_id AS variation_id, '.
    'phenotype_id, '.
    'a1.value AS associated_gene, '.
    'a2.value AS risk_allele, '.
    'a3.value AS p_value, '.
    'pf.study_id, '.
    'pf.is_significant, '.
    'a4.value AS variation_names '.
  'FROM (variation v, phenotype_feature pf) '.
  'LEFT JOIN ( '.
    'phenotype_feature_attrib a1 '.
    'JOIN attrib_type at1 ON a1.attrib_type_id = at1.attrib_type_id '.
  ') ON (a1.phenotype_feature_id = pf.phenotype_feature_id AND at1.code = "associated_gene") '.
  'LEFT JOIN ( '.
    'phenotype_feature_attrib a2 '.
    'JOIN attrib_type at2 ON a2.attrib_type_id = at2.attrib_type_id '.
  ') ON (a2.phenotype_feature_id = pf.phenotype_feature_id AND at2.code = "risk_allele") '.
  'LEFT JOIN ( '.
    'phenotype_feature_attrib a3 '.
    'JOIN attrib_type at3 ON a3.attrib_type_id = at3.attrib_type_id '.
  ') ON (a3.phenotype_feature_id = pf.phenotype_feature_id AND at3.code = "p_value") '.
  'LEFT JOIN ( '.
    'phenotype_feature_attrib a4 '.
    'JOIN attrib_type at4 ON a4.attrib_type_id = at4.attrib_type_id '.
  ') ON (a4.phenotype_feature_id = pf.phenotype_feature_id AND at4.code = "variation_names") '.
  'WHERE v.name = pf.object_id AND pf.type = "Variation";';
  
  $dbh->do($drop_sql) or $self->throw($dbh->errstr);
  $dbh->do($create_sql) or $self->throw($dbh->errstr);
}

sub supporting_structural_variation {
  my ($self, $dbh) = @_;
  
  my $drop_sql = 'DROP VIEW IF EXISTS MTMP_supporting_structural_variation;';
  
  my $create_sql =
  'CREATE VIEW MTMP_supporting_structural_variation AS '.
  'SELECT '.
    'sv.structural_variation_id AS supporting_structural_variation_id, '.
    'sva.structural_variation_id, '.
    'sv.variation_name, '.
    'a1.value AS class_name, '.
    'seq.name AS seq_region_name, '.
    'svf.outer_start, '.
    'svf.seq_region_start, '.
    'svf.inner_start, '.
    'svf.inner_end, '.
    'svf.seq_region_end, '.
    'svf.outer_end, '.
    'svf.seq_region_strand, '.
    'clinical_significance, '.
    'i1.name AS sample_name, '.
    'i2.name AS strain_name, '.
    'p.description AS phenotype '.
  'FROM structural_variation sv '.
  'LEFT JOIN phenotype_feature pf ON (sv.variation_name=pf.object_id AND pf.type="SupportingStructuralVariation") '.
  'LEFT JOIN phenotype p ON (p.phenotype_id=pf.phenotype_id) '.
  'LEFT JOIN structural_variation_sample svs ON (svs.structural_variation_id=sv.structural_variation_id) '.
  'LEFT JOIN individual i1 ON (i1.individual_id=svs.individual_id) '.
  'LEFT JOIN individual i2 ON (i2.individual_id=svs.strain_id), '.
  'structural_variation_feature svf '.
  'LEFT JOIN seq_region seq ON (svf.seq_region_id=seq.seq_region_id), '.
  'attrib a1, '.
  'structural_variation_association sva '.
  'WHERE sv.structural_variation_id = sva.supporting_structural_variation_id '.
  'AND sva.supporting_structural_variation_id=svf.structural_variation_id '.
  'AND a1.attrib_id=sv.class_attrib_id;';
  
  $dbh->do($drop_sql) or $self->throw($dbh->errstr);
  $dbh->do($create_sql) or $self->throw($dbh->errstr);
}

1;
