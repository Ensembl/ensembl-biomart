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
    'drop_mtmp'         => 0,
    'sv_exists'         => 0,
    'regulatory_exists' => 1,
    'motif_exits'       => 1,
    'show_sams'         => 1,
    'show_pops'         => 1,
    'tmp_dir'           => '/tmp',
  };
}

sub run {
  my ($self) = @_;
  
  my $variation_feature_script = $self->param_required('variation_feature_script');
  my $variation_mtmp_script    = $self->param_required('variation_mtmp_script');

  if ($self->param('sv_exists')) {
    $self->run_script($variation_mtmp_script, 'mode', 'variation_set_structural_variation');
    # Creating the Supporting structural variation view
    $self->supporting_structural_variation;
  }
  
  # Always need transcript_variation table; currently don't have
  # regulation data in EG, but if we have it in the future it will
  # automatically be detected and these options switched on by the
  # preceding pipeline module.
  $self->run_script($variation_feature_script, 'table', 'transcript_variation');

  if ($self->param('motif_exists')) {
    $self->run_script($variation_feature_script, 'table', 'motif_feature_variation');
  }

  if ($self->param('regulatory_exists')) {
    $self->run_script($variation_feature_script, 'table', 'regulatory_feature_variation');
  }

  $self->order_consequences;
  
  $self->run_script($variation_mtmp_script, 'mode', 'variation_set_variation');

  
  # Create the MTMP_evidence view using the Variation script
  $self->run_script($variation_mtmp_script, 'mode', 'evidence');
  
  # Reconstitute old tables and views that are still needed by biomart. 
  if ($self->param('show_sams')) {
    $self->sample_genotype;
  }

  if ($self->param('show_pops')) {
    $self->run_script($variation_mtmp_script, 'mode', 'population_genotype');
  }
}

sub run_script {
  my ($self, $script, $table_param_name, $table) = @_;
  my $drop_mtmp = $self->param_required('drop_mtmp');
  my $variation_import_lib = $self->param_required('variation_import_lib');
  my $tmp_dir = $self->param_required('tmp_dir');
  
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  
  # Drop table if exist and drop_mtmp parameter set to 1
  # We don't want to drop the MTMP_transcript_variation table as it
  # gets automatically renerated by the Transcript variation pipeline
  if ($drop_mtmp and $table ne "transcript_variation") {
    my $drop_sql = "DROP TABLE IF EXISTS MTMP_$table;";
    $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
  }
  
  if ($self->does_table_exist($table)) {
    $self->warning("MTMP_$table already exists for this species");
  }
  else{
    $dbc->disconnect_if_idle();
    my $hive_dbc = $self->dbc;
    $hive_dbc->disconnect_if_idle();
    ## Is this really how we're doing it?
    my $cmd = "perl -I$variation_import_lib $script ".
      " --host ".$dbc->host.
      " --port ".$dbc->port.
      " --user ".$dbc->username.
      " --pass ".$dbc->password.
      " --db ".$dbc->dbname.
      " --$table_param_name $table".
      " --tmpdir $tmp_dir ".
      " --tmpfile mtmp_".$table."_".$self->param_required('species').".txt";

    if (system($cmd)) {
      my $drop_sql = "DROP TABLE IF EXISTS MTMP_$table;";
      $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
      $self->throw("Loading failed when running $cmd");
    }
  }
  $dbc->disconnect_if_idle();
}

sub sample_genotype {
  my ($self) = @_;
  
  my $drop_mtmp = $self->param_required('drop_mtmp');
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $tmp_dir = $self->param_required('tmp_dir');
  my $output_file = "$tmp_dir/mtmp_sg_".$self->param_required('species').".txt";
  
  my $hive_dbc = $self->dbc;
  $hive_dbc->disconnect_if_idle();

  if ($drop_mtmp) {
    my $drop_sql = 'DROP TABLE IF EXISTS MTMP_sample_genotype;';
    $dbc->db_handle->do($drop_sql) or $self->throw($dbc->db_handle->errstr);
  }

  my $create_sql =
  'CREATE TABLE MTMP_sample_genotype ( '.
    '`variation_id` int(10) unsigned NOT NULL, '.
    '`allele_1` char(1) DEFAULT NULL, '.
    '`allele_2` char(1) DEFAULT NULL, '.
    '`sample_id` int(11) DEFAULT NULL, '.
    'KEY `variation_idx` (`variation_id`), '.
    'KEY `sample_idx` (`sample_id`) '.
  ') ENGINE=MyISAM DEFAULT CHARSET=latin1;';
  
  my $alleles_sql =
  'SELECT '.
    'genotype_code_id, '.
    'MAX(IF(haplotype_id=1, allele, 0)) AS allele_1, '.
    'MAX(IF(haplotype_id=2, allele, 0)) AS allele_2 '.
  'FROM '.
    'genotype_code INNER JOIN '.
    'allele_code USING (allele_code_id) '.
  'GROUP BY '.
    'genotype_code_id;';
  
  my $genotypes_sql =
  'SELECT variation_id, genotypes FROM compressed_genotype_var;';
  
  my $load_sql = 
  "LOAD DATA LOCAL INFILE '$output_file' INTO TABLE MTMP_sample_genotype;";
  
  my $alleles = $dbc->db_handle->selectall_arrayref($alleles_sql) or $self->throw($dbc->db_handle->errstr);
  my %alleles = map { shift @$_, [ @$_ ]} @$alleles;
  
  open my $fh, '>', $output_file or $self->throw("Error opening $output_file - $!");
  
  my $sth = $dbc->db_handle->prepare($genotypes_sql);
  $sth->execute();
  while (my ($variation_id, $compressed_genotypes) = $sth->fetchrow_array()) {
    my @genotypes = unpack("(ww)*", $compressed_genotypes);
    
    while (@genotypes) {
      my $sample_id = shift @genotypes;
      my $genotype_code_id = shift @genotypes;
      my $allele_1 = $alleles{$genotype_code_id}[0];
      my $allele_2 = $alleles{$genotype_code_id}[1];
      next if (length($allele_1) > 1) or (length($allele_2) > 1) or ($allele_1 eq 0) or ($allele_2 eq 0);
      
      print $fh
        join("\t", $variation_id, $allele_1, $allele_2, $sample_id).
        "\n";
    }
  }
  
  close $fh;
  
  $dbc->db_handle->do($create_sql) or $self->throw($dbc->db_handle->errstr);
  $dbc->db_handle->do($load_sql) or $self->throw($dbc->db_handle->errstr);

  unlink "$output_file" || warn "Failed to remove temp file: $output_file :$!\n";

  $dbc->disconnect_if_idle();
}

sub supporting_structural_variation {
 my ($self) = @_;

 my $drop_mtmp = $self->param_required('drop_mtmp');
 my $dbc = $self->get_DBAdaptor('variation')->dbc();

 my $hive_dbc = $self->dbc;
 $hive_dbc->disconnect_if_idle();

 if ($drop_mtmp) {
   my $drop_sql = 'DROP VIEW IF EXISTS MTMP_supporting_structural_variation;';
   $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
 }
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
   's.name AS sample_name, '.
   'i.name AS strain_name, '.
   'sv.copy_number AS copy_number '.
 'FROM structural_variation sv '.
 'LEFT JOIN structural_variation_sample svs ON (svs.structural_variation_id=sv.structural_variation_id) '.
 'LEFT JOIN sample s ON (s.sample_id=svs.sample_id) '.
 'LEFT JOIN individual i ON (i.individual_id=s.individual_id AND s.display!="UNDISPLAYABLE"), '.
 'structural_variation_feature svf '.
 'LEFT JOIN seq_region seq ON (svf.seq_region_id=seq.seq_region_id), '.
 'attrib a1, '.
 'structural_variation_association sva '.
 'WHERE sv.structural_variation_id = sva.supporting_structural_variation_id '.
 'AND sva.supporting_structural_variation_id=svf.structural_variation_id '.
 'AND a1.attrib_id=sv.class_attrib_id;';

 $dbc->sql_helper->execute_update(-SQL=>$create_sql);
  
 $dbc->disconnect_if_idle();
}

sub order_consequences {
  my ($self) = @_;
  
  my $hive_dbc = $self->dbc;
  $hive_dbc->disconnect_if_idle();
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $table = 'MTMP_transcript_variation';
  my $column = 'consequence_types';
  my $sth = $dbc->db_handle->column_info(undef, undef, $table, $column);
  my $column_info = $sth->fetchrow_hashref() or $self->throw($dbc->db_handle->errstr);
  my $consequences = $$column_info{'mysql_type_name'};
  $consequences =~ s/set\((.*)\)/$1/;
  my @consequences = sort { lc($a) cmp lc($b) } split(/,/, $consequences);
  $consequences = join(',', @consequences);
  my $sql = "ALTER TABLE $table MODIFY COLUMN $column SET($consequences);";
  $dbc->db_handle->do($sql) or $self->throw($dbc->db_handle->errstr);

  $dbc->disconnect_if_idle();
}

# Check if a MTMP table already exists
# MTMP_transcript_variation and MTMP_variation_set_variation are quite
# big for some species so if the table is already there keep it
sub does_table_exist {
  my ($self,$table_name) = @_;
  
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $sth = $dbc->db_handle->table_info(undef, undef, "MTMP_$table_name", 'TABLE');
  
  $sth->execute or $self->throw($dbc->db_handle->errstr);
  my @info = $sth->fetchrow_array;
  
  my $exists = scalar @info;
  $dbc->disconnect_if_idle();
  return $exists;
}

1;
