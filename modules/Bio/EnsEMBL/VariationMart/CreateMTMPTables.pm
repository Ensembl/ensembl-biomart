=head1 LICENSE

Copyright [2009-2021] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::VariationMart::CreateMTMPTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::VariationMart::Base');

sub param_defaults {
  return {
    'drop_mtmp'         => 0,
    'drop_mtmp_tv'   => 0,
    'sv_exists'         => 0,
    'regulatory_exists' => 1,
    'motif_exits'       => 1,
    'show_sams'         => 1,
    'show_pops'         => 1,
    'scratch_dir'           => '/scratch',
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

sub sample_genotype {
  my ($self) = @_;
  
  my $drop_mtmp = $self->param_required('drop_mtmp');
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $scratch_dir = $self->param_required('scratch_dir');
  my $output_file = "$scratch_dir/mtmp_sg_".$self->param_required('species').".txt";
  
  my $hive_dbc = $self->dbc;
  $hive_dbc->disconnect_if_idle();

  if ($drop_mtmp) {
    my $drop_sql = q/DROP TABLE IF EXISTS MTMP_sample_genotype;/;
    $dbc->db_handle->do($drop_sql) or $self->throw($dbc->db_handle->errstr);
  }

  my $create_sql =
  qq/CREATE TABLE MTMP_sample_genotype (
    variation_id int(10) unsigned NOT NULL,
    allele_1 char(1) DEFAULT NULL,
    allele_2 char(1) DEFAULT NULL,
    sample_id int(11) DEFAULT NULL,
    KEY variation_idx (variation_id),
    KEY sample_idx (sample_id)
  ) ENGINE=MyISAM DEFAULT CHARSET=latin1;/;
  
  my $alleles_sql =
  qq/SELECT
    genotype_code_id,
    MAX(IF(haplotype_id=1, allele, 0)) AS allele_1,
    MAX(IF(haplotype_id=2, allele, 0)) AS allele_2
   FROM
    genotype_code INNER JOIN
    allele_code USING (allele_code_id)
  GROUP BY
    genotype_code_id;/;
  
  my $genotypes_sql =
  q/SELECT variation_id, genotypes FROM compressed_genotype_var;/;
  
  my $load_sql = qq/LOAD DATA LOCAL INFILE '$output_file' INTO TABLE MTMP_sample_genotype;/;
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
   my $drop_sql = q/DROP VIEW IF EXISTS MTMP_supporting_structural_variation;/;
   $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
 }
 my $create_sql =
 qq/CREATE VIEW MTMP_supporting_structural_variation AS
  SELECT
   sv.structural_variation_id AS supporting_structural_variation_id,
   sva.structural_variation_id,
   sv.variation_name,
   a1.value AS class_name,
   seq.name AS seq_region_name,
   svf.outer_start,
   svf.seq_region_start,
   svf.inner_start,
   svf.inner_end,
   svf.seq_region_end,
   svf.outer_end,
   svf.seq_region_strand,
   clinical_significance,
   s.name AS sample_name,
   i.name AS strain_name,
   sv.copy_number AS copy_number
 FROM structural_variation sv LEFT JOIN
   structural_variation_sample svs ON (svs.structural_variation_id=sv.structural_variation_id) LEFT JOIN
   sample s ON (s.sample_id=svs.sample_id) LEFT JOIN
   individual i ON (i.individual_id=s.individual_id AND s.display!='UNDISPLAYABLE'), structural_variation_feature svf LEFT JOIN
   seq_region seq ON (svf.seq_region_id=seq.seq_region_id), attrib a1, structural_variation_association sva
 WHERE sv.structural_variation_id = sva.supporting_structural_variation_id
   AND sva.supporting_structural_variation_id=svf.structural_variation_id
   AND a1.attrib_id=sv.class_attrib_id;/;

 $dbc->sql_helper->execute_update(-SQL=>$create_sql);
 $dbc->disconnect_if_idle();
}
1;
