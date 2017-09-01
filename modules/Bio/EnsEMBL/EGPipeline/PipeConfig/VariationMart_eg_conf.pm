=head1 LICENSE

Copyright [1999-2014] EMBL-European Bioinformatics Institute
and Wellcome Trust Sanger Institute

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


=pod

=head1 NAME

Bio::EnsEMBL::EGPipeline::PipeConfig::VariationMart_eg_conf

=head1 DESCRIPTION

Configuration for running the Variation Mart pipeline, which
constructs a mart database from core and variation databases.

=head1 Author

James Allen

=cut

package Bio::EnsEMBL::EGPipeline::PipeConfig::VariationMart_eg_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::PipeConfig::VariationMart_conf');

sub default_options {
  my ($self) = @_;
  return {
    %{$self->SUPER::default_options},
    
    mart_db_name          => $self->o('division_name').'_snp_mart_'.$self->o('eg_release'),
    pipeline_name         => 'variation_mart_'.$self->o('division_name').'_'.$self->o('eg_release'),
    drop_mtmp             => 1,
    sample_threshold      => 20000,
    population_threshold  => 500,
    optimize_tables       => 1,
    populate_mart_rc_name => 'normal',
    species_suffix        => '_eg',
    drop_mart_db          => 1,
    genomic_features_dbname => $self->o('division_name').'_genomic_features_mart_'.$self->o('eg_release'),

    
    # Most mart table configuration is in VariationMart_conf, but e! and EG
    # differ in the absence/presence of the poly__dm table.
    snp_indep_tables => [
      'snp__variation__main',
      'snp__poly__dm',
      'snp__population_genotype__dm',
      'snp__variation_annotation__dm',
      'snp__variation_citation__dm',
      'snp__variation_set_variation__dm',
      'snp__variation_synonym__dm',
    ],

    snp_cull_tables => {
      'snp__poly__dm'                    => 'name_2019',
      'snp__population_genotype__dm'     => 'name_2019',
      'snp__variation_annotation__dm'    => 'name_2021',
      'snp__variation_citation__dm'      => 'authors_20137',
      'snp__variation_set_variation__dm' => 'name_2077',
      'snp__variation_synonym__dm'       => 'name_2030',
    },
  };
}

1;
