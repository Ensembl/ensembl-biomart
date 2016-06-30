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

Bio::EnsEMBL::EGPipeline::PipeConfig::VariationMart_ensembl_conf

=head1 DESCRIPTION

Configuration for running the Variation Mart pipeline, which
constructs a mart database from core and variation databases.

=head1 Author

James Allen

=cut

package Bio::EnsEMBL::EGPipeline::PipeConfig::VariationMart_ensembl_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::PipeConfig::VariationMart_conf');

sub default_options {
  my ($self) = @_;
  return {
    %{$self->SUPER::default_options},
    
    division_name         => undef,
    mart_db_name          => 'snp_mart_'.$self->o('eg_release'),
    drop_mtmp             => 0,
    sample_threshold      => 0,
    population_threshold  => 500,
    skip_meta_data        => 1,
    optimize_tables       => 1,
    populate_mart_rc_name => '8Gb_job',
    
    # Most mart table configuration is in VariationMart_conf, but e! and EG
    # differ in the absence/presence of the poly__dm table.
    snp_indep_tables => [
      'snp__variation__main',
      'snp__population_genotype__dm',
      'snp__variation_annotation__dm',
      'snp__variation_citation__dm',
      'snp__variation_set_variation__dm',
      'snp__variation_synonym__dm',
    ],

    snp_cull_tables => {
      'snp__population_genotype__dm'     => 'name_2019',
      'snp__variation_annotation__dm'    => 'name_2021',
      'snp__variation_citation__dm'      => 'authors_20137',
      'snp__variation_set_variation__dm' => 'name_2077',
      'snp__variation_synonym__dm'       => 'name_2030',
    },
  };
}

sub resource_classes {
  my $self = shift;
  return {
    'normal'            => {'LSF' => '-q normal -M500 -R"select[mem>500] rusage[mem=500]"'},
    'default'           => {'LSF' => '-q long -M500 -R"select[mem>500] rusage[mem=500]"'},
    '16Gb_mem_16Gb_tmp' => {'LSF' => '-q long -M16000 -R"select[mem>16000] rusage[mem=16000]"' },
    '1Gb_job'           => {'LSF' => '-q normal -M1000  -R"select[mem>1000]  rusage[mem=1000]"' },
    '2Gb_job'           => {'LSF' => '-q normal -M2000  -R"select[mem>2000]  rusage[mem=2000]"' },
    '8Gb_job'           => {'LSF' => '-q normal -M8000  -R"select[mem>8000]  rusage[mem=8000]"' },
    '24Gb_job'          => {'LSF' => '-q normal -M24000 -R"select[mem>24000] rusage[mem=24000]"' },
    '30Gb_job'          => {'LSF' => '-q normal -M30000 -R"select[mem>30000] rusage[mem=30000]"' },
    'urgent_hcluster'   => {'LSF' => '-q yesterday' },
  }
}

1;
