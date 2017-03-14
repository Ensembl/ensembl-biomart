
=pod 
=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION  

=head1 LICENSE
    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
         http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.
=head1 CONTACT
    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates
=cut

package Bio::EnsEMBL::PipeConfig::BuildMart_conf;

use strict;
use warnings;
use Data::Dumper;
use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf')
  ; # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Bio::EnsEMBL::ApiVersion;
use Cwd;

sub resource_classes {
  my ($self) = @_;
  return { 'default' => { 'LSF' => '-q production-rh7' },
           'mem'     => { 'LSF' => '-q production-rh7 -M 2500 -R "rusage[mem=2500]"'} };
}

sub default_options {
  my ($self) = @_;
  return { %{ $self->SUPER::default_options },
           'user'      => undef,
           'pass'      => undef,
           'port'      => undef,
           'host'      => undef,
           'mart'      => undef,
           'datasets'  => [],
           'compara'   => undef,
           'base_dir'  => getcwd,
	         'template' => undef,
           'base_name' => 'gene',
           'template_name' => 'genes',
           'division' => '',
           'registry'      => $self->o('registry'),
           'genomic_features_mart' => '',
           'max_dropdown' => '256',
           'tables_dir' => $self->o('base_dir').'/ensembl-biomart/gene_mart/tables'};
}

=head2 pipeline_wide_parameters
=cut

sub pipeline_wide_parameters {
  my ($self) = @_;
  return {
    %{ $self->SUPER::pipeline_wide_parameters
      } # here we inherit anything from the base class, then add our own stuff
  };
}

=head2 pipeline_analyses
=cut

sub pipeline_analyses {
  my ($self) = @_;
  my $analyses = [
    { -logic_name  => 'generate_names',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'perl #base_dir#/ensembl-biomart/scripts/generate_names.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -div #division# -registry #registry#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir'),
        'division'  => $self->o('division'),
        'registry'  => $self->o('registry') },
      -input_ids         => [ {} ],
      -analysis_capacity => 1,
      -meadow_type       => 'LOCAL',
      -flow_into => {1 => 'dataset_factory'},
      },
      {
        -logic_name => 'dataset_factory',
        -module     => 'Bio::EnsEMBL::BioMart::DatasetFactory',
        -wait_for   => 'generate_names',
        -parameters => { 'mart'     => $self->o('mart'),
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'datasets' => $self->o('datasets'),
                       'base_dir' => $self->o('base_dir'),
                       'registry' => $self->o('registry'), },
        -input_ids => [ {} ],
        -flow_into => { 1 => [ 'calculate_sequence', 'add_compara',
                             'add_xrefs', 'add_slims', 'AddExtraMartIndexesGene', 'AddExtraMartIndexesTranscript', 'AddExtraMartIndexesTranslation' ],
                      2 => ['tidy_tables','optimize','generate_meta'] },
        -meadow_type => 'LOCAL'
    },
    {
      -logic_name  => 'calculate_sequence',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'perl #base_dir#/ensembl-biomart/scripts/calculate_sequence_data.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -dataset_basename #base_name# -registry #registry#',
        'mart'      => $self->o('mart'),
        'user'      => $self->o('user'),
        'pass'      => $self->o('pass'),
        'host'      => $self->o('host'),
        'port'      => $self->o('port'),
        'registry' => $self->o('registry'),
        'base_dir'  => $self->o('base_dir'),
        'base_name' => $self->o('base_name')
        },
      -rc_name          => 'mem',
      -analysis_capacity => 10
    },
    {
      -logic_name  => 'add_compara',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'perl #base_dir#/ensembl-biomart/scripts/add_compara.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -compara #compara# -dataset #dataset# -name #base_name#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'compara'  => $self->o('compara'),
        'base_dir' => $self->o('base_dir'),
        'base_name' => $self->o('base_name') },
      -analysis_capacity => 10
    },
    {
      -logic_name  => 'tidy_tables',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -wait_for    => [ 'add_compara', 'calculate_sequence'],
      -parameters  => {
        'cmd' =>
'perl #base_dir#/ensembl-biomart/scripts/tidy_tables.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir') },
      -analysis_capacity => 1
    },
    {
      -logic_name  => 'add_xrefs',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'perl #base_dir#/ensembl-biomart/scripts/generate_ontology_extension.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir') },
      -analysis_capacity => 10,
      -wait_for          => ['tidy_tables'],
    },
    {
      -logic_name  => 'add_slims',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'perl #base_dir#/ensembl-biomart/scripts/generate_slim.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -registry #registry# -name #base_name#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir'),
        'registry' => $self->o('registry'),
        'base_name' => $self->o('base_name'),
         },
      -analysis_capacity => 10,
      -wait_for          => ['tidy_tables'],
      -rc_name          => 'mem',
    },
    {
      -logic_name        => 'AddExtraMartIndexesGene',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'gene__main',
                              mart_table_prefix => '#dataset#'."_".$self->o('base_name'),
                              mart_host => $self->o('host'),
                              mart_port => $self->o('port'),
                              mart_user => $self->o('user'),
                              mart_pass => $self->o('pass'),
                              mart_db_name =>  $self->o('mart'),
                            },
      -max_retry_count   => 0,
      -analysis_capacity => 10,
      -rc_name           => 'default',
    },
    {
      -logic_name        => 'AddExtraMartIndexesTranscript',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'transcript__main',
                              mart_table_prefix => '#dataset#'."_".$self->o('base_name'),
                              mart_host => $self->o('host'),
                              mart_port => $self->o('port'),
                              mart_user => $self->o('user'),
                              mart_pass => $self->o('pass'),
                              mart_db_name =>  $self->o('mart'),
                            },
      -max_retry_count   => 0,
      -analysis_capacity => 10,
      -rc_name           => 'default',
    },
    {
      -logic_name        => 'AddExtraMartIndexesTranslation',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'translation__main',
                              mart_table_prefix => '#dataset#'."_".$self->o('base_name'),
                              mart_host => $self->o('host'),
                              mart_port => $self->o('port'),
                              mart_user => $self->o('user'),
                              mart_pass => $self->o('pass'),
                              mart_db_name =>  $self->o('mart'),
                            },
      -max_retry_count   => 0,
      -analysis_capacity => 10,
      -rc_name           => 'default',
    },
    {
      -logic_name  => 'optimize',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -wait_for    => [ 'add_xrefs', 'add_slims'],
      -parameters  => {
        'cmd' =>
'mysqlcheck -h#host# -u#user# -p#pass# -P#port# --optimize "#mart#"',
        'mart' => $self->o('mart'),
        'user' => $self->o('user'),
        'pass' => $self->o('pass'),
        'host' => $self->o('host'),
        'port' => $self->o('port')
        },
      -analysis_capacity => 1
    },
    {
      -logic_name  => 'generate_meta',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -wait_for    => ['generate_names','optimize'],
      -parameters  => {
        'cmd' =>
        'perl #base_dir#/ensembl-biomart/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown#',
                       'mart'     => $self->o('mart'),
                       'template'     => $self->o('template'),
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'base_dir' => $self->o('base_dir'),
                       'template_name' => $self->o('template_name'),
                       'genomic_features_mart' => $self->o('genomic_features_mart'),
                       'max_dropdown' => $self->o('max_dropdown'),
                       'base_name' => $self->o('base_name') },
      -analysis_capacity => 1
    }
  ];
  return $analyses;
} ## end sub pipeline_analyses
1;
