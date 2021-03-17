
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

package Bio::EnsEMBL::PipeConfig::BuildRegulationMart_conf;

use strict;
use warnings;
use Data::Dumper;
use Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf;
 # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use base ('Bio::EnsEMBL::Hive::PipeConfig::EnsemblGeneric_conf');
use Bio::EnsEMBL::ApiVersion;
use Cwd;

sub resource_classes {
  my ($self) = @_;
  return { 'default' => { 'LSF' => '-q production-rh74' },
           'mem'     => { 'LSF' => '-q production-rh74 -M 2500 -R "rusage[mem=2500]"'} };
}

sub default_options {
  my ($self) = @_;
  return { %{ $self->SUPER::default_options },
           'user'      => undef,
           'pass'      => undef,
           'port'      => undef,
           'host'      => undef,
           'olduser'   => undef,
           'oldport'   => undef,
           'oldhost'   => undef,
           'mart'      => undef,
           'datasets'  => [],
           'species'      => [],
           'base_dir'  => getcwd,
           'base_name' => 'external_feature',
           'division' => '',
           'registry'      => $self->o('registry'),
           'genomic_features_mart' => '',
           'max_dropdown' => '256',
           'tables_dir' => $self->o('base_dir').'/regulation_mart/tables',
           'test_dir' => '/hps/nobackup2/production/ensembl/'.$self->o('ENV', 'USER').'/mart_test',
           'old_mart' => undef,
           'old_release' => undef,
           'new_release' => undef,
           'grch37' => 0},
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

sub pipeline_create_commands {
  my ($self) = @_;

  return [
    @{$self->SUPER::pipeline_create_commands},
    'mkdir -p '.$self->o('test_dir'),
  ];
}


=head2 pipeline_analyses
=cut

sub pipeline_analyses {
  my ($self) = @_;
  my $analyses = [
    {
      -logic_name  => 'generate_names',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
          'perl #base_dir#/scripts/generate_names.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -div #division# -registry #registry#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir'),
        'division' => $self->o('division'),
        'registry' => $self->o('registry') },
      -input_ids         => [ {} ],
      -analysis_capacity => 1,
      -flow_into => {
        '1->A' => ['dataset_factory'],
        'A->1' => ['tidy_tables']
      },
    },
    {
      -logic_name => 'dataset_factory',
      -module     => 'Bio::EnsEMBL::BioMart::DatasetFactory',
      -parameters => {
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'datasets' => $self->o('datasets'),
        'base_dir' => $self->o('base_dir'),
        'registry' => $self->o('registry'),
        'species'  => $self->o('species'),
          'base_name' => "features"
      },
      -flow_into => {
        1 => WHEN(
          '(#dataset# ne "dmelanogaster")' => ['AddExtraMartIndexesExternalFeatures',
                                               'AddExtraMartIndexesPeaks',
                                               'AddExtraMartIndexesMiRNATargetFeatures',
                                               # 'AddExtraMartIndexesMotifFeatures',
                                               'AddExtraMartIndexesRegulatoryFeatures'],
          ELSE 'AddExtraMartIndexesExternalFeatures',
        )
      }
    },
    {
      -logic_name        => 'AddExtraMartIndexesExternalFeatures',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'external_feature__main',
                              mart_table_prefix => '#dataset#'."_"."external_feature",
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
      -logic_name        => 'AddExtraMartIndexesPeaks',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'peak__main',
                              mart_table_prefix => '#dataset#'."_"."peak",
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
      -logic_name        => 'AddExtraMartIndexesMiRNATargetFeatures',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'mirna_target_feature__main',
                              mart_table_prefix => '#dataset#'."_"."mirna_target_feature",
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
#    {
#      -logic_name        => 'AddExtraMartIndexesMotifFeatures',
#      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
#      -parameters        => {
#                              tables_dir => $self->o('tables_dir'),
#                              table => 'motif_feature__main',
#                              mart_table_prefix => '#dataset#'."_"."motif_feature",
#                              mart_host => $self->o('host'),
#                              mart_port => $self->o('port'),
#                              mart_user => $self->o('user'),
#                              mart_pass => $self->o('pass'),
#                              mart_db_name =>  $self->o('mart'),
#                            },
#      -max_retry_count   => 0,
#      -analysis_capacity => 10,
#      -rc_name           => 'default',
#    },
    {
      -logic_name        => 'AddExtraMartIndexesRegulatoryFeatures',
      -module            => 'Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes',
      -parameters        => {
                              tables_dir => $self->o('tables_dir'),
                              table => 'regulatory_feature__main',
                              mart_table_prefix => '#dataset#'."_"."regulatory_feature",
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
      -logic_name  => 'tidy_tables',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'perl #base_dir#/scripts/tidy_tables.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir') },
      -analysis_capacity => 1,
      -flow_into => ['optimize'],
    },
    {
      -logic_name  => 'optimize',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
'mysqlcheck -h#host# -u#user# -p#pass# -P#port# --optimize "#mart#"',
        'mart' => $self->o('mart'),
        'user' => $self->o('user'),
        'pass' => $self->o('pass'),
        'host' => $self->o('host'),
        'port' => $self->o('port')
        },
      -analysis_capacity => 1,
      -flow_into => ['generate_meta_external_features'],
    },
    {
      -logic_name  => 'generate_meta_external_features',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
        'perl #base_dir#/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown#',
                       'mart'     => $self->o('mart'),
                       'template'     => $self->o('base_dir').'/scripts/templates/external_feature_template_template.xml',
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'base_dir' => $self->o('base_dir'),
                       'template_name' => 'external_features',
                       'genomic_features_mart' => $self->o('genomic_features_mart'),
                       'max_dropdown' => $self->o('max_dropdown'),
                       'base_name' => 'external_feature' },
      -analysis_capacity => 1,
      -flow_into => ['generate_meta_peaks'],
    },
    {
      -logic_name  => 'generate_meta_peaks',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
        'perl #base_dir#/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown#',
                       'mart'     => $self->o('mart'),
                       'template'     => $self->o('base_dir').'/scripts/templates/peak_template_template.xml',
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'base_dir' => $self->o('base_dir'),
                       'template_name' => 'peaks',
                       'genomic_features_mart' => $self->o('genomic_features_mart'),
                       'max_dropdown' => $self->o('max_dropdown'),
                       'base_name' => 'peak' },
      -analysis_capacity => 1,
      -flow_into => ['generate_meta_mirna_target_features']
    },
    {
      -logic_name  => 'generate_meta_mirna_target_features',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
        'perl #base_dir#/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown#',
                       'mart'     => $self->o('mart'),
                       'template'     => $self->o('base_dir').'/scripts/templates/mirna_target_feature_template_template.xml',
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'base_dir' => $self->o('base_dir'),
                       'template_name' => 'mirna_target_features',
                       'genomic_features_mart' => $self->o('genomic_features_mart'),
                       'max_dropdown' => $self->o('max_dropdown'),
                       'base_name' => 'mirna_target_feature' },
      -analysis_capacity => 1,
      -flow_into => ['generate_meta_motif_features']
    },
    {
      -logic_name  => 'generate_meta_motif_features',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
        'perl #base_dir#/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown#',
                       'mart'     => $self->o('mart'),
                       'template'     => $self->o('base_dir').'/scripts/templates/motif_feature_template_template.xml',
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'base_dir' => $self->o('base_dir'),
                       'template_name' => 'motif_features',
                       'genomic_features_mart' => $self->o('genomic_features_mart'),
                       'max_dropdown' => $self->o('max_dropdown'),
                       'base_name' => 'motif_feature' },
      -analysis_capacity => 1,
      -flow_into => ['generate_meta_regulatory_features']
    },
    {
      -logic_name  => 'generate_meta_regulatory_features',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -parameters  => {
        'cmd' =>
        'perl #base_dir#/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown#',
                       'mart'     => $self->o('mart'),
                       'template'     => $self->o('base_dir').'/scripts/templates/regulatory_feature_template_template.xml',
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'base_dir' => $self->o('base_dir'),
                       'template_name' => 'regulatory_features',
                       'genomic_features_mart' => $self->o('genomic_features_mart'),
                       'max_dropdown' => $self->o('max_dropdown'),
                       'base_name' => 'regulatory_feature' },
      -analysis_capacity => 1,
      -flow_into    => 'run_tests',
    },
    { -logic_name  => 'run_tests',
        -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
        -meadow_type => 'LSF',
        -flow_into    => 'check_tests',
        -parameters  => {
                         'cmd' =>
                         'cd #test_dir#;perl #base_dir#/scripts/pre_configuration_mart_healthcheck.pl -newuser #user# -newpass #pass# -newport #port# -newhost #host# -olduser #olduser# -oldport #oldport# -oldhost #oldhost# -new_dbname #mart# -old_dbname #old_mart# -old_rel #old_release# -new_rel #new_release# -empty_column 1 -grch37 #grch37# -mart regulation_mart',
                         'mart'   => $self->o('mart'),
                         'user'     => $self->o('user'),
                         'pass'     => $self->o('pass'),
                         'host'     => $self->o('host'),
                         'port'     => $self->o('port'),
                         'olduser'     => $self->o('olduser'),
                         'oldhost'     => $self->o('oldhost'),
                         'oldport'     => $self->o('oldport'),
                         'old_mart'     => $self->o('old_mart'),
                         'test_dir'    => $self->o('test_dir'),
                         'old_release' => $self->o('old_release'),
                         'new_release' => $self->o('new_release'),
                         'base_dir' => $self->o('base_dir'),
                         'grch37' => $self->o('grch37') },
        -analysis_capacity => 1 },
      { -logic_name  => 'check_tests',
        -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
        -meadow_type => 'LSF',
        -max_retry_count   => 0,
        -parameters  => {
                         'cmd' =>
                         'EXIT_CODE=0;failed_tests=`find #test_dir#/#old_mart#_#oldhost#_vs_#mart#_#host#.* -type f ! -empty -print`;if [ -n "$failed_tests" ]; then >&2 echo "Some tests have failed please check ${failed_tests}";EXIT_CODE=1;fi;exit $EXIT_CODE',
                         'mart'   => $self->o('mart'),
                         'host'     => $self->o('host'),
                         'oldhost'     => $self->o('oldhost'),
                         'old_mart'     => $self->o('old_mart'),
                         'test_dir'    => $self->o('test_dir'),
        -analysis_capacity => 1 },
      }

  ];
  return $analyses;
} ## end sub pipeline_analyses
1;
