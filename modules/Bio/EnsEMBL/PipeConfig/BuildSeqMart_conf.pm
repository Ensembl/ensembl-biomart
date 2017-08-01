
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

package Bio::EnsEMBL::PipeConfig::BuildSeqMart_conf;

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
  return { 
	  'default' => { 'LSF' => '-q production-rh7' },
	  'himem' => { 'LSF' => '-q production-rh7 -M 16384 -R "rusage[mem=16384]"' }
	 };
}

sub default_options {
  my ($self) = @_;
  return { %{ $self->SUPER::default_options },
           'user'      => undef,
           'pass'      => undef,
           'port'      => undef,
           'host'      => undef,
           'mart'      => undef,
           'division'      => undef,
           'datasets'  => [],
           'compara'   => undef,
           'base_dir'  => getcwd,
	         'suffix' => '',
           'base_name' => 'gene',
 };
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
'perl #base_dir#/ensembl-biomart/scripts/generate_names.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -div #division#',
        'mart'     => $self->o('mart'),
        'user'     => $self->o('user'),
        'pass'     => $self->o('pass'),
        'host'     => $self->o('host'),
        'port'     => $self->o('port'),
        'base_dir' => $self->o('base_dir'),
        'division'  => $self->o('division') },
      -input_ids         => [ {} ],
      -analysis_capacity => 1,
      -meadow_type       => 'LOCAL',
      -flow_into => {1 => 'dataset_factory'},
      },
      {
        -logic_name => 'dataset_factory',
        -module     => 'Bio::EnsEMBL::BioMart::DatasetFactory',
        -parameters => { 'mart'     => $self->o('mart'),
                       'user'     => $self->o('user'),
                       'pass'     => $self->o('pass'),
                       'host'     => $self->o('host'),
                       'port'     => $self->o('port'),
                       'datasets' => $self->o('datasets'),
                       'base_dir' => $self->o('base_dir')},
        -flow_into => { 1 => [ 'build_sequence' ],
                      2 => [ 'optimize', 'generate_meta' ]
                    },
        -meadow_type => 'LOCAL'
    },
    { -logic_name  => 'build_sequence',
      -module     => 'Bio::EnsEMBL::BioMart::BuildSequenceMart',
      -meadow_type => 'LSF',
      -parameters  => {
        'mart'      => $self->o('mart'),
        'user'      => $self->o('user'),
        'pass'      => $self->o('pass'),
        'host'      => $self->o('host'),
        'port'      => $self->o('port') },
      -rc_name           => 'himem',
      -analysis_capacity => 10 },
    { -logic_name  => 'optimize',
      -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
      -meadow_type => 'LSF',
      -wait_for    => [ 'build_sequence' ],
      -parameters  => {
        'cmd' =>
'mysqlcheck -h#host# -u#user# -p#pass# -P#port# --optimize "#mart#"',
        'mart' => $self->o('mart'),
        'user' => $self->o('user'),
        'pass' => $self->o('pass'),
        'host' => $self->o('host'),
        'port' => $self->o('port') },
      -analysis_capacity => 1 },
      { -logic_name  => 'generate_meta',
        -module      => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
        -meadow_type => 'LSF',
        -wait_for    => 'optimize',
        -parameters  => {
                         'cmd' =>
                         'perl #base_dir#/ensembl-biomart/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #base_dir#/ensembl-biomart/scripts/templates/sequence_template_template.xml  -ds_basename genomic_sequence -template_name sequences',
                         'mart'   => $self->o('mart'),
                         'user'     => $self->o('user'),
                         'pass'     => $self->o('pass'),
                         'host'     => $self->o('host'),
                         'port'     => $self->o('port'),
                         'base_dir' => $self->o('base_dir') },
        -input_ids         => [ {} ],
        -analysis_capacity => 1 }
      
  ];
  return $analyses;
} ## end sub pipeline_analyses
1;
