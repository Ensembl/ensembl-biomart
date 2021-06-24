=pod
=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION  

=head1 LICENSE
    Copyright [1999-2021] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
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
use base ('Bio::EnsEMBL::PipeConfig::GenericMart_conf');
use Bio::EnsEMBL::ApiVersion;


sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
        'user'        => undef,
        'pass'        => undef,
        'port'        => undef,
        'host'        => undef,
        'olduser'     => undef,
        'oldport'     => undef,
        'oldhost'     => undef,
        'mart'        => undef,
        'datasets'    => [],
        'compara'     => undef,
        'suffix'      => '',
        'base_name'   => 'gene',
        'old_mart'    => undef,
        'old_release' => undef,
        'new_release' => undef,
        'grch37'      => 0
    };
}

sub pipeline_analyses {
    my ($self) = @_;
    my $analyses = [
        {
            -logic_name        => 'cleanup_old_database',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type       => 'LSF',
            -flow_into         => 'sequence_dataset_factory',
            -input_ids         => [ {} ],
            -parameters        => {
                'cmd'  =>
                    'mysql -h#host# -u#user# -p#pass# -P#port# -e "DROP DATABASE IF EXISTS #mart#; CREATE DATABASE #mart#;"',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port')
            },
            -analysis_capacity => 1
        },
        {
            -logic_name => 'sequence_dataset_factory',
            -module     => 'Bio::EnsEMBL::BioMart::SequenceDatasetFactory',
            -parameters => {
                'division' => $self->o('division'),
                'suffix'   => $self->o('suffix'),
                'user'     => $self->o('user'),
                'pass'     => $self->o('pass'),
                'host'     => $self->o('host'),
                'port'     => $self->o('port'),
                'mart'     => $self->o('mart'),
                'datasets' => $self->o('datasets'),
                'base_dir' => $self->o('base_dir') },
            -flow_into  => {
                '1->A' => [ 'build_sequence' ],
                'A->2' => [ 'optimize' ]
            }
        },
        {
            -logic_name        => 'build_sequence',
            -module            => 'Bio::EnsEMBL::BioMart::BuildSequenceMart',
            -meadow_type       => 'LSF',
            -parameters        => {
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port')
            },
            -rc_name           => 'himem',
            -analysis_capacity => 10
        },
        {
            -logic_name        => 'optimize',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type       => 'LSF',
            -flow_into         => 'generate_meta',
            -parameters        => {
                'cmd'  =>
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
            -logic_name        => 'generate_meta',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type       => 'LSF',
            -flow_into         => 'run_tests',
            -parameters        => {
                'cmd'      =>
                    'perl #base_dir#/ensembl-biomart/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #base_dir#/ensembl-biomart/scripts/templates/sequence_template_template.xml  -ds_basename genomic_sequence -template_name sequences',
                'mart'     => $self->o('mart'),
                'user'     => $self->o('user'),
                'pass'     => $self->o('pass'),
                'host'     => $self->o('host'),
                'port'     => $self->o('port'),
                'base_dir' => $self->o('base_dir')
            },
            -analysis_capacity => 1 },
        {
            -logic_name        => 'run_tests',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type       => 'LSF',
            -flow_into         => 'check_tests',
            -parameters        => {
                'cmd'         =>
                    'cd #test_dir#;perl #base_dir#/ensembl-biomart/scripts/pre_configuration_mart_healthcheck.pl -newuser #user# -newpass #pass# -newport #port# -newhost #host# -olduser #olduser# -oldport #oldport# -oldhost #oldhost# -new_dbname #mart# -old_dbname #old_mart# -old_rel #old_release# -new_rel #new_release# -empty_column 1 -grch37 #grch37# -mart sequence_mart',
                'mart'        => $self->o('mart'),
                'user'        => $self->o('user'),
                'pass'        => $self->o('pass'),
                'host'        => $self->o('host'),
                'port'        => $self->o('port'),
                'olduser'     => $self->o('olduser'),
                'oldhost'     => $self->o('oldhost'),
                'oldport'     => $self->o('oldport'),
                'old_mart'    => $self->o('old_mart'),
                'test_dir'    => $self->o('test_dir'),
                'old_release' => $self->o('old_release'),
                'new_release' => $self->o('new_release'),
                'base_dir'    => $self->o('base_dir'),
                'grch37'      => $self->o('grch37')
            },
            -analysis_capacity => 1
        },
        {
            -logic_name      => 'check_tests',
            -module          => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type     => 'LSF',
            -max_retry_count => 0,
            -parameters      => {
                'cmd'              =>
                    'EXIT_CODE=0;failed_tests=`find #test_dir#/#old_mart#_#oldhost#_vs_#mart#_#host#.* -type f ! -empty -print`;if [ -n "$failed_tests" ]; then >&2 echo "Some tests have failed please check ${failed_tests}";EXIT_CODE=1;fi;exit $EXIT_CODE',
                'mart'             => $self->o('mart'),
                'host'             => $self->o('host'),
                'oldhost'          => $self->o('oldhost'),
                'old_mart'         => $self->o('old_mart'),
                'test_dir'         => $self->o('test_dir'),
                -analysis_capacity => 1
            },
        }
    ];
    return $analyses;
} ## end sub pipeline_analyses
1;
