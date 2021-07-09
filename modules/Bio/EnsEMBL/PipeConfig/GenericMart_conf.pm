=pod
=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION

=head1 LICENSE
    Copyright [2021] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
         http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT
    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates
=cut

package Bio::EnsEMBL::PipeConfig::GenericMart_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');

use Bio::EnsEMBL::ApiVersion;
use File::Spec::Functions qw(catdir);
use Cwd;


sub resource_classes {
    my ($self) = @_;
    return {
        'default' => { 'LSF' => '-q production' },
        'low'     => { 'LSF' => '-q production -M 2048 -R "rusage[mem=2048]"' },
        'mem'     => { 'LSF' => '-q production -M 8192 -R "rusage[mem=8192]"' },
        'himem'   => { 'LSF' => '-q production -M 16384 -R "rusage[mem=16384]"' }
    };
}


sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
        'base_dir'    => $self->o('ENV', 'BASE_DIR'),
        'env_user'    => $self->o('ENV', 'USER'),
        'run_all'     => 0,
        'species'     => [],
        'antispecies' => [],
        'division'    => '',
        'mart_dir'    => getcwd,
        'scratch_dir' => catdir('/hps/scratch', $self->o('env_user'), $self->o('pipeline_name')),
        'test_dir'    => catdir('/hps/nobackup/flicek/ensembl/production', $self->o('env_user'),
            'mart_test', $self->o('pipeline_name')),
    }
}

sub pipeline_create_commands {
    my ($self) = @_;

    return [
        @{$self->SUPER::pipeline_create_commands},
        'mkdir -p ' . $self->o('test_dir'),
        'mkdir -p ' . $self->o('scratch_dir'),
    ];
}


1;