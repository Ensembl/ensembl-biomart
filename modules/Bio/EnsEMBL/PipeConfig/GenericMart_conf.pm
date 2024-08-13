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
    my $self = shift;


    ## Sting it together
    my %time = (H => ' --time=1:00:00',
        D         => ' --time=1-00:00:00',
        W         => ' --time=7-00:00:00',);

    my %memory = ('1000M' => '1000',
        '200M'           => '200',
        '500M'           => '500',
        '1GB'            => '1000',
        '2GB'            => '2000',
        '3GB'            => '3000',
        '4GB'            => '4000',
        '8GB'            => '8000',
        '16GB'           => '16000',
        '32GB'           => '32000',);

    my $pq = ' --partition=standard';
    my $dq = ' --partition=datamover';

    my %output = (
        #Default is a duplicate of 100M
        'default'   => { 'LSF' => '-q production', 'SLURM' => $pq . $time{'H'} . ' --mem=' . $memory{'1000M'} . 'm' },
        'default_D' => { 'LSF' => '-q production', 'SLURM' => $pq . $time{'D'} . ' --mem=' . $memory{'1000M'} . 'm' },
        'default_W' => { 'LSF' => '-q production', 'SLURM' => $pq . $time{'W'} . ' --mem=' . $memory{'1000M'} . 'm' },
        #Data mover nodes
        'dm'        => { 'LSF' => '-q datamover', 'SLURM' => $dq . $time{'H'} . ' --mem=' . $memory{'100M'} . 'm' },
        'dm_D'      => { 'LSF' => '-q datamover', 'SLURM' => $dq . $time{'D'} . ' --mem=' . $memory{'100M'} . 'm' },
        'dm_W'      => { 'LSF' => '-q datamover', 'SLURM' => $dq . $time{'W'} . ' --mem=' . $memory{'100M'} . 'm' },

        'low'   => { 'LSF' => '-q production -M 2048 -R "rusage[mem=2048]"', 'SLURM' => $pq . $time{'H'} . ' --mem=' . $memory{'2000M'} . 'm' },
        'mem'   => { 'LSF' => '-q production -M 8192 -R "rusage[mem=8192]"', 'SLURM' => $pq . $time{'H'} . ' --mem=8192m' },
        'himem'   => { 'LSF' => '-q production -M 16384 -R "rusage[mem=16384]"', 'SLURM' => $pq . $time{'H'} . ' --mem=16384m' },

    );
    #Create a dictionary of all possible time and memory combinations. Format would be:
    #2G={
    #   'SLURM' => ' --partition=standard --time=1:00:00  --mem=2000m',
    #   'LSF' => '-q $self->o(production_queue) -M 2000 -R "rusage[mem=2000]"'
    # };

    while ((my $time_key, my $time_value) = each(%time)) {
        while ((my $memory_key, my $memory_value) = each(%memory)) {
            if ($time_key eq 'H') {
                $output{$memory_key} = { 'LSF' => '-q production -M ' . $memory_value . ' -R "rusage[mem=' . $memory_value . ']"',
                    'SLURM'                    => $pq . $time_value . '  --mem=' . $memory_value . 'm' }
            }
            else {
                $output{$memory_key . '_' . $time_key} = { 'LSF' => '-q production -M ' . $memory_value . ' -R "rusage[mem=' . $memory_value . ']"',
                    'SLURM'                                      => $pq . $time_value . '  --mem=' . $memory_value . 'm' }
            }
        }
    }

    return \%output;

};



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
        'scratch_dir' => catdir('/hps/scratch/flicek/ensembl/production', $self->o('env_user'), $self->o('pipeline_name')),
        'test_dir'    => catdir('/hps/nobackup/flicek/ensembl/production', $self->o('env_user'), 'mart_test',
            $self->o('pipeline_name'))
    };
}

sub pipeline_wide_parameters {
    my ($self) = @_;
    return {
        %{$self->SUPER::pipeline_wide_parameters},
        'scratch_dir' => $self->o('scratch_dir'),
        'test_dir'    => $self->o('test_dir')
    };
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
