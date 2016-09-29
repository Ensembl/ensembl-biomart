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


package Bio::EnsEMBL::PipeConfig::create_MTMP_tables_Ensembl_conf;

use strict;
use warnings;
use Data::Dumper;
use base ('Bio::EnsEMBL::Hive::PipeConfig::EnsemblGeneric_conf');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Bio::EnsEMBL::ApiVersion;
use Cwd;

sub resource_classes {
    my ($self) = @_;
    return {'normal' => {'LSF' => '-q normal -M500 -R"select[mem>500] rusage[mem=500]"'},
            'mem'    => {'LSF' => '-q normal -M2500 -R"select[mem>2500] rusage[mem=2500]"'}
    };
}

sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
        'user'=>undef,
        'pass'=>undef,
        'port'=>undef,
        'host'=>undef,
        'release'=>software_version(),
        'eg_release'=>undef,
        'base_dir'=>getcwd,
        'division'=>'ensembl',
        'tmp_directory'=>undef,
        'pipeline_name' => 'create_MTMP_tables_ensembl_gene_mart_'.$self->o('release'),
        'species' => undef,
        'antispecies' => undef,
        'run_all'  => undef,
    }
}

=head2 pipeline_wide_parameters
=cut

#sub pipeline_wide_parameters {
#    my ($self) = @_;
#    return {
#        %{$self->SUPER::pipeline_wide_parameters}          # here we inherit anything from the base class, then add our own stuff
#    };
#}

# Force an automatic loading of the registry in all workers.
sub beekeeper_extra_cmdline_options {
  my $self = shift;
  return "-reg_conf ".$self->o("registry");
}

sub pipeline_create_commands {
    my ($self) = @_;
    return [
      # inheriting database and hive tables' creation
      @{$self->SUPER::pipeline_create_commands},
      'mkdir -p '.$self->o('tmp_directory'),
    ];
}

=head2 pipeline_analyses
=cut
    
    sub pipeline_analyses {
        my ($self) = @_;
    my $analyses = [
        {
            -logic_name      => 'ScheduleSpecies',
            -module          => 'Bio::EnsEMBL::EGPipeline::Common::RunnableDB::EGSpeciesFactory',
            -parameters      => {
                            species     => $self->o('species'),
                            antispecies => $self->o('antispecies'),
                            division    => $self->o('division'),
                            run_all     => $self->o('run_all'),
                          },
            -max_retry_count => 0,
            -input_ids=>[{}],
            -rc_name         => 'normal',
            -flow_into       => {
                            '4' => 'MTMP_phenotype',
                            '5' => 'MTMP_probes',
                          },
            -meadow_type     => 'LOCAL',
        },
        {
            -logic_name    => 'MTMP_phenotype',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #base_dir#/ensembl-variation/scripts/misc/mart_phenotypes.pl -user #user# -pass #pass# -port #port# -host #host# -tmpdir #tmp_dir# -tmpfile tmp_#species#.txt -pattern #species#',
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'base_dir' => $self->o('base_dir'),
                'tmp_dir'  => $self->o('tmp_directory'),
            },
            -analysis_capacity => 10
        },
        {   
            -logic_name    => 'MTMP_probes',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'cd #base_dir#/ensembl-production_private/biomart/Manual_scripts/Probes; ./create_MTMP_probes.sh -h #host# -u #user# -p #pass# -P #port# -s #species# -r #release# -b .',  
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'release' => $self->o('release'),
                'base_dir' => $self->o('base_dir'),
            },
            -analysis_capacity => 10
        },
         
        ];
    return $analyses;
}
1;
