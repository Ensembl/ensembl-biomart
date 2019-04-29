
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

package Bio::EnsEMBL::PipeConfig::BuildGeneMartMTMPTables_conf;

use strict;
use warnings;
use Data::Dumper;
use Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf;
 # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use base ('Bio::EnsEMBL::Hive::PipeConfig::EnsemblGeneric_conf');
use Cwd;

sub resource_classes {
  my ($self) = @_;
  return { 'default' => { 'LSF' => '-q production-rh74' },
           'normal'            => {'LSF' => '-q production-rh74 -M  4000 -R "rusage[mem=4000]"'},
    '16Gb_mem_16Gb_scratch' => {'LSF' => '-q production-rh74 -M 16000 -R "rusage[mem=16000,scratch=16000]"'}
    };
}

sub default_options {
  my ($self) = @_;
  return { %{ $self->SUPER::default_options },
           'mart_db_name' => '',
           'drop_mart_db' => 1,
           'antispecies'  => [],
           'species'      => [],
           'division'     => [],
           'run_all'      => 0,
           'base_dir'  => getcwd,
           'registry'      => $self->o('registry'),
           'drop_mtmp_tv_vsv' => 0,
           'drop_mtmp_variation'      => 1,
           'drop_mtmp_probestuff'     => 1,
           # The following are required for building MTMP tables.
           'variation_import_lib' => $self->o('base_dir').'/ensembl-variation/scripts/import',
           'variation_feature_script' => $self->o('base_dir').'/ensembl-variation/scripts/misc/mart_variation_effect.pl',
           'variation_mtmp_script' => $self->o('base_dir').'/ensembl-variation/scripts/misc/create_MTMP_tables.pl',
           'mart_phenotypes_script' => $self->o('base_dir').'/ensembl-variation/scripts/misc/mart_phenotypes.pl',
           'scratch_dir'                  => '/scratch',
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

# Force an automatic loading of the registry in all workers.
sub beekeeper_extra_cmdline_options {
  my $self = shift;
  return "-reg_conf ".$self->o("registry");
}


=head2 pipeline_analyses
=cut

sub pipeline_analyses {
  my ($self) = @_;
  my $analyses = [
        {
      -logic_name      => 'InitialiseMartDB',
      -module          => 'Bio::EnsEMBL::EGPipeline::VariationMart::InitialiseMartDB',
      -input_ids       => [ {} ],
      -parameters      => {
                            mart_db_name => $self->o('mart_db_name'),
                            drop_mart_db => $self->o('drop_mart_db'),
                          },
      -max_retry_count => 0,
      -rc_name         => 'default',
      -flow_into       => {
                            '1' => ['ScheduleSpecies']
                          }
    },

    {
      -logic_name      => 'ScheduleSpecies',
      -module          => 'Bio::EnsEMBL::Production::Pipeline::Common::SpeciesFactory',
      -parameters      => {
                            species     => $self->o('species'),
                            antispecies => $self->o('antispecies'),
                            division    => $self->o('division'),
                            run_all     => $self->o('run_all'),
                          },
      -max_retry_count => 0,
      -rc_name         => 'normal',
      -flow_into       => {
                            '4' => 'CreateMTMPVariation',
                            '6' => 'CreateMTMPProbestuffHelper',     
                          }
    },
    {
      -logic_name        => 'CreateMTMPProbestuffHelper',
      -module            => 'Bio::EnsEMBL::BioMart::CreateMTMPProbestuffHelper',
      -parameters        => {
                              drop_mtmp     => $self->o('drop_mtmp_probestuff'),
                              registry                 => $self->o('registry'),
                            },
      -max_retry_count   => 3,
      -analysis_capacity => 5,
      -can_be_empty      => 1,
      -rc_name           => '16Gb_mem_16Gb_scratch',
    },
    {
      -logic_name        => 'CreateMTMPVariation',
      -module            => 'Bio::EnsEMBL::BioMart::CreateMTMPVariation',
      -parameters        => {
                              drop_mtmp               => $self->o('drop_mtmp_variation'),
                              drop_mtmp_tv_vsv         => $self->o('drop_mtmp_tv_vsv'),
                              variation_import_lib     => $self->o('variation_import_lib'),
                              variation_feature_script => $self->o('variation_feature_script'),
                              variation_mtmp_script    => $self->o('variation_mtmp_script'),
                              registry                 => $self->o('registry'),
                              scratch_dir                  => $self->o('scratch_dir'),
                              mart_phenotypes_script   => $self->o('mart_phenotypes_script'),
                            },
      -max_retry_count   => 3,
      -analysis_capacity => 5,
      -can_be_empty      => 1,
      -rc_name           => '16Gb_mem_16Gb_scratch',
    },
  ];
  return $analyses;
} ## end sub pipeline_analyses
1;
