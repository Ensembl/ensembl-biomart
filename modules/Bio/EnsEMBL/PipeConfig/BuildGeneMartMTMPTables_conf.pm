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

package Bio::EnsEMBL::PipeConfig::BuildGeneMartMTMPTables_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::PipeConfig::GenericMart_conf');


sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
        run_all                  => 0,
        drop_mart_db             => 1,
        drop_mtmp_probestuff     => 1,
        drop_mtmp_tv             => 0,
        drop_mtmp_variation      => 1,
        scripts_dir              => $self->o('base_dir') . '/ensembl-variation/scripts/',
        variation_import_lib     => $self->o('scripts_dir') . 'import',
        variation_feature_script => $self->o('scripts_dir') . 'misc/mart_variation_effect.pl',
        variation_mtmp_script    => $self->o('scripts_dir') . 'misc/create_MTMP_tables.pl',
        mart_phenotypes_script   => $self->o('scripts_dir') . 'misc/mart_phenotypes.pl',
    };
}

sub beekeeper_extra_cmdline_options {
    my $self = shift;
    return "-reg_conf " . $self->o("registry");
}

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
            -flow_into       => [ 'SpeciesFactory' ]
        },
        {
            -logic_name      => 'SpeciesFactory',
            -module          => 'Bio::EnsEMBL::Production::Pipeline::Common::SpeciesFactory',
            -parameters      => {
                species     => $self->o('species'),
                antispecies => $self->o('antispecies'),
                division    => $self->o('division'),
                run_all     => $self->o('run_all'),
            },
            -max_retry_count => 0,
            -flow_into       => {
                '4' => 'CheckExcludedSpeciesVariation',
                '6' => 'CheckExcludedSpeciesRegulation',
            }
        },
        {
            -logic_name      => 'CheckExcludedSpeciesVariation',
            -module          => 'Bio::EnsEMBL::BioMart::CheckExcludedSpecies',
            -max_retry_count => 0,
            -parameters      => {
                base_dir => $self->o('base_dir')
            },
            -flow_into       => {
                '3' => 'CreateMTMPVariation',
            }
        },
        {
            -logic_name      => 'CheckExcludedSpeciesRegulation',
            -module          => 'Bio::EnsEMBL::BioMart::CheckExcludedSpecies',
            -max_retry_count => 0,
            -parameters      => {
                base_dir => $self->o('base_dir'),
            },
            -flow_into       => {
                '3' => 'CreateMTMPProbestuffHelper',
            }
        },
        {
            -logic_name        => 'CreateMTMPProbestuffHelper',
            -module            => 'Bio::EnsEMBL::BioMart::CreateMTMPProbestuffHelper',
            -parameters        => {
                drop_mtmp => $self->o('drop_mtmp_probestuff'),
                registry  => $self->o('registry'),
            },
            -max_retry_count   => 1,
            -analysis_capacity => 50,
            -rc_name           => 'mem',
        },
        {
            -logic_name        => 'CreateMTMPVariation',
            -module            => 'Bio::EnsEMBL::BioMart::CreateMTMPVariation',
            -parameters        => {
                registry                 => $self->o('registry'),
                scratch_dir              => $self->o('scratch_dir'),
                drop_mtmp                => $self->o('drop_mtmp_variation'),
                drop_mtmp_tv             => $self->o('drop_mtmp_tv'),
                variation_import_lib     => $self->o('variation_import_lib'),
                variation_feature_script => $self->o('variation_feature_script'),
                variation_mtmp_script    => $self->o('variation_mtmp_script'),
                mart_phenotypes_script   => $self->o('mart_phenotypes_script'),
            },
            -max_retry_count   => 1,
            -analysis_capacity => 50,
            -rc_name           => 'mem',
        },
    ];

    return $analyses;
}

1;
