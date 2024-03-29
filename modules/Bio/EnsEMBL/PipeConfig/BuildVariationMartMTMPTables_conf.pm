=pod
=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION  

=head1 LICENSE
    Copyright [1999-2022] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
         http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.
=head1 CONTACT
    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates
=cut

package Bio::EnsEMBL::PipeConfig::BuildVariationMartMTMPTables_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::PipeConfig::VariationMart_conf');

sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
    }
}

sub pipeline_analyses {
    my ($self) = @_;
    my $analyses = [
        {
            -logic_name      => 'InitialiseMartDB',
            -module          => 'Bio::EnsEMBL::VariationMart::InitialiseMartDB',
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
                '4' => 'DropMartTables',
            }
        },
        {
            -logic_name        => 'DropMartTables',
            -module            => 'Bio::EnsEMBL::VariationMart::DropMartTables',
            -parameters        => {
                drop_mart_tables => $self->o('drop_mart_tables'),
            },
            -max_retry_count   => 0,
            -analysis_capacity => 5,
            -flow_into         => [ 'CopyOrGenerate' ],
        },

        {
            -logic_name      => 'CopyOrGenerate',
            -module          => 'Bio::EnsEMBL::VariationMart::CopyOrGenerate',
            -parameters      => {
                mtmp_tables_exist     => $self->o('mtmp_tables_exist'),
                copy_species          => $self->o('copy_species'),
                copy_all              => $self->o('copy_all'),
                sample_threshold      => $self->o('sample_threshold'),
                population_threshold  => $self->o('population_threshold'),
                always_skip_genotypes => $self->o('always_skip_genotypes'),
                never_skip_genotypes  => $self->o('never_skip_genotypes'),
                division_name         => $self->o('division_name'),
                species_suffix        => $self->o('species_suffix'),
                ensembl_cvs_root_dir  => $self->o('base_dir')
            },
            -max_retry_count => 0,
            -flow_into       => {
                '3' => [ 'CreateMTMPTables' ],
                '4'    => [ 'CopyMart' ],
            }
        },
        {
            -logic_name        => 'CreateMTMPTables',
            -module            => 'Bio::EnsEMBL::VariationMart::CreateMTMPTables',
            -parameters        => {
                drop_mtmp                => $self->o('drop_mtmp'),
                drop_mtmp_tv             => $self->o('drop_mtmp_tv'),
                variation_import_lib     => $self->o('variation_import_lib'),
                variation_feature_script => $self->o('variation_feature_script'),
                variation_mtmp_script    => $self->o('variation_mtmp_script'),
                registry                 => $self->o('registry'),
                scratch_dir              => $self->o('scratch_dir'),
            },
            -max_retry_count   => 3,
            -analysis_capacity => 5,
            -can_be_empty      => 1,
            -rc_name           => '16Gb_mem',
        },
        {
            -logic_name        => 'CopyMart',
            -module            => 'Bio::EnsEMBL::VariationMart::CopyMart',
            -parameters        => {
                previous_mart => $self->o('previous_mart'),
            },
            -max_retry_count   => 0,
            -analysis_capacity => 5,
            -can_be_empty      => 1,
            -rc_name           => '16Gb_mem',
        },

    ];

    return $analyses;
}



1;
