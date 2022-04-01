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

package Bio::EnsEMBL::PipeConfig::BuildVariationMartMTMPTables_eg_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::PipeConfig::BuildVariationMartMTMPTables_conf');

sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},

        mart_db_name            => $self->o('division_name') . '_snp_mart_' . $self->o('eg_release'),
        pipeline_name           => 'variation_mart_' . $self->o('division_name') . '_' . $self->o('eg_release'),
        drop_mtmp               => 1,
        drop_mtmp_tv            => 0,
        sample_threshold        => 20000,
        population_threshold    => 500,
        optimize_tables         => 1,
        populate_mart_rc_name   => '4Gb_mem',
        species_suffix          => '_eg',
        genomic_features_dbname => $self->o('division_name') . '_genomic_features_mart_' . $self->o('eg_release'),


        # Most mart table configuration is in VariationMart_conf, but e! and EG
        # differ in the absence/presence of the poly__dm table.
        snp_indep_tables        => [
            'snp__variation__main',
            'snp__poly__dm',
            'snp__population_genotype__dm',
            'snp__variation_annotation__dm',
            'snp__variation_citation__dm',
            'snp__variation_set_variation__dm',
            'snp__variation_synonym__dm',
        ],

        snp_cull_tables         => {
            'snp__poly__dm'                    => 'name_2019',
            'snp__population_genotype__dm'     => 'name_2019',
            'snp__variation_annotation__dm'    => 'name_2021',
            'snp__variation_citation__dm'      => 'authors_20137',
            'snp__variation_set_variation__dm' => 'name_2077',
            'snp__variation_synonym__dm'       => 'name_2030',
        },
    };
}

1;
