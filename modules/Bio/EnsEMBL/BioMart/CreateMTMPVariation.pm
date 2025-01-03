=head1 LICENSE

Copyright [2009-2025] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

package Bio::EnsEMBL::BioMart::CreateMTMPVariation;

use strict;
use warnings;

use base ('Bio::EnsEMBL::VariationMart::Base');

sub param_defaults {
  return {
    'drop_mtmp'         => 0,
    'drop_mtmp_tv'   => 0,
  };
}

sub run {
  my ($self) = @_;
  my $variation_feature_script = $self->param_required('variation_feature_script');
  my $variation_mtmp_script    = $self->param_required('variation_mtmp_script');
  my $mart_phenotypes_script   = $self->param_required('mart_phenotypes_script');

  # Always need transcript_variation table;
  $self->run_script($variation_feature_script, 'table', 'transcript_variation');
  $self->order_consequences;
  
  # Create the MTMP_evidence view using the Variation script
  $self->run_script($variation_mtmp_script, 'mode', 'evidence');

  # Create the MTMP_phenotype view using the Variation script
  $self->run_script($mart_phenotypes_script, '', 'phenotype');
}

1;
