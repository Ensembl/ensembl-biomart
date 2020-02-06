=head1 LICENSE

Copyright [2009-2020] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::BioMart::CheckExcludedSpecies;

use strict;
use warnings;
use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');
use MartUtils;
use Bio::EnsEMBL::BioMart::Mart qw(genome_to_include);
use Bio::EnsEMBL::MetaData::Base qw(process_division_names);

sub run {
  my ($self) = @_;
  my $div     = $self->param('division');
  my $species           = $self->param_required('species');
  my $base_dir          = $self->param('base_dir');
  my $included_species;
  my ($division,$division_name)=process_division_names($div);
  # Use division to find the release in metadata database
  if ($division_name eq "vertebrates"){
    # Load species to exclude from the Vertebrates marts
    $included_species = genome_to_include($division_name,$base_dir);
    if (!grep( /$species/, @$included_species) ){
      $self->warning("Excluding $species from mart");
      return;
    }
  }
  $self->dataflow_output_id({
    'species'          => $species
  }, 4);
  $self->dataflow_output_id({
    'species'          => $species
  }, 6);
}

1;
