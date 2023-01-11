=head1 LICENSE

Copyright [2009-2023] EMBL-European Bioinformatics Institute

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
use base ('Bio::EnsEMBL::Hive::Process');

use Bio::EnsEMBL::BioMart::Mart qw(genome_to_include);
use Bio::EnsEMBL::Registry;

sub run {
  my ($self) = @_;
  my $species  = $self->param_required('species');
  my $base_dir = $self->param_required('base_dir');
  my $included_species;

  my $dba = Bio::EnsEMBL::Registry->get_DBAdaptor($species, 'core');
  my $mca = $dba->get_adaptor('MetaContainer');
  my $division = $mca->get_division;

  if ($division =~ /vertebrates/i) {
    $included_species = genome_to_include($division, $base_dir);
    if (!grep( /$species/, @$included_species) ){
      $self->complete_early("Excluding $species from mart");
    }
  }
}

sub write_output {
  my ($self) = @_;
  my $species = $self->param_required('species');

  $self->dataflow_output_id({'species'=> $species}, 3);
}

1;
