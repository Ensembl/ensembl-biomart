=head1 LICENSE

Copyright [2009-2014] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::EGPipeline::VariationMart::CopyOrGenerate;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return {
    'drop_mart_tables' => 0,
    'copy_species'     => [],
    'copy_all'         => 0,
  };
}

sub write_output {
  my ($self) = @_;
  
  my $species = $self->param_required('species');
  my $drop_mart_tables = $self->param('drop_mart_tables');
  my $mtmp_tables_exist = $self->param('mtmp_tables_exist');
  my $copy_species = $self->param('copy_species');
  my $copy_all = $self->param('copy_all');
  
  $species =~ /^(\w).+_(\w+)$/;
  my $mart_table_prefix = "$1$2_eg";
  
  my $copy = 0;
  if ($self->param('copy_all')) {
    $copy = 1;
  } elsif (defined $copy_species) {
    foreach my $pspecies (@$copy_species) {
      $copy = 1 if $species eq $pspecies;
    }
  }
  
  my $vdbh = $self->get_DBAdaptor('variation')->dbc()->db_handle;
  my $count_sql = 'SELECT COUNT(*) FROM structural_variation;';
  my ($svs) = $vdbh->selectrow_array($count_sql) or $self->throw($vdbh->errstr);
  my $sv_exists = $svs ? 1 : 0;
  
  if ($drop_mart_tables) {
    $self->dataflow_output_id({'mart_table_prefix' => $mart_table_prefix}, 2);
  }
  
  if (!$mtmp_tables_exist) {
    $self->dataflow_output_id({
      'mart_table_prefix' => $mart_table_prefix,
      'sv_exists' => $sv_exists,
    }, 3);
  }
  
  if ($copy) {
    $self->dataflow_output_id({'mart_table_prefix' => $mart_table_prefix}, 4);
  } else {
    $self->dataflow_output_id({
      'mart_table_prefix' => $mart_table_prefix,
      'sv_exists' => $sv_exists,
    }, 5);
  }
}

1;
