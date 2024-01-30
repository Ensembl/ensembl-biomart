=head1 LICENSE

Copyright [2009-2024] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::VariationMart::PartitionTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::VariationMart::Base');

sub param_defaults {
  return {
    'partition_size' => 100000,
  };
}

sub run {
  my ($self) = @_;
  
  my $partition_size = $self->param_required('partition_size');
  my $max_key_id = $self->param_required('max_key_id');
  my $base_where_sql = $self->param_required('base_where_sql');
  
  my ($lower, $upper) = (1, $partition_size);
  my @bounds = ([$lower, $upper]);
  while ($upper < $max_key_id) {
    $lower = $upper + 1;
    $upper = $upper + $partition_size;
    push @bounds, [$lower, $upper];
  }
  
  my @where_sql;
  foreach my $bounds (@bounds) {
    my $lower = $$bounds[0];
    my $upper = $$bounds[1];
    my $where_sql = "$base_where_sql $lower AND $upper ";
    push @where_sql, $where_sql;
  }
  
  $self->param('where_sql', \@where_sql);
}

sub write_output {
  my ($self) = @_;
  
  foreach my $where_sql ( @{$self->param('where_sql')} ) {    
    $self->dataflow_output_id({'where_sql' => $where_sql, 'table' => $self->param('table'), 'species' => $self->param('species')}, 1);
  }
}

1;
