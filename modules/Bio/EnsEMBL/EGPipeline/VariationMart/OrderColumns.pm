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

package Bio::EnsEMBL::EGPipeline::VariationMart::OrderColumns;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub run {
  my ($self) = @_;
  
  my $table = $self->param_required('table');
  my $columns;
  if ($table =~ 'structural_variation_feature'){
    $columns='name_2034, seq_region_start_20104, seq_region_end_20104';
    $self->order_columns($table, $columns);
  }
  elsif ($table =~ 'variation_feature'){
    $columns='name_1059, seq_region_start_2026, seq_region_end_2026';
    $self->order_columns($table, $columns);
  }
}

sub order_columns {
  my ($self, $table, $columns) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix\_$table";
  
  my $order_sql = "ALTER TABLE $mart_table ORDER by $columns;";
  my $mart_dbc = $self->mart_dbc;
  $mart_dbc->sql_helper->execute_update(-SQL=>$order_sql) or $self->throw($mart_dbc->errstr);
  $mart_dbc->disconnect_if_idle;
}

1;
