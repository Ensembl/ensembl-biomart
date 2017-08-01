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

package Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartIndexes;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');
use File::Spec::Functions qw(catdir);

sub param_defaults {
  return {};
}

sub run {
  my ($self) = @_;
  
  my $table = $self->param_required('table');
  
  $self->create_index($table);
}

sub create_index {
  my ($self, $table) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix$table";
  my $sql_file = catdir($self->param_required('tables_dir'), $table, 'index.sql');
  
  my $hive_dbc = $self->dbc;
  $hive_dbc->disconnect_if_idle();

  my $index_sql = $self->read_string($sql_file);
  $index_sql =~ s/SPECIES_ABBREV/$mart_table_prefix/gm;
  my $mart_dbc = $self->mart_dbc;
  $mart_dbc->sql_helper->execute_update(-SQL=>$index_sql);
  $mart_dbc->disconnect_if_idle();
}

sub read_string {
  my ($self, $filename) = @_;
  
  local $/ = undef;
  open my $fh, '<', $filename or $self->throw("Error opening $filename - $!\n");
  my $contents = <$fh>;
  close $fh;
  return $contents;
}


1;
