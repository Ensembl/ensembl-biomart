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
  my $mart_dbh = $self->mart_dbh;
  
  $self->create_index($table, $mart_dbh);
}

sub create_index {
  my ($self, $table, $mart_dbh) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix$table";
  my $sql_file = catdir($self->param_required('tables_dir'), $table, 'index.sql');
  
  my @index_sql = $self->read_array($sql_file);
  foreach my $index_sql (@index_sql) {
    $index_sql =~ s/SPECIES_ABBREV/$mart_table_prefix/gm;
    $mart_dbh->do($index_sql) or $self->throw($mart_dbh->errstr);
  }
}

sub read_array {
  my ($self, $filename) = @_;
  
  open my $fh, '<', $filename or $self->throw("Error opening $filename - $!\n");
  my @contents = <$fh>;
  close $fh;
  return @contents;
}

1;
