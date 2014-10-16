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

package Bio::EnsEMBL::EGPipeline::VariationMart::CullMartTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub run {
  my ($self) = @_;
  
  my %snp_cull_tables = %{$self->param_required('snp_cull_tables')};
  my $mart_dbh = $self->mart_dbh;
  
  foreach my $table (keys %snp_cull_tables) {
    $self->cull_table($table, $snp_cull_tables{$table}, $mart_dbh);
  }
  
  if ($self->param('sv_exists')) {
    my %sv_cull_tables = %{$self->param_required('sv_cull_tables')};
    foreach my $table (keys %sv_cull_tables) {
      $self->cull_table($table, $sv_cull_tables{$table}, $mart_dbh);
    }
  }
}

sub cull_table {
  my ($self, $table, $column, $mart_dbh) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix\_$table";
  
  my $tables_sql = "SHOW TABLES LIKE '$mart_table';";
  my $tables = $mart_dbh->selectcol_arrayref($tables_sql) or $self->throw($mart_dbh->errstr);
  if (@$tables) {  
    my $count_sql = "SELECT COUNT(*) FROM $mart_table WHERE $column IS NOT NULL";
    my ($rows) = $mart_dbh->selectrow_array($count_sql) or $self->throw($mart_dbh->errstr);
    
    if ($rows == 0) {
      my $drop_sql = "DROP TABLE $mart_table";
      $mart_dbh->do($drop_sql) or $self->throw($mart_dbh->errstr);
    }
  }
}

1;
