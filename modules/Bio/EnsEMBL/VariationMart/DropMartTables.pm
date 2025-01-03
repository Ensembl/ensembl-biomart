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

package Bio::EnsEMBL::VariationMart::DropMartTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::VariationMart::Base');
use MartUtils qw(generate_dataset_name_from_db_name);

sub run {
  my ($self) = @_;
  
  my $species = $self->param_required('species');
  
  if ($self->param('drop_mart_tables')) {
    my $database = $self->get_DBAdaptor('core')->dbc()->dbname;
    my $mart_table_prefix = generate_dataset_name_from_db_name($database);
    
    my $mart_dbh = $self->mart_dbh();
    
    my $tables_sql = "SHOW TABLES LIKE '$mart_table_prefix%';";
    my $tables = $mart_dbh->selectcol_arrayref($tables_sql) or $self->throw($mart_dbh->errstr);
    foreach my $table (@$tables) {
      my $drop_sql = "DROP TABLE $table;";
      $mart_dbh->do($drop_sql) or $self->throw($mart_dbh->errstr);
    }
  }
}

1;
