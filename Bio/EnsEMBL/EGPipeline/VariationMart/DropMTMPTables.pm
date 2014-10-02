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

package Bio::EnsEMBL::EGPipeline::VariationMart::DropMTMPTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub run {
  my ($self) = @_;
  
  my $dbh = $self->get_DBAdaptor('variation')->dbc()->db_handle;
  my $tables_sql = 'SHOW TABLES LIKE "MTMP%";'
  my $tables = $dbh->selectcol_arrayref($tables_sql) or $self->throw($dbh->errstr);
  foreach my $table (@$tables) {
    my $drop_sql = "DROP TABLE $table;";
    $dbh->do($drop_sql) or $self->throw($dbh->errstr);
  }
}

1;
