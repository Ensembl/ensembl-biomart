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

package Bio::EnsEMBL::VariationMart::AnalyzeTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::VariationMart::Base');

sub param_defaults {
  my ($self) = @_;
  
  return {
    'optimize_tables' => 0,
  };
  
}

sub run {
  my ($self) = @_;
    
  my $mart_dbh = $self->mart_dbh;
  my $command = $self->param('optimize_tables') ? 'OPTIMIZE' : 'ANALYZE';
  my $tables = $mart_dbh->selectcol_arrayref('SHOW TABLES;') or $self->throw($mart_dbh->errstr);
  foreach my $table (@$tables) {
    my $sql = "$command TABLE $table;";
    $mart_dbh->do($sql) or $self->throw($mart_dbh->errstr);
  }
}

1;
