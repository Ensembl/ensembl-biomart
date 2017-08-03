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

package Bio::EnsEMBL::EGPipeline::VariationMart::Base;

use strict;
use warnings;

use base qw/Bio::EnsEMBL::Hive::Process/;

sub mart_dba {
  my ($self) = @_;
  
  my $mart_dba = Bio::EnsEMBL::DBSQL::DBAdaptor->new(
    -host   => $self->param_required('mart_host'),
    -port   => $self->param_required('mart_port'),
    -user   => $self->param_required('mart_user'),
    -pass   => $self->param_required('mart_pass'),
    -dbname => $self->param_required('mart_db_name'),
  );
  
  return $mart_dba; 
}

sub mart_dbc {
  my ($self) = @_;
  
  return $self->mart_dba->dbc();
}

sub mart_dbh {
  my ($self) = @_;
  
  return $self->mart_dbc->db_handle(); 
}

1;
