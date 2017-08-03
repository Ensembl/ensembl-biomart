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

package Bio::EnsEMBL::EGPipeline::VariationMart::InitialiseMartDB;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return {
    'drop_mart_db' => 0,
  };
}

sub run {
  my ($self) = @_;
  
  my $mart_db_name = $self->param_required('mart_db_name');
  my $drop_mart_db = $self->param_required('drop_mart_db');
  
  # The mart database must be alongside the variation and core databases,
  # in order for subsequent SQL commands to work. Check that the registry
  # is pointing at a single server, then pop a new mart db there. If one
  # already exists, only overwrite if explicitly told to do so.
  my @dba = @{ Bio::EnsEMBL::Registry->get_all_DBAdaptors() };
  my $dba = pop @dba;
  my $dbc = $dba->dbc();
  my $host = $dbc->host;
  foreach my $db (@dba) {
    if ($db->dbc()->host ne $host) {
      $self->throw('Registry file must connect to a single database host.');
    }
  }
  
  
  if ($drop_mart_db) {
    my $drop_sql = "DROP DATABASE IF EXISTS $mart_db_name;";
    $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
  }
  
  my $create_sql = "CREATE DATABASE IF NOT EXISTS $mart_db_name;";
  $dbc->sql_helper->execute_update(-SQL=>$create_sql);
  
  $self->param('mart_host', $host);
  $self->param('mart_port', $dbc->port);
  $self->param('mart_user', $dbc->user);
  $self->param('mart_pass', $dbc->pass);
  $dbc->disconnect_if_idle();
}

sub write_output {
  my ($self) = @_;
 
  $self->dataflow_output_id({
    'mart_host'    => $self->param('mart_host'),
    'mart_port'    => $self->param('mart_port'),
    'mart_user'    => $self->param('mart_user'),
    'mart_pass'    => $self->param('mart_pass'),
    'mart_db_name' => $self->param('mart_db_name'),
  }, 1);
}

1;
