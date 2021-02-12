=head1 LICENSE

Copyright [2009-2021] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::EGPipeline::VariationMart::CopyMart;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  my ($self) = @_;
  
  return {};
}

sub fetch_input {
  my ($self) = @_;
  
  my $mart_db_name = $self->param_required('mart_db_name');
  my %previous_mart = %{$self->param_required('previous_mart')};
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  
  if (!defined $previous_mart{'-dbname'}) {
    my ($mart_db_stem, $mart_db_version) = $mart_db_name =~ /^(.+_)(\d+)$/;
    $previous_mart{'-dbname'} = $mart_db_stem.($mart_db_version - 1);
    $self->param('previous_mart', \%previous_mart);
  }
  
  my $previous_mart_dba = Bio::EnsEMBL::DBSQL::DBAdaptor->new(%previous_mart);
  my $previous_mart_dbc = $previous_mart_dba->dbc();
  
  my $tables_sql = "SHOW TABLES LIKE '$mart_table_prefix%';";
  my $tables = $previous_mart_dbc->sql_helper->execute(-SQL=>$tables_sql);
  my %checksum_list;
  foreach my $table (@$tables) {
    my $checksum_sql = "CHECKSUM TABLE $table;";
    my (undef, $checksum) = $previous_mart_dbc->sql_helper->execute_simple(-SQL=>$checksum_sql)->[0];
    $checksum_list{$table} = $checksum;
  }
  
  $self->param('tables', $tables);
  $self->param('checksum_list', \%checksum_list);
  $previous_mart_dbc->disconnect_if_idle();
}

sub run {
  my ($self) = @_;
  
  # Some of these tables can be very big, so do them one at a time,
  # to decrease the risk of failure, and so that we don't need to
  # grab a large amount of memory and tmp space from the node.
  my @tables = @{$self->param('tables')};
  
  $self->existing_table_check();
  
  foreach my $table (@tables) {
    my $output_file = $self->param('scratch_dir')."/$table.sql";
    $self->dump_mart_table($table, $output_file);
    $self->load_mart_table($output_file);
    $self->check_mart_table($table);
    unlink $output_file;
  }
}

sub existing_table_check {
  my ($self) = @_;
  
  my $mart_dbc = $self->mart_dbc;  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $tables_sql = "SHOW TABLES LIKE '$mart_table_prefix%';";
  my $tables = $mart_dbc->sql_helper->execute(-SQL=>$tables_sql);
  if (@$tables) {
    my $err = "Existing '$mart_table_prefix%' tables cannot be overwritten ".
      "unless you set the init_pipeline.pl parameter: '-drop_mart_tables 1'";
    $self->throw($err);
  }
  $mart_dbc->disconnect_if_idle();
}

sub dump_mart_table {
  my ($self, $table, $output_file) = @_;
  
  my %previous_mart = %{$self->param_required('previous_mart')};
  
  my @params = (
    '--host='.$previous_mart{'-host'},
    '--port='.$previous_mart{'-port'},
    '--user='.$previous_mart{'-user'},
    '--skip-lock-tables',
    $previous_mart{'-dbname'},
    $table,
  );
  
  my $cmd = 'mysqldump '.join(' ', @params).' > '.$output_file;
  if (system($cmd)) {
    $self->throw("Dumping failed when running $cmd");
  }
}

sub load_mart_table {
  my ($self, $output_file) = @_;
  
  my @params = (
    '--host='.$self->param_required('mart_host'),
    '--port='.$self->param_required('mart_port'),
    '--user='.$self->param_required('mart_user'),
    '--password='.$self->param_required('mart_pass'),
    $self->param_required('mart_db_name'),
  );
  
  my $cmd = 'mysql '.join(' ', @params).' < '.$output_file;
  if (system($cmd)) {
    $self->throw("Loading failed when running $cmd");
  }
}

sub check_mart_table {
  my ($self, $table) = @_;
  
  my $mart_dbc = $self->mart_dbc; 
  my $checksum_list = $self->param('checksum_list');
  
  my $checksum_sql = "CHECKSUM TABLE $table;";
  my (undef, $checksum) = $mart_dbc->sql_helper->execute_simple(-SQL=>$checksum_sql)->[0];
  if ($$checksum_list{$table} ne $checksum) {
    $self->throw("CHECKSUM failed for table '$table'");
  }
  $mart_dbc->disconnect_if_idle();
}

1;
