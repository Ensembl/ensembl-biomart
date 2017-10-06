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

package Bio::EnsEMBL::EGPipeline::VariationMart::PopulateMart;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');
use File::Spec::Functions qw(catdir);

sub param_defaults {
  return {
    'tmp_dir' => '/tmp',
  };
}

sub run {
  my ($self) = @_;
  
  my $table = $self->param_required('table');
  my $job_id = $self->input_job->dbID();
  my $output_file = $self->param('tmp_dir')."/$table.$job_id.sql";
  
  $self->dump_data($table, $output_file);
  $self->load_data($table, $output_file);
  unlink $output_file;
  
}

sub dump_data {
  my ($self, $table, $output_file) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix\_$table";
  my $sql_file = catdir($self->param_required('tables_dir'), $table, 'select.sql');
  
  my $core_db = $self->get_DBAdaptor('core')->dbc()->dbname;
  my $variation_db = $self->get_DBAdaptor('variation')->dbc()->dbname;
  
  my $select_sql = $self->read_string($sql_file);
  $select_sql =~ s/CORE_DB/$core_db/gm;
  $select_sql =~ s/VAR_DB/$variation_db/gm;
  $select_sql =~ s/SPECIES_ABBREV/$mart_table_prefix/gm;
  
  my $where_sql = $self->param_required('where_sql');
  $select_sql .= " $where_sql";
  
  my @params = (
    '--host='.$self->param_required('mart_host'),
    '--port='.$self->param_required('mart_port'),
    '--user='.$self->param_required('mart_user'),
    '--password='.$self->param_required('mart_pass'),
    $self->param_required('mart_db_name'),
  );
  
  my $cmd = 'mysql '.join(' ', @params)." -ss -r -e '$select_sql' | sed 's/NULL/\\\\N/g' > $output_file";
  if (system($cmd)) {
    $self->throw("Loading failed when running $cmd");
  }

  $self->get_DBAdaptor('core')->dbc()->disconnect_if_idle();
  $self->get_DBAdaptor('variation')->dbc()->disconnect_if_idle();
}

sub load_data {
  my ($self, $table, $output_file) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix\_$table";
  
  my $load_sql = "LOAD DATA LOCAL INFILE '$output_file' INTO TABLE $mart_table;";
  
  my $mart_dbc = $self->mart_dbc;
  $mart_dbc->sql_helper->execute_update("SET bulk_insert_buffer_size = 536870912;");
  $mart_dbc->sql_helper->execute_update(-SQL=>$load_sql);
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
