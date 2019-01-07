=head1 LICENSE

Copyright [2009-2019] EMBL-European Bioinformatics Institute

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
use Bio::EnsEMBL::DBSQL::DBAdaptor;
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

sub get_DBAdaptor {
  my ($self, $type) = @_;

  $type ||= 'core';
  my $species = ($type =~ /^(production|taxonomy)$/) ? 'multi' : $self->param_required('species');

  return Bio::EnsEMBL::Registry->get_DBAdaptor($species, $type);
}

# Check if a MTMP table already exists
# MTMP_transcript_variation and MTMP_variation_set_variation are quite
# big for some species so if the table is already there keep it
sub does_table_exist {
  my ($self,$table_name) = @_;
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $sth = $dbc->db_handle->table_info(undef, undef, "MTMP_$table_name", 'TABLE');
  $sth->execute or $self->throw($dbc->db_handle->errstr);
  my @info = $sth->fetchrow_array;
  my $exists = scalar @info;
  $dbc->disconnect_if_idle();
  return $exists;
}

sub order_consequences {
  my ($self) = @_;
  my $hive_dbc = $self->dbc;
  $hive_dbc->disconnect_if_idle();
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $table = 'MTMP_transcript_variation';
  my $column = 'consequence_types';
  my $sth = $dbc->db_handle->column_info(undef, undef, $table, $column);
  my $column_info = $sth->fetchrow_hashref() or $self->throw($dbc->db_handle->errstr);
  my $consequences = $$column_info{'mysql_type_name'};
  $consequences =~ s/set\((.*)\)/$1/;
  my @consequences = sort { lc($a) cmp lc($b) } split(/,/, $consequences);
  $consequences = join(',', @consequences);
  my $sql = "ALTER TABLE $table MODIFY COLUMN $column SET($consequences);";
  $dbc->db_handle->do($sql) or $self->throw($dbc->db_handle->errstr);
  $dbc->disconnect_if_idle();
}

sub run_script {
  my ($self, $script, $table_param_name, $table) = @_;
  my $dbc = $self->get_DBAdaptor('variation')->dbc();
  my $drop_mtmp = $self->param_required('drop_mtmp');
  my $variation_import_lib = $self->param_required('variation_import_lib');
  my $tmp_dir = $self->param_required('tmp_dir');
  my $drop_mtmp_tv_vsv = $self->param_required('drop_mtmp_tv_vsv');


  # Drop table if exist and drop_mtmp parameter set to 1
  # We don't want to drop the MTMP_transcript_variation or MTMP_variation_set_variation tables as they
  # gets automatically renerated by the Transcript variation pipeline for the variarion mart
  if ($drop_mtmp){
    if (!$drop_mtmp_tv_vsv and (($table eq "transcript_variation") or ($table eq "variation_set_variation"))){
      1;
    }
    else {
      my $drop_sql = "DROP TABLE IF EXISTS MTMP_$table;";
      $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
    }
  }
  if ($self->does_table_exist($table)) {
    $self->warning("MTMP_$table already exists for this species");
  }
  else{
    $dbc->disconnect_if_idle();
    my $hive_dbc = $self->dbc;
    $hive_dbc->disconnect_if_idle();
    my $cmd = "perl -I$variation_import_lib $script ".
      " --host ".$dbc->host.
      " --port ".$dbc->port.
      " --user ".$dbc->username.
      " --pass ".$dbc->password.
      " --db ".$dbc->dbname.
      " --tmpdir $tmp_dir ".
      " --tmpfile mtmp_".$table."_".$self->param_required('species').".txt";
    if ($table_param_name and $table){
      $cmd = $cmd." --$table_param_name $table";
    }
    if (system($cmd)) {
      my $drop_sql = "DROP TABLE IF EXISTS MTMP_$table;";
      $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
      $self->throw("Loading failed when running $cmd");
    }
  }
  $dbc->disconnect_if_idle();
}

1;
