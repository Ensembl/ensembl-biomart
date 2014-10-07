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

package Bio::EnsEMBL::EGPipeline::VariationMart::CreateMartTables;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');
use File::Spec::Functions qw(catdir);

sub param_defaults {
  return {
    'variation_feature' => 0,
    'consequences' =>
      {'snp__mart_transcript_variation__dm' => 'consequence_types_2090'},
  };
}

sub run {
  my ($self) = @_;
  
  my $mart_dbh = $self->mart_dbh;
  
  $self->remove_unused_tables();
  
  foreach my $table ( @{$self->param_required('snp_tables')} ) {
    $self->create_table($table, $mart_dbh);
  }
  
  if ($self->param('sv_exists')) {
    foreach my $table ( @{$self->param_required('sv_tables')} ) {
      $self->create_table($table, $mart_dbh);
    }
  }
}

sub remove_unused_tables {
  my ($self) = @_;
  
  my ($mfs, $rfs) = (0, 0);
  my $fdba = $self->get_DBAdaptor('funcgen');
  if (defined $fdba) {
    my $fdbh = $fdba->dbc()->db_handle;
    my $count_mf_sql = 'SELECT COUNT(*) FROM motif_feature;';
    my $count_rf_sql = 'SELECT COUNT(*) FROM regulatory_feature;';
    ($mfs) = $fdbh->selectrow_array($count_mf_sql) or $self->throw($fdbh->errstr);
    ($rfs) = $fdbh->selectrow_array($count_rf_sql) or $self->throw($fdbh->errstr);
  }
  
  my @tables;
  foreach my $snp_table (@{$self->param_required('snp_tables')}) {
    if ($snp_table =~ /motif_feature/) {
      next unless $mfs;
    }
    if ($snp_table =~ /regulatory_feature/) {
      next unless $rfs;
    }
    push @tables, $snp_table;
  }
  $self->param('snp_tables', \@tables);
}

sub create_table {
  my ($self, $table, $mart_dbh) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix\_$table";
  my $sql_file = catdir($self->param_required('tables_dir'), $table, 'select.sql');
  
  $self->existing_table_check($mart_dbh, $mart_table);
  
  my $core_db = $self->get_DBAdaptor('core')->dbc()->dbname;
  my $variation_db = $self->get_DBAdaptor('variation')->dbc()->dbname;
  
  my $select_sql = $self->read_string($sql_file);
  $select_sql =~ s/CORE_DB/$core_db/gm;
  $select_sql =~ s/VAR_DB/$variation_db/gm;
  $select_sql =~ s/SPECIES_ABBREV/$mart_table_prefix/gm;
  
  my $create_sql = "CREATE TABLE $mart_table AS $select_sql LIMIT 1;";
  my $truncate_sql = "TRUNCATE TABLE $mart_table;";
  
  $mart_dbh->do($create_sql) or $self->throw($mart_dbh->errstr);
  $mart_dbh->do($truncate_sql) or $self->throw($mart_dbh->errstr);
  
  $self->order_consequences($mart_dbh, $mart_table);
}

sub existing_table_check {
  my ($self, $mart_dbh, $mart_table) = @_;
  
  my $tables_sql = "SHOW TABLES LIKE '$mart_table';";
  my $tables = $mart_dbh->selectcol_arrayref($tables_sql) or $self->throw($mart_dbh->errstr);
  if (@$tables) {
    my $err = "Existing '$mart_table' table cannot be overwritten ".
      "unless you set the init_pipeline.pl parameter: '-drop_mart_tables 1'";
    $self->throw($err);
  }
}

sub read_string {
  my ($self, $filename) = @_;
  
  local $/ = undef;
  open my $fh, '<', $filename or $self->throw("Error opening $filename - $!\n");
  my $contents = <$fh>;
  close $fh;
  return $contents;
}

sub order_consequences {
  my ($self, $mart_dbh, $mart_table) = @_;
  
  my %consequences = %{$self->param('consequences')};
  foreach my $table (keys %consequences) {
    if ($mart_table =~ /$table/) {
      my $column = $consequences{$table};
      my $sth = $mart_dbh->column_info(undef, undef, $mart_table, $column);
      my $column_info = $sth->fetchrow_hashref() or $self->throw($mart_dbh->errstr);
      my $consequences = $$column_info{'mysql_type_name'};
      $consequences =~ s/set\((.*)\)/$1/;
      my @consequences = sort { lc($a) cmp lc($b) } split(/,/, $consequences);
      $consequences = join(',', @consequences);
      my $sql = "ALTER TABLE $mart_table MODIFY COLUMN $column SET($consequences);";
      $mart_dbh->do($sql) or $self->throw($mart_dbh->errstr);
    }
  }
}

sub write_output {
  my ($self) = @_;
  
  my @v = (
    'variation',
    'v',
    'variation_id',
    ['v.somatic = 0']);
  my @vf = (
    'variation_feature',
    'vf',
    'variation_feature_id',
    undef);
  my @sv = (
    'structural_variation',
    'sv',
    'structural_variation_id',
    ['sv.is_evidence = 0', 'sv.somatic = 0']);
  my @svf = (
    'structural_variation_feature',
    'svf',
    'structural_variation_feature_id',
    undef);
  
  my ($max_key_id, $base_where_sql);
  if ($self->param('variation_feature')) {
    $max_key_id = $self->max_key_id(@vf);
    $base_where_sql = $self->base_where_sql(@vf);
  } else {
    $max_key_id = $self->max_key_id(@v);
    $base_where_sql = $self->base_where_sql(@v);
  }
  
  foreach my $table ( @{$self->param_required('snp_tables')} ) {
    $self->dataflow_output_id({
      'table'          => $table,
      'max_key_id'     => $max_key_id,
      'base_where_sql' => $base_where_sql,
    }, 1);
  }
  
  if ($self->param('sv_exists')) {
    my ($sv_max_key_id, $sv_base_where_sql);
    if ($self->param('variation_feature')) {
      $sv_max_key_id = $self->max_key_id(@svf);
      $sv_base_where_sql = $self->base_where_sql(@svf);
    } else {
      $sv_max_key_id = $self->max_key_id(@sv);
      $sv_base_where_sql = $self->base_where_sql(@sv);
    }
    
    foreach my $table ( @{$self->param_required('sv_tables')} ) {
      $self->dataflow_output_id({
        'table'          => $table,
        'max_key_id'     => $sv_max_key_id,
        'base_where_sql' => $sv_base_where_sql,
      }, 1);
    }
  }
}

sub max_key_id {
  my ($self, $table, $alias, $column, $conditions) = @_;
  
  my $vdbh = $self->get_DBAdaptor('variation')->dbc()->db_handle;
  
  # Don't bother getting the numbers exact, assume that the column has
  # roughly consecutive IDs; the partition size is thus an upper limit, really.
  my $sql = "SELECT MAX($column) FROM $table $alias;";
  if (defined $conditions) {
    my $where = ' WHERE '.join(' AND ', @$conditions);
    $sql =~ s/;$/$where;/;
  }
  my ($max_var_id) = $vdbh->selectrow_array($sql) or $self->throw($vdbh->errstr);
  return $max_var_id;
}

sub base_where_sql {
  my ($self, $table, $alias, $column, $conditions) = @_;
  
  my $base_where_sql = ' WHERE '; 
  if (defined $conditions) {
    $base_where_sql .= join(' AND ', @$conditions);
    $base_where_sql .= ' AND ';
  }
  $base_where_sql .= "$alias.$column BETWEEN ";
  return $base_where_sql;
}

1;
