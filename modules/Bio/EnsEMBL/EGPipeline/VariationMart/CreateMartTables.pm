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
    'sv_exists'         => 0,
    'sv_som_exists'     => 0,
    'regulatory_exists' => 0,
    'motif_exists'      => 0,
    'show_sams'         => 1,
    'show_pops'         => 1,
    'variation_feature' => 0,
    'consequences'      =>
      {'snp__mart_transcript_variation__dm' => 'consequence_types_2090'},
  };
}

sub run {
  my ($self) = @_;
  
  my @tables=(); 
  $self->remove_unused_tables();
  
  foreach my $table ( @{$self->param_required('snp_tables')} ) {
    $self->create_table($table);
  }

  if ($self->param_required('species') eq 'homo_sapiens')
  {
    foreach my $table ( @{$self->param_required('snp_som_tables')} ) {
      $self->create_table($table);
    }
    if ($self->param('sv_som_exists') and $self->param_required('species') eq 'homo_sapiens') {
      foreach my $table ( @{$self->param_required('sv_som_tables')} ) {
        $self->create_table($table);
      }
    }
  }
  
  if ($self->param('sv_exists')) {
    foreach my $table ( @{$self->param_required('sv_tables')} ) {
      $self->create_table($table);
    }
  }
}

sub remove_unused_tables {
  my ($self) = @_;
  
  my $show_sams = $self->param_required('show_sams');
  my $show_pops = $self->param_required('show_pops');
  my $mfs       = $self->param_required('motif_exists');
  my $rfs       = $self->param_required('regulatory_exists');

  my @tables;
  foreach my $snp_table (@{$self->param_required('snp_tables')}) {
    if ($snp_table =~ /__motif_feature/) {
      next unless $mfs;
    }
    if ($snp_table =~ /__regulatory_feature/) {
      next unless $rfs;
    }
    if ($snp_table =~ /__m*poly__/) {
      next unless $show_sams;
    }
    if ($snp_table =~ /__population_genotype__/) {
      next unless $show_pops;
    }
    push @tables, $snp_table;
  }
  $self->param('snp_tables', \@tables);
  if ($self->param_required('snp_som_tables')){
    my @som_tables;
    foreach my $snp_som_table (@{$self->param_required('snp_som_tables')}) {
      if ($snp_som_table =~ /__motif_feature/) {
        next unless $mfs;
      }
      if ($snp_som_table =~ /__regulatory_feature/) {
        next unless $rfs;
      }
      if ($snp_som_table =~ /__m*poly__/) {
        next unless $show_sams;
      }
      if ($snp_som_table =~ /__population_genotype__/) {
        next unless $show_pops;
      }
      push @som_tables, $snp_som_table;
    }
    $self->param('snp_som_tables', \@som_tables); 
  }
}

sub create_table {
  my ($self, $table) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_table = "$mart_table_prefix\_$table";
  my $sql_file = catdir($self->param_required('tables_dir'), $table, 'select.sql');
  my $mart_dbc = $self->mart_dbc;

  $self->existing_table_check($mart_table);
  
  my $core_db = $self->get_DBAdaptor('core')->dbc()->dbname;
  my $variation_db = $self->get_DBAdaptor('variation')->dbc()->dbname;
  
  my $select_sql = $self->read_string($sql_file);
  $select_sql =~ s/CORE_DB/$core_db/gm;
  $select_sql =~ s/VAR_DB/$variation_db/gm;
  $select_sql =~ s/SPECIES_ABBREV/$mart_table_prefix/gm;
  
  my $create_sql = "CREATE TABLE $mart_table AS $select_sql LIMIT 1;";
  my $truncate_sql = "TRUNCATE TABLE $mart_table;";
  
  $mart_dbc->sql_helper->execute_update(-SQL=>$create_sql);
  $mart_dbc->sql_helper->execute_update(-SQL=>$truncate_sql);
  
  $mart_dbc->disconnect_if_idle();
  $self->order_consequences($mart_table);
}

sub existing_table_check {
  my ($self, $mart_table) = @_;
  
  my $mart_dbc = $self->mart_dbc;
  my $tables_sql = "SHOW TABLES LIKE '$mart_table';";
  my $tables = $mart_dbc->sql_helper->execute(-SQL=>$tables_sql);
  if (@$tables) {
    my $err = "Existing '$mart_table' table cannot be overwritten ".
      "unless you set the init_pipeline.pl parameter: '-drop_mart_tables 1'";
    $self->throw($err);
  }
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

sub order_consequences {
  my ($self, $mart_table) = @_;
  
  my $mart_dbh = $self->mart_dbh;
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
    ['v.somatic = 0', 'v.display = 1']);
  my @vsom = (
      'variation',
    'v',
    'variation_id',
    ['v.somatic = 1', 'v.display = 1']);
  my @t = (
    'transcript',
    't',
    'transcript_id',
    ['t.source != "LRG database"']);
  my @vf = (
    'variation_feature',
    'vf',
    'variation_feature_id',
    ['vf.somatic = 0', 'vf.display = 1']);
  my @vfsom = (
    'variation_feature',
    'vf',
    'variation_feature_id',
    ['vf.somatic = 1', 'vf.display = 1']);
  my @sv = (
    'structural_variation',
    'sv',
    'structural_variation_id',
    ['sv.is_evidence = 0', 'sv.somatic = 0']);
  my @svsom = (
    'structural_variation',
    'sv',
    'structural_variation_id',
    ['sv.is_evidence = 0', 'sv.somatic = 1']);
  my @svf = (
    'structural_variation_feature',
    'svf',
    'structural_variation_feature_id',
    ['svf.somatic = 0']);
   my @svfsom = (
    'structural_variation_feature',
    'svf',
    'structural_variation_feature_id',
    ['svf.somatic = 1']);

  my ($max_key_id, $base_where_sql, $max_key_id_som, $base_where_sql_som);
  if (defined $self->param('base_name')){
    if ($self->param('base_name') =~ m/gene/i ){
      $max_key_id = $self->max_key_id("core",@t);
      $base_where_sql = $self->base_where_sql(@t);
      if ($self->param_required('species') eq 'homo_sapiens')
      {
        $max_key_id_som = $self->max_key_id("core",@t);
        $base_where_sql_som = $self->base_where_sql(@t);
      }
    }
  }
  elsif ($self->param('variation_feature')) {
    $max_key_id = $self->max_key_id("variation",@vf);
    $base_where_sql = $self->base_where_sql(@vf);
    if ($self->param_required('species') eq 'homo_sapiens')
    {
      $max_key_id_som = $self->max_key_id("variation",@vfsom);
      $base_where_sql_som = $self->base_where_sql(@vfsom);
    }
  } else {
    $max_key_id = $self->max_key_id("variation",@v);
    $base_where_sql = $self->base_where_sql(@v);
    if ($self->param_required('species') eq 'homo_sapiens')
    {
      $max_key_id_som = $self->max_key_id("variation",@vsom);
    $base_where_sql_som = $self->base_where_sql(@vsom);
    }
  }

  foreach my $table ( @{$self->param_required('snp_tables')} ) {
    $self->dataflow_output_id({
      'table'          => $table,
      'max_key_id'     => $max_key_id,
      'base_where_sql' => $base_where_sql,
    }, 1);
  }

  if ($self->param_required('species') eq 'homo_sapiens')
  {
    foreach my $table ( @{$self->param_required('snp_som_tables')} ) {
      $self->dataflow_output_id({
          'table'          => $table,
          'max_key_id'     => $max_key_id_som,
          'base_where_sql' => $base_where_sql_som,
          }, 1);
    }
  }
  if ($self->param('sv_exists')) {
    my ($sv_max_key_id, $sv_base_where_sql);
    if ($self->param('variation_feature')) {
      $sv_max_key_id = $self->max_key_id(@svf);
      $sv_base_where_sql = $self->base_where_sql(@svf); 
    }
    else {
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

    if ($self->param('sv_som_exists') and $self->param_required('species') eq 'homo_sapiens')
    {
      my ($sv_max_key_id_som, $sv_base_where_sql_som);
      if ($self->param('variation_feature')) {
        $sv_max_key_id_som = $self->max_key_id(@svfsom);
        $sv_base_where_sql_som = $self->base_where_sql(@svfsom);
      }
      elsif ($self->param_required('species') eq 'homo_sapiens')  {
        $sv_max_key_id_som = $self->max_key_id(@svsom);
        $sv_base_where_sql_som = $self->base_where_sql(@svsom);
      }
    
      foreach my $table ( @{$self->param_required('sv_som_tables')} ) {
      $self->dataflow_output_id({
        'table'          => $table,
        'max_key_id'     => $sv_max_key_id_som,
        'base_where_sql' => $sv_base_where_sql_som,
      }, 1);
    }
  }
}

sub max_key_id {
  my ($self, $database, $table, $alias, $column, $conditions) = @_;
  
  my $vdbc = $self->get_DBAdaptor($database)->dbc();
  
  # Don't bother getting the numbers exact, assume that the column has
  # roughly consecutive IDs; the partition size is thus an upper limit, really.
  my $sql = "SELECT MAX($column) FROM $table $alias;";
  if (defined $conditions) {
    my $where = ' WHERE '.join(' AND ', @$conditions);
    $sql =~ s/;$/$where;/;
  }
  my ($max_var_id) = $vdbc->sql_helper->execute_simple(-SQL=>$sql)->[0];
  $vdbc->disconnect_if_idle();
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
