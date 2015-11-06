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

package Bio::EnsEMBL::EGPipeline::VariationMart::CopyOrGenerate;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return {
    'drop_mart_tables'      => 0,
    'mtmp_tables_exist'     => 0,
    'sample_threshold'      => 100,
    'population_threshold'  => 100,
    'always_skip_genotypes' => [],
    'never_skip_genotypes'  => [],
    'division'              => [],
    'copy_species'          => [],
    'copy_all'              => 0,
  };
}

sub write_output {
  my ($self) = @_;
  
  my $species = $self->param_required('species');
  my $drop_mart_tables = $self->param('drop_mart_tables');
  my $mtmp_tables_exist = $self->param('mtmp_tables_exist');
  my $copy_species = $self->param('copy_species');
  my $copy_all = $self->param('copy_all');
  my $division = $self->param('division');
  my $mart_table_prefix;
  
  if (@$division){
    $species =~ /^(\w).+_(\w+)$/;
    $mart_table_prefix = "$1$2_eg";
  }
  else
  {
    $species =~ /^(\w).+_(\w+)$/;
    $mart_table_prefix = "$1$2";
  }
  
  my $copy = 0;
  if ($self->param('copy_all')) {
    $copy = 1;
  } elsif (defined $copy_species) {
    foreach my $pspecies (@$copy_species) {
      $copy = 1 if $species eq $pspecies;
    }
  }
  
  my ($sv_exists, $sv_som_exists, $show_sams, $show_pops) = $self->data_display();
  
  if ($drop_mart_tables) {
    $self->dataflow_output_id({'mart_table_prefix' => $mart_table_prefix}, 2);
  }
  
  if (!$mtmp_tables_exist) {
    $self->dataflow_output_id({
      'mart_table_prefix' => $mart_table_prefix,
      'sv_exists' => $sv_exists,
      'sv_som_exists' => $sv_som_exists,
      'show_sams' => $show_sams,
      'show_pops' => $show_pops,
    }, 3);
  }
  
  if ($copy) {
    $self->dataflow_output_id({'mart_table_prefix' => $mart_table_prefix}, 4);
  } else {
    $self->dataflow_output_id({
      'mart_table_prefix' => $mart_table_prefix,
      'sv_exists' => $sv_exists,
      'sv_som_exists' => $sv_som_exists,
      'show_sams' => $show_sams,
      'show_pops' => $show_pops,
    }, 5);
  }
}

sub data_display {
  my ($self) = @_;
  
  my $species       = $self->param_required('species');
  my $sam_threshold = $self->param('sample_threshold');
  my $pop_threshold = $self->param('population_threshold');
  my $always_skip   = $self->param('always_skip_genotypes');
  my $never_skip    = $self->param('never_skip_genotypes');
  
  my %always_skip = map {$_ => 1} @$always_skip;
  my %never_skip  = map {$_ => 1} @$never_skip;
  
  my $vdbh = $self->get_DBAdaptor('variation')->dbc()->db_handle;
  my $sv_sql = 'SELECT COUNT(*) FROM structural_variation where somatic=0;';
  my ($svs) = $vdbh->selectrow_array($sv_sql) or $self->throw($vdbh->errstr);
  my $sv_exists = $svs ? 1 : 0;
  
  my $sv_som_sql = 'SELECT COUNT(*) FROM structural_variation where somatic=1;';
  my ($svs_som) = $vdbh->selectrow_array($sv_som_sql) or $self->throw($vdbh->errstr);
  my $sv_som_exists = $svs_som ? 1 : 0;

  my ($show_sams, $show_pops, $sams, $pops);
  if (exists $always_skip{$species}) {
    $show_sams = 0;
    $show_pops = 0;
    $self->warning("Genotypes always skipped for $species");
  } else {
    my $sam_sql = 'SELECT COUNT(*) FROM sample WHERE display NOT IN ("LD", "UNDISPLAYABLE");';
    ($sams) = $vdbh->selectrow_array($sam_sql) or $self->throw($vdbh->errstr);
    
    my $pop_sql = 'SELECT COUNT(*) FROM population WHERE display NOT IN ("LD", "UNDISPLAYABLE");';
    ($pops) = $vdbh->selectrow_array($pop_sql) or $self->throw($vdbh->errstr);
    
    if (exists $never_skip{$species}) {
      $show_sams = $sams ? 1 : 0;
      $show_pops = $pops ? 1 : 0;
      $self->warning("Genotypes never skipped for $species");
    } 
    else{
      $show_sams = $sams <= $sam_threshold ? 1 : 0;
      $show_pops = $pops <= $pop_threshold ? 1 : 0;
    }
  }
  
  $self->data_display_report($svs, $svs_som, $sv_exists, $sv_som_exists, $sams, $show_sams, $pops, $show_pops);
  
  return ($sv_exists, $sv_som_exists, $show_sams, $show_pops);
}

sub data_display_report {
  my ($self, $svs, $svs_som, $sv_exists, $sv_som_exists, $sams, $show_sams, $pops, $show_pops) = @_;
  
  my $species = $self->param_required('species');
  
  my $filter_table_msg = "Species: $species\n";
  if ($sv_exists) {
    $filter_table_msg .= "\t$svs structural variations will be displayed.\n";
  }
  if ($sv_som_exists)
  {
    $filter_table_msg .= "\t$svs_som structural variations somatic will be displayed.\n";
  }
  if ($show_sams) {
    $filter_table_msg .= "\tGenotypes will be displayed for $sams samples.\n";
  } else {
    $filter_table_msg .= "\tGenotypes will not be displayed for samples.\n";
  }
  if ($show_pops) {
    $filter_table_msg .= "\tGenotypes will be displayed for $pops populations.\n";
  } else {
    $filter_table_msg .= "\tGenotypes will not be displayed for populations.\n";
  }
  
  $self->warning($filter_table_msg);
}

1;
