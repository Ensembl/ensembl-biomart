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
    'drop_mart_tables'     => 0,
    'mtmp_tables_exist'    => 0,
    'individual_threshold' => 100,
    'population_threshold' => 100,
    'copy_species'         => [],
    'copy_all'             => 0,
  };
}

sub write_output {
  my ($self) = @_;
  
  my $species = $self->param_required('species');
  my $drop_mart_tables = $self->param('drop_mart_tables');
  my $mtmp_tables_exist = $self->param('mtmp_tables_exist');
  my $copy_species = $self->param('copy_species');
  my $copy_all = $self->param('copy_all');
  
  $species =~ /^(\w).+_(\w+)$/;
  my $mart_table_prefix = "$1$2_eg";
  
  my $copy = 0;
  if ($self->param('copy_all')) {
    $copy = 1;
  } elsif (defined $copy_species) {
    foreach my $pspecies (@$copy_species) {
      $copy = 1 if $species eq $pspecies;
    }
  }
  
  my ($sv_exists, $show_inds, $show_pops) = $self->data_display();
  
  if ($drop_mart_tables) {
    $self->dataflow_output_id({'mart_table_prefix' => $mart_table_prefix}, 2);
  }
  
  if (!$mtmp_tables_exist) {
    $self->dataflow_output_id({
      'mart_table_prefix' => $mart_table_prefix,
      'sv_exists' => $sv_exists,
      'show_inds' => $show_inds,
      'show_pops' => $show_pops,
    }, 3);
  }
  
  if ($copy) {
    $self->dataflow_output_id({'mart_table_prefix' => $mart_table_prefix}, 4);
  } else {
    $self->dataflow_output_id({
      'mart_table_prefix' => $mart_table_prefix,
      'sv_exists' => $sv_exists,
      'show_inds' => $show_inds,
      'show_pops' => $show_pops,
    }, 5);
  }
}

sub data_display {
  my ($self) = @_;
  
  my $ind_threshold = $self->param('individual_threshold');
  my $pop_threshold = $self->param('population_threshold');
  
  my $vdbh = $self->get_DBAdaptor('variation')->dbc()->db_handle;
  my $sv_sql = 'SELECT COUNT(*) FROM structural_variation;';
  my ($svs) = $vdbh->selectrow_array($sv_sql) or $self->throw($vdbh->errstr);
  my $sv_exists = $svs ? 1 : 0;
  
  my $ind_sql = 'SELECT COUNT(*) FROM individual WHERE display NOT IN ("LD", "UNDISPLAYABLE");';
  my ($inds) = $vdbh->selectrow_array($ind_sql) or $self->throw($vdbh->errstr);
  my $show_inds = $inds <= $ind_threshold ? 1 : 0;
  
  my $pop_sql = 'SELECT COUNT(*) FROM population WHERE display NOT IN ("LD", "UNDISPLAYABLE");';
  my ($pops) = $vdbh->selectrow_array($pop_sql) or $self->throw($vdbh->errstr);
  my $show_pops = $pops <= $pop_threshold ? 1 : 0;
  
  $self->data_display_report($svs, $sv_exists, $inds, $show_inds, $pops, $show_pops);
  
  return ($sv_exists, $show_inds, $show_pops);
}

sub data_display_report {
  my ($self, $svs, $sv_exists, $inds, $show_inds, $pops, $show_pops) = @_;
  
  my $species = $self->param_required('species');
  
  my $filter_table_msg = "Species: $species\n";
  if ($sv_exists) {
    $filter_table_msg .= "\t$svs structural variations will be displayed.\n";
  }
  if ($show_inds) {
    $filter_table_msg .= "\tGenotypes will be displayed for $inds individuals.\n";
  } else {
    $filter_table_msg .= "\tGenotypes will not be displayed for individuals.\n";
  }
  if ($show_pops) {
    $filter_table_msg .= "\tGenotypes will be displayed for $pops populations.\n";
  } else {
    $filter_table_msg .= "\tGenotypes will not be displayed for populations.\n";
  }
  
  $self->warning($filter_table_msg);
  print STDERR $filter_table_msg;
}

1;
