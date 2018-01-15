
=head1 LICENSE

Copyright [2009-2018] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::EGPipeline::VariationMart::AddMetaData;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return { 'dataset_name'   => 'snp',
           'dataset_main'   => 'variation__main',
           'description'    => 'variations'
         };
}

sub run {
  my ($self) = @_;
  my $mart_dbc = $self->mart_dbc;
  if ( $self->param_required('dataset_name') eq 'structvar' ) {
    my $tables =
      $mart_dbc->sql_helper->execute(-SQL=>'SHOW TABLES LIKE "%structvar%";');
    if (@$tables) {
      $self->run_names_script();
      $self->run_meta_script();
    }
  }
  elsif ( $self->param_required('dataset_name') eq 'snp_som' ) {
    my $tables =
      $mart_dbc->sql_helper->execute(-SQL=>'SHOW TABLES LIKE "%snp_som%";');
    if (@$tables) {
      $self->run_names_script();
      $self->run_meta_script();
    }
  }
  elsif ( $self->param_required('dataset_name') eq 'structvar_som' ) {
    my $tables =
      $mart_dbc->sql_helper->execute(-SQL=>'SHOW TABLES LIKE "%structvar_som%";');
    if (@$tables) {
      $self->run_names_script();
      $self->run_meta_script();
    }
  }
  else {
    $self->run_names_script();
    $self->run_meta_script();
  }
  $mart_dbc->disconnect_if_idle();
}

sub run_names_script {
  my ($self) = @_;

  my $scripts_lib = $self->param_required('scripts_lib');
  my $generate_names_script =
    $self->param_required('generate_names_script');

  my $cmd =
    "perl -I$scripts_lib $generate_names_script " . " -host " .
    $self->param_required('mart_host') . " -port " .
    $self->param_required('mart_port') . " -user " .
    $self->param_required('mart_user') . " -pass " .
    $self->param_required('mart_pass') . " -mart " .
    $self->param_required('mart_db_name') . " -div " .
    lc($self->param_required('division_name'));

  if ( system($cmd) ) {
    $self->throw("Loading failed when running $cmd");
  }
}

sub run_meta_script {
  my ($self) = @_;

  my $scripts_lib = $self->param_required('scripts_lib');
  my $generate_meta_script =
    $self->param_required('generate_meta_script');

  my $cmd =
    "perl -I$scripts_lib $generate_meta_script " . " -host " .
    $self->param_required('mart_host') . " -port " .
    $self->param_required('mart_port') . " -user " .
    $self->param_required('mart_user') . " -pass " .
    $self->param_required('mart_pass') . " -dbname " .
    $self->param_required('mart_db_name') . " -ds_basename " .
    $self->param_required('dataset_name') . " -template_name " .
    $self->param_required('description') . " -template " .
    $self->param_required('template_template');
  $cmd = $cmd . " -genomic_features_dbname " . $self->param_required('genomic_features_dbname') if $self->param_required('genomic_features_dbname') ne '';
  $cmd = $cmd . " -max_dropdown " . $self->param_required('max_dropdown') if $self->param_required('max_dropdown') ne '';
  if ( system($cmd) ) {
    $self->throw("Loading failed when running $cmd");
  }
} ## end sub run_meta_script

1;
