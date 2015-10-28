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

package Bio::EnsEMBL::EGPipeline::VariationMart::AddMetaData;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return {
    'dataset_name'   => 'snp',
    'dataset_main'   => 'variation__main',
    'species_suffix' => '_eg',
    'description'    => 'variations',
    'tmp_dir'     => '/tmp',
  };
}

sub run {
  my ($self) = @_;
  
  if ($self->param_required('dataset_name') eq 'structvar') {
    my $mart_dbh = $self->mart_dbh();
    my $tables = $mart_dbh->selectcol_arrayref('SHOW TABLES LIKE "%structvar%";') or $self->throw($mart_dbh->errstr);
    if (@$tables) {
      $self->run_names_script();
      $self->run_template_script();
    }
    $mart_dbh->do('DROP TABLE IF EXISTS dataset_names;') or $self->throw($mart_dbh->errstr);
  } else {
    $self->run_names_script();
    $self->run_template_script();
  }
}

sub run_names_script {
  my ($self) = @_;
  
  my $scripts_lib = $self->param_required('scripts_lib');
  my $generate_names_script = $self->param_required('generate_names_script');
  
  my $cmd = "perl -I$scripts_lib $generate_names_script ".
    " -host ".$self->param_required('mart_host').
    " -port ".$self->param_required('mart_port').
    " -user ".$self->param_required('mart_user').
    " -pass ".$self->param_required('mart_pass').
    " -mart ".$self->param_required('mart_db_name').
    " -name ".$self->param_required('dataset_name').
    " -suffix ".$self->param_required('species_suffix').
    " -main ".$self->param_required('dataset_main').
    " -release ".$self->param_required('ensembl_release');
  
  if (system($cmd)) {
    $self->throw("Loading failed when running $cmd");
  }
}

sub run_template_script {
  my ($self) = @_;
  
  my $scripts_lib = $self->param_required('scripts_lib');
  my $generate_template_script = $self->param_required('generate_template_script');
  
  my $cmd = "perl -I$scripts_lib $generate_template_script ".
    " -host ".$self->param_required('mart_host').
    " -port ".$self->param_required('mart_port').
    " -user ".$self->param_required('mart_user').
    " -pass ".$self->param_required('mart_pass').
    " -mart ".$self->param_required('mart_db_name').
    " -dataset ".$self->param_required('dataset_name').
    " -description ".$self->param_required('description').
    " -template ".$self->param_required('template_template').
    " -ds_template ".$self->param_required('dataset_template').
    " -tmp_dir ".$self->param_required('tmp_dir').
    " -release ".$self->param_required('ensembl_release');
  
  if (system($cmd)) {
    $self->throw("Loading failed when running $cmd");
  }
}

1;
