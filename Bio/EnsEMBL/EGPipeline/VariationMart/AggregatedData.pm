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

package Bio::EnsEMBL::EGPipeline::VariationMart::AggregatedData;

use strict;
use warnings;

use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');

sub param_defaults {
  return {
    'sv_exists' => 0,
  };
}

sub run {
  my ($self) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $mart_dbh = $self->mart_dbh;
  my $variation_db = $self->get_DBAdaptor('variation')->dbc()->dbname;
  
  $self->variation_annotation_bool($mart_table_prefix, $mart_dbh, $variation_db);
  $self->variation_citation_bool($mart_table_prefix, $mart_dbh, $variation_db);
  $self->variation_feature_count($mart_table_prefix, $mart_dbh, $variation_db);
  
  if ($self->param('sv_exists')) {
    $self->structural_variation_feature_count($mart_table_prefix, $mart_dbh, $variation_db);
  }
}

sub variation_annotation_bool {
  my ($self, $mart_table_prefix, $mart_dbh, $variation_db) = @_;
  
  my $initialise_sql =
    'ALTER TABLE '.$mart_table_prefix.'_snp__variation__main '.
    'ADD COLUMN variation_annotation_bool int(11) DEFAULT 0';
  
  my $update_sql =
    'UPDATE '.
    $mart_table_prefix.'_snp__variation__main v_m INNER JOIN '.
    $variation_db.'.MTMP_variation_annotation va ON v_m.variation_id_2025_key = va.variation_id '.
    'SET v_m.variation_annotation_bool = 1;';
  
  $mart_dbh->do($initialise_sql) or $self->throw($mart_dbh->errstr);
  $mart_dbh->do($update_sql) or $self->throw($mart_dbh->errstr);
}

sub variation_citation_bool {
  my ($self, $mart_table_prefix, $mart_dbh, $variation_db) = @_;
  
  my $initialise_sql =
    'ALTER TABLE '.$mart_table_prefix.'_snp__variation__main '.
    'ADD COLUMN variation_citation_bool int(11) DEFAULT 0';
  
  my $update_sql =
    'UPDATE '.
    $mart_table_prefix.'_snp__variation__main v_m INNER JOIN '.
    $variation_db.'.variation_citation vc ON v_m.variation_id_2025_key = vc.variation_id '.
    'SET v_m.variation_citation_bool = 1;';
  
  $mart_dbh->do($initialise_sql) or $self->throw($mart_dbh->errstr);
  $mart_dbh->do($update_sql) or $self->throw($mart_dbh->errstr);
}

sub variation_feature_count {
  my ($self, $mart_table_prefix, $mart_dbh, $variation_db) = @_;
  
  my $initialise_sql =
    'ALTER TABLE '.$mart_table_prefix.'_snp__variation__main '.
    'ADD COLUMN variation_feature_count int(11) DEFAULT 0';
  
  my $update_sql =
    'UPDATE '.
    $mart_table_prefix.'_snp__variation__main v_m '.
    'SET v_m.variation_feature_count = '.
      '(SELECT COUNT(vf.variation_id) FROM '.$variation_db.'.variation_feature vf '.
      'WHERE v_m.variation_id_2025_key = vf.variation_id);';
  
  $mart_dbh->do($initialise_sql) or $self->throw($mart_dbh->errstr);
  $mart_dbh->do($update_sql) or $self->throw($mart_dbh->errstr);
}

sub structural_variation_feature_count {
  my ($self, $mart_table_prefix, $mart_dbh, $variation_db) = @_;
  
  my $initialise_sql =
    'ALTER TABLE '.$mart_table_prefix.'_structvar__structural_variation__main '.
    'ADD COLUMN structural_variation_feature_count int(11) DEFAULT 0';
  
  my $update_sql =
    'UPDATE '.
    $mart_table_prefix.'_structvar__structural_variation__main sv_m '.
    'SET sv_m.structural_variation_feature_count = '.
      '(SELECT COUNT(svf.structural_variation_id) FROM '.$variation_db.'.structural_variation_feature svf '.
      'WHERE sv_m.structural_variation_id_2072_key = svf.structural_variation_id);';
  
  $mart_dbh->do($initialise_sql) or $self->throw($mart_dbh->errstr);
  $mart_dbh->do($update_sql) or $self->throw($mart_dbh->errstr);
}

1;
