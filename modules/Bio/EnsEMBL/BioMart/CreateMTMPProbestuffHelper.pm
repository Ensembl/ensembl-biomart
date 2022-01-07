=head1 LICENSE

Copyright [2009-2022] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::BioMart::CreateMTMPProbestuffHelper;

use base ('Bio::EnsEMBL::VariationMart::Base');

use strict;
use warnings;
sub param_defaults {
  return {
    'drop_mtmp'         => 0,
  };
}

sub run {
  my ($self) = @_;
  my $drop_mtmp = $self->param_required('drop_mtmp');
  my $dbc = $self->get_DBAdaptor('funcgen')->dbc();
  my $table='MTMP_probestuff_helper';
  my $species = $self->param_required('species');

  # Drop table if exist and drop_mtmp parameter set to 1
  # We don't want to drop the MTMP_transcript_variation or MTMP_variation_set_variation tables as they
  # gets automatically renerated by the Transcript variation pipeline
  if ($drop_mtmp){
      my $drop_sql = "DROP TABLE IF EXISTS $table;";
      $dbc->sql_helper->execute_update(-SQL=>$drop_sql);
  }
  if ($self->does_table_exist($table)) {
    $self->warning("$table already exists for this species");
  }
  else{
    my $hive_dbc = $self->dbc;
    $hive_dbc->disconnect_if_idle();
    my $create_sql =
    qq/CREATE TABLE $table (
      array_name VARCHAR(40),
      transcript_stable_id  VARCHAR(128),
      display_label         VARCHAR(100),
      array_vendor_and_name VARCHAR(80),
      is_probeset_array     tinyint(1),
      KEY transcript_idx (transcript_stable_id),
      KEY vendor_name_idx (array_vendor_and_name)
    )ENGINE=MyISAM DEFAULT CHARSET=latin1;/;

    my $insert1_sql =
    qq/INSERT INTO $table
      SELECT distinct
        array.name                     AS array_name,
        probe_set_transcript.stable_id AS transcript_stable_id,
        probe_set.name                 AS display_label,
        CONCAT(array.vendor, '_', REPLACE(REPLACE(array.name, '-', '_'), '.', '_')) AS array_vendor_and_name,
        array.is_probeset_array        AS is_probeset_array
      FROM array JOIN
        array_chip using (array_id) JOIN
        probe using (array_chip_id) JOIN
        probe_set using (probe_set_id) JOIN
        probe_set_transcript using (probe_set_id)
      WHERE array.is_probeset_array=true;/;

    my $insert2_sql =
      qq/INSERT INTO $table
      SELECT distinct
        array.name                     AS array_name,
        probe_transcript.stable_id     AS transcript_stable_id,
        probe.name                     AS display_label,
        CONCAT(array.vendor, '_', REPLACE(REPLACE(array.name, '-', '_'), '.', '_')) AS array_vendor_and_name,
        array.is_probeset_array        AS is_probeset_array
      FROM array JOIN
        array_chip using (array_id) JOIN
        probe using (array_chip_id) JOIN
        probe_transcript using (probe_id)
      WHERE
        array.is_probeset_array=false;/;

    $dbc->db_handle->do($create_sql) or $self->throw($dbc->db_handle->errstr);
    $dbc->db_handle->do($insert1_sql) or $self->throw($dbc->db_handle->errstr);
    $dbc->db_handle->do($insert2_sql) or $self->throw($dbc->db_handle->errstr);

    #The folowing probe name was to long for Rabbit and the name was exceding the mysql limit of 64 characters in biomart: ocuniculus_gene_ensembl__eFG_AGILENT_SurePrint_GPL16709_4x44k__dm
    if ($species eq "oryctolagus_cuniculus"){
      my $array_name_fix=
      qq/UPDATE $table
      SET array_vendor_and_name ='AGILENT_SurePrnt_GPL16709_4x44k'
      WHERE array_vendor_and_name='AGILENT_SurePrint_GPL16709_4x44k';/;
      $dbc->db_handle->do($array_name_fix) or $self->throw($dbc->db_handle->errstr);
    }
    #Same for chicken ggallus_gene_ensembl__eFG_AGILENT_AGILENT_059389_Custom_Chicken_GE_8X60k__dm
    elsif ($species eq "gallus_gallus"){
      my $array_name_fix=
      qq/UPDATE $table
      SET array_vendor_and_name ='AGILENT_059389_Chicken_GE_8X60k'
      WHERE array_vendor_and_name='AGILENT_AGILENT_059389_Custom_Chicken_GE_8X60k';/;
      $dbc->db_handle->do($array_name_fix) or $self->throw($dbc->db_handle->errstr);
    }
    #Same for Barley hvulgare_eg_gene__eFG_AGILENT_AgilentBarleyGeneExpressionMicroarray__dm
    elsif ($species eq "hordeum_vulgare"){
      my $array_name_fix=
      qq/UPDATE $table
      SET array_vendor_and_name ='AGILENT_Barley Gene Expression Microarray'
      WHERE array_vendor_and_name='AGILENT_Agilent Barley Gene Expression Microarray';/;
      $dbc->db_handle->do($array_name_fix) or $self->throw($dbc->db_handle->errstr);
    }
  }
  #optimize MTMP table
  my $optimize_sql = qq/OPTIMIZE TABLE $table;/;
  $dbc->db_handle->do($optimize_sql) or $self->throw($dbc->db_handle->errstr);
  $dbc->disconnect_if_idle();
}

# Check if a MTMP table already exists
# MTMP_transcript_variation and MTMP_variation_set_variation are quite
# big for some species so if the table is already there keep it
sub does_table_exist {
  my ($self,$table_name) = @_;
  my $dbc = $self->get_DBAdaptor('funcgen')->dbc();
  my $sth = $dbc->db_handle->table_info(undef, undef, "$table_name", 'TABLE');
  $sth->execute or $self->throw($dbc->db_handle->errstr);
  my @info = $sth->fetchrow_array;
  my $exists = scalar @info;
  $dbc->disconnect_if_idle();
  return $exists;
}

1;
