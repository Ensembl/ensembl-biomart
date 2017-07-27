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

package Bio::EnsEMBL::BioMart::ConcatColumns;
use base ('Bio::EnsEMBL::EGPipeline::VariationMart::Base');


use strict;
use warnings;

sub run {
  my ($self) = @_;
  my $concat_columns = $self->param_required('concat_columns');
  
  foreach my $table (keys %{$concat_columns})
  {
    $self->create_stable_id_version_column($table,$concat_columns->{$table}->[0],$concat_columns->{$table}->[1]);
  }
}

sub create_stable_id_version_column {
  my ($self, $table, $column1, $column2) = @_;
  
  my $mart_table_prefix = $self->param_required('mart_table_prefix');
  my $concat_separator = '.';
  my $mart_table = "$mart_table_prefix$table"; 
  
  my $mart_dbc = $self->mart_dbc;
  # Check that column 1 and 2 are not null
  my $column1_values = $mart_dbc->sql_helper()->execute_simple( -SQL =>"select count($column1) from $mart_table where $column1 is not null" );
  my $column2_values = $mart_dbc->sql_helper()->execute_simple( -SQL =>"select count($column2) from $mart_table where $column2 is not null" );

  if (defined $column1_values->[0] and defined $column2_values->[0]) {
    if ($column1_values->[0] > 0 and $column2_values->[0] > 0) {
      #Drop column if exist
      $self->drop_column_if_exist($mart_dbc,$mart_table, $table."_stable_id_version");

      # Get first column type
      my $column1_types = $mart_dbc->sql_helper->execute_simple(-SQL=>"SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '$mart_table' AND COLUMN_NAME = '$column1';"); 
      my $column1_type = $column1_types->[0];
      # Create new column
      $mart_dbc->sql_helper->execute_update(-SQL=>"ALTER TABLE $mart_table ADD COLUMN ${table}_stable_id_version $column1_type;");
      # Concat table
      $mart_dbc->sql_helper->execute_update(-SQL=>"UPDATE $mart_table SET ${table}_stable_id_version = CONCAT($column1, '$concat_separator', $column2);");
      # Create index
      $mart_dbc->sql_helper->execute_update(-SQL=>"ALTER TABLE $mart_table ADD INDEX (${table}_stable_id_version);");
      # Replicate column in child tables.
      # E.g: from Gene main table to Transcript and Translation
      if ($table =~ m/gene/i) {
        # Add the column to the Transcript main table
        $self->replicate_column_child_tables($mart_dbc,$mart_table,$mart_table_prefix,"transcript__main",'gene__main_stable_id_version','gene_id_1020_key',$column1_type,$column1,$column2,$concat_separator);
        # Add the column to the Translation main table
        $self->replicate_column_child_tables($mart_dbc,$mart_table,$mart_table_prefix,"translation__main",'gene__main_stable_id_version','gene_id_1020_key',$column1_type,$column1,$column2,$concat_separator);
      }
      elsif ($table =~ m/transcript/i) {
        # Add the column to the Translation main table
        $self->replicate_column_child_tables($mart_dbc,$mart_table,$mart_table_prefix,"translation__main",'transcript__main_stable_id_version','transcript_id_1064_key',$column1_type,$column1,$column2,$concat_separator);
      }
    }
  }

  $mart_dbc->disconnect_if_idle();
}

sub drop_column_if_exist {
  my ($self, $mart_dbc, $mart_table, $column) = @_;
  #DROP column if it exist
  my $column1_exist = $mart_dbc->sql_helper()->execute_simple( -SQL =>"SELECT count(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '$mart_table' AND COLUMN_NAME = '$column';" );
  if (defined $column1_exist->[0]) {
    if ($column1_exist->[0] > 0) {
      $mart_dbc->sql_helper->execute_update(-SQL=>"ALTER TABLE $mart_table DROP COLUMN $column;");
    }
  }
}

sub replicate_column_child_tables {
  my ($self, $mart_dbc, $mart_table, $mart_table_prefix, $child_table_name, $column, $main_table_key, $column_type, $column1,$column2,$concat_separator) = @_;
  # Add the column to the Transcript main table
  my $child_mart_table = "$mart_table_prefix$child_table_name";
  #Drop column if exist
  $self->drop_column_if_exist($mart_dbc,$child_mart_table,$column);
  # Create new column
  $mart_dbc->sql_helper->execute_update(-SQL=>"ALTER TABLE $child_mart_table ADD COLUMN $column $column_type;");
  # Concat table
  $mart_dbc->sql_helper->execute_update(-SQL=>"UPDATE $child_mart_table INNER JOIN $mart_table ON $mart_table.$main_table_key = $child_mart_table.$main_table_key SET $child_mart_table.$column = CONCAT($mart_table.$column1, '$concat_separator', $mart_table.$column2);");
  # Create index
  $mart_dbc->sql_helper->execute_update(-SQL=>"ALTER TABLE $child_mart_table ADD INDEX ($column);");
}

1;
