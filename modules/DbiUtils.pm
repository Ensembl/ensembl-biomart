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


# $Source$
# $Revision$
# $Date$
# $Author$
#
# Some utils for common DBI tasks

use warnings;
use strict;
package DbiUtils;
use DBI;
use Exporter qw/import/;
our @EXPORT_OK = qw(create_table_from_query create_indices get_indexed_columns table_exists drop_and_create_table drop_table create_table get_string get_strings get_rows get_row get_hash get_tables get_databases query_to_strings query_to_hash has_column row_count);

sub create_table_from_query {
    my ($db_handle,$src_mart,$src_table,$target_mart,$target_table,$query) = @_;
    $db_handle->do("create table $target_mart.$target_table like $src_mart.$src_table");
    $db_handle->do("insert into $target_mart.$target_table ($query)");

}

sub create_indices {
    my ($db_handle,$table_name,$cols) = @_[0,1,2];
    foreach my $col (@$cols) {       
	print "Creating index on $table_name $col\n";
	$db_handle->do("ALTER TABLE $table_name ADD INDEX ($col)");
    }    
}

my %col_info = ();
sub get_indexed_columns {
    my ($db_handle,$table_name) = @_[0,1];
    my $key = $db_handle->{Name}.'.'.$table_name;
    my $cols = $col_info{$key};
    if(!$cols) {
	my $sth = $db_handle->prepare("SHOW INDEX FROM $table_name");
	$cols = [];
	$sth->execute();
	while(my @data = $sth->fetchrow_array()) {
	    push @$cols, $data[4];
	}
	$col_info{$key} = $cols;
    }
    return @$cols;
}

sub table_exists {
    my ($db_handle,$table_name) = @_[0,1];
    my $exists = 0;
    my $sth =$db_handle->prepare("SHOW TABLES LIKE '$table_name'");
    $sth->execute();
    return $sth->fetchrow_array();
}

sub drop_and_create_table {
    my ($db_handle,$table_name,$cols,$args) = @_[0,1,2,3];
    $db_handle->do("DROP TABLE IF EXISTS $table_name");
    create_table($db_handle,$table_name,$cols,$args);
}

sub drop_table {
    my ($db_handle,$table_name) = @_[0,1];   
    my $sql = "DROP TABLE $table_name";
    $db_handle->do($sql);
}

sub create_table {
    my ($db_handle,$table_name,$cols,$args) = @_[0,1,2,3];   
    my $sql = "CREATE TABLE $table_name (".join(',',@$cols).") $args";
    $db_handle->do($sql);
}

sub get_string {
    my $sth = shift;
    $sth->execute(@_); 
    ($sth->fetchrow_array())[0];
}

sub get_strings {
    my $sth = shift;
    my @strings = ();
    $sth->execute();
    while(my @data = $sth->fetchrow_array()) {
	push(@strings,$data[0]);
    }    
    return @strings;
}

sub get_rows {
    my $sth = shift;
    my @strings = ();
    $sth->execute();
    while(my @data = $sth->fetchrow_array()) {
	push(@strings,\@data);
    }    
    return @strings;
}

sub get_row {
    my $sth = shift;
    $sth->execute(@_);
    $sth->fetchrow_array();
}

sub get_hash {
    my $sth = shift;
    my %hash = ();
    $sth->execute();
    while(my @data = $sth->fetchrow_array()) {
	$hash{$data[0]} = $data[1];
    }    
    %hash;
}

sub get_tables {
    my $db_handle = shift;
    my $show_tables = $db_handle->prepare('show tables');
    my @tables = get_strings($show_tables);
    $show_tables->finish();
    @tables;
}

sub get_databases {
    my $db_handle = shift;
    my $show_tables = $db_handle->prepare('show databases');
    my @dbs = get_strings($show_tables);    
    $show_tables->finish();
    @dbs;
}

sub query_to_strings {
    my $db_handle = shift;
    my $sql = shift;
    my $sth = $db_handle->prepare($sql);
    my @strs = get_strings($sth);    
    $sth->finish();
    @strs;
}

sub query_to_hash {
    my $db_handle = shift;
    my $sql = shift;
    my $sth = $db_handle->prepare($sql);
    my %hash = get_hash($sth);   
    $sth->finish();
    return %hash;
}

sub has_column {
    my $db_handle = shift;
    my $table_name = shift;
    my $column_name = shift;
    my @cols = query_to_strings($db_handle,"describe $table_name");
    any { $_ eq $column_name } @cols;
}

sub row_count {
   my $db_handle = shift;
   my $table_name = shift;
   my @strs = query_to_strings($db_handle,"select count(*) from ${table_name}");
   $strs[0];
}

1;
