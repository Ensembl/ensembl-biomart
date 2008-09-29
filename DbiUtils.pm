# $Source$
# $Revision$
# $Date$
# $Author$
#
# Some utils for common DBI tasks

use warnings;
use strict;
use DBI;

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
    @strings;
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
    %hash;
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
