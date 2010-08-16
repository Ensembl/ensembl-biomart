#!/bin/env perl
#
# $Source$
# $Revision$
# $Date$
# $Author$
#
# Script for splitting datasets from a multi-species mart 

use warnings;
use strict;
use DBI;
use Carp;
use Log::Log4perl qw(:easy);
use List::MoreUtils qw(any);
use Data::Dumper;
use DbiUtils;
use MartUtils;
use Getopt::Long;

Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();

# db params
my $db_host = 'mysql-cluster-eg-prod-1.ebi.ac.uk';
my $db_port = '4238';
my $db_user = 'ensrw';
my $db_pwd = 'writ3rp1';
my $mart_db = 'bacterial_mart_5';

sub usage {
    print "Usage: $0 [-h <host>] [-P <port>] [-u user <user>] [-p <pwd>] [-src_mart <src>] [-target_mart <targ>]\n";
    print "-h <host> Default is $db_host\n";
    print "-P <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <target mart> Default is $mart_db\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
	            { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

$mart_handle->do("use $mart_db");

# 1. delete from tables in hash 
my %tables_to_tidy = (
    '%__transcript_variation__dm'=>'seq_region_id_2026',
    '%__splicing_event__dm'=>'name_1078'
);

for my $table_pattern (keys %tables_to_tidy) {
    $logger->info("FInding tables like $table_pattern");
    my $col = $tables_to_tidy{$table_pattern};
    for my $table (query_to_strings($mart_handle,"show tables like '$table_pattern'")) {
	$logger->info("Deleting rows from $table where $col is null");
	$mart_handle->do("DELETE FROM $table WHERE $col IS NULL");
    }
}

# 2. find empty tables and drop them
my @tables = query_to_strings($mart_handle,"select table_name from information_schema.tables where table_schema='$mart_db' and TABLE_ROWS=0");
for my $table (@tables) {
    $logger->info("Dropping empty table $table");
    $mart_handle->do("DROP TABLE $table");    
}

# 3. remove TEMP tables and rename tables to lowercase
foreach my $table (get_tables($mart_handle)) {
    if($table =~ /TEMP/) {
	my $sql = "DROP TABLE $table";
	print $sql."\n"; 
	$mart_handle->do($sql);
    } elsif($table =~ m/[A-Z]+/) {
	my $sql = "RENAME TABLE $table TO ".lc($table);
	print $sql."\n"; 
	$mart_handle->do($sql);
    }
}

$mart_handle->disconnect();

$logger->info("Complete");




