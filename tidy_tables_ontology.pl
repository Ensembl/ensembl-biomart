#!/bin/env perl
# Copyright [2009-2014] EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
my $db_host;
my $db_port;
my $db_user;
my $db_pwd;
my $mart_db;

sub usage {
  print
"Usage: $0 [-host <host>] [-port <port>] [-u user <user>] [-pass <pwd>] [-mart <mart db>] [-help]\n";
  print "-host <host> Default is $db_host\n";
  print "-port <port> Default is $db_port\n";
  print "-u <host> Default is $db_user\n";
  print "-pass <password> Default is top secret unless you know cat\n";
  print "-mart <target mart> Default is $mart_db\n";
  print "-help - this usage\n";
  exit 1;
}

my $options_okay = GetOptions("host=s" => \$db_host,
			      "port=s" => \$db_port,
			      "user=s" => \$db_user,
			      "pass=s" => \$db_pwd,
			      "mart=s" => \$mart_db,
                              "help"   => sub { usage() }
                             );

if (!$options_okay || !defined $mart_db) {
  usage();
}

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle =
  DBI->connect($mart_string, $db_user, $db_pwd, {RaiseError => 1}) or
  croak "Could not connect to $mart_string";

$mart_handle->do("use $mart_db");

my %tables_to_tidy = ('closure\_%\_\_closure__main' => 'name_302');

# 1. remove tables where we have no data beyond the key
for my $table_pattern (keys %tables_to_tidy) {
  $logger->info("Finding tables like $table_pattern");
  my $col = $tables_to_tidy{$table_pattern};
  for my $table (
	query_to_strings($mart_handle, "show tables like '$table_pattern'"))
  {
	$logger->info(
				"Analysing the rows from $table based on $col ...");
	eval {
	  my $cnt_before = get_string(
				 $mart_handle->prepare(
				   "SELECT COUNT(*) FROM $table")
	  );
	  $mart_handle->do("DELETE FROM $table WHERE $col IS NULL");
	  my $cnt_after = get_string(
				 $mart_handle->prepare(
				   "SELECT COUNT(*) FROM $table")
	  );
	  $logger->info("$table had $cnt_before rows, now contains $cnt_after valid rows");
	  if ($cnt_after == 0) {
		$logger->info("Dropping 'empty' table $table");
		$mart_handle->do("DROP TABLE $table");
	  }
	  if ($cnt_before > $cnt_after) {
	    $logger->info("Optimizing 'smaller' table $table");
	    $mart_handle->do("OPTIMIZE TABLE $table");
	  }
	};
	if ($@) {
	  warn "Could not delete from $table:" . $@;
	}
  }
}

# 2. find other empty tables and drop them
my @tables = query_to_strings(
  $mart_handle,
"select table_name from information_schema.tables where table_schema='$mart_db' and TABLE_ROWS=0"
);
for my $table (@tables) {
  $logger->info("Dropping empty table $table");
  $mart_handle->do("DROP TABLE $table");
}

# 3. remove TEMP tables and rename tables to lowercase
foreach my $table (get_tables($mart_handle)) {
  if ($table =~ /TEMP/) {
	my $sql = "DROP TABLE $table";
	print $sql. "\n";
	$mart_handle->do($sql);
  }
  }
}

$mart_handle->disconnect();

$logger->info("Complete");

