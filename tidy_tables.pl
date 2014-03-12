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
my $db_host = 'mysql-cluster-eg-prod-1.ebi.ac.uk';
my $db_port = '4238';
my $db_user = 'ensrw';
my $db_pwd  = 'writ3rp1';
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

my %tables_to_tidy;

if ($mart_db =~ /snp_mart/) {
  %tables_to_tidy = (
				 '%\_\_mpoly\_\_dm'                   => 'name_2019',
				 '%\_\_variation\_set\_variation\_\_dm' => 'description_2077',
				 '%snp\_\_variation\_annotation\_\_dm'    => 'description_2033',
				 '%snp\_\_variation\_annotation\_\_dm'    => 'name_2021' ,
				 '%structural\_\_variation\_annotation\_\_dm'    => 'name_2019',
				 '%structural\_\_variation\_annotation\_\_dm'    => 'description_2033',
      );
}
else {
  %tables_to_tidy = (
			   '%\_transcript\_variation\_\_dm'     => 'seq_region_id_2026',
			   '%\_transcript\_variation\_som\_\_dm' => 'seq_region_id_2026',
			   '%\_\_splicing\_event\_\_dm'          => 'name_1078',
			   '%\_\_splicing\_event\_feature\_\_dm'  => 'name_1059',
			   '%\_\_exp\_atlas\_%\_\_dm'             => 'stable_id_1066',
			   '%\_\_exp\_est\_%\_\_dm'               => 'stable_id_1066',
			   '%\_\_exp\_zfin\_%\_\_dm'              => 'stable_id_1066',
			   '%\_\_go\_%\_\_dm'               => 'ontology_id_1006');
}

# 1. remove tables where we have no data beyond the key
for my $table_pattern (keys %tables_to_tidy) {
  $logger->info("Finding tables like $table_pattern");
  my $col = $tables_to_tidy{$table_pattern};
  for my $table (
	query_to_strings($mart_handle, "show tables like '$table_pattern'"))
  {
	$logger->info(
				"Checking for rows from $table where $col is not null");
	eval {
	  my $cnt = get_string(
				 $mart_handle->prepare(
				   "SELECT COUNT(*) FROM $table WHERE $col IS NOT NULL")
	  );
	  $logger->info("$table contains $cnt valid rows");
	  if ($cnt == 0) {
		$logger->info("Dropping 'empty' table $table");
		$mart_handle->do("DROP TABLE $table");
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
  elsif ($table =~ m/[A-Z]+/) {
	my $sql = "DROP TABLE IF EXISTS " . lc($table);
	print $sql. "\n";
	$mart_handle->do($sql);
	$sql = "RENAME TABLE $table TO " . lc($table);
	print $sql. "\n";
	$mart_handle->do($sql);
  }
}

$mart_handle->disconnect();

$logger->info("Complete");

