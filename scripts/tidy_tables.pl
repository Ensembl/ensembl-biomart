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
use FindBin;
use lib "$FindBin::Bin/../modules";
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

my %tables_to_tidy;
my %columns_to_tidy;

if ($mart_db =~ /snp_mart/) {
  %tables_to_tidy = (
				 '%\_\_mpoly\_\_dm'                   =>  ['name_2019'],
				 '%\_\_variation\_set\_variation\_\_dm' => ['description_2077'],
				 '%snp\_\_variation\_annotation\_\_dm'    => ['description_2033','name_2021'],
				 '%structural\_\_variation\_annotation\_\_dm'    => ['name_2019','description_2019'],
      );
  %columns_to_tidy = (
  	             '%snp\_\_mart\_transcript\_variation\_\_dm'    => ['sift_score_2090','polyphen_score_2090'],
  	);
}
elsif ($mart_db =~ /ontology_mart/) {
  %tables_to_tidy = (
				 'closure\_%\_\_closure__main' => ['name_302']
  );
}
else {
  %tables_to_tidy = (
			   '%\_transcript\_variation\_som\_\_dm' => ['seq_region_start_2026'],
			   '%\_\_go\_%\_\_dm'               => ['dbprimary_acc_1074'],
			   '%\_\_tra\_%\_\_dm'               => ['value_1065'],
			   '%\_\ox\_%\_\_dm'               => ['dbprimary_acc_1074'],
			   '%\_\_phenotype\_\_dm'       => ['description_20125']);
  %columns_to_tidy = (
               '%\_\_gene\_\_main'    => ['display_label_1074','db_display_name_1018','phenotype_bool','version_1020'],
               '%\_\_transcript\_\_main'    => ['display_label_1074_r1','db_display_name_1018_r1','phenotype_bool','version_1064','version_1020'],
               '%\_\_translation\_\_main'    => ['stable_id_408','description_408','family_bool','phenotype_bool','version_1064','version_1020','version_1068']
  );
}

# 1. remove tables where we have no data beyond the key
for my $table_pattern (keys %tables_to_tidy) {
  $logger->info("Finding tables like $table_pattern");
  foreach my $col  (@{$tables_to_tidy{$table_pattern}}) {
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

# 4. Drop columns that are empty
for my $table_pattern (keys %columns_to_tidy) {
  $logger->info("Finding tables like $table_pattern");
  foreach my $col (@{$columns_to_tidy{$table_pattern}}) {
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
		  $logger->info("Dropping 'empty' column $col from $table");
		  $mart_handle->do("ALTER TABLE $table DROP COLUMN $col");
	    }
	  };
	  if ($@) {
	    warn "Could not delete column $col from $table:" . $@;
	  }
    }
  }
}

$mart_handle->disconnect();

$logger->info("Complete");

