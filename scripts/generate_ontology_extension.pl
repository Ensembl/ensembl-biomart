#!/bin/env perl
# Copyright [2009-2023] EMBL-European Bioinformatics Institute
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


use warnings;
use strict;

use DBI;
use Carp;
use Log::Log4perl qw(:easy);
use List::MoreUtils qw(any);
use Data::Dumper;
use FindBin;
use lib "$FindBin::Bin/../modules";
use DbiUtils qw(drop_table get_string get_strings get_rows get_row);
use MartUtils;
use Getopt::Long;
use POSIX;



my $db_host;
my $db_port;
my $db_user;
my $db_pwd;
my $mart_db;
my $dataset;
my $verbose;

sub usage {
    print "Usage: $0 [-h <host>] [-port <port>] [-u user <user>] [-p <pwd>] [-mart <mart>] [-databset <dataset_name>]\n";
    print "-h <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-dataset <dataset_db>\n";
    print "-mart <mart_db>\n";
    exit 1;
};

my $options_okay = GetOptions (
			       "h|host=s"=>\$db_host,
			       "P|port=i"=>\$db_port,
			       "u|user=s"=>\$db_user,
			       "p|pass=s"=>\$db_pwd,
			       "mart=s"=>\$mart_db,
			       "dataset:s"=>\$dataset,
			       "verbose"=>\$verbose,
			       "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

if (!defined $mart_db) {
    usage();
}
if(defined $verbose) {
	Log::Log4perl->easy_init($DEBUG);	
} else {
	Log::Log4perl->easy_init($INFO);
}

my $logger = get_logger();

# open a connection
# work out the name of the core
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

my @datasets = ();
if(defined $dataset) {
    push @datasets, $dataset;
} else {
     @datasets = get_strings($mart_handle->prepare("SELECT distinct(name) FROM dataset_names"));
}

for $dataset (@datasets) {
    my $core_db = get_string($mart_handle->prepare("SELECT src_db FROM dataset_names WHERE name='$dataset'"));
    $logger->info("Found database $core_db for dataset $dataset");
    if(!defined $core_db) {
	croak "Could not find core database for dataset $dataset";
    }
    
    for my $row (get_dbs($mart_handle,$core_db)) {
	my $external_db = $row->[0];
	my $object_type = $row->[1];
	$logger->info("Creating base table for $dataset $external_db on $mart_db using $core_db");
	
	create_base_table($mart_handle,$mart_db,$core_db,$dataset,$external_db,$object_type);
	for my $condition (get_conditions($mart_handle,$core_db,$external_db)) {
	    $logger->info("Adding condition $condition for $dataset $external_db on $mart_db using $core_db");
	    add_condition($mart_handle,$mart_db,$core_db,$dataset,$condition,$external_db);
	}
    }
}

sub create_base_table {
    my ($mart_handle,$mart_db,$core_db,$dataset,$external_db,$object_type) = @_;
    my $drop_base_table = qq/drop table if exists 
${mart_db}.${dataset}_gene__${external_db}_extension__dm/; 
    $logger->debug($drop_base_table);
    $mart_handle->do($drop_base_table);
    
    my $key = 'transcript_id_1064_key';
    my $ensemblObjType = 'transcript';
    if (  $object_type eq 'translation' ) {
      $key = 'translation_id_1068_key';
      $ensemblObjType = 'translation';
    }

    my $create_base_table = qq/
    create table 
${mart_db}.${dataset}_gene__${external_db}_extension__dm as
select
  distinct t.${ensemblObjType}_id as ${key},
  ox.object_xref_id  as object_xref_id,
  tx.dbprimary_acc as subject_acc,
  tx.display_label as subject_label,
  sx.dbprimary_acc source_acc,
  sx.display_label source_label,
  sd.db_name source_db,
  ag.associated_group_id group_id,
  ag.description group_des
from
  ${core_db}.object_xref ox
  join ${core_db}.xref tx on (ox.xref_id=tx.xref_id)
  join ${core_db}.external_db td on (tx.external_db_id=td.external_db_id and td.db_name='$external_db')
  join ${core_db}.associated_xref ax on (ox.object_xref_id=ax.object_xref_id)
  join ${core_db}.associated_group ag on (ax.associated_group_id=ag.associated_group_id)
  join ${core_db}.xref sx on (sx.xref_id=ax.source_xref_id)
  join ${core_db}.external_db sd on (sx.external_db_id=sd.external_db_id)
  right join ${core_db}.${ensemblObjType} t on (t.${ensemblObjType}_id=ox.ensembl_id and ox.ensembl_object_type='${object_type}');/;
    $logger->debug($create_base_table);
    $mart_handle->do($create_base_table);

    return;
}

sub get_conditions {
    my ($mart_handle,$core_db,$external_db) = @_;
    my $get_conditions = qq/select distinct(condition_type) 
from ${core_db}.associated_xref ax
join ${core_db}.object_xref ox using (object_xref_id)
join ${core_db}.xref tx on (tx.xref_id=ox.xref_id)
join ${core_db}.external_db td on (tx.external_db_id=td.external_db_id)
join ${core_db}.xref axt on (ax.xref_id=axt.xref_id)
where td.db_name='$external_db' and axt.dbprimary_acc !='PBO:2100001'/;
    $logger->debug($get_conditions);
    my $sth = $mart_handle->prepare($get_conditions);
    return get_strings($sth);
}

sub get_dbs {
 my ($mart_handle,$core_db) = @_;
    my $get_conditions = qq/
select distinct lower(td.db_name), lower(ox.ensembl_object_type) 
from $core_db.associated_xref
join ${core_db}.object_xref ox using (object_xref_id)
join ${core_db}.xref tx on (tx.xref_id=ox.xref_id)
join ${core_db}.external_db td on (tx.external_db_id=td.external_db_id)/;
    $logger->debug($get_conditions);
    my $sth = $mart_handle->prepare($get_conditions);
    return get_rows($sth);
}

sub add_condition {
    my ($mart_handle,$mart_db,$core_db,$dataset,$condition,$external_db) = @_;
    my $condition_match = $condition;
    $condition =~ s/\s+/_/g;
    my $add_condition = qq/create table ${mart_db}.TMP as
select oe.*,
cx.dbprimary_acc ${condition}_acc,
cx.display_label ${condition}_label,
cd.db_name ${condition}_db
from
${mart_db}.${dataset}_gene__${external_db}_extension__dm oe
left join $core_db.associated_xref ax on (oe.object_xref_id=ax.object_xref_id 
and ax.associated_group_id=oe.group_id 
and ax.condition_type='$condition_match')
left join $core_db.xref cx on (ax.xref_id=cx.xref_id)
left join $core_db.external_db cd on (cx.external_db_id=cd.external_db_id)/;
    $logger->debug($add_condition);
    $mart_handle->do($add_condition);

    my $drop_table = qq/drop table ${mart_db}.${dataset}_gene__${external_db}_extension__dm/;
    $logger->debug($drop_table);
    $mart_handle->do($drop_table);

    my $rename_table = qq/rename table ${mart_db}.TMP 
to ${mart_db}.${dataset}_gene__${external_db}_extension__dm/;
    $logger->debug($rename_table);
    $mart_handle->do($rename_table);

    return;
}
