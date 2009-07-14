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
my $db_host = 'mysql-eg-production-1';
my $db_port = '4161';
my $db_user = 'ensrw';
my $db_pwd = 'writ3r';
my $mart_db = 'protist_mart_54';
my $release = 54;
my $suffix = '';

my %table_res = (
    qr/protein_feature/ => {
	qr/Superfamily/ => 'superfam',
	qr/scanprosite/ => 'scanpro'
    }
);

sub transform_table {
    my $table = shift;
    foreach my $tre (keys %table_res) {
	if($table=~ /$tre/) {
	    my %res = %{$table_res{$tre}};
	    foreach my $from (keys %res) {
		$table =~ s/$from/$res{$from}/;
	    }
	}
    }
    $table;
}

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
    "release=s"=>\$release,
    "suffix=s"=>\$suffix,
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

# create a names table to keep track of whats what
my $names_table = 'dataset_names';
drop_and_create_table($mart_handle, $names_table,
		      ['name varchar(100)',
		       'src_dataset varchar(100)',
		       'src_db varchar(100)',
		       'species_id varchar(100)',
		       'species_name varchar(100)',
		       'sql_name varchar(100)',
		       'version varchar(100)',
		       'collection varchar(100)'
		      ],
		      'ENGINE=MyISAM DEFAULT CHARSET=latin1'
    );

my $names_insert = $mart_handle->prepare("INSERT INTO $names_table VALUES(?,?,?,?,?,?,?,NULL)");

my @src_tables = get_tables($mart_handle);
my @src_dbs;
foreach my $db (get_databases($mart_handle)) {
    if($db =~ m/core_[0-9]+_($release)_[0-9A-z]+/) {
	print "$db\n";
	push @src_dbs, $db;
    }
}


$logger->info("Listing datasets from $mart_db");
# 1. identify datasets based on main tables
my @datasets = get_datasets(\@src_tables);

my $dataset_basename = 'gene';

# 2. for each dataset
foreach my $dataset (@datasets) {

    $logger->info("Naming $dataset");
    # get original database
    my $base_datasetname = $dataset;
    $base_datasetname =~ s/$suffix//;
    my $ens_db = get_ensembl_db_single(\@src_dbs,$base_datasetname,$release);
    if(!$ens_db) {
	croak "Could not find original source db for dataset $base_datasetname\n";
    }   
    $logger->debug("$dataset derived from $ens_db");
    my $ens_db_string =  "DBI:mysql:$ens_db:$db_host:$db_port";
    my $ens_dbh =  DBI->connect($ens_db_string, $db_user, $db_pwd,
				{ RaiseError => 1 }
	) or croak "Could not connect to $ens_db_string";

    # get hash of species IDs
    my @species_ids = query_to_strings($ens_dbh,"select distinct(species_id) from meta where species_id is not null");

    foreach my $species_id (@species_ids) {

	## use the species ID to get a hash of everything we need and write it into the names_table
	my %species_names = query_to_hash($ens_dbh,"select meta_key,meta_value from meta where species_id='$species_id'");	
	
	$names_insert->execute(	    
	    $dataset,
	    $dataset,
	    $ens_db,
	    $species_names{'species.proteome_id'},
	    $species_names{'species.db_name'} || $species_names{'species.ensembl_alias_name'},
	    $species_names{'species.sql_name'},
	    $species_names{'assembly.default'}  || $species_names{'genebuild.version'} 
	    ); 
    }
	
    
}

$mart_handle->disconnect();

$logger->info("Complete");




