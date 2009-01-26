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
my $db_host = 'mysql-eg-production-1.ebi.ac.uk';
my $db_port = '4161';
my $db_user = 'admin';
my $db_pwd = 'iPBi22yI';
my $src_mart_db = 'base_bacterial_mart_52';
my $target_mart_db = 'bacterial_mart_52';

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
    print "-src_mart <source mart> Default is $src_mart_db\n";
    print "-target_mart <target mart> Default is $target_mart_db\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
    "src_mart=s"=>\$src_mart_db,
    "target_mart=s"=>\$target_mart_db,
    "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

my $delete = ($src_mart_db eq $target_mart_db);

my $src_string = "DBI:mysql:$src_mart_db:$db_host:$db_port";
my $src_handle = DBI->connect($src_string, $db_user, $db_pwd,
	            { RaiseError => 1 }
    ) or croak "Could not connect to $src_string";

my $target_string = "DBI:mysql:$target_mart_db:$db_host:$db_port";
my $target_handle = DBI->connect($target_string, $db_user, $db_pwd,
				 { RaiseError => 1 }
    )  or croak "Could not connect to $target_string";

if(!$delete) {
    $logger->info("Dropping/recreating $target_mart_db");
    $target_handle->do("drop database $target_mart_db");
    $target_handle->do("create database $target_mart_db");
}
$target_handle->do("use $target_mart_db");

# create a names table to keep track of whats what
my $names_table = 'dataset_names';
drop_and_create_table($target_handle, $names_table,
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

my $names_insert = $target_handle->prepare("INSERT INTO $names_table VALUES(?,?,?,?,?,?,?,?)");

my @src_tables = get_tables($src_handle);
my @src_dbs = get_databases($src_handle);

$logger->info("Listing datasets from $src_mart_db");
# 1. identify datasets based on main tables
my @datasets = get_datasets(\@src_tables);

my $dataset_basename = 'gene';

my $translation_table=$dataset_basename.'__translation__main';
my $translation_key='translation_id_1068_key';
my $transcript_table=$dataset_basename.'__transcript__main';
my $transcript_key='transcript_id_1064_key';
my $gene_table=$dataset_basename.'__gene__main';
my $gene_key='gene_id_1020_key';
my $seq_region_key='seq_region_id_1020';

my @key_tables = ($translation_table,$transcript_table,$gene_table);
my %key_table_ids = (
    $translation_table=>$translation_key,
    $transcript_table=>$transcript_key,
    $gene_table=>$gene_key
);

# 2. for each dataset
foreach my $dataset (@datasets) {

    $logger->info("Splitting $dataset");
    # get original database
    my $ens_db = get_ensembl_db_collection(\@src_dbs,$dataset);
    if(!$ens_db) {
	croak "Could not find original source db for dataset $dataset\n";
    }   
    $logger->debug("$dataset derived from $ens_db");
    my $ens_db_string =  "DBI:mysql:$ens_db:$db_host:$db_port";
    my $ens_dbh =  DBI->connect($ens_db_string, $db_user, $db_pwd,
				{ RaiseError => 1 }
	) or croak "Could not connect to $ens_db_string";

    # count the original tables
    my %src_table_counts = ();
    # make somewhere 
    my %target_table_counts = ();
    foreach my $src_table (@src_tables) {	
	if($src_table=~ m/$dataset/) {
	    $src_table_counts{$src_table} = row_count($src_handle, $src_table);
	    $target_table_counts{$src_table} = ();
	}
    }

    # get hash of species IDs
    my %species_ids = query_to_hash($ens_dbh,"select species_id,meta_value from meta where meta_key='species.sql_name'");

    foreach my $species_id (keys (%species_ids)) {

	## use the species ID to get a hash of everything we need and write it into the names_table
	my %species_names = query_to_hash($ens_dbh,"select meta_key,meta_value from meta where species_id='$species_id'");	
	my $sub_dataset = $dataset.'_'.$species_names{'species.proteome_id'};
	# suppress numbers of datasets
	$sub_dataset =~ s/[_-]+/_/g;
	my $collection = $ens_db;
	$collection =~ s/^(.+)_collection.*/$1/;
	$names_insert->execute(	    
	    $sub_dataset,
	    $dataset,
	    $ens_db,
	    $species_names{'species.proteome_id'},
	    $species_names{'species.db_name'},
	    $species_names{'species.sql_name'},
	    $species_names{'genebuild.version'},
	    $collection
	    ); 
	
	$logger->info("Splitting into dataset $sub_dataset");
	# for each species, get a list of seq_region_ids that are valid
	# 1. create a condensed gene table
	my $src_gene_table = "${dataset}_$gene_table";
	my $target_gene_table = "${sub_dataset}_$gene_table";
	#$target_gene_table =~ s/gene_ensembl/gene/;

	$logger->info("Creating $target_mart_db.$target_gene_table");
	my $sql = "create table $target_mart_db.$target_gene_table as ".
	    "select s.* from $src_mart_db.$src_gene_table s ".
            "join $ens_db.seq_region sr on $seq_region_key=sr.seq_region_id ".
	    "join $ens_db.coord_system cs on cs.coord_system_id=sr.coord_system_id ".
	    "where cs.species_id=$species_id";
	$logger->debug("Executing $sql");
	$target_handle->do($sql);
	$logger->debug("Executing $sql");

	$target_table_counts{$src_gene_table}{$target_gene_table} = row_count($target_handle, $target_gene_table);

	# 2. create a condensed transcript table
	my $src_transcript_table = "${dataset}_$transcript_table";
	my $target_transcript_table = "${sub_dataset}_$transcript_table";
	$target_transcript_table =~ s/gene_ensembl/gene/;
	$logger->info("Creating $target_mart_db.$target_transcript_table");
	$sql = "create table $target_mart_db.$target_transcript_table as ".
	    "select t.* from $src_mart_db.$src_transcript_table t join $target_mart_db.$target_gene_table g on  g.$gene_key=t.$gene_key";
	$logger->debug("Executing $sql");
	$target_handle->do($sql);
	$target_table_counts{$src_transcript_table}{$target_transcript_table} = row_count($target_handle, $target_transcript_table);
	
	# 3. create a condensed translation table	
	my $src_translation_table = "${dataset}_$translation_table";
	my $target_translation_table = "${sub_dataset}_$translation_table";
	$target_translation_table =~ s/gene_ensembl/gene/;
	$logger->info("Creating $target_mart_db.$target_translation_table");
	$sql = "create table $target_mart_db.$target_translation_table as ".
	    "select t.* from $src_mart_db.$src_translation_table t join $target_mart_db.$target_transcript_table g on t.$transcript_key = g.$transcript_key";
	$logger->debug("Executing $sql");
	$target_handle->do($sql);
	$target_table_counts{$src_translation_table}{$target_translation_table} = row_count($target_handle, $target_translation_table);

	my %processed_tables = ();
	# 4. slice out satellite tables for each key table in turn
	foreach my $key_table (@key_tables) {
	    my $key_table_id = $key_table_ids{$key_table};
	    my $src_key_table = $dataset . '_' . $key_table;
	    my $target_key_table = $sub_dataset . '_' . $key_table;
	    $target_key_table =~ s/gene_ensembl/gene/;
	    $logger->info("Finding satellite tables for $key_table using $key_table_id");
	    $processed_tables{$src_key_table}=1;
	    foreach my $src_table (@src_tables) {
		if($src_table=~ m/^$dataset/ && !$processed_tables{$src_table} && has_column($src_handle,$src_table,$key_table_id)) {
		    my $target_table = transform_table($src_table);
		    $target_table =~ s/^$dataset/$sub_dataset/;
		    $target_table =~ s/gene_ensembl/gene/;
		    $logger->info("Need to split $src_table into $target_table using $src_key_table.$key_table_id");
		    my $sql = "create table $target_mart_db.$target_table as select s.* from $src_mart_db.$src_table s join $target_mart_db.$target_key_table t on s.$key_table_id=t.$key_table_id";
		    $logger->debug("Executing $sql");
		    $target_handle->do($sql);
		    $processed_tables{$src_table} = 1;
		    $target_table_counts{$src_table}{$target_table} = row_count($target_handle, $target_table);		    
		}
	    }
	}
	
    }
    $ens_dbh->disconnect();

    $logger->info("Checking output for $dataset");

    # report on sums
    foreach my $src_table (keys(%src_table_counts)) {
	if($src_table=~ m/^$dataset/) {
	    my $src_count = $src_table_counts{$src_table};
	    $logger->info("$src_table expected $src_count");
	    my $total = 0;
	    my %target_table_counts_sub = %{$target_table_counts{$src_table}};
	    foreach my $target_table (keys(%target_table_counts_sub)) {
		my $target_table_count = $target_table_counts_sub{$target_table}; 
		$logger->debug("$target_table got $target_table_count");
		$total += $target_table_count;
	    }
	    if($total!=$src_count) {
		$logger->warn("Failure: $src_table contains $src_count but split into $total total");
	    } else {
		$logger->info("$src_table contains expected count of $total");
	    }
	    if($delete) {
		$logger->info("Dropping $src_table");
		drop_table($src_handle,$src_table);
	    }
	}
    }
    
}

$src_handle->disconnect();
$target_handle->disconnect();

$logger->info("Complete");




