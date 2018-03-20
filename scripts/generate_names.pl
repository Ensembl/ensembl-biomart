#!/bin/env perl
# Copyright [2009-2018] EMBL-European Bioinformatics Institute
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
use FindBin;
use lib "$FindBin::Bin/../modules";
use DbiUtils;
use MartUtils;
use Getopt::Long;
use POSIX;
use Bio::EnsEMBL::Registry;
use Bio::EnsEMBL::ApiVersion qw/software_version/;
Log::Log4perl->easy_init($INFO);

my $logger = get_logger();

# db params
my $db_host;
my $db_port;
my $db_user;
my $db_pwd;
my $mart_db;
my $release          = software_version();
my $div              = undef;
my $species_id_start = undef;
my $registry         = undef;

my %table_res =
  ( qr/protein_feature/ =>
    { qr/Superfamily/ => 'superfam', qr/scanprosite/ => 'scanpro' } );

sub transform_table {
  my $table = shift;
  foreach my $tre ( keys %table_res ) {
    if ( $table =~ /$tre/ ) {
      my %res = %{ $table_res{$tre} };
      foreach my $from ( keys %res ) {
        $table =~ s/$from/$res{$from}/;
      }
    }
  }
  $table;
}

sub usage {
  print
"Usage: $0 [-host <host>] [-port <port>] [ -user <user>] [-pass <pwd>] [-mart <target mart>] [-div <plant|protist|metazoa|fung|vectorbase|ensembl>] [-species_id_start <species id number start>] \n";
  print "-host <host> Default is $db_host\n";
  print "-port <port> Default is $db_port\n";
  print "-user <host> Default is $db_user\n";
  print "-pass <password> Default is top secret unless you know cat\n";
  print "-mart <target mart> Default is $mart_db\n";
  print
"-species_id_start <species id number start> (optional, start number for species_id, avoid duplicated numbers if the core databases are located on two servers. Default is 0)\n";
  print
"-div <plant|protist|metazoa|fung|vectorbase> set taxonomic division for species.proteome_id value\n";
  print
"     -div option also sets core database name according to division specific naming practices\n";
  exit 1;
}

my $options_okay = GetOptions(
                             "host=s"             => \$db_host,
                             "port=s"             => \$db_port,
                             "user=s"             => \$db_user,
                             "pass=s"             => \$db_pwd,
                             "mart=s"             => \$mart_db,
                             "registry=s"         => \$registry,
                             "div=s"              => \$div,
                             "species_id_start=s" => \$species_id_start,
                             "help"               => sub { usage() } );

if ( !$options_okay ) {
  usage();
}

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle =
  DBI->connect( $mart_string, $db_user, $db_pwd, { RaiseError => 1 } )
  or
  croak "Could not connect to $mart_string";

$mart_handle->do("use $mart_db");

# create a names table to keep track of whats what
my $names_table = 'dataset_names';
drop_and_create_table( $mart_handle,
                       $names_table,
                       [ 'name varchar(100)',
                         'src_dataset varchar(100)',
                         'src_db varchar(100)',
                         'species_id varchar(100)',
                         'tax_id int(10)',
                         'species_name varchar(100)',
                         'sql_name varchar(100)',
                         'assembly varchar(100)',
                         'genebuild varchar(100)',
                         'collection varchar(100)',
                         'has_chromosomes tinyint'],
                       'ENGINE=MyISAM DEFAULT CHARSET=latin1' );

my $names_insert = $mart_handle->prepare(
    "INSERT IGNORE INTO $names_table VALUES(?,?,?,?,?,?,?,?,?,NULL,?)");

my @src_tables = get_tables($mart_handle);
my %src_dbs;

my $regexp = undef;

if ( defined $div && $div eq 'ensembl' ) {
  $regexp = ".*_core_${release}_.*";
}
elsif ( defined $div && $div eq 'parasite' ) {
  $regexp = ".*_core(_[0-9]+){0,1}_${release}_.*";
}
else {
  $regexp = ".*_core_[0-9]+_${release}_.*";
}

# load registry
if ( defined $registry ) {
  Bio::EnsEMBL::Registry->load_all($registry);
}
else {
  Bio::EnsEMBL::Registry->load_registry_from_db( -host => $db_host,
                                                 -user => $db_user,
                                                 -pass => $db_pwd,
                                                 -port => $db_port,
                                                 -db_version => $release
  );
}

for my $dba (
   @{ Bio::EnsEMBL::Registry->get_all_DBAdaptors( -group => 'core' ) } )
{
  if ( $dba->dbc()->dbname() =~ /$regexp/ ) {
    $src_dbs{ $dba->dbc()->dbname() } = $dba;
  }
}

$logger->info("Listing datasets from $mart_db");
# 1. identify datasets based on main tables
my $re = '__main';


# 2. for each dataset

my $pId;

unless ($div) {
  if    ( $mart_db =~ m/protists/ )  { $div = 'protists' }
  elsif ( $mart_db =~ m/plants/ )    { $div = 'plants' }
  elsif ( $mart_db =~ m/metazoa/ )  { $div = 'metazoa' }
  elsif ( $mart_db =~ m/fungi/ )     { $div = 'fungi' }
  elsif ( $mart_db =~ m/vb/ )       { $div = 'vectorbase' }
  elsif ( $mart_db =~ m/parasite/ ) { $div = 'parasite' }
  elsif ( $mart_db =~ m/ensembl/ )  { $div = 'ensembl' }
  else {
    die
"-div division not defined, and unable infer from database name $mart_db\n";
  }
}

my $suffix = '';
if    ( $div eq 'protists' ) { $pId = 10000; $suffix = '_eg'; }
elsif ( $div eq 'plants' )   { $pId = 20000; $suffix = '_eg'; }
elsif ( $div eq 'metazoa' ) { $pId = 30000; $suffix = '_eg'; }
elsif ( $div eq 'fungi' )    { $pId = 40000; $suffix = '_eg'; }
elsif ( $div eq 'vectorbase' ) { $pId = 50000; $suffix = '_eg'; }
elsif ( $div eq 'parasite' )   { $pId = 60000 }
elsif ( $div eq 'ensembl' ) {
  if ( defined $species_id_start ) {
    $pId = $species_id_start;
  }
  else {
    $pId = 0;
  }
}
else {
  croak
"Don't know how to deal with mart $mart_db - doesn't match known divisions\n";
}
if ($mart_db =~ "ontology"){
  $suffix='_[A-Za-z1-9]*';
}

my @datasets = get_datasets( \@src_tables, $re, $suffix );

# For the mouse gene mart, we need mouse and rat in the dataset_name table..
if ($mart_db =~ "mouse_mart"){
  push @datasets,"mmusculus" unless grep{$_ eq "mmusculus"} @datasets;
  push @datasets,"rnorvegicus" unless grep{$_ eq "rnorvegicus"} @datasets;
}
# For the Vectorbase gene mart, we also need dmelanogaster_eg in the dataset_name table.
elsif ($mart_db =~ "vb_gene_mart") {
  push @datasets, "dmelanogaster_eg" unless grep{$_ eq "dmelanogaster_eg"} @datasets;
}


unless ( scalar @datasets > 0 ) {
  croak "No datasets found - bailing out!\n";
}

foreach my $dataset (@datasets) {

  $logger->info("Naming $dataset");
  # get original database
  my $base_datasetname = $dataset;
  $base_datasetname =~ s/$suffix//;

  my $ens_db;
  if ( $div eq 'parasite' ) {
    $ens_db = $base_datasetname =~ /prj[a-z]{2}[0-9]+$/ ?
      get_ensembl_db_single_parasite( [ keys(%src_dbs) ],
                                      $base_datasetname, $release ) :
      get_ensembl_db_single( [ keys(%src_dbs) ],
                             $base_datasetname, $release );
  }
  elsif ($mart_db =~ "ontology"){
    $names_insert = $mart_handle->prepare(
    "INSERT IGNORE INTO $names_table VALUES(?,?,NULL,?,NULL,?,NULL,NULL,NULL,NULL,NULL)");
    $names_insert->execute( $dataset,
                            $dataset,
                            $pId,
                            $dataset,
                            );
    $pId++;
    next;
  }
  else {
    $ens_db = get_ensembl_db_single( [ keys(%src_dbs) ],
                                     $base_datasetname, $release );
  }
  if ( !$ens_db ) {
    croak
"Could not find original source db for dataset $base_datasetname\n";
  }
  $logger->debug("$dataset derived from $ens_db");
  my $ens_dbh = $src_dbs{$ens_db}->dbc()->db_handle();
  my $meta_insert = $ens_dbh->prepare(
"INSERT IGNORE INTO meta(species_id,meta_key,meta_value) VALUES(?,'species.biomart_dataset',?)"
  );

  # get hash of species IDs
  my @species_ids = query_to_strings( $ens_dbh,
    "select distinct(species_id) from meta where species_id is not null"
  );

  foreach my $species_id (@species_ids) {

    ## use the species ID to get a hash of everything we need and write it into the names_table
    my %species_names =
      query_to_hash(
      $ens_dbh,
"select meta_key,meta_value from meta where species_id='$species_id'" );

    if ( !defined $species_names{'species.proteome_id'} ||
         !isdigit $species_names{'species.proteome_id'} )
    {
      $species_names{'species.proteome_id'} = ++$pId;
    }
    my $assembly   = $species_names{'assembly.name'};
    my $gb_version = $species_names{'genebuild.version'};
    if ( $div eq "ensembl" ) {
      $gb_version =
        $species_names{'genebuild.last_geneset_update'} ||
        $species_names{'genebuild.start_date'}          ||
        $species_names{'genebuild_version'};
    }

    my $has_chromosomes = get_string(
      $ens_dbh->prepare(
"select count(distinct coord_system_id) from coord_system join seq_region using (coord_system_id) join seq_region_attrib using (seq_region_id) join attrib_type using (attrib_type_id) where code='karyotype_rank' and species_id=${species_id}"
      ) );

    $names_insert->execute( $dataset,
                            $dataset,
                            $ens_db,
                            $species_names{'species.proteome_id'},
                            $species_names{'species.taxonomy_id'},
                            $species_names{'species.display_name'},
                            $species_names{'species.production_name'},
                            $assembly,
                            $gb_version,
                            $has_chromosomes );

    # Add a meta key on the core database
    # Do that only when templating gene mart - not SNP mart nor e! marts
    if ( ($mart_db !~ /snp/i) and ($div ne 'ensembl') ) {
      $meta_insert->execute( $species_id, $dataset );
    }

  } ## end foreach my $species_id (@species_ids)
  $ens_dbh->disconnect();

} ## end foreach my $dataset (@datasets)

$mart_handle->disconnect();

$logger->info("Complete");

