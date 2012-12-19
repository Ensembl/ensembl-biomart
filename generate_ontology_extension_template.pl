#!/bin/env perl

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
use POSIX;

Log::Log4perl->easy_init($INFO);

my $logger = get_logger();

my $db_host = 'mysql-cluster-eg-prod-1.ebi.ac.uk';
my $db_port = 4238;
my $db_user = 'ensrw';
my $db_pwd  = 'writ3rp1';
my $mart_db;
my $dataset;

sub usage {
  print "Usage: $0 [-h <host>] [-port <port>] [-u user <user>] [-p <pwd>] [-mart <mart>] [-databset <dataset_name>]\n";
  print "-h <host> Default is $db_host\n";
  print "-port <port> Default is $db_port\n";
  print "-u <host> Default is $db_user\n";
  print "-p <password> Default is top secret unless you know cat\n";
  print "-mart <mart_db>\n";
  exit 1;
}

my $options_okay = GetOptions("h=s"    => \$db_host,
							  "port=i" => \$db_port,
							  "u=s"    => \$db_user,
							  "p=s"    => \$db_pwd,
							  "mart=s" => \$mart_db,
							  "help"   => sub { usage() });

if (!$options_okay) {
  usage();
}

if (!defined $mart_db) {
  usage();
}

# open a connection
# work out the name of the core
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd, {RaiseError => 1}) or croak "Could not connect to $mart_string";

my $tables = {};
for my $table (get_strings($mart_handle->prepare("show tables like '%_gene__%_extension__dm'"))) {
  my $base_table = $table;
  $base_table =~ s/.+_gene__(.+)/$1/;
  $logger->info("Examining $table as $base_table");
  # get conditions for this table now
  for my $condition (grep { $_ ne 'subject_label' && $_ ne 'source_label' } get_strings($mart_handle->prepare("select distinct(column_name) from information_schema.columns where table_name='$table' and table_schema='$mart_db' and column_name like '%_label'"))) {
	$condition =~ s/_label//;
	push @{$tables->{$base_table}}, $condition;
  }
}

my $filter_file = "ontology_filters.xml";
$logger->info("Writing filters to $filter_file");
open my $filters, ">", $filter_file;
my $attribute_file = "ontology_attributes.xml";
$logger->info("Writing attributes to $attribute_file");
open my $attributes, ">", $attribute_file;
my $key = 'transcript_id_1064_key';
for my $table (keys %$tables) {
  my $ontology = $table;
  $ontology =~ s/_extension__dm//;
  my $ontology_name = uc($ontology);
  print $attributes "<!-- ${table} attributes -->\n";
  print $filters "<!-- ${table} filters -->\n";
  for my $condition (@{$tables->{$table}}) {
	print $attributes <<"ATTR";
        <AttributeDescription displayName="${ontology_name} ${condition}" field="${condition}_label" 
            internalName="${ontology}_${condition}_label" key="${key}" 
        	maxLength="3" tableConstraint="${table}" useDefault="true"/>
        <AttributeDescription displayName="${ontology_name} ${condition} accession" field="${condition}_acc" 
            internalName="${ontology}_${condition}_acc" key="${key}" 
        	maxLength="3" tableConstraint="${table}" useDefault="true"/>
        <AttributeDescription displayName="${ontology_name} ${condition} database" field="${condition}_db" 
            internalName="${ontology}_${condition}_db" key="${key}" 
        	maxLength="3" tableConstraint="${table}" useDefault="true"/>
ATTR
	print $filters <<"FILTERS";
      <FilterCollection internalName="with_${ontology}_${condition}" 
      	displayName="${ontology_name} annotations with ${condition}" 
      	description="${ontology_name} entries with ${condition}">
        <FilterDescription description="Limit to ${ontology_name} entries with ${condition}" 
            displayName="${ontology_name} entries with ${condition}" 
        	displayType="list" field="${condition}_db" internalName="with_${ontology}_${condition}" 
        	key="${key}" legal_qualifiers="only,excluded" 
        	otherFilters="MULTI" qualifier="only" style="radio" tableConstraint="main" type="boolean">
          <Option displayName="Only" internalName="only" value="only"/>
          <Option displayName="Excluded" internalName="excluded" value="excluded"/>
        </FilterDescription>
      </FilterCollection>
      <FilterCollection internalName="${ontology}_${condition}" 
        displayName="${ontology_name} annotations with specified ${condition}" 
      	description="${ontology_name} annotations with specified ${condition}">
        <FilterDescription displayName="${ontology_name} annotations with specified ${condition}" 
        	displayType="list" field="${condition}_label" internalName="${ontology}_${condition}" 
        	key="${key}" legal_qualifiers="=" qualifier="=" multipleValues="1" style="menu" 
        	tableConstraint="${table}" type="text">
          <SpecificFilterContent internalName="replaceMe"/>
        </FilterDescription>
      </FilterCollection>
      <FilterCollection internalName="${ontology}_${condition}_acc" 
        displayName="${ontology_name} annotations with specified ${condition} accession" 
      	description="${ontology_name} annotations with specified ${condition} accession">
        <FilterDescription displayName="${ontology_name} annotations with specified ${condition}" 
        	displayType="list" field="${condition}_acc" internalName="${ontology}_${condition}_acc" 
        	key="${key}" legal_qualifiers="=" qualifier="=" multipleValues="1" tableConstraint="${table}" type="text"/>
      </FilterCollection>
FILTERS

  } ## end for my $condition (@{$tables...})
} ## end for my $table (keys %$tables)
close $filters;
close $attributes;
