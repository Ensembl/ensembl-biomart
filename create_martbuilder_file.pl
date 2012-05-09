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
use Getopt::Long;
use Data::Dumper;
use Bio::EnsEMBL::DBSQL::DBConnection;
use Bio::EnsEMBL::Utils::CliHelper;
use Carp;

my $cli_helper = Bio::EnsEMBL::Utils::CliHelper->new();

# get the basic options for connecting to a database server
my $optsd = $cli_helper->get_dba_opts();
# add the print option
push(@{$optsd},"division:s");
push(@{$optsd},"template:s");
push(@{$optsd},"mart:s");

# process the command line with the supplied options plus a help subroutine
my $opts = $cli_helper->process_args($optsd,\&usage);

$opts->{dbname} ||= 'ensembl_production';

if(!defined $opts->{host}) {
    $opts->{host} = 'mysql-eg-pan-1.ebi.ac.uk';
    $opts->{port} = 4276;
    $opts->{user} = 'ensro';
    delete $opts->{'pass'};
}

if(!defined $opts->{division} || !defined $opts->{template}|| !defined $opts->{mart}) {
    usage();
}

print "Connecting to $opts->{dbname}\n";
# use the args to create a DBA
my $dba = Bio::EnsEMBL::DBSQL::DBConnection->new(-USER => $opts->{user}, -PASS => $opts->{pass},
-DBNAME=>$opts->{dbname}, -HOST=>$opts->{host}, -PORT=>$opts->{port});

print "Getting db lists from $opts->{dbname}\n";
# 1. assemble core species list
my @cores = @{get_list($dba,$opts->{division},'core')};

# 2. assemble variation list
my @variation = @{get_list($dba,$opts->{division},'variation')};
# 3. assemble funcgen list
my @funcgen = @{get_list($dba,$opts->{division},'funcgen')};

my $core_str = join ',',@cores;
print "Cores found: $core_str\n";
my $var_str = join ',',@variation;
print "Variation found: $var_str\n";
my $func_str = join ',',@funcgen;
print "Funcgen found: $func_str\n";

my $inname = $opts->{template};
print "Reading $inname\n";
open(my $in_file, "<", $inname) or croak "Could not open $inname";

my $outname = $opts->{mart}.'.xml';
print "Writing $outname\n";
open(my $out_file, '>', $outname) or croak "Could not open $outname";

my $mart = $opts->{mart};
while (<$in_file>) {
    s/core_species_list/$core_str/g;
    s/funcgen_species_list/$func_str/g;
    s/variation_species_list/$var_str/g;
    s/division_mart_[0-9]+/$opts->{mart}/g;
    print $out_file $_;
}

close $in_file;
close $out_file;

sub get_list {
    my ($dba,$division,$type) = @_;
    my @list = ();
    for my $db (@{$dba->sql_helper()->execute_simple(-SQL=>'select db_name from division join division_species using (division_id) join species using (species_id) join db using (species_id) where division.name=? and db_type=? and db.is_current=1 and species.is_current=1',-PARAMS=>[$division,$type])}) {
	$db=~s/([a-z])[^_]+_([^_]+)/$1$2_eg/;
	push @list, $db;
    }
    return \@list;
}

sub usage {
	my $indent = ' ' x length($0);
	print <<EOF; exit(0);

  -h|host              Database host to connect to (default is mysql-eg-pan-1.ebi.ac.uk)

  -port                Database port to connect to

  -u|user              Database username 

  -p|pass              Password for user 

  -d|dbname            Database name (default is ensembl_production)

  -mart                Name of mart to generate

  -template            Template file to read from

  -division            Name of division (e.g. EnsemblFungi)

EOF
}
