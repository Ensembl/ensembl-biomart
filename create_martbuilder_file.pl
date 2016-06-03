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
use Getopt::Long;
use Data::Dumper;
use Bio::EnsEMBL::DBSQL::DBConnection;
use Bio::EnsEMBL::Utils::CliHelper;
use Carp;

my $cli_helper = Bio::EnsEMBL::Utils::CliHelper->new();

# get the basic options for connecting to a database server
my $optsd = [@{$cli_helper->get_dba_opts()},@{$cli_helper->get_dba_opts('pan')}];
# add the print option
push(@{$optsd},"division:s");
push(@{$optsd},"template:s");
push(@{$optsd},"mart:s");
push(@{$optsd},"collection");
push(@{$optsd},"eg:s");
push(@{$optsd},"ens:s");

# process the command line with the supplied options plus a help subroutine
my $opts = $cli_helper->process_args($optsd,\&usage);

$opts->{pandbname} ||= 'ensembl_production';

if(!defined $opts->{panhost}) {
    $opts->{panhost} = 'mysql-eg-pan-prod.ebi.ac.uk';
    $opts->{panport} = 4276;
    $opts->{panuser} = 'ensro';
    delete $opts->{panpass};
}

if(!defined $opts->{division} || !defined $opts->{template}|| !defined $opts->{mart} || !defined $opts->{eg} || !defined $opts->{ens} || !defined $opts->{host}) {
    usage();
}

print "Connecting to $opts->{pandbname}\n";
# use the args to create a DBA
my $dba = Bio::EnsEMBL::DBSQL::DBConnection->new(-USER => $opts->{panuser}, -PASS => $opts->{panpass},
-DBNAME=>$opts->{pandbname}, -HOST=>$opts->{panhost}, -PORT=>$opts->{panport});

print "Getting db lists from $opts->{pandbname}\n";
# 1. assemble core species list
my @cores = @{get_list($dba,$opts->{division},'core')};
my @variation = ();
my @funcgen = ();
if(defined $opts->{collection}) {
    # keep collection only
    @cores = grep {$_ =~ m/collection/} @cores;
    # collections have no variation or funcgen at the moment
} else {
    # strip out collections
    @cores = grep {$_ !~ m/collection/} @cores;
    # 2. assemble variation list
    @variation = @{get_list($dba,$opts->{division},'variation')};
    # 3. assemble funcgen list
    @funcgen = @{get_list($dba,$opts->{division},'funcgen')};
}

my $core_str = join ',',@cores;
print "Cores found: $core_str\n";
my $var_str = join ',',@variation;
print "Variation found: $var_str\n";
my $func_str = join ',',@funcgen;
print "Funcgen found: $func_str\n";

my $inname = $opts->{template};
print "Reading $inname\n";
open(my $in_file, "<", $inname) or croak "Could not open $inname";

my $outname = $opts->{mart}.((defined $opts->{collection})?'_collection':'').'.xml';
print "Writing $outname\n";
open(my $out_file, '>', $outname) or croak "Could not open $outname";

my $mart = $opts->{mart};
while (<$in_file>) {
    s/core_species_list/$core_str/g;
    s/funcgen_species_list/$func_str/g;
    s/variation_species_list/$var_str/g;
    s/%EG%/$opts->{eg}/g;
    s/%ENS%/$opts->{ens}/g;
    s/%HOST%/$opts->{host}/g;
    s/%USER%/$opts->{user}/g;
    s/%PORT%/$opts->{port}/g;
    s/%PASS%/$opts->{pass}/g;
    s/division_mart_[0-9]+/$opts->{mart}/g;
    print $out_file $_;
}

close $in_file;
close $out_file;

sub get_list {
    my ($dba,$division,$type) = @_;
    my @list = ();
    for my $db (@{$dba->sql_helper()->execute_simple(-SQL=>'select db_name from division join division_species using (division_id) join species using (species_id) join db using (species_id) where division.name=? and db_type=? and db.is_current=1 and species.is_current=1',-PARAMS=>[$division,$type])}) {
	if($division eq 'EnsemblParasite') {
          $db =~ s/([a-z])[^_]+_([^_]+)_([^_]+)/$2$3_eg/; # Need to use the BioProject to differentiate between the duplicate genome projects; name becomes too long if we use the species+BioProject
	} else {
            if(defined $opts->{collection}) {
                $db =~ s/^[^_]+_([^_]+_collection)/$1_eg/;
            } else {
                $db =~ s/([a-z])[^_]+_([^_]+)/$1$2_eg/;
            }
        }
        push @list, $db;
    }
    return \@list;
}

sub usage {
	my $indent = ' ' x length($0);
	print <<EOF; exit(0);

  -h|host              Database host to connect to (default is mysql-eg-pan-prod.ebi.ac.uk)

  -port                Database port to connect to

  -u|user              Database username 

  -p|pass              Password for user 

  -d|pandbname         Database name (default is ensembl_production)

  -mart                Name of mart to generate

  -template            Template file to read from

  -division            Name of division (e.g. EnsemblFungi)

  -ens                 Ensembl version number

  -eg                  Ensembl Genomes version number

EOF
}
