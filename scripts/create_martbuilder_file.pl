#!/bin/env perl
# Copyright [2009-2021] EMBL-European Bioinformatics Institute
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
use Bio::EnsEMBL::Utils::CliHelper;
use Carp;
use Bio::EnsEMBL::MetaData::DBSQL::MetaDataDBAdaptor;
use MartUtils qw(generate_dataset_name_from_db_name);
use Bio::EnsEMBL::BioMart::Mart qw(genome_to_include);

my $cli_helper = Bio::EnsEMBL::Utils::CliHelper->new();

# get the basic options for connecting to a database server
my $optsd = [ @{$cli_helper->get_dba_opts()}, @{$cli_helper->get_dba_opts('m')} ];
# add the print option
push(@{$optsd}, "division:s");
push(@{$optsd}, "template:s");
push(@{$optsd}, "mart:s");
push(@{$optsd}, "eg:s");
push(@{$optsd}, "ens:s");
push(@{$optsd}, "runner_host:s");
push(@{$optsd}, "runner_port:s");
push(@{$optsd}, "grch37:i");

# process the command line with the supplied options plus a help subroutine
my $opts = $cli_helper->process_args($optsd, \&usage);

$opts->{mdbname} ||= 'ensembl_metadata';
$opts->{runner_port} ||= 8888;

if (!defined $opts->{division} || !defined $opts->{template} || !defined $opts->{mart} || !defined $opts->{eg} || !defined $opts->{ens} || !defined $opts->{host} || !defined $opts->{mhost} || !defined $opts->{runner_host}) {
    usage();
}

print "Connecting to $opts->{mdbname}\n";
# use the args to create a DBA
my $dba = Bio::EnsEMBL::MetaData::DBSQL::MetaDataDBAdaptor->new(-USER => $opts->{muser}, -PASS => $opts->{mpass},
    -DBNAME                                                           => $opts->{mdbname}, -HOST => $opts->{mhost}, -PORT => $opts->{mport});

my ($core, $variation, $funcgen, $excluded_species) = ([], [], [], []);
my $mart = $opts->{mart};
# Assemble list of databases for each species by db_type
if (defined $opts->{grch37}) {
    print "Getting db lists from $opts->{host}\n";
    Bio::EnsEMBL::Registry->load_registry_from_db(
        -host       => $opts->{host},
        -user       => $opts->{user},
        -pass       => $opts->{pass},
        -port       => $opts->{port},
        -db_version => $opts->{ens});

    my $core_dba = Bio::EnsEMBL::Registry->get_DBAdaptor('homo_sapiens', 'core');
    push $core, $core_dba->dbc()->dbname();
    my $variarion_dba = Bio::EnsEMBL::Registry->get_DBAdaptor('homo_sapiens', 'variation');
    push $variation, $variarion_dba->dbc()->dbname();
    my $regulation_dba = Bio::EnsEMBL::Registry->get_DBAdaptor('homo_sapiens', 'funcgen');
    push $funcgen, $regulation_dba->dbc()->dbname();
    $core_dba->dbc()->disconnect_if_idle();
    $variarion_dba->dbc()->disconnect_if_idle();
    $regulation_dba->dbc()->disconnect_if_idle();
    $mart = $mart . '_grch37';
}
else {
    print "Getting db lists from $opts->{mdbname}\n";
    ($core, $variation, $funcgen, $excluded_species) = get_list($dba);
}

# Print number of database type and genome name in mart for each genome of a division
my $core_str = join ',', @$core;
print scalar(@$core) . " Cores found: $core_str\n";
my $var_str = join ',', @$variation;
print scalar(@$variation) . " Variation found: $var_str\n";
my $func_str = join ',', @$funcgen;
print scalar(@$funcgen) . " Funcgen found: $func_str\n";
if (scalar(@$excluded_species)) {
    my $excluded_str = join ',', @$excluded_species;
    print scalar(@$excluded_species) . " Excluded species from mart: $excluded_str\n";
}

my ($partitionRegex, $partitionExpression, $name);
if ($opts->{division} eq "EnsemblVertebrates") {
    $partitionRegex = $opts->{ens};
    $partitionExpression = '$1$2$3';
    $name = "gene_ensembl";
}
else {
    $partitionRegex = $opts->{eg} . "_" . $opts->{ens};
    $partitionExpression = '$1$2$3_eg';
    $name = 'gene';
}

my $inname = $opts->{template};
print "Reading $inname\n";
open(my $in_file, "<", $inname) or croak "Could not open $inname";

my $outname = $mart . '.xml';
print "Writing $outname\n";
open(my $out_file, '>', $outname) or croak "Could not open $outname";

while (<$in_file>) {
    s/core_species_list/$core_str/g;
    s/funcgen_species_list/$func_str/g;
    s/variation_species_list/$var_str/g;
    s/%EG%/$opts->{eg}/g;
    s/%ENS%/$opts->{ens}/g;
    s/%PARTITION_REGEX%/$partitionRegex/g;
    s/%PARTITION_EXPRESSION%/$partitionExpression/g;
    s/%NAME%/$name/g;
    s/%HOST%/$opts->{host}/g;
    s/%USER%/$opts->{user}/g;
    s/%PORT%/$opts->{port}/g;
    s/%PASS%/$opts->{pass}/g;
    s/%RUNNER_HOST%/$opts->{runner_host}/g;
    s/%RUNNER_PORT%/$opts->{runner_port}/g;
    s/division_mart_[0-9]+/$opts->{mart}/g;
    print $out_file $_;
}

close $in_file;
close $out_file;

# Get a list of genome mart name for each database type of given division and release using the metadata database
sub get_list {
    my ($dba) = @_;
    my @core = ();
    my @variation = ();
    my @funcgen = ();
    my @excluded_species = ();
    #Get metadata adaptors
    my $gdba = $dba->get_GenomeInfoAdaptor();
    my $dbdba = $dba->get_DatabaseInfoAdaptor();
    my $rdba = $dba->get_DataReleaseInfoAdaptor();
    my $release;
    my $included_species;
    # Use division to find the release in metadata database
    if ($opts->{division} eq "EnsemblVertebrates") {
        $release = $rdba->fetch_by_ensembl_release($opts->{ens});
        # Load species to include in the Vertebrates marts
        $included_species = genome_to_include($opts->{division}, $ENV{BASE_DIR});
    }
    else {
        $release = $rdba->fetch_by_ensembl_genomes_release($opts->{eg});
    }
    $gdba->data_release($release);
    # Get all the genomes for a given division and release
    my $genomes = $gdba->fetch_all_by_division($opts->{division});
    foreach my $genome (@$genomes) {
        # Special hack for the ensembl mart as we don't want the mouse strains in it.
        if ($genome->name() =~ m/^mus_musculus_/) {
            if ($opts->{mart} =~ "ensembl_mart" and $opts->{division} eq "EnsemblVertebrates") {
                next;
            }
        }
        # Special hack for the mouse mart as we only want the mouse strains in it
        elsif ($opts->{mart} =~ "mouse_mart" and $opts->{division} eq "EnsemblVertebrates") {
            next;
        }
        # For Vertebrates, we are excluding some species from the marts
        if ($opts->{division} eq "EnsemblVertebrates" and $opts->{mart} !~ "mouse_mart") {
            my $genome_name = $genome->name();
            if (!grep(/$genome_name/, @$included_species)) {
                push(@excluded_species, $genome_name);
                next;
            }
        }
        # Get all the databases associated to a genome
        foreach my $database (@{$genome->databases()}) {
            # Generate mart name using regexes
            my $mart_name = generate_dataset_name_from_db_name($database->dbname);
            # Change name for non-vertebrates
            if ($opts->{division} ne "EnsemblVertebrates") {
                $mart_name = $mart_name . "_eg";
            }
            # For core databases, exclude collections as mart can't deal with the volume of data
            if ($database->type eq "core" and $database->dbname !~ m/collection/) {
                push(@core, $mart_name);
            }
            # Get variation and funcgen databases
            elsif ($database->type eq "variation") {
                # We now have empty variation databases linked to VCF files like chlorocebus_sabaeus
                if ($genome->has_variations()) {
                    push(@variation, $mart_name);
                }
            }
            elsif ($database->type eq "funcgen") {
                push(@funcgen, $mart_name);
            }
        }
    }
    return (\@core, \@variation, \@funcgen, \@excluded_species);
}

sub usage {
    my $indent = ' ' x length($0);
    print <<EOF; exit(0);

  -h|host              Database host to connect to

  -port                Database port to connect to

  -u|user              Database username 

  -p|pass              Password for user 

  -d|pandbname         Metadata database name (default is ensembl_metadata)

  -mart                Name of mart to generate

  -template            Template file to read from

  -division            Name of division (e.g. EnsemblFungi, EnsemblMetazoa, EnsemblPlants, EnsemblProtists, EnsemblMetazoa)

  -ens                 Ensembl version number (e.g: 95)

  -eg                  Ensembl Genomes version number (e.g: 42)

  -grch37              Flag for GRCh37 release

EOF
}
