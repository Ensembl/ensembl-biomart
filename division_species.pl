#!/usr/bin/env/perl
use strict;
use warnings;

use Getopt::Long qw(:config no_ignore_case);
use Bio::EnsEMBL::Registry;
Bio::EnsEMBL::Registry->no_version_check(1);

my ($host, $port, $user, $pass, $release, $division, $group);

GetOptions(
  "host=s", \$host,
  "P|port=i", \$port,
  "user=s", \$user,
  "p|password=s", \$pass,
  "release=i", \$release,
  "division:s", \$division,
  "group:s", \$group,
);
die "Option -r[elease] is required" unless $release;
die "Option -d[ivision] is required" unless $division;
$group = 'core' unless $group;

if ($division !~ /^Ensembl/) {
  $division = "Ensembl".ucfirst($division);
}

# List all species in a division for a given release. 

my $registry = 'Bio::EnsEMBL::Registry';
$registry->load_registry_from_db(
  -host       => $host,
  -port       => $port,
  -user       => $user,
  -pass       => $pass,
  -db_version => $release,
  -no_cache   => 1,
);

my @dba = @{ $registry->get_all_DBAdaptors(-group => $group) };
foreach my $dba (sort {$a->dbc()->dbname() cmp $b->dbc()->dbname()} @dba) {
  my $dbname = $dba->dbc()->dbname();
  next unless $dbname =~ /$group/;
  my ($species) = $dbname =~ /(\w+)_$group/;

  my $db_division;
  if ($group =~ /^(funcgen|variation)$/) {
    my $core_dba = $registry->get_DBAdaptor($species, 'core');
    $db_division = $core_dba->get_MetaContainer()->get_division();
  } else {
    $db_division = $dba->get_MetaContainer()->get_division();
  }
  next unless defined $db_division;

  if ($division eq 'EnsemblBacteria') {
    $species = $dba->species();
  }

  if ($db_division eq $division) {
    print "$species\n";
  }
  $dba->dbc()->disconnect_if_idle();
}
