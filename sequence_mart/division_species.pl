#!/usr/bin/env/perl
use strict;
use warnings;

use Getopt::Long qw(:config no_ignore_case);
use Bio::EnsEMBL::Registry;
Bio::EnsEMBL::Registry->no_version_check(1);

my ($host, $port, $user, $pass, $release, $division);

GetOptions(
  "host=s", \$host,
  "P|port=i", \$port,
  "user=s", \$user,
  "p|password=s", \$pass,
  "release=i", \$release,
  "division:s", \$division,
);
die "Option -r[elease] is required" unless $release;
die "Option -d[ivision] is required" unless $division;

if ($division ne 'vb' && $division !~ /^Ensembl/) {
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

my @dba = @{ $registry->get_all_DBAdaptors(-group => 'core') };
foreach my $dba (sort {$a->dbc()->dbname() cmp $b->dbc()->dbname()} grep {$_->dbc()->dbname() !~ m/_collection_/} @dba) {
  my $species = $dba->species();

  if ($division eq 'vb') {
    my $status = $dba->get_MetaContainer()->single_value_by_key('vb.release.status');
    if ($status && $status eq 'release') {
      print "$species\n";
    }
    
  } else {
    my $db_division = $dba->get_MetaContainer()->get_division();
    if (! defined $db_division) {
      warn "$species does not have a 'species.division' meta key";
    } elsif ($db_division eq $division) {
      print "$species\n";
    }
  }
  
  $dba->dbc()->disconnect_if_idle();
}
