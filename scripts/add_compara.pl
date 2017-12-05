#!/usr/bin/env perl
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


# Script to populate homology dimension tables for partitioned marts from a set of SQL templates

use warnings;
use strict;
use DBI;
use Data::Dumper;
use Carp;
use Log::Log4perl qw(:easy);
use FindBin;
use lib "$FindBin::Bin/../modules";
use DbiUtils;
use MartUtils;
use Cwd;
use File::Copy;
use Getopt::Long;
use File::Slurp qw/read_file/;

Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();

my $db_host;
my $db_port;
my $db_user;
my $db_pwd;
my $mart_db;
my $compara_db;
my $dataset_name;
my $limit_species;
my $basename='gene';
my $template;

my $template_dir = "$FindBin::Bin/templates";

sub usage {
    print "Usage: $0 [-h <host>] [-port <port>] [-u user <user>] [-p <pwd>] [-mart <mart>] [-compara <compara db>] [-name <name>] [-template <template>] \n";
    print "-h <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <mart_db> Default is $mart_db\n";
    print "-compara <compara_db> Default is $compara_db\n";
    print "-name <base name> Default is $basename\n";
    exit 1;
};

my $options_okay = GetOptions (
    "host=s"=>\$db_host,
    "port=i"=>\$db_port,
    "user=s"=>\$db_user,
    "pass=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "compara=s"=>\$compara_db,
    "dataset=s"=>\$dataset_name,
    "species=s"=>\$limit_species,
    "name=s"=>\$basename,
    "help"=>sub {usage()}
);

if (!$options_okay) {
    usage();
}

if (!defined $mart_db || !defined $compara_db) {
    usage();
}

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle =
    DBI->connect($mart_string, $db_user, $db_pwd, { RaiseError => 1 })
    or croak "Could not connect to $mart_string";

sub get_species_sets {
    my ($sth,$dataset,$dataset2) = @_;
    $dataset=$dataset || '';
    my @species_sets = ();
    $sth->execute($dataset,$dataset2,$dataset,$dataset2);
    while(my @data = $sth->fetchrow_array()) {
        my $tld = $data[3];
        push(@species_sets,{id=>$data[0],name=>$data[1], tld=>$tld});
    }
    @species_sets;
}

sub write_species {
    my ($dataset, $basename, $species_id, $species_name, $speciesTld, $sql_file_name) = @_;
    my $ds = $dataset.'_'.$basename;
    my $indexN = 0; 
    my $all_sql = read_file($sql_file_name);
    for my $sql (split(';',$all_sql)) {
      next unless $sql =~ m/\w+/;
            my $indexName = 'I_'.$species_id.'_'.++$indexN;
            $sql =~ s/%srcSchema%/$compara_db/g;
            $sql =~ s/%martSchema%/$mart_db/g;
            $sql =~ s/%dataSet%/$ds/g;
            $sql =~ s/%speciesTld%/$speciesTld/g;
            $sql =~ s/%method_link_species_set_id%/$species_id/g;
            $sql =~ s/%indexName%/$indexName/g;
                $logger->info($sql);
                my $sth = $mart_handle->prepare($sql);
                $sth->execute();
	  }
}

sub write_family {
    my ($dataset, $basename) = @_;
    eval {
	# ignore failure
    $mart_handle->do(
	qq/ALTER TABLE ${dataset}_${basename}__translation__main
ADD COLUMN stable_id_408      VARCHAR(40) DEFAULT NULL,
ADD COLUMN description_408    TEXT
/
	);
    };

    $mart_handle->do(
	qq/
UPDATE ${dataset}_${basename}__translation__main m
JOIN ${compara_db}.seq_member s on (s.stable_id=m.stable_id_1070)
JOIN ${compara_db}.family_member fm using (seq_member_id)
JOIN ${compara_db}.family f using (family_id)
set m.stable_id_408     = f.stable_id,
m.description_408   = f.description
/
	);

    eval {
	# ignore failure
    $mart_handle->do(qq/ALTER TABLE ${dataset}_${basename}__translation__main
ADD INDEX stable_id_408_idx(stable_id_408)/);
    };

    # Add boolean column
    eval {
  # ignore failure
    $mart_handle->do(qq/ALTER TABLE ${dataset}_${basename}__translation__main
ADD COLUMN (family_bool integer default NULL)/);
    $mart_handle->do(qq/UPDATE ${dataset}_${basename}__translation__main
SET family_bool=1 WHERE stable_id_408 IS NOT NULL/);
    $mart_handle->do(qq/ALTER TABLE ${dataset}_${basename}__translation__main
ADD INDEX family_bool_idx(family_bool)/);
    };

    return;
}


my $species_homolog_sql = qq/select ms.method_link_species_set_id, g.name, CONCAT(CONCAT(n.src_dataset,'_'),n.species_id), n.name
from $compara_db.species_set s
join $compara_db.method_link_species_set ms using (species_set_id)
join $compara_db.method_link m using (method_link_id)
join $compara_db.genome_db g using (genome_db_id)
join $mart_db.dataset_names n on (n.sql_name=g.name or n.species_name=g.name)
where
s.species_set_id in (
  select distinct (ss.species_set_id) from
  $compara_db.species_set ss
  join $compara_db.genome_db gg
  using (genome_db_id)
  where (gg.name=? or gg.name=?)
)
AND g.name<>? AND g.name<>?
AND m.type='ENSEMBL_ORTHOLOGUES'/;

my $species_paralog_sql = qq/select ms.method_link_species_set_id, g.name, CONCAT(CONCAT(n.src_dataset,'_'),n.species_id)
from $compara_db.species_set s
join $compara_db.method_link_species_set ms using (species_set_id)
join $compara_db.method_link m using (method_link_id)
join $compara_db.genome_db g using (genome_db_id)
join $mart_db.dataset_names n on (n.sql_name=g.name or n.species_name=g.name)
where
s.species_set_id in (
  select distinct (ss.species_set_id) from
  $compara_db.species_set ss
  join $compara_db.genome_db gg
  using (genome_db_id)
  where (gg.name=? or gg.name=?)
)
AND (g.name=? OR g.name=?) AND m.type='ENSEMBL_PARALOGUES'
and ms.method_link_species_set_id in
(select distinct method_link_species_set_id  from $compara_db.homology where description='within_species_paralog')/;

my $species_homoeolog_across_species_sql = qq/select ms.method_link_species_set_id, g.name, CONCAT(CONCAT(n.src_dataset,'_'),n.species_id), n.name
    from $compara_db.species_set s
    join $compara_db.method_link_species_set ms using (species_set_id)
    join $compara_db.method_link m using (method_link_id)
    join $compara_db.genome_db g using (genome_db_id)
    join $mart_db.dataset_names n on (n.sql_name=g.name or n.species_name=g.name)
    where
    s.species_set_id in (
        select distinct (ss.species_set_id) from
        $compara_db.species_set ss
        join $compara_db.genome_db gg
        using (genome_db_id)
        where (gg.name=? or gg.name=?)
    )
    AND g.name<>? AND g.name<>?
    AND m.type='ENSEMBL_HOMOEOLOGUES'/;

# I guess for this one, we should make sute the species_set has only one entry

my $species_homoeolog_within_species_sql = qq/
    select ms.method_link_species_set_id, g.name, CONCAT(CONCAT(n.src_dataset,'_'),n.species_id)
    from $compara_db.species_set s
    join $compara_db.method_link_species_set ms using (species_set_id)
    join $compara_db.method_link m using (method_link_id)
    join $compara_db.genome_db g using (genome_db_id)
    join $mart_db.dataset_names n on (n.sql_name=g.name or n.species_name=g.name)
    where
    s.species_set_id in (
        select distinct (ss.species_set_id) from
        $compara_db.species_set ss
        join $compara_db.genome_db gg
        using (genome_db_id)
        where (gg.name=? or gg.name=?)
    )
    AND (g.name=? OR g.name=?) AND m.type='ENSEMBL_HOMOEOLOGUES'/;

my $species_homolog_sth = $mart_handle->prepare($species_homolog_sql);
my $species_paralog_sth = $mart_handle->prepare($species_paralog_sql);
my $species_homoeolog_across_species_sth = $mart_handle->prepare($species_homoeolog_across_species_sql);
my $species_homoeolog_within_species_sth = $mart_handle->prepare($species_homoeolog_within_species_sql);
my $homolog_sql = $template_dir.'/generate_homolog.sql.template';
my $paralog_sql = $template_dir.'/generate_paralog.sql.template';
my $homoeolog_sql = $template_dir.'/generate_homoeolog.sql.template';

my $get_species_id_sth = $mart_handle->prepare('select species_id from dataset_names where name=?');
my $get_species_clade_sth = $mart_handle->prepare('select src_dataset from dataset_names where name=?');

my @datasets = defined($dataset_name)?($dataset_name):get_dataset_names($mart_handle);
for my $dataset (sort @datasets) {
  my $ds_name_sql = get_sql_name_for_dataset($mart_handle,$dataset);
  next if (defined $limit_species && $ds_name_sql ne $limit_species);
  my $ds_name_full = get_species_name_for_dataset($mart_handle,$dataset);
  $logger->info("Processing dataset $ds_name_sql as $dataset");
  for my $table_type (('gene','transcript','translation')) {
    my $table_name = $dataset.'_'.$basename.'__'.$table_type.'__main';
    for my $type (qw(homoeolog)) {
      for my $col (query_to_strings($mart_handle,"show columns from $table_name like '${type}_%_bool'")) {
        $mart_handle->do("alter table $table_name drop column $col") || croak "Could not drop column $table_name.$col";
        }
    }
  }
  # Check if the division has family data
  if (row_count($mart_handle,"$compara_db.family") > 0){
    # add family
    $logger->info("Adding family data");
    write_family($dataset, $basename);
  }
  # work out species name from $dataset
  # get list of method_link_species_set_id/name pairs for homolog partners
  for my $species_set (sort {$a->{name} cmp $b->{name}} get_species_sets($species_homolog_sth,$ds_name_sql,$ds_name_full)) {
    $logger->info('Processing '.$ds_name_sql.' homologs for '.$species_set->{name}.' as '.$species_set->{tld}." (mlss=".$species_set->{id}.")"); 
    for my $table_type (('gene','transcript','translation')) {
      my $table_name = $dataset.'_'.$basename.'__'.$table_type.'__main';
      my $sql = "show columns from $table_name like 'homolog_".$species_set->{tld}."_bool'";
      for my $col (query_to_strings($mart_handle,$sql)) {
        $logger->info("Dropping $table_name $col");
        $mart_handle->do("alter table $table_name drop column $col") or croak "Could not drop column $table_name.$col";
      }
    }
    write_species($dataset, $basename, $species_set->{id}, $species_set->{name}, $species_set->{tld}, $homolog_sql);
    $logger->info('Completed '.$ds_name_sql.' homologs for '.$species_set->{name}.' as '.$species_set->{tld});
  }

  # get paralogs
  my $id = get_string($get_species_id_sth,$dataset);
  my $clade = get_string($get_species_clade_sth,$dataset);
  $logger->info("Processing paralogs for $ds_name_sql as $dataset");
  my $paralog_mlss_id = get_string($species_paralog_sth,$ds_name_sql,$ds_name_full,$ds_name_sql,$ds_name_full);
  if($paralog_mlss_id && $id) {
  for my $table_type (('gene','transcript','translation')) {
    my $table_name = $dataset.'_'.$basename.'__'.$table_type.'__main';

    for my $col (query_to_strings($mart_handle,"show columns from $table_name like 'paralog_".$dataset."_bool'")) {
        $logger->info("Dropping $table_name $col");
        $mart_handle->do("alter table $table_name drop column $col") or croak "Could not drop column $table_name.$col";
    }
}
    write_species($dataset, $basename, $paralog_mlss_id, $dataset, $dataset, $paralog_sql);
    $logger->info("Completed paralogs for $ds_name_sql as $dataset");
  }

  # work out species name from $dataset
  # get list of method_link_species_set_id/name pairs for homoeolog partners

  # at the moment, this is not relevant. It would only be relevant if triticum_aestivum component genomes become independent genomes.

  for my $species_set (get_species_sets($species_homoeolog_across_species_sth,$ds_name_sql,$ds_name_full)) {
    $logger->info('Processing '.$ds_name_sql.' homoeologs for '.$species_set->{name}.' as '.$species_set->{tld});
    write_species($dataset, $basename, $species_set->{id}, $species_set->{name}, $species_set->{tld}, $homoeolog_sql);
    $logger->info('Completed '.$ds_name_sql.' homoeologs for '.$species_set->{name}.' as '.$species_set->{tld});
  }

  # get homoeologs, this time, within the species only
  # this one should produce data
  $logger->info("Processing '.$ds_name_sql.' homoeologs for $ds_name_sql as $dataset");
  my $homoeolog_mlss_id = get_string($species_homoeolog_within_species_sth,$ds_name_sql,$ds_name_full,$ds_name_sql,$ds_name_full);
  if($homoeolog_mlss_id && $id) {
    write_species($dataset, $basename, $homoeolog_mlss_id, $dataset, $dataset, $homoeolog_sql);
    $logger->info("Completed '.$ds_name_sql.' homoeologs for $ds_name_sql as $dataset");
  }

}

$logger->info("Completed processing");
$species_homolog_sth->finish() or carp "Could not close statement handle";
$species_paralog_sth->finish() or carp "Could not close statement handle";
$species_homoeolog_across_species_sth->finish() or carp "Could not close statement handle";
$species_homoeolog_within_species_sth->finish() or carp "Could not close statement handle";
$get_species_id_sth->finish() or carp "Could not close statement handle";
$get_species_clade_sth->finish() or carp "Could not close statement handle";
$mart_handle->disconnect() or carp "Could not close handle to $mart_string";
