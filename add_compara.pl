#!/bin/env perl

# $Source$
# $Revision$
# $Date$
# $Author$
#
# Script to populate homology dimension tables for partitioned marts from a set of SQL templates

use warnings;
use strict;
use DBI;
use Data::Dumper;
use Carp;
use Log::Log4perl qw(:easy);
use DbiUtils;
use MartUtils;
use Cwd;
use File::Copy;
use Getopt::Long;

Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();

# db params
my $db_host = '127.0.0.1';
my $db_port = '4158';
my $db_user = 'admin';
my $db_pwd = 'L9xn1VpN';
my $mart_db = 'split_new_bacterial_mart_51';
my $compara_db ='ensembl_compara_bacteria_homology_0_51';

sub usage {
    print "Usage: $0 [-h <host>] [-P <port>] [-u user <user>] [-p <pwd>] [-src_mart <src>] [-target_mart <targ>]\n";
    print "-h <host> Default is $db_host\n";
    print "-P <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <mart_db> Default is $mart_db\n";
    print "-compara <compara_db> Default is $compara_db\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "compara=s"=>\$compara_db,
    "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

my %statement_cache = ();
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

sub get_species_sets {
    my ($sth,$dataset) = @_;
    my @species_sets = ();
    $sth->execute($dataset,$dataset);
    while(my @data = $sth->fetchrow_array()) {
	push(@species_sets,{id=>$data[0],name=>$data[1], tld=>$data[2]});
    }    
    @species_sets;
}

sub write_species {
    my ($dataset, $species_id, $species_name, $speciesTld, $sql_file_name) = @_;
    my $ds = $dataset.'_gene';
    open my $sql_file, '<', $sql_file_name or croak "Could not open SQL file $sql_file_name for reading";
    my $indexN = 0;
    while (my $sql = <$sql_file>) {
	chomp($sql);
	if($sql ne q{} && !($sql =~ m/^#/)) {
	    my $indexName = 'I_'.$species_id.'_'.++$indexN;
	    $sql =~ s/;$//;
	    $sql =~ s/%srcSchema%/$compara_db/g;
	    $sql =~ s/%martSchema%/$mart_db/g;
	    $sql =~ s/%dataSet%/$ds/g;
	    $sql =~ s/%speciesTld%/$speciesTld/g;
	    $sql =~ s/%method_link_species_set_id%/$species_id/g;
	    $sql =~ s/%indexName%/$indexName/;
	    #print "Executing $sql\n";
	    my $sth = $statement_cache{$sql};
	    if(!$sth) {
		$sth = $mart_handle->prepare($sql);
		$statement_cache{$sql} = $sth;
	    }
	    $sth->execute();
	}
    }
    close($sql_file);
}

my $species_sql = qq{
select ms.method_link_species_set_id, g.name, CONCAT(CONCAT(n.src_dataset,'_'),n.species_id) from $compara_db.species_set s
join $compara_db.method_link_species_set ms using (species_set_id)
join $compara_db.method_link m using (method_link_id)
join $compara_db.genome_db g using (genome_db_id)
join $mart_db.dataset_names n on n.name=g.name
where
s.species_set_id in (
select distinct (ss.species_set_id) from
       $compara_db.species_set ss
join $compara_db.genome_db gg
     using (genome_db_id)
where gg.name=?
)
AND g.name<>?
};

my $species_homolog_sth = $mart_handle->prepare($species_sql . ' AND m.type=\'ENSEMBL_ORTHOLOGUES\'');
my $species_paralog_sth = $mart_handle->prepare($species_sql . ' AND m.type=\'ENSEMBL_PARALOGUES\'');
my $homolog_sql = './templates/generate_homolog.sql.template';
my $paralog_sql = './templates/generate_paralog.sql.template';

# iterate over each dataset
my @datasets = get_dataset_names($mart_handle);
for my $dataset (@datasets) {
    $logger->info("Processing $dataset");
    my $table_name = $dataset.'_gene__gene__main';
    for my $type (qw(homolog paralog)) {
	for my $col (query_to_strings($mart_handle,"show columns from $table_name like '${type}_%_bool'")) {
	    $mart_handle->do("alter table $table_name drop column $col") or croak "Could not drop column $table_name.$col";
	}
    }
    # work out species name from $dataset
    # get list of method_link_species_set_id/name pairs for homolog partners
    for my $species_set (get_species_sets($species_homolog_sth,$dataset)) {
	$logger->info('Processing homologs for '.$species_set->{name}.' as '.$species_set->{tld});
	write_species($dataset, $species_set->{id}, $species_set->{name}, $species_set->{tld}, $homolog_sql);
    }
    for my $species_set (get_species_sets($species_paralog_sth,$dataset)) {
	$logger->info("Processing paralogs for ".$species_set->{name}.' as '.$species_set->{tld});
	write_species($dataset, $species_set->{id}, $species_set->{name},$species_set->{tld}, $paralog_sql);
    }
}
$logger->info("Completed processing");
$species_homolog_sth->finish() or carp "Could not close statement handle";
$species_paralog_sth->finish() or carp "Could not close statement handle";
$mart_handle->disconnect() or carp "Could not close handle to $mart_string";

