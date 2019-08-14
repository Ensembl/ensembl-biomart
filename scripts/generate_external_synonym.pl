#!/bin/env perl
use warnings;
use strict;

use DBI;
use Getopt::Long qw(:config no_ignore_case);
use Bio::EnsEMBL::Registry;
use DbiUtils;
use MartUtils;
use Log::Log4perl qw(:easy);


my ($reg_file, $host, $port, $user, $pass, $mart,$dataset, $basename);

GetOptions(
  "reg_file=s", \$reg_file,
  "host=s",     \$host,
  "P|port=i",   \$port,
  "user=s",     \$user,
  "p|pass=s",   \$pass,
  "mart=s",     \$mart,
  "dataset=s",   \$dataset,
  "basename=s", \$basename,
  
);

die '--reg_file required' unless defined $reg_file;
die '--host required'     unless defined $host;
die '--port required'     unless defined $port;
die '--user required'     unless defined $user;
die '--pass required'     unless defined $pass;
die '--mart required'     unless defined $mart;
die '--dataset required'  unless defined $dataset;
$basename='gene'  unless defined $basename;


Log::Log4perl->easy_init($INFO);
my $logger = get_logger();

 
my $registry = 'Bio::EnsEMBL::Registry';
$registry->load_all($reg_file);

my $mart_string = "DBI:mysql:$mart:$host:$port";
my $mart_handle = DBI->connect(
  $mart_string, $user, $pass,
	{ RaiseError => 1 }
) or die "Could not connect to $mart_string";

my $core_db = get_string($mart_handle->prepare("SELECT src_db FROM dataset_names WHERE name='$dataset'"));
$logger->info(" Connecting to ${core_db} ...");
my $species_name = get_sql_name_for_dataset( $mart_handle, $dataset );
my $dba = Bio::EnsEMBL::Registry->get_DBAdaptor( $species_name, 'core' );  
my $sql = "SELECT COUNT(*) FROM external_synonym;";
my $sth = $dba->dbc->db_handle->prepare($sql);
$sth->execute();
my $results = $sth->fetchall_arrayref();
if ($$results[0][0] > 1) {
  create_base_table($mart_handle, $mart, $core_db, $dataset);
}

sub create_base_table {
  my ($mart_handle, $mart, $core, $dataset) = @_;

  my $mart_table = "$mart.$dataset\_${basename}__external_synonym__dm";
  my $mart_tmp = "$mart.$dataset\_synonym_TMP";
  
  $mart_handle->do("DROP TABLE IF EXISTS $mart_table");
  $mart_handle->do("DROP TABLE IF EXISTS $mart_tmp");
    
  my $key = 'gene_id_1020_key';
  $logger->info(" Populating ${mart_tmp} ...");
  my $create_tmp_sql = qq/
    create table $mart_tmp as select
    g.gene_id, es.synonym as external_synonym from 
    $core.external_synonym es INNER JOIN
    $core.xref x on x.xref_id = es.xref_id INNER JOIN
    $core.object_xref ox on ox.xref_id = x.xref_id INNER JOIN
    $core.gene g on g.gene_id=ox.ensembl_id
    where
      ox.ensembl_object_type = 'Gene'
  /;
  $mart_handle->do($create_tmp_sql);
  $logger->info(" Populating ${mart_table} ...");
  my $create_sql = qq/create table $mart_table as select g.gene_id as $key, es.external_synonym from
  $core.gene g LEFT join
  $mart_tmp es USING (gene_id)
  /;
  $mart_handle->do($create_sql);
  $logger->info(" Creating indexes on ${mart_table} ...");
  $mart_handle->do("alter table ${mart_table} add index (${key}), add index (external_synonym);");
  $mart_handle->do("DROP table ${mart_tmp};")
}