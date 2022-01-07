
#!/bin/env perl
# Copyright [2009-2022] EMBL-European Bioinformatics Institute
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
 
## Author
# Mark Mcdowall <mcdowall@ebi.ac.uk> and Thomas Maurel <maurel@ebi.ac.uk>

## Maintainer
# Thomas Maurel <maurel@ebi.ac.uk>

## Get the GO level:
#  select distinct object_xref.ensembl_object_type from object_xref join xref on (object_xref.xref_id=xref.xref_id) join external_db on (xref.external_db_id=external_db.external_db_id) join ensemblgenomes_ontology_26_79.term as t on (t.accession=xref.dbprimary_acc) where external_db.db_name='GO';

## Generates ox_goslim_goa - Translation
#  select distinct t2.name as description_1074, t2.accession as display_label_1074, object_xref.ensembl_id as translation_id_1068_key, t2.accession as dbprimary_acc_1074 from object_xref join xref on (object_xref.xref_id=xref.xref_id) join external_db on (xref.external_db_id=external_db.external_db_id) join ensemblgenomes_ontology_26_79.term as t on (t.accession=xref.dbprimary_acc) join ensemblgenomes_ontology_26_79.closure as c on (t.term_id=c.child_term_id) join ensemblgenomes_ontology_26_79.aux_GO_goslim_aspergillus_map as s on (c.parent_term_id=s.term_id)  join ensembl_ontology_82.term as t2 on (t2.term_id=s.subset_term_id) where external_db.db_name='GO' order by object_xref.ensembl_id;

## Generates ontology_goslim - Translation
#  select distinct ontology_xref.linkage_type as linkage_type_1024, t2.ontology_id as ontology_id_1006, t2.definition as definition_1006, object_xref.ensembl_id as translation_id_1068_key, t2.is_root as is_root_1006, t2.name as name_1006, t2.accession as dbprimary_acc_1074 from object_xref join xref on (object_xref.xref_id=xref.xref_id) join external_db on (xref.external_db_id=external_db.external_db_id) join ontology_xref on (object_xref.object_xref_id=ontology_xref.object_xref_id) join ensemblgenomes_ontology_26_79.term as t on (t.accession=xref.dbprimary_acc) join ensemblgenomes_ontology_26_79.closure as c on (t.term_id=c.child_term_id) join ensemblgenomes_ontology_26_79.aux_GO_goslim_aspergillus_map as s on (c.parent_term_id=s.term_id)  join ensembl_ontology_82.term as t2 on (t2.term_id=s.subset_term_id) where external_db.db_name='GO' order by object_xref.ensembl_id;

use warnings;
use strict;
use DBI;
use Data::Dumper;
use Carp;
use Log::Log4perl qw(:easy);
use FindBin;
use lib "$FindBin::Bin/../modules";
use DbiUtils qw(get_string get_strings);
use MartUtils qw(get_sql_name_for_dataset);
use Cwd;
use File::Copy;
use Getopt::Long;
use Bio::EnsEMBL::Registry;

# db params
my $db_host;
my $db_port;
my $db_user;
my $db_pwd;
my $mart_db;
my $dataset;
my $basename = "gene";
my $verbose = 1;
my $registry;

sub usage {
    print "Usage: $0 [-host <host>] [-port <port>] [-user <user>] [-pass <pwd>] [-mart <mart db>] [-template <template file path>] [-description <description>] [-dataset <dataset name>] [-ds_template <datanase name template>] [-output_dir <output directory>]\n";
    print "-host <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-user <host> Default is $db_user\n";
    print "-pass <password> Default is top secret unless you know cat\n";
    print "-mart <mart db>\n";
    exit 1;
};

my $options_okay = GetOptions (
    "host=s"=>\$db_host,
    "port=i"=>\$db_port,
    "user=s"=>\$db_user,
    "pass=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "registry=s"=>\$registry,
    "dataset=s"=>\$dataset,    
    "name=s"=>\$basename,
    "verbose|v"=>\$verbose,
    "registry:s"=>\$registry,                           
    "h|help"=>sub {usage()}
    );

if(defined $verbose) {
    Log::Log4perl->easy_init($DEBUG);	
} else {
    Log::Log4perl->easy_init($INFO);
}

my $logger = get_logger();

# open a connection
# work out the name of the core
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

# load registry
if(defined $registry) {
    $logger->info("Loading registry from file $registry");
  Bio::EnsEMBL::Registry->load_all($registry);
} else {
    $logger->info("Loading registry from $db_host");
  Bio::EnsEMBL::Registry->load_registry_from_db(
                                                -host       => $db_host,
                                                -user       => $db_user,
                                                -pass       => $db_pwd,
                                                -port       => $db_port);
}

my @datasets;
if(defined $dataset) {
    push @datasets, $dataset;
} else {
    @datasets = get_strings($mart_handle->prepare("SELECT distinct(name) FROM dataset_names"));
}


my $ontology_dba = Bio::EnsEMBL::Registry->get_DBAdaptor("multi","ontology");
my $ontology_db = $ontology_dba->dbc()->dbname();
$logger->info("Using ontology database $ontology_db");

for $dataset (@datasets) {
    my $core_db = get_string($mart_handle->prepare("SELECT src_db FROM dataset_names WHERE name='$dataset'"));

    my $species_name = get_sql_name_for_dataset( $mart_handle, $dataset );
    my $dba =
      Bio::EnsEMBL::Registry->get_DBAdaptor( $species_name, 'core' );

    $logger->info("Processing core database $core_db for dataset $dataset");
    if(!defined $core_db) {
	croak "Could not find core database for dataset $dataset";
    }

    my $level = $dba->dbc()->sql_helper()->execute_single_result(-SQL=>qq/select distinct object_xref.ensembl_object_type from ${core_db}.object_xref join ${core_db}.xref on (object_xref.xref_id=xref.xref_id) join ${core_db}.external_db on (xref.external_db_id=external_db.external_db_id) where external_db.db_name='GO'/);
    
    my $key_column;
    if($level eq "Transcript") {
        $key_column = "transcript_id_1064_key";
    } else {
        $key_column = "translation_id_1068_key";
    }

    $logger->info("Dealing with GO terms on $level (key=$key_column)");

    my $level_type = lc($level);
    
    my $slim="aux_GO_goslim_generic_map";
    my $slim_short="generic";
    if($core_db =~ m/^saccharomyces_cerevisiae_.*/) {
        $slim="aux_GO_goslim_yeast_map";
        $slim_short="yeast";
    } elsif($core_db =~ m/^schizosaccharomyces_pombe_.*/) {
        $slim="aux_GO_goslim_pombe_map";
        $slim_short="pombe";
    } elsif($core_db =~ m/^aspergillus_.*/) {
        $slim="aux_GO_goslim_aspergillus_map";
        $slim_short="aspergillus";
    } elsif($mart_db =~ m/^plants_mart_.*/) {
        $slim="aux_GO_goslim_plant_map";
        $slim_short="plant";
    }

    $logger->info("Processing $slim ($slim_short)");

    $logger->info(" Drop if exist ${dataset}_${basename}__ox_goslim_goa__dm ...");
    $mart_handle->do("drop table if exists ${dataset}_${basename}__ox_goslim_goa__dm");
    $logger->info(" Creating ${dataset}_${basename}__ox_goslim_goa__dm ...");

    $mart_handle->do(qq/CREATE TABLE ${dataset}_${basename}__ox_goslim_goa__dm (
  `description_1074` varchar(255) NOT NULL,
  `display_label_1074` varchar(64) NOT NULL,
  `${key_column}` int(10) unsigned NOT NULL,
  `dbprimary_acc_1074` varchar(64) NOT NULL)/
                    );

    my $sth = $mart_handle->prepare(qq/INSERT INTO  ${dataset}_${basename}__ox_goslim_goa__dm(description_1074, display_label_1074, ${key_column}, dbprimary_acc_1074) VALUES(?,?,?,?)/);
    $dba->dbc()->sql_helper->execute_no_return(-SQL=>qq/select distinct t2.name as description_1074, t2.accession as display_label_1074, object_xref.ensembl_id as ${key_column}, t2.accession as dbprimary_acc_1074 
from ${core_db}.object_xref
join ${core_db}.xref on (object_xref.xref_id=xref.xref_id)
join ${core_db}.external_db on (xref.external_db_id=external_db.external_db_id)
join ${ontology_db}.term as t on (t.accession=xref.dbprimary_acc)
join ${ontology_db}.closure as c on (t.term_id=c.child_term_id)
join ${ontology_db}.${slim} as s on (c.parent_term_id=s.term_id)
join ${ontology_db}.term as t2 on (t2.term_id=s.subset_term_id)
where external_db.db_name='GO' order by object_xref.ensembl_id/,
                                               -CALLBACK=>sub {
                                                 my ($row) = @_;
                                                 $sth->execute(@$row);
                                                 return;
                                               }
                                              );
    $sth->finish();
    
    $logger->info(" Creating indexes on ${dataset}_${basename}__ox_goslim_goa__dm ...");
    $mart_handle->do("alter table ${dataset}_${basename}__ox_goslim_goa__dm add index (dbprimary_acc_1074), add index (${key_column});");

    $logger->info(" Drop if exist ${dataset}_${basename}__ontology_goslim_goa__dm ...");
    $mart_handle->do("drop table if exists ${dataset}_${basename}__ontology_goslim_goa__dm");

    # The ontology goslim goa table is only created for EG species.
    if (${basename} !~ 'gene_ensembl') {
      $logger->info(" Creating ${dataset}_${basename}__ontology_goslim_goa__dm ...");

my $create_sql = qq/
CREATE TABLE `${dataset}_${basename}__ontology_goslim_goa__dm` (
  `linkage_type_1024` varchar(3) DEFAULT NULL,
  `ontology_id_1006` int(10) unsigned NOT NULL,
  `definition_1006` text,
  `${key_column}` int(10) unsigned NOT NULL,
  `is_root_1006` int(11) NOT NULL DEFAULT '0',
  `name_1006` varchar(255) NOT NULL,
  `dbprimary_acc_1074` varchar(64) NOT NULL)
/;
$logger->debug("Executing $create_sql");
    $mart_handle->do($create_sql);

    my $sth = $mart_handle->prepare(qq/INSERT INTO  ${dataset}_${basename}__ontology_goslim_goa__dm() VALUES(?,?,?,?,?,?,?)/);
    $dba->dbc()->sql_helper->execute_no_return(-SQL=>qq/
select distinct ontology_xref.linkage_type as linkage_type_1024, t2.ontology_id as ontology_id_1006, t2.definition as definition_1006, object_xref.ensembl_id as ${key_column}, t2.is_root as is_root_1006, t2.name as name_1006, t2.accession as dbprimary_acc_1074 from ${core_db}.object_xref join ${core_db}.xref on (object_xref.xref_id=xref.xref_id) join ${core_db}.external_db on (xref.external_db_id=external_db.external_db_id) join ${core_db}.ontology_xref on (object_xref.object_xref_id=ontology_xref.object_xref_id) join ${ontology_db}.term as t on (t.accession=xref.dbprimary_acc) join ${ontology_db}.closure as c on (t.term_id=c.child_term_id) join ${ontology_db}.${slim} as s on (c.parent_term_id=s.term_id) join ${ontology_db}.term as t2 on (t2.term_id=s.subset_term_id) where external_db.db_name='GO' order by object_xref.ensembl_id/,
                                               -CALLBACK=>sub {
                                                 my ($row) = @_;
                                                 $sth->execute(@$row);
                                                 return;
                                               }
                                              );
    $sth->finish();

    
      $logger->info(" Creating indexes on ${dataset}_${basename}__ontology_goslim_goa__dm ...");
      $mart_handle->do("alter table ${dataset}_${basename}__ontology_goslim_goa__dm add index (dbprimary_acc_1074), add index (linkage_type_1024), add index (${key_column});");
    }
    if ($level eq 'Transcript') {

        eval {
            $logger->info(" Modifying ${dataset}_${basename}__transcript__main ...");
            $mart_handle->do("alter table ${mart_db}.${dataset}_${basename}__transcript__main add column (ox_goslim_goa_bool integer default 0);");
        };
        if($@) {            
            $logger->info("Ignoring error with existing column index...");
        }
        
        $logger->info(" Updating column ${dataset}_${basename}__transcript__main ...");
        $mart_handle->do("update ${mart_db}.${dataset}_${basename}__transcript__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${dataset}_${basename}__ox_goslim_goa__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null and b.display_label_1074 is null));");
        
        eval {
            $logger->info(" Creating index I_goslim_${dataset} ...");
            $mart_handle->do("create index I_goslim_${dataset} on ${mart_db}.${dataset}_${basename}__transcript__main(ox_goslim_goa_bool);");
        };
        if($@) {            
            $logger->info("Ignoring error with existing column index...");
        }

        eval {
            $logger->info(" Modifying ${dataset}_${basename}__translation__main ...");
            $mart_handle->do("alter table ${mart_db}.${dataset}_${basename}__translation__main add column (ox_goslim_goa_bool integer default 0);");
        };
        if($@) {            
            $logger->info("Ignoring error with existing column index...");
        }

        
        $logger->info(" Updating column ${dataset}_${basename}__translation__main ...");
        $mart_handle->do("update ${mart_db}.${dataset}_${basename}__translation__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${dataset}_${basename}__ox_goslim_goa__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null and b.display_label_1074 is null));");
	eval {
	  $logger->info(" Creating index I_goslim_${dataset} ..." );
	};

	eval {
	  $mart_handle->do("create index I_goslim_${dataset} on ${mart_db}.${dataset}_${basename}__translation__main(ox_goslim_goa_bool);");
	};
    } else {
        $logger->info(" Modifying ${dataset}_${basename}__${level_type}__main ...");
        
        eval {
            $mart_handle->do("alter table ${mart_db}.${dataset}_${basename}__${level_type}__main add column (ox_goslim_goa_bool integer default 0);");
        }; 
        if($@) {
            $logger->info("Ignoring error with existing column...");
        }

        $logger->info(" Updating column ${dataset}_${basename}__${level_type}__main ...");
        $mart_handle->do("update ${mart_db}.${dataset}_${basename}__${level_type}__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${dataset}_${basename}__ox_goslim_goa__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null and b.display_label_1074 is null));");
        
        eval {
            $logger->info(" Creating index I_goslim_${dataset} ..." );
            $mart_handle->do("create index I_goslim_${dataset} on ${mart_db}.${dataset}_${basename}__${level_type}__main(ox_goslim_goa_bool);");
        };
        if($@) {            
            $logger->info("Ignoring error with existing column index...");
        }
    }
    $logger->info("Completed processing $dataset");

    $dba->dbc()->disconnect_if_idle();

}
