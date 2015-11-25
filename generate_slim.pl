#!/bin/env perl
# Copyright [2009-2015] EMBL-European Bioinformatics Institute
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
use DbiUtils;
use MartUtils;
use Cwd;
use File::Copy;
use Getopt::Long;

# db params
my $db_host = '127.0.0.1';
my $db_port = 4238;
my $db_user = 'ensrw';
my $db_pwd = 'writ3rp1';
my $mart_db;
my $dataset;
my $basename = "gene";
my $verbose;

sub usage {
    print "Usage: $0 [-host <host>] [-port <port>] [-user <user>] [-pass <pwd>] [-mart <mart db>] [-release <e! release number>] [-template <template file path>] [-description <description>] [-dataset <dataset name>] [-ds_template <datanase name template>] [-output_dir <output directory>]\n";
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
    "dataset=s"=>\$dataset,    
    "name=s"=>\$basename,
    "verbose|v"=>\$verbose,
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

my @datasets = ();
if(defined $dataset) {
    push @datasets, $dataset;
} else {
    @datasets = get_strings($mart_handle->prepare("SELECT distinct(name) FROM dataset_names"));
}

my ($ontology_db) = grep {m/_ontology_[0-9]+_[0-9]+/} get_strings($mart_handle->prepare("SHOW DATABASES LIKE '%ontology%'"));
$logger->info("Using ontology database $ontology_db");

for $dataset (@datasets) {
    my $core_db = get_string($mart_handle->prepare("SELECT src_db FROM dataset_names WHERE name='$dataset'"));
    $logger->info("Processing core database $core_db for dataset $dataset");
    if(!defined $core_db) {
	croak "Could not find core database for dataset $dataset";
    }

    my ($level) = get_strings($mart_handle->prepare("select distinct object_xref.ensembl_object_type from ${core_db}.object_xref join ${core_db}.xref on (object_xref.xref_id=xref.xref_id) join ${core_db}.external_db on (xref.external_db_id=external_db.external_db_id) where external_db.db_name='GO';"));
    
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
    
#    if($mart_db =~ m/ensembl_mart_.*/) {  
#  if [ $division == "ensembl" ]
#  then 
#    echo "  Drop if exist ${short_name}_gene_ensembl__ox_goslim_goa__dm ..." 
#    mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "drop table if exists ${short_name}_gene_ensembl__ox_goslim_goa__dm;"
#    echo "  Creating ${short_name}_gene_ensembl__ox_goslim_goa__dm ..."
#    mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "create table ${short_name}_gene_ensembl__ox_goslim_goa__dm select distinct t2.name as description_1074, object_xref.ensembl_id as ${key_column[$level]}, t2.accession as dbprimary_acc_1074 from ${db}.object_xref join ${db}.xref on (object_xref.xref_id=xref.xref_id) join ${db}.external_db on (xref.external_db_id=external_db.external_db_id) join ${ontology_db}.term as t on (t.accession=xref.dbprimary_acc) join ${ontology_db}.closure as c on (t.term_id=c.child_term_id) join ${ontology_db}.${slim} as s on (c.parent_term_id=s.term_id) join ensembl_ontology_82.term as t2 on (t2.term_id=s.subset_term_id) where external_db.db_name='GO' order by object_xref.ensembl_id;"
# 
#    echo "  Creating indexes on ${short_name}_gene_ensembl__ox_goslim_goa__dm ..."
#    mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "alter table ${short_name}_gene_ensembl__ox_goslim_goa__dm add index (dbprimary_acc_1074), add index (${key_column});"
# 
#    if [ ${key_column} == "transcript_id_1064_key" ]
#    then
#      echo "  Modifying ${short_name}_gene_ensembl__transcript__main ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "alter table ${mart_db}.${short_name}_gene_ensembl__transcript__main add column (ox_goslim_goa_bool integer default 0);"
#
#      echo "  Updating column ${short_name}_gene_ensembl__transcript__main ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "update ${mart_db}.${short_name}_gene_ensembl__transcript__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${short_name}_gene_ensembl__ox_goslim_goa__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null));"
#
#      levelType=$(echo $level | tr '[:upper:]' '[:lower:]')
#      echo "  Creating index I_goslim_${short_name} ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "create index I_goslim_${short_name} on ${mart_db}.${short_name}_gene_ensembl__transcript__main(ox_goslim_goa_bool);"
#
#      echo "  Modifying ${short_name}_gene_ensembl__translation__main ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "alter table ${mart_db}.${short_name}_gene_ensembl__translation__main add column (ox_goslim_goa_bool integer default 0);"
#
#      echo "  Updating column ${short_name}_gene_ensembl__translation__main ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "update ${mart_db}.${short_name}_gene_ensembl__translation__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${short_name}_gene_ensembl__ox_goslim_goa__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null));"
#
#      levelType=$(echo $level | tr '[:upper:]' '[:lower:]')
#      echo "  Creating index I_goslim_${short_name} ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "create index I_goslim_${short_name} on ${mart_db}.${short_name}_gene_ensembl__translation__main(ox_goslim_goa_bool);"
#    elif [ ${key_column} == "translation_id_1068_key" ]
#    then
#      echo "  Modifying ${short_name}_gene_ensembl__translation__main ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "alter table ${mart_db}.${short_name}_gene_ensembl__translation__main add column (ox_goslim_goa_bool integer default 0);"
#  
#      echo "  Updating column ${short_name}_gene_ensembl__translation__main ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "update ${mart_db}.${short_name}_gene_ensembl__translation__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${short_name}_gene_ensembl__ox_goslim_goa__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null));"
#  
#      levelType=$(echo $level | tr '[:upper:]' '[:lower:]')
#      echo "  Creating index I_goslim_${short_name} ..."
#      mysql -u$DBUSER -p$DBPASS -h$DBHOST -P$DBPORT ${mart_db} -s -N -e "create index I_goslim_${short_name} on ${mart_db}.${short_name}_gene_ensembl__translation__main(ox_goslim_goa_bool);"
#     fi
#        croak "Ensembl mart is not currently supported";
#    } else {
    
    $logger->info(" Drop if exist ${dataset}_${basename}__ox_goslim_goa__dm ...");
    $mart_handle->do("drop table if exists ${dataset}_${basename}__ox_goslim_goa__dm");
    $logger->info(" Creating ${dataset}_${basename}__ox_goslim_goa__dm ...");
    $mart_handle->do("create table ${dataset}_${basename}__ox_goslim_goa__dm select distinct t2.name as description_1074, t2.accession as display_label_1074, object_xref.ensembl_id as ${key_column}, t2.accession as dbprimary_acc_1074 from ${core_db}.object_xref join ${core_db}.xref on (object_xref.xref_id=xref.xref_id) join ${core_db}.external_db on (xref.external_db_id=external_db.external_db_id) join ${ontology_db}.term as t on (t.accession=xref.dbprimary_acc) join ${ontology_db}.closure as c on (t.term_id=c.child_term_id) join ${ontology_db}.${slim} as s on (c.parent_term_id=s.term_id) join ${ontology_db}.term as t2 on (t2.term_id=s.subset_term_id) where external_db.db_name='GO' order by object_xref.ensembl_id;");
    $logger->info(" Creating indexes on ${dataset}_${basename}__ox_goslim_goa__dm ...");
    $mart_handle->do("alter table ${dataset}_${basename}__ox_goslim_goa__dm add index (dbprimary_acc_1074), add index (${key_column});");
    
    $logger->info(" Drop if exist ${dataset}_${basename}__ontology_goslim_goa__dm ...");
    $mart_handle->do("drop table if exists ${dataset}_${basename}__ontology_goslim_goa__dm");
    
    $logger->info(" Creating ${dataset}_${basename}__ontology_goslim_goa__dm ...");
    $mart_handle->do("create table ${dataset}_${basename}__ontology_goslim_goa__dm select distinct ontology_xref.linkage_type as linkage_type_1024, t2.ontology_id as ontology_id_1006, t2.definition as definition_1006, object_xref.ensembl_id as ${key_column}, t2.is_root as is_root_1006, t2.name as name_1006, t2.accession as dbprimary_acc_1074 from ${core_db}.object_xref join ${core_db}.xref on (object_xref.xref_id=xref.xref_id) join ${core_db}.external_db on (xref.external_db_id=external_db.external_db_id) join ${core_db}.ontology_xref on (object_xref.object_xref_id=ontology_xref.object_xref_id) join ${ontology_db}.term as t on (t.accession=xref.dbprimary_acc) join ${ontology_db}.closure as c on (t.term_id=c.child_term_id) join ${ontology_db}.${slim} as s on (c.parent_term_id=s.term_id) join ${ontology_db}.term as t2 on (t2.term_id=s.subset_term_id) where external_db.db_name='GO' order by object_xref.ensembl_id;");
    
    $logger->info(" Creating indexes on ${dataset}_${basename}__ontology_goslim_goa__dm ...");
    $mart_handle->do("alter table ${dataset}_${basename}__ontology_goslim_goa__dm add index (dbprimary_acc_1074), add index (linkage_type_1024), add index (${key_column});");
    
    if ($level eq 'Transcript') {
        
        $logger->info(" Modifying ${dataset}_${basename}__transcript__main ...");
        $mart_handle->do("alter table ${mart_db}.${dataset}_${basename}__transcript__main add column (ox_goslim_goa_bool integer default 0);");
        
        $logger->info(" Updating column ${dataset}_${basename}__transcript__main ...");
        $mart_handle->do("update ${mart_db}.${dataset}_${basename}__transcript__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${dataset}_${basename}__ox_go__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null and b.display_label_1074 is null));");
        
        $logger->info(" Creating index I_goslim_${dataset} ...");
        $mart_handle->do("create index I_goslim_${dataset} on ${mart_db}.${dataset}_${basename}__transcript__main(ox_goslim_goa_bool);");
        
        $logger->info(" Modifying ${dataset}_${basename}__translation__main ...");
        $mart_handle->do("alter table ${mart_db}.${dataset}_${basename}__translation__main add column (ox_goslim_goa_bool integer default 0);");
        
        $logger->info(" Updating column ${dataset}_${basename}__translation__main ...");
        $mart_handle->do("update ${mart_db}.${dataset}_${basename}__translation__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${dataset}_${basename}__ox_go__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null and b.display_label_1074 is null));");
        
        $logger->info(" Creating index I_goslim_${dataset} ..." );
        $mart_handle->do("create index I_goslim_${dataset} on ${mart_db}.${dataset}_${basename}__translation__main(ox_goslim_goa_bool);");
    } else {
        $logger->info(" Modifying ${dataset}_${basename}__${level_type}__main ...");
        $mart_handle->do("alter table ${mart_db}.${dataset}_${basename}__${level_type}__main add column (ox_goslim_goa_bool integer default 0);");
        
        $logger->info(" Updating column ${dataset}_${basename}__${level_type}__main ...");
        $mart_handle->do("update ${mart_db}.${dataset}_${basename}__${level_type}__main a set ox_goslim_goa_bool=(select case count(1) when 0 then null else 1 end from ${mart_db}.${dataset}_${basename}__ox_go__dm b where a.${key_column}=b.${key_column} and not (b.description_1074 is null and b.dbprimary_acc_1074 is null and b.display_label_1074 is null));");
        
        $logger->info(" Creating index I_goslim_${dataset} ..." );
        $mart_handle->do("create index I_goslim_${dataset} on ${mart_db}.${dataset}_${basename}__${level_type}__main(ox_goslim_goa_bool);");
    }
}
