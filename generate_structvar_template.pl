#!/bin/env perl

# $Source$
# $Revision$
# $Date$
# $Author$
#
# Script to populate meta tables (without create table statements) for partitioned structvar mart datasets

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
my $db_port = 4238;
my $db_user = 'ensrw';
my $db_pwd = 'writ3rp1';
my $mart_db;
my $release;
my $template_template_file = "templates/structvar_template_template.xml";
my $ds_name = 'structvar';
my $template_file_name = 'templates/dataset_structvar_template.xml';
my $description = 'structural variations';
sub usage {
    print "Usage: $0 [-host <host>] [-port <port>] [-user <user>] [-pass <pwd>] [-mart <mart db>] [-release <e! release number>] [-template <template file path>] [-description <description>] [-dataset <dataset name>] [-ds_template <datanase name template>]\n";
    print "-host <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-user <host> Default is $db_user\n";
    print "-pass <password> Default is top secret unless you know cat\n";
    print "-mart <mart db>\n";
    print "-template <template file path>\n";
    print "-ds_template <ds template>\n";
    print "-dataset <dataset name>\n";
    print "-description <description>\n";
    print "-release <e! releaseN>\n";
    exit 1;
};

my $options_okay = GetOptions (
    "host=s"=>\$db_host,
    "port=i"=>\$db_port,
    "user=s"=>\$db_user,
    "pass=s"=>\$db_pwd,
    "release=i"=>\$release,
    "mart=s"=>\$mart_db,
    "dataset=s"=>\$ds_name,
    "template=s"=>\$template_template_file,
    "ds_template=s"=>\$template_file_name,
    "description=s"=>\$description,
    "h|help"=>sub {usage()}
    );

print STDERR "pass: $db_pwd, mart_db, $mart_db, template_template_file, $template_template_file\n";

if(! defined $db_host || ! defined $db_port || ! defined $db_pwd || ! defined $template_template_file || ! defined $mart_db || !defined $release) {
    print STDERR "Missing arguments\n";
    usage();
}

sub write_dataset_xml {
    my $dataset_names = shift;
    my $fname = './output/'.$dataset_names->{dataset}.'.xml';
    open my $dataset_file, '>', $fname or croak "Could not open $fname for writing"; 
    open my $template_file, '<', $template_file_name or croak "Could not open $template_file_name";
    while (my $line = <$template_file>) {
	$line =~ s/%name%/$$dataset_names{dataset}_${ds_name}/g;
	$line =~ s/%id%/$$dataset_names{species_id}/g;
	$line =~ s/%des%/$$dataset_names{species_name}/g;
	$line =~ s/%version%/$$dataset_names{version_num}/g;
	print $dataset_file $line;
    }
    close($template_file);
    close($dataset_file);
    `gzip -c $fname > $fname.gz`;
    `md5sum $fname.gz > $fname.gz.md5`;
}

sub write_replace_file {
    my ($template,$output,$placeholders) = @_;
    open my $output_file, '>', $output or croak "Could not open $output";
    open my $template_file, '<', $template or croak "Could not open $template";
    while( my $content = <$template_file>) {
	foreach my $placeholder (keys(%$placeholders)) {
	    my $contents = $placeholders->{$placeholder};
	    if ($content =~ m/$placeholder/) {
		$content =~ s/$placeholder/$contents/;
	    }
	}
	if($content =~ m/(.*tableConstraint=")([^"]+)(".*)/s) {
	    $content = $1 . lc($2) . $3;
	}	    
        print $output_file $content;
    }
    close($output_file);
    close($template_file);
}

sub get_dataset_element {
    my $dataset = shift;

    '<DynamicDataset aliases="mouse_formatter1=,mouse_formatter2=,mouse_formatter3=,species1='.
	$dataset->{species_name}.
	',species2='.$dataset->{species_uc_name}.
	',species3='.$dataset->{dataset}.
	',species4='.$dataset->{short_name}.
	',collection_path='.$dataset->{colstr}.
	',version='.$dataset->{version_num}.
#	',tax_id='.$dataset->{tax_id}.
	',link_version='.$dataset->{dataset}.
	'_'.$release.',default=true" internalName="'.
	$dataset->{dataset}.'_'.$ds_name.'"/>'
}

sub get_dataset_exportable {
    my $dataset = shift;
    my $text = << "EXP_END";
    <Exportable attributes="$dataset->{dataset}_gene" 
	default="1" internalName="$dataset->{dataset}_gene_stable_id" 
	linkName="$dataset->{dataset}_gene_stable_id" 
	name="$dataset->{dataset}_gene_stable_id" type="link"/>
EXP_END
    $text;
}

sub get_dataset_exportable_link {
    my $dataset = shift;
    my $text = << "EXPL_END";
        <AttributeDescription field="homol_id" hideDisplay="true" internalName="$dataset->{dataset}_gene_id" key="gene_id_1020_key" maxLength="128" tableConstraint="homologs_$dataset->{dataset}__dm"/>
EXPL_END
    $text;
}


sub write_template_xml {
    my $datasets = shift;
    my $datasets_text='';
    my $exportables_text='';
    my $exportables_link_text='';
    my $poly_attrs_text = '';
    foreach my $dataset (@$datasets) {
	print "Generating elems for ".$dataset->{dataset};
	print Dumper($dataset);
	$datasets_text .= get_dataset_element($dataset);
	$exportables_text .= get_dataset_exportable($dataset);
	$exportables_link_text .= get_dataset_exportable_link($dataset);
   }
    my %placeholders = (
	'.*<Replace id="datasets"/>'=>$datasets_text,
	'.*<Replace id="exportables"/>'=>$exportables_text,
	'.*<Replace id="exportables_link"/>'=>$exportables_link_text,
	'.*<Replace id="poly_attrs"/>'=>$poly_attrs_text
	);
    write_replace_file($template_template_file,'output/template.xml',\%placeholders);
    `gzip -c output/template.xml > output/template.xml.gz`;
}


sub update_meta_file {
    my ($template,$output,$placeholder,$prefix,$suffix,$separator,$datasets,$ds_closure) = @_;  
    my $datasets_text=$prefix;  
    my $first=0;
    foreach my $dataset (@$datasets) {
	if($first>0) {
	    $datasets_text .= $separator;
	}
	my $dst = &$ds_closure($dataset);
	$datasets_text .= $dst;
	$first++;
    }
    $datasets_text.=$suffix;
    write_replace_file($template,$output,$placeholder,$datasets_text);
}

my $table_args ="ENGINE=MyISAM DEFAULT CHARSET=latin1";
sub create_metatable {
    my ($db_handle,$table_name,$cols) = @_[0,1,2];
    drop_and_create_table($db_handle,$table_name,$cols,$table_args);
}

sub write_metatables {
    my ($mart_handle, $datasets) = @_[0,1];
    my $pwd = &Cwd::cwd();

    $logger->info("Populating template tables");
    $logger->info("ds_name, $ds_name");

    # populate template tables

    ## meta_template__xml__dm
    my $sth = $mart_handle->prepare('INSERT INTO meta_template__xml__dm VALUES (?,?)');
    $sth->execute($ds_name, file_to_bytes("$pwd/output/template.xml.gz")) 
		  or croak "Could not load file into meta_template__xml__dm";
    $sth->finish();
 
    $logger->info("Populating dataset tables");
    my $meta_conf__xml__dm = $mart_handle->prepare('INSERT INTO meta_conf__xml__dm VALUES (?,?,?,?)');
    my $meta_conf__user__dm = $mart_handle->prepare('INSERT INTO meta_conf__user__dm VALUES(?,\'default\')');
    my $meta_conf__interface__dm = $mart_handle->prepare('INSERT INTO meta_conf__interface__dm VALUES(?,\'default\')');
    my $meta_conf__dataset__main = $mart_handle->prepare("INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,'Ensembl $description','TableSet',1,?)");
    my $meta_template__template__main = $mart_handle->prepare('INSERT INTO meta_template__template__main VALUES(?,?)');

    # populate dataset tables

    foreach my $dataset (@$datasets) { 

	#my $speciesId = $dataset->{species_id};
	#my $dataset_id = $speciesId;
	
        # We can't use the speciesId as a datasetId as it is already taken, so set it up differently
        
        my $get_next_value_sql = "SELECT max(dataset_id_key) FROM meta_conf__dataset__main";
        my $get_next_value_sth = $mart_handle->prepare($get_next_value_sql);
        $get_next_value_sth->execute();
        my ($datasetId) = $get_next_value_sth->fetchrow_array();
        $datasetId++;
	$get_next_value_sth->finish();
	
	$logger->info("using $datasetId as a dataset_id");
	
	# meta_conf__xml__dm

	$logger->info("Writing metadata for species ".$dataset->{species_name});
	$meta_conf__xml__dm->execute($datasetId,
				     file_to_bytes("$pwd/output/$dataset->{dataset}.xml"),
				     file_to_bytes("$pwd/output/$dataset->{dataset}.xml.gz"),
				     file_to_bytes("$pwd/output/$dataset->{dataset}.xml.gz.md5")
	    ) or croak "Could not update meta_conf__xml__dm";
	# meta_conf__user__dm
	$meta_conf__user__dm->execute($datasetId) 
	    or croak "Could not update meta_conf__user__dm";
	# meta_conf__interface__dm
	$meta_conf__interface__dm->execute($datasetId)  
	    or croak "Could not update meta_conf__interface__dm";
	# meta_conf__dataset__main 
	print Dumper($dataset);
	$meta_conf__dataset__main->execute(
	    $datasetId,
	    "$dataset->{dataset}_$ds_name",
	    "$dataset->{species_name} $description ($dataset->{version_num})",
	    $dataset->{version_num}) or croak "Could not update meta_conf__dataset__main";
	# meta_template__template__main
	$meta_template__template__main->execute($datasetId,$ds_name)  
	    or croak "Could not update meta_template__template__dm";
    }
    $meta_conf__xml__dm->finish();
    $meta_conf__user__dm->finish();
    $meta_conf__interface__dm->finish();
    $meta_conf__dataset__main->finish();
    $meta_template__template__main->finish();
    
    $logger->info("Population complete");
}

sub get_short_name {
    my ($db_name,$species_id) = @_;
    return uc($species_id);
} 

sub get_version {
    my $ens_db = shift;
    $ens_db =~ m/^.*_([0-9]+[a-z]*)$/;
    $1;
}

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

my @datasets = ();
my $dataset_sth = $mart_handle->prepare('SELECT src_dataset,src_db,species_id,species_name,version,collection,sql_name FROM dataset_names WHERE name=?');

# get names of datasets from names table
my $i=0;
foreach my $dataset (get_dataset_names($mart_handle)) {
    $logger->info("Processing $dataset");
    # get other naming info from names table
    my %dataset_names = ();
    $dataset_names{dataset}=$dataset;
    ($dataset_names{baseset}, $dataset_names{src_db},$dataset_names{species_id},$dataset_names{species_name},$dataset_names{version_num},$dataset_names{collection},$dataset_names{species_uc_name}) = get_row($dataset_sth,$dataset);
    if(!$dataset_names{species_id}) {
	$dataset_names{species_id} = ++$i;
    }
    if(!$dataset_names{species_uc_name}) {
	$dataset_names{species_uc_name} = $dataset_names{species_name};
	$dataset_names{species_uc_name} =~ s/\s+/_/g;
    }
    $dataset_names{short_name} = get_short_name($dataset_names{species_name},$dataset_names{species_id});
    $dataset_names{colstr} = '';
    if(defined $dataset_names{collection}) {
	$dataset_names{colstr} = '/'.$dataset_names{collection};
    }
    #$logger->debug(join(',',values(%dataset_names)));
    push(@datasets,\%dataset_names);
    write_dataset_xml(\%dataset_names)
}
$dataset_sth->finish();

@datasets = sort {$a->{species_name} cmp $b->{species_name}} @datasets;

# 2. write template files
write_template_xml(\@datasets);


write_metatables($mart_handle, \@datasets);

$mart_handle->disconnect() or croak "Could not close handle to $mart_string";

