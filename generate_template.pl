#!/bin/env perl

# $Source$
# $Revision$
# $Date$
# $Author$
#
# Script to populate meta tables for partitioned marts from a set of crude XML templates

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
my $db_port = '4161';
my $db_user = 'admin';
my $db_pwd = 'iPBi22yI';
#my $db_port = '3306';
#my $db_user = 'eg';
#my $db_pwd = 'eg';
my $mart_db = 'bacterial_mart_52';
my $release = '52';

sub usage {
    print "Usage: $0 [-h <host>] [-P <port>] [-u user <user>] [-p <pwd>] [-src_mart <src>] [-target_mart <targ>]\n";
    print "-h <host> Default is $db_host\n";
    print "-P <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <mart> Default is $mart_db\n";
    print "-release <releaseN> Default is $release\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
    "release=s"=>\$release,
    "mart=s"=>\$mart_db,
    "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

sub write_dataset_xml {
    my $dataset_names = shift;
    my $fname = './output/'.$dataset_names->{dataset}.'.xml';
    open my $dataset_file, '>', $fname or croak "Could not open $fname for writing"; 
    my $template_file_name = 'templates/dataset_template.xml';
    open my $template_file, '<', $template_file_name or croak "Could not open $template_file_name";
    while (my $line = <$template_file>) {
	$line =~ s/%name%/$$dataset_names{dataset}_gene/g;
	$line =~ s/%id%/$$dataset_names{species_id}/g;
	$line =~ s/%des%/$$dataset_names{species_name}/g;
	$line =~ s/%version%/$$dataset_names{version_num}/g;     
	print $dataset_file $line;
    }
    close($template_file);
    close($dataset_file);
    `gzip -c $fname > $fname.gz`;
    `md5sum $fname.gz > $fname.gz.md5`;}

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
        print $output_file $content;
    }
    close($output_file);
    close($template_file);
}

sub get_dataset_element {
    my $dataset = shift;
    my $colstr = '';
    if(defined $dataset->{collection}) {
	$colstr = '/'.$dataset->{collection};
    }
    '<DynamicDataset aliases="mouse_formatter1=,mouse_formatter2=,mouse_formatter3=,species1='.
	${$dataset}{species_name}.
	',species2='.$dataset->{species_uc_name}.
	',species3='.$dataset->{dataset}.
	',species4='.$dataset->{short_name}.
	',collection_path='.$colstr.
	',version='.$dataset->{version_num}.
	',link_version='.$dataset->{dataset}.
	'_'.$release.',default=true" internalName="'.
	$dataset->{dataset}.'_gene"/>'
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

sub get_dataset_homolog_filter {
    my $dataset = shift;
    my $text = << "HOMOFIL_END";
<Option displayName="Orthologous $dataset->{species_name} Genes" displayType="list" field="homolog_$dataset->{baseset}_$dataset->{species_id}_bool" hidden="false" internalName="with_$dataset->{dataset}_homolog" isSelectable="true" key="gene_id_1020_key" legal_qualifiers="only,excluded" qualifier="only" style="radio" tableConstraint="main" type="boolean"><Option displayName="Only" hidden="false" internalName="only" value="only" /><Option displayName="Excluded" hidden="false" internalName="excluded" value="excluded" /></Option>
HOMOFIL_END
    $text;
}

sub get_dataset_paralog_filter {
    my $dataset = shift;
    my $text = << "PARAFIL_END";
<Option displayName="Paralogous $dataset->{species_name} Genes" displayType="list" field="paralog_$dataset->{baseset}_$dataset->{species_id}_bool" hidden="false" internalName="with_$dataset->{dataset}_paralog" isSelectable="true" key="gene_id_1020_key" legal_qualifiers="only,excluded" qualifier="only" style="radio" tableConstraint="main" type="boolean"><Option displayName="Only" hidden="false" internalName="only" isSelectable="true" value="only" /><Option displayName="Excluded" hidden="false" internalName="excluded" isSelectable="true" value="excluded" /></Option>
PARAFIL_END
    $text;
}

sub get_dataset_homolog_attribute {
    my $dataset = shift;
    my $text = << "HOMOATT_END";
    <AttributeGroup displayName="$dataset->{species_name} ORTHOLOGS:" hidden="false" internalName="$dataset->{dataset}_orthologs">
      <AttributeCollection displayName="Ortholog Attributes" hidden="false" internalName="homologs_$dataset->{dataset}">
        <AttributeDescription displayName="$dataset->{species_name} Ensembl Gene ID" field="stable_id_4016_r2" hidden="false" internalName="$dataset->{dataset}_ensembl_gene" key="gene_id_1020_key" linkoutURL="exturl|http://www.ensembl.org/Xenopus_tropicalis/geneview?gene=%s" maxLength="20" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Chromosome" field="chr_name_4016_r2" hidden="false" internalName="$dataset->{dataset}_chromosome" key="gene_id_1020_key" maxLength="9" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Chr Start (bp)" field="chr_start_4016_r2" hidden="false" internalName="$dataset->{dataset}_chrom_start" key="gene_id_1020_key" maxLength="10" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Chr End (bp)" field="chr_end_4016_r2" hidden="false" internalName="$dataset->{dataset}_chrom_end" key="gene_id_1020_key" maxLength="10" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="Orthology Type" field="description_4014" hidden="false" internalName="$dataset->{dataset}_orthology_type" key="gene_id_1020_key" maxLength="15" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="Ensembl Peptide ID" field="stable_id_4016_r1" hidden="false" internalName="$dataset->{dataset}_ensembl_peptide" key="gene_id_1020_key" maxLength="20" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="% Identity" field="perc_id_4015" hidden="false" internalName="$dataset->{dataset}_percent_identity" key="gene_id_1020_key" maxLength="3" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Ensembl Peptide ID" field="stable_id_4016_r3" hidden="false" internalName="$dataset->{dataset}_homolog_ensembl_peptide" key="gene_id_1020_key" maxLength="20" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} % Identity" field="perc_id_4015_r1" hidden="false" internalName="$dataset->{dataset}_homolog_percent_identity" key="gene_id_1020_key" maxLength="3" tableConstraint="homolog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
      </AttributeCollection>
    </AttributeGroup>
HOMOATT_END
   $text;
}

sub get_dataset_paralog_attribute {
    my $dataset = shift;
    my $text = << "PARAATT_END";
      <AttributeCollection displayName="$dataset->{species_name} Paralog Attributes" hidden="false" internalName="paralogs_xtropicalis">
        <AttributeDescription displayName="$dataset->{species_name} Paralog Ensembl Gene ID" field="stable_id_4016_r2" hidden="false" internalName="$dataset->{dataset}_paralog_ensembl_gene" key="gene_id_1020_key" linkoutURL="exturl|http://www.ensembl.org/*species2*/geneview?gene=%s" maxLength="140" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Paralog Chromosome" field="chr_name_4016_r2" hidden="false" internalName="$dataset->{dataset}_paralog_chromosome" key="gene_id_1020_key" maxLength="40" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Paralog Chr Start (bp)" field="chr_start_4016_r2" hidden="false" internalName="$dataset->{dataset}_paralog_chrom_start" key="gene_id_1020_key" maxLength="10" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Paralog Chr End (bp)" field="chr_end_4016_r2" hidden="false" internalName="$dataset->{dataset}_paralog_chrom_end" key="gene_id_1020_key" maxLength="10" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="Ensembl Peptide ID" field="stable_id_4016_r1" hidden="false" internalName="$dataset->{dataset}_paralog_ensembl_peptide" key="gene_id_1020_key" maxLength="40" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="% Coverage" field="perc_cov_4015" hidden="false" internalName="$dataset->{dataset}_paralog_percent_coverage" key="gene_id_1020_key" maxLength="10" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="% Identity" field="perc_id_4015" hidden="false" internalName="$dataset->{dataset}_paralog_percent_identity" key="gene_id_1020_key" maxLength="10" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Paralog Ensembl Peptide ID" field="stable_id_4016_r3" hidden="false" internalName="$dataset->{dataset}_paralog_paralog_ensembl_peptide" key="gene_id_1020_key" maxLength="40" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Paralog % Coverage" field="perc_cov_4015_r1" hidden="false" internalName="$dataset->{dataset}_paralog_paralog_percent_coverage" key="gene_id_1020_key" maxLength="10" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="$dataset->{species_name} Paralog % Identity" field="perc_id_4015_r1" hidden="false" internalName="$dataset->{dataset}_paralog_paralog_percent_identity" key="gene_id_1020_key" maxLength="10" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
        <AttributeDescription displayName="Ancestor" field="subtype_4014" hidden="false" internalName="$dataset->{dataset}_paralog_ancestor" key="gene_id_1020_key" maxLength="24" tableConstraint="paralog_$dataset->{baseset}_$dataset->{species_id}__dm"/>
      </AttributeCollection>
PARAATT_END
    $text;
}


sub write_template_xml {
    my $datasets = shift;
    my $datasets_text='';
    my $homology_filters_text='';
    my $homology_attributes_text='';
    my $paralogy_attributes_text='';
    my $exportables_text='';
    my $exportables_link_text='';
    foreach my $dataset (@$datasets) {
	$datasets_text .= get_dataset_element($dataset)
	    ."\n";
	$exportables_text .= get_dataset_exportable($dataset);
	$exportables_link_text .= get_dataset_exportable_link($dataset);
	$homology_filters_text .= get_dataset_homolog_filter($dataset);
	$homology_filters_text .= get_dataset_paralog_filter($dataset);
	$homology_attributes_text .= get_dataset_homolog_attribute($dataset);
	$paralogy_attributes_text .= get_dataset_paralog_attribute($dataset);
    }
    my %placeholders = (
	'%datasets%'=>$datasets_text,
	'%homology_filters%'=>$homology_filters_text,
	'%homology_attributes%'=>$homology_attributes_text,
	'%paralogy_attributes%'=>$paralogy_attributes_text,
	'%exportables%'=>$exportables_text,
	'%exportables_link%'=>$exportables_link_text
	);
    write_replace_file('templates/template_template.xml','output/template.xml',\%placeholders);
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

    $logger->info("Creating meta tables");

    # create tables
    create_metatable($mart_handle,'meta_version__version__main',
		     ['version varchar(10) default NULL']);
    create_metatable($mart_handle,'meta_template__xml__dm',
		     ['template varchar(100) default NULL',
		      'compressed_xml longblob',
		      'UNIQUE KEY template (template)']);
    create_metatable($mart_handle,'meta_conf__xml__dm',
		     ['dataset_id_key int(11) NOT NULL',
		      'xml longblob',
		      'compressed_xml longblob',
		      'message_digest blob',
		      'UNIQUE KEY dataset_id_key (dataset_id_key)']);
    create_metatable($mart_handle,'meta_conf__user__dm',
		     ['dataset_id_key int(11) default NULL',
		      'mart_user varchar(100) default NULL',
		      'UNIQUE KEY dataset_id_key (dataset_id_key,mart_user)']);
    create_metatable($mart_handle,'meta_conf__interface__dm',
		     ['dataset_id_key int(11) default NULL',
		      'interface varchar(100) default NULL',
		      'UNIQUE KEY dataset_id_key (dataset_id_key,interface)']);
    create_metatable($mart_handle,'meta_template__template__main',
		     ['dataset_id_key int(11) NOT NULL',
		      'template varchar(100) NOT NULL']);
    create_metatable($mart_handle,'meta_conf__dataset__main',[ 
			 'dataset_id_key int(11) NOT NULL',
			 'dataset varchar(100) default NULL',
			 'display_name varchar(200) default NULL',
			 'description varchar(200) default NULL',
			 'type varchar(20) default NULL',
			 'visible int(1) unsigned default NULL',
			 'version varchar(25) default NULL',
			 'modified timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP',
			 'UNIQUE KEY dataset_id_key (dataset_id_key)']);

    $logger->info("Populating template tables");
    # populate template tables
    ## meta_version__version__main
    $mart_handle->do("INSERT INTO meta_version__version__main VALUES ('0.6')");
    ## meta_template__xml__dm
    my $dataset_name = 'gene';
    my $sth = $mart_handle->prepare('INSERT INTO meta_template__xml__dm VALUES (?,?)');
    $sth->execute($dataset_name, file_to_bytes("$pwd/output/template.xml.gz")) 
		  or croak "Could not load file into meta_template__xml__dm";
    $sth->finish();
 
    $logger->info("Populating dataset tables");
    my $meta_conf__xml__dm = $mart_handle->prepare('INSERT INTO meta_conf__xml__dm VALUES (?,?,?,?)');
    my $meta_conf__user__dm = $mart_handle->prepare('INSERT INTO meta_conf__user__dm VALUES(?,\'default\')');
    my $meta_conf__interface__dm = $mart_handle->prepare('INSERT INTO meta_conf__interface__dm VALUES(?,\'default\')');
    my $meta_conf__dataset__main = $mart_handle->prepare("INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,'Ensembl Genes','TableSet',1,?)");
    my $meta_template__template__main = $mart_handle->prepare('INSERT INTO meta_template__template__main VALUES(?,?)');
    # populate dataset tables
    foreach my $dataset (@$datasets) { 
	# meta_conf__xml__dm
	$logger->info("Writing metadata for species ".$dataset->{species_id});
	$meta_conf__xml__dm->execute($dataset->{species_id},
				     file_to_bytes("$pwd/output/$dataset->{dataset}.xml"),
				     file_to_bytes("$pwd/output/$dataset->{dataset}.xml.gz"),
				     file_to_bytes("$pwd/output/$dataset->{dataset}.xml.gz.md5")
	    ) or croak "Could not update meta_conf__xml__dm";
	# meta_conf__user__dm
	$meta_conf__user__dm->execute($dataset->{species_id}) 
	    or croak "Could not update meta_conf__user__dm";
	# meta_conf__interface__dm
	$meta_conf__interface__dm->execute($dataset->{species_id})  
	    or croak "Could not update meta_conf__interface__dm";
	# meta_conf__dataset__main 
	$meta_conf__dataset__main->execute(
	    $dataset->{species_id},
	    "$dataset->{dataset}_gene",
	    "$dataset->{species_name} genes ($dataset->{version_num})",
	    $dataset->{version_num}) or croak "Could not update meta_conf__dataset__main";
	# meta_template__template__main
	$meta_template__template__main->execute($dataset->{species_id},$dataset_name)  
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
    uc($species_id);
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
my $dataset_sth = $mart_handle->prepare('SELECT src_dataset,src_db,species_id,species_name,version,collection FROM dataset_names WHERE name=?');

# get names of datasets from names table
foreach my $dataset (get_dataset_names($mart_handle)) {
    $logger->info("Processing $dataset");
    # get other naming info from names table
    my %dataset_names = ();
    $dataset_names{dataset}=$dataset;
    ($dataset_names{baseset}, $dataset_names{src_db},$dataset_names{species_id},$dataset_names{species_name},$dataset_names{version_num},$dataset_names{collection}) = get_row($dataset_sth,$dataset);
    $dataset_names{species_uc_name} = $dataset_names{species_name};
    $dataset_names{species_uc_name} =~ s/\s+/_/g;
    $dataset_names{short_name} = get_short_name($dataset_names{species_name},$dataset_names{species_id});
    $logger->debug(join(',',values(%dataset_names)));
    push(@datasets,\%dataset_names);
    write_dataset_xml(\%dataset_names)
}
$dataset_sth->finish();

# 2. write template files
write_template_xml(\@datasets);

write_metatables($mart_handle, \@datasets);

$mart_handle->disconnect() or croak "Could not close handle to $mart_string";

