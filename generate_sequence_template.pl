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
my $release = 51;

sub write_dataset_xml {
    my $dataset_names = shift;
    my $fname = './output/'.$dataset_names->{dataset}.'.xml';
    open my $dataset_file, '>', $fname or croak "Could not open $fname for writing"; 
    my $template_file_name = 'templates/dataset_sequence_template.xml';
    open my $template_file, '<', $template_file_name or croak "Could not open $template_file_name";
    while (my $line = <$template_file>) {
	$line =~ s/%name%/$$dataset_names{dataset}/g;
	$line =~ s/%id%/$$dataset_names{species_id}/g;
	$line =~ s/%des%/$$dataset_names{species_name}/g;
	$line =~ s/%version%/$$dataset_names{version_num}/g;
	$line =~ s/%species%/$$dataset_names{short_species_name}/g;
	$line =~ s/%release%/$release/g;
	print $dataset_file $line;
    }
    close($template_file);
    close($dataset_file);
    `gzip -c $fname > $fname.gz`;
    `md5sum $fname.gz > $fname.gz.md5`;}

sub write_replace_file {
    my ($template,$output,$datasets) = @_[0,1,2,3];
    open my $output_file, '>', $output or croak "Could not open $output";
    open my $template_file, '<', $template or croak "Could not open $template";
    while( my $line = <$template_file>) {
	$line =~ s/%name%/$datasets->{dataset}/g;
	$line =~ s/%id%/$datasets->{species_id}/g;
	$line =~ s/%des%/$datasets->{species_name}/g;
	#$line =~ s/%version%/$$dataset_names{version_num}/g;   
	$line =~ s/%species%/$datasets->{short_species_name}/g;
	$line =~ s/%release%/$release/g;

        print $output_file $line;
    }
    close($output_file);
    close($template_file);
}


sub write_template_xml {
    my $datasets = shift;
    my $datasets_text='';
    foreach my $dataset (@$datasets) {
	$datasets_text = $datasets_text . 
	    '<DynamicDataset internalName="'.
	    $dataset->{dataset}.'"/>'."\n";
	my $template_filename = $dataset->{template};
	write_replace_file('templates/sequence_template_template.xml',"output/$template_filename",$dataset);
	`gzip -c output/$template_filename > output/$template_filename.gz`;
    }
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
    my ($mart_handle, $datasets_aref) = @_[0,1];
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
    $mart_handle->do("INSERT INTO meta_version__version__main VALUES ('0.7')");
    ## meta_template__xml__dm

    my $meta_conf__xml__dm       = $mart_handle->prepare('INSERT INTO meta_conf__xml__dm VALUES (?,?,?,?)');
    my $meta_conf__user__dm      = $mart_handle->prepare('INSERT INTO meta_conf__user__dm VALUES(?,\'default\')');
    my $meta_conf__interface__dm = $mart_handle->prepare('INSERT INTO meta_conf__interface__dm VALUES(?,\'default\')');
    my $meta_conf__dataset__main = $mart_handle->prepare("INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,'Ensembl Sequences','GenomicSequence',0,?)");
    my $meta_template__template__main = $mart_handle->prepare('INSERT INTO meta_template__template__main VALUES(?,?)');
    
    foreach my $dataset_href (@$datasets_aref) {
	
	my $template_filename = $dataset_href->{template};
	my $dataset_name      = $dataset_href->{dataset};

	my $sth = $mart_handle->prepare('INSERT INTO meta_template__xml__dm VALUES (?,?)');
	$sth->execute($dataset_href->{dataset}, file_to_bytes("$pwd/output/" . $template_filename)) 
	    or croak "Could not load template file,$template_filename, into meta_template__xml__dm";
	$sth->finish();
 
	$logger->info("Populating dataset tables");
	
	# meta_conf__xml__dm
	$meta_conf__xml__dm->execute($dataset_href->{species_id},
				     file_to_bytes("$pwd/output/$dataset_href->{dataset}.xml"),
				     file_to_bytes("$pwd/output/$dataset_href->{dataset}.xml.gz"),
				     file_to_bytes("$pwd/output/$dataset_href->{dataset}.xml.gz.md5")
				     ) or croak "Could not update meta_conf__xml__dm";
	# meta_conf__user__dm
	$meta_conf__user__dm->execute($dataset_href->{species_id}) 
	    or croak "Could not update meta_conf__user__dm";

	# meta_conf__interface__dm
	$meta_conf__interface__dm->execute($dataset_href->{species_id})  
	    or croak "Could not update meta_conf__interface__dm";

	# meta_conf__dataset__main 
	$meta_conf__dataset__main->execute(
					   $dataset_href->{species_id},
					   "$dataset_href->{dataset}",
					   "$dataset_href->{species_name} sequences ($dataset_href->{version_num})",
					   $dataset_href->{version_num}) or croak "Could not update meta_conf__dataset__main";

	# meta_template__template__main
	$meta_template__template__main->execute($dataset_href->{species_id},$dataset_name)  
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
    uc(substr($db_name,0,3).$species_id);
} 

sub get_version {
    my $ens_db = shift;
    $ens_db =~ m/^.*_([0-9]+[a-z]*)$/;
    $1;
}

# db params
#my $db_host = 'mysql-eg-devel-1.ebi.ac.uk';
#my $db_port = '4126';
#my $db_user = 'admin';
#my $db_pwd = 'tGc3Vs2O';
my $db_host = 'mysql-eg-staging-1.ebi.ac.uk';
my $db_port = '4160';
my $db_user = 'admin';
my $db_pwd = '6KSFrax4';

my $mart_db = 'ensembl_bacterial_sequence_mart';

sub usage {
    print "Usage: $0 [-h <host>] [-P <port>] [-u user <user>] [-p <pwd>] [-mart <target mart>] [-rel <release number>]\n";
    print "-h <host> Default is $db_host\n";
    print "-P <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <mart> Default is $mart_db\n";
    print "-rel <mart release number> Default is $release\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "rel=s"=>\$release,
    "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

my @mart_tables = get_tables($mart_handle);
my @mart_dbs = get_databases($mart_handle);

my @datasets = ();
foreach my $dataset (get_sequence_datasets(\@mart_tables)) {
    $logger->info("Processing $dataset");
    my %dataset_names = ();
    my $template_filename = $dataset . "_genomic_sequence_template.template.xml";
    $dataset_names{dataset}=$dataset . "_genomic_sequence";
    $dataset_names{template}=$template_filename; 
    $dataset_names{short_species_name}=$dataset;

    $logger->info("dataset name: $dataset");
    $logger->info("template filename, $template_filename");
    
    ($dataset_names{baseset},$dataset_names{species_id}) = ($dataset =~ /^(.+)_([0-9]+)$/) [0,1];
    my $ens_db = get_ensembl_db(\@mart_dbs,$dataset_names{baseset});
    if(!$ens_db) {
	croak "Could not find original source db for dataset $dataset_names{baseset}\n";
    }
    my $species_sth = $mart_handle->prepare("select m2.meta_value from $ens_db.meta m1 join $ens_db.meta m2 on (m1.species_id=m2.species_id and m2.meta_key='species.db_name')where m1.meta_key='species.db_alias' and m1.meta_value=?");  
    $logger->debug("Maps to $ens_db");
    $logger->debug("Species ID = $dataset_names{species_id}");
    $dataset_names{species_name} = get_string($species_sth,$dataset_names{species_id});
    $logger->debug("Species name = $dataset_names{species_name}");
    $dataset_names{species_uc_name} = $dataset_names{species_name};
    $dataset_names{species_uc_name} =~ s/\s+/_/g;
    $dataset_names{short_name} = get_short_name($dataset_names{species_name},$dataset_names{species_id});
    $dataset_names{version_num} = $dataset_names{short_name}.'_'.get_version($ens_db);
    $logger->debug(join(',',values(%dataset_names)));
    push(@datasets,\%dataset_names);
    write_dataset_xml(\%dataset_names);

}

# 2. write template files
write_template_xml(\@datasets);

## 3. write and load metafiles
#foreach my $file (write_metafiles(\@datasets)) {
#    $logger->info("Loading $file into database");
#    my $command = "mysql -u$db_user -p$db_pwd -P$db_port -h$db_host $mart_db < $file";
#    $logger->debug("Running $command");
#    system($command);
#}

write_metatables($mart_handle, \@datasets);

$mart_handle->disconnect() or croak "Could not close handle to $mart_string";

