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

sub write_dataset_xml {
    my $dataset_names = shift;
    my $fname = './output/'.$dataset_names->{dataset}.'.xml';
    open my $dataset_file, '>', $fname or croak "Could not open $fname for writing"; 
    my $template_file_name = 'templates/dataset_template.xml';
    open my $template_file, '<', $template_file_name or croak "Could not open $template_file_name";
    while (my $line = <$template_file>) {
	$line =~ s/%name%/$$dataset_names{dataset}/g;
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
    my ($template,$output,$placeholder,$contents) = @_[0,1,2,3];
    open my $output_file, '>', $output or croak "Could not open $output";
    open my $template_file, '<', $template or croak "Could not open $template";
    while( my $content = <$template_file>) {
	if ($content =~ m/$placeholder/) {
	    $content =~ s/$placeholder/$contents/;
	}
        print $output_file $content;
    }
    close($output_file);
    close($template_file);
}


sub write_template_xml {
    my $datasets = shift;
    my $datasets_text='';
    foreach my $dataset (@$datasets) {
	$datasets_text = $datasets_text . 
	    '<DynamicDataset aliases="mouse_formatter1=,mouse_formatter2=,mouse_formatter3=,species1='.
	    ${$dataset}{species_name}.
	    ',species2='.$dataset->{species_uc_name}.
	    ',species3='.$dataset->{dataset}.
	    ',species4='.$dataset->{short_name}.
	    ',version='.$dataset->{version_num}.
	    ',link_version='.$dataset->{dataset}.
	    '_51,default=true" internalName="'.
	    $dataset->{dataset}.'_gene_ensembl"/>'."\n";
    }
    write_replace_file('templates/template_template.xml','output/template.xml','%datasets%',$datasets_text);
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

sub write_metafiles {

    my $datasets = shift;
    my $pwd = &Cwd::cwd();

    my @files = ();

    # a. copy meta_version__version__main
    $logger->debug('Copying files');
    copy('templates/meta_version__version__main.dump','output/meta_version__version__main.dump');
    
    # b. update meta_template__xml__dm.dump
    my $outfile = './output/meta_template__xml__dm.dump';   
    $logger->debug("Rewriting $outfile");
    push(@files,$outfile);
    write_replace_file('templates/meta_template__xml__dm.dump',
		       $outfile,
		       '%insert%',
		       sprintf('INSERT INTO `meta_template__xml__dm` VALUES (\'gene_ensembl\',null); LOAD DATA LOCAL INFILE "%s/output/template.xml.gz" INTO TABLE meta_template__xml__dm(compressed_xml) FIELDS TERMINATED BY \'\' FIELDS ENCLOSED BY \'\';',$pwd)."\n");
    

    # c. meta_template__template__main.dump
    $outfile =  './output/meta_template__template__main.dump';
    push(@files,$outfile);
    $logger->debug("Updating $outfile");
    update_meta_file('templates/meta_template__template__main.dump',
		    $outfile,
		     '%insert%','INSERT INTO `meta_template__template__main` VALUES',";\n",',',$datasets, 
		     sub {   
			 my $dataset = shift; 														     
			 "($dataset->{species_id},'gene_ensembl')"    
		     }
	);


    # d. meta_conf__dataset__main.dump
    $outfile =  './output/meta_conf__dataset__main.dump';
    $logger->debug("Updating $outfile");
    push(@files,$outfile);
    update_meta_file("templates/meta_conf__dataset__main.dump",
		     $outfile,
                     '%insert%','INSERT INTO `meta_conf__dataset__main` VALUES',
		     ";\n",",",
		     $datasets, 
		     sub { 
			 my $dataset = shift;
			 "($dataset->{species_id},'$dataset->{dataset}_gene_ensembl','$dataset->{species_name} genes ($dataset->{version_num})','Ensembl Genes','TableSet',1,'$dataset->{version_num}','2008-07-18 13:11:12')";
		     }
	);
    
    # e. meta_conf__interface__dm.dump 
    $outfile =  './output/meta_conf__interface__dm.dump';
    $logger->debug("Updating $outfile");
    push(@files,$outfile);
    update_meta_file("templates/meta_conf__interface__dm.dump",
		     $outfile,
		     '%insert%','INSERT INTO `meta_conf__interface__dm` VALUES',
		     ";\n",',',
		     $datasets,
		     sub {
		       my $dataset = shift;		       
		       "($dataset->{species_id},'default')"    
		     }
	);
    
    # f. meta_conf__user__dm.dump
    $outfile = './output/meta_conf__user__dm.dump';
    $logger->debug("Updating $outfile");
    push(@files,$outfile);
    update_meta_file("templates/meta_conf__user__dm.dump",
		     $outfile,
		     '%insert%','INSERT INTO `meta_conf__user__dm` VALUES',
		     ";\n",',',
		     $datasets,	
		     sub {
			 my $dataset = shift;		       
			 "($dataset->{species_id},'default')"    
		     }
	);
    
    # g. meta_conf__xml__dm.dump
    $outfile = './output/meta_conf__xml__dm.dump';
    $logger->debug("Updating $outfile");
    push(@files,$outfile);
    update_meta_file("templates/meta_conf__xml__dm.dump",
		     $outfile,
		     '%insert%','',
		     '','',
		     $datasets,
		     sub {
			 my $dataset = shift;		       
			 "INSERT INTO \`meta_conf__xml__dm\` VALUES ($dataset->{species_id},null,null,null);\nLOAD DATA LOCAL INFILE \"$pwd/output/$dataset->{dataset}.xml\" INTO TABLE meta_conf__xml__dm(xml) FIELDS TERMINATED BY '' FIELDS ENCLOSED BY '';\nLOAD DATA LOCAL INFILE \"$pwd/output/$dataset->{dataset}.xml.gz\" INTO TABLE meta_conf__xml__dm(compressed_xml)  FIELDS TERMINATED BY '' FIELDS ENCLOSED BY '';\n LOAD DATA LOCAL INFILE \"$pwd/output/$dataset->{dataset}.xml.gz.md5\" INTO TABLE meta_conf__xml__dm(message_digest)  FIELDS TERMINATED BY '' FIELDS ENCLOSED BY '';\n"
		     }
	);
    @files;
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
    my $dataset_name = 'gene_ensembl';
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
	    "$dataset->{dataset}_gene_ensembl",
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
    uc(substr($db_name,0,3).$species_id);
} 

sub get_version {
    my $ens_db = shift;
    $ens_db =~ m/^.*_([0-9]+[a-z]*)$/;
    $1;
}

# db params
my $db_host = '127.0.0.1';
my $db_port = '4126';
my $db_user = 'admin';
my $db_pwd = 'tGc3Vs2O';
#my $db_port = '3306';
#my $db_user = 'eg';
#my $db_pwd = 'eg';
my $mart_db = 'bacterial_mart_51';

sub usage {
    print "Usage: $0 [-h <host>] [-P <port>] [-u user <user>] [-p <pwd>] [-src_mart <src>] [-target_mart <targ>]\n";
    print "-h <host> Default is $db_host\n";
    print "-P <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <mart> Default is $mart_db\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
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

foreach my $dataset (get_datasets(\@mart_tables)) {
    $logger->info("Processing $dataset");
    my %dataset_names = ();
    $dataset_names{dataset}=$dataset;
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
    write_dataset_xml(\%dataset_names)
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

