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
use Getopt::Long;
use Bio::EnsEMBL::Registry;

Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();
my $release = 51;
my $output_dir = "./output";
my $mart_version = "0.7";

sub all_species {
    my $species_aref = [];
    my %hash;

    foreach my $adap (@{Bio::EnsEMBL::Registry->get_all_DBAdaptors(-group => "core")}){
        if(!defined($hash{$adap->species})){
            if($adap->species =~ /ancestral sequences/i){ # ignore "Ancestral sequences"
                print STDERR "ignoring it!\n";
            }
            else{
                push @$species_aref, $adap->species;
                $hash{$adap->species} = 1;
            }
        }

    }

    return $species_aref;
}


sub write_dataset_xml {
    my $dataset_names = shift;
    my $fname = "./$output_dir/".$dataset_names->{dataset}.'.xml';
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
	write_replace_file('templates/sequence_template_template.xml',"$output_dir/$template_filename",$dataset);
	`gzip -c $output_dir/$template_filename > $output_dir/$template_filename.gz`;
    }
}

my $table_args ="ENGINE=MyISAM DEFAULT CHARSET=latin1";
sub create_metatable {
    my ($db_handle,$table_name,$cols) = @_[0,1,2];
    drop_and_create_table($db_handle,$table_name,$cols,$table_args);
}

sub write_metatables {
    my ($seq_mart_handle, $datasets_aref) = @_[0,1];
    my $pwd = &Cwd::cwd();

    $logger->info("Creating meta tables");

    # create tables
    create_metatable($seq_mart_handle,'meta_version__version__main',
		     ['version varchar(10) default NULL']);
    create_metatable($seq_mart_handle,'meta_template__xml__dm',
		     ['template varchar(100) default NULL',
		      'compressed_xml longblob',
		      'UNIQUE KEY template (template)']);
    create_metatable($seq_mart_handle,'meta_conf__xml__dm',
		     ['dataset_id_key int(11) NOT NULL',
		      'xml longblob',
		      'compressed_xml longblob',
		      'message_digest blob',
		      'UNIQUE KEY dataset_id_key (dataset_id_key)']);
    create_metatable($seq_mart_handle,'meta_conf__user__dm',
		     ['dataset_id_key int(11) default NULL',
		      'mart_user varchar(100) default NULL',
		      'UNIQUE KEY dataset_id_key (dataset_id_key,mart_user)']);
    create_metatable($seq_mart_handle,'meta_conf__interface__dm',
		     ['dataset_id_key int(11) default NULL',
		      'interface varchar(100) default NULL',
		      'UNIQUE KEY dataset_id_key (dataset_id_key,interface)']);
    create_metatable($seq_mart_handle,'meta_template__template__main',
		     ['dataset_id_key int(11) NOT NULL',
		      'template varchar(100) NOT NULL']);
    create_metatable($seq_mart_handle,'meta_conf__dataset__main',[ 
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
    $seq_mart_handle->do("INSERT INTO meta_version__version__main VALUES ('$mart_version')");
    ## meta_template__xml__dm

    my $meta_conf__xml__dm       = $seq_mart_handle->prepare('INSERT INTO meta_conf__xml__dm VALUES (?,?,?,?)');
    my $meta_conf__user__dm      = $seq_mart_handle->prepare('INSERT INTO meta_conf__user__dm VALUES(?,\'default\')');
    my $meta_conf__interface__dm = $seq_mart_handle->prepare('INSERT INTO meta_conf__interface__dm VALUES(?,\'default\')');
    my $meta_conf__dataset__main = $seq_mart_handle->prepare("INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,'Ensembl Sequences','GenomicSequence',0,?)");
    my $meta_template__template__main = $seq_mart_handle->prepare('INSERT INTO meta_template__template__main VALUES(?,?)');
    
    foreach my $dataset_href (@$datasets_aref) {
	
	my $template_filename = $dataset_href->{template};
	my $dataset_name      = $dataset_href->{dataset};

	my $sth = $seq_mart_handle->prepare('INSERT INTO meta_template__xml__dm VALUES (?,?)');
	$sth->execute($dataset_href->{dataset}, file_to_bytes("$pwd/$output_dir/" . $template_filename)) 
	    or croak "Could not load template file,$template_filename, into meta_template__xml__dm";
	$sth->finish();
 
	$logger->info("Populating dataset tables");
	
	# meta_conf__xml__dm
	$meta_conf__xml__dm->execute($dataset_href->{species_id},
				     file_to_bytes("$pwd/$output_dir/$dataset_href->{dataset}.xml"),
				     file_to_bytes("$pwd/$output_dir/$dataset_href->{dataset}.xml.gz"),
				     file_to_bytes("$pwd/$output_dir/$dataset_href->{dataset}.xml.gz.md5")
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



# db params
#my $db_host = 'mysql-eg-devel-1.ebi.ac.uk';
#my $db_port = '4126';
#my $db_user = 'admin';
#my $db_pwd = 'tGc3Vs2O';
my $db_host = 'mysql-eg-staging-1.ebi.ac.uk';
my $db_port = '4160';
my $db_user = 'admin';
my $db_pwd = '6KSFrax4';

my $seq_mart_db = 'ensembl_bacterial_sequence_mart_51';

sub usage {
    print "Usage: $0 [-h <host>] [-port <port>] [-u user <user>] [-pwd <pwd>] [-seq_mart <target mart database>] [-release <release number>]\n";
    print "-h <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-pwd <password> Default is top secret unless you know cat\n";
    print "-seq_mart <mart> Default is $seq_mart_db\n";
    print "-release <ensembl release number> Default is $release\n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "port=s"=>\$db_port,
    "u=s"=>\$db_user,
    "pwd=s"=>\$db_pwd,
    "seq_mart=s"=>\$seq_mart_db,
    "release=s"=>\$release,
    "help"=>sub {usage()}
    );

if(!$options_okay) {
    usage();
}

print STDERR "working with release, $release\n";

if (! -d $output_dir) {
    print STDERR "create output directory, $output_dir\n";
    qx/mkdir $output_dir/;
}

my $seq_mart_string = "DBI:mysql:$seq_mart_db:$db_host:$db_port";
my $seq_mart_handle = DBI->connect($seq_mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $seq_mart_string with user $db_user and pwd, $db_pwd";

# Use Registry rather than dataset_names table from gene mart
# to get the dataset name, the species id etc.
Bio::EnsEMBL::Registry->load_registry_from_db(
                                              -host => $db_host,
                                              -user => $db_user,
                                              -pass => $db_pwd,
                                              -port => $db_port,
                                              -db_version => $release);
my $species_names_aref = all_species();

my @datasets = ();
foreach my $species_name (@$species_names_aref) {
    $logger->info("Processing species, '$species_name'");

    my $meta_container =
           Bio::EnsEMBL::Registry->get_adaptor( "$species_name", 'Core', 'MetaContainer' );
    if (! defined $meta_container) {
        die "meta_container couldn't be instanciated for species, \"$species_name\"\n";
    }
    
    ###################################################
    #
    # MAKE SURE the dataset_name, the species_id, etc. 
    # are consistent with the one generated 
    # for the gene mart dbs !!
    #
    ###################################################

    my $dataset_name = @{$meta_container->list_value_by_key('species.sql_name')}[0];
    if (! defined $dataset_name) {
        die "sql_name meta attribute not defined for species, '$species_name'!\n";
    }

    print STDERR "species_name, $species_name\n";

    my $species_id = @{$meta_container->list_value_by_key('species.proteome_id')}[0];
    if (! defined $species_id) {
        die "'species.proteome_id' meta attribute not defined for species, '$species_name'!\n";
    }

    print STDERR "species_id, $species_id\n";

    my $src_db = $meta_container->db->{_dbc}->{_dbname};

    print STDERR "src_db, $src_db\n";

    my $baseset = undef;
    if ($src_db =~ /^(\w)\w+_(\w+)_collection.+$/) {
	$baseset = $1 . $2;
    }
    else {
	$src_db =~ /^(\w+)_collection.+$/;
	$baseset = $1;
    }

    print STDERR "baseset: $baseset\n";
    
    my $version_num = @{$meta_container->list_value_by_key('genebuild.version')}[0];
    if (! defined $version_num) {
        die "'genebuild.version' meta attribute not defined for species, '$version_num'!\n";
    }
    print STDERR "version_num, $version_num\n";
    

    my %dataset_href = ();
    my $template_filename = $dataset_name . "_genomic_sequence_template.template.xml";
    $dataset_href{dataset}=$dataset_name . "_genomic_sequence";
    $dataset_href{template}=$template_filename;
    $dataset_href{short_species_name}=$dataset_name;
    
    $logger->info("dataset name: $dataset_name");
    $logger->info("template filename, $template_filename");
    
    ($dataset_href{baseset}, $dataset_href{src_db},$dataset_href{species_id},$dataset_href{species_name},$dataset_href{version_num}) = ($baseset,$src_db,$species_id,$species_name,$version_num);
    $dataset_href{species_uc_name} = $dataset_href{species_name};
    $dataset_href{species_uc_name} =~ s/\s+/_/g;
    $dataset_href{short_name} = get_short_name($dataset_href{species_name},$dataset_href{species_id});
    
    $logger->debug(join(',',values(%dataset_href)));
    
    push(@datasets,\%dataset_href);
    write_dataset_xml(\%dataset_href);

}

# 2. write template files
write_template_xml(\@datasets);

## 3. write and load metafiles

write_metatables($seq_mart_handle, \@datasets);

$seq_mart_handle->disconnect() or croak "Could not close handle to $seq_mart_string";
