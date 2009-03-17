# $Source$
# $Revision$
# $Date$
# $Author$
#
# Some common utils for manipulating marts

use warnings;
use strict;
use DBI;
use Carp;
use Log::Log4perl qw(:easy);
use DbiUtils;

#sub get_species_name_for_dataset {
#    my ($dbh,$ds_name) = @_;
#    my $sth = $dbh->prepare("SELECT sql_name FROM dataset_names WHERE name='$ds_name'");
#    return get_string($sth);
#}

sub get_sql_name_for_dataset {
    my ($dbh,$ds_name) = @_;
    my $sth = $dbh->prepare("SELECT sql_name FROM dataset_names WHERE name='$ds_name'");
    return get_string($sth);
}

sub get_species_name_for_dataset {
    my ($dbh,$ds_name) = @_;
    my $sth = $dbh->prepare("SELECT species_name FROM dataset_names WHERE name='$ds_name'");
    return get_string($sth);
}

sub file_to_bytes {
    my $file_name = $_[0];
    open my $file, "<", $file_name or croak "Could not open $file_name";
    my $bytes = do { local $/; <$file> } or croak "Could not read $file_name into memory";
    $bytes;
}

sub get_dataset_names_for_clade {
    my ($dbh,$clade) = @_;
    query_to_strings($dbh,"SELECT name FROM dataset_names WHERE src_dataset='$clade'");
}

sub get_dataset_names {
    my $dbh = shift;
    query_to_strings($dbh,'SELECT name FROM dataset_names');
}

sub get_datasets {
    my $src_tables = shift;
    my $regexp = 'gene__gene__main';
    return get_datasets_regexp ($src_tables, $regexp);
}

sub get_datasets_regexp {
    my $src_tables_aref = shift;
    my $regexp = shift;
    my @datasets = ();
    foreach my $src_table (@$src_tables_aref) {
	if( $src_table =~ m/(.*)_$regexp/ ) {
	    push @datasets,$1;
	}
    }
    return @datasets;
}

#@deprecated
sub get_sequence_datasets {
    my $regexp = "genomic_sequence__dna_chunks__main";
    return get_datasets_regexp (@_, $regexp);
}

sub get_ensembl_db_single {
    my ($src_dbs,$dataset) = @_;
    return get_ensembl_db($src_dbs,$dataset, sub {
	my $var = shift;
	$var =~ s/^(.)[^_]*_([^_]+)_core_*\d*_[0-9]+_[0-9]+[a-z]*$/$1$2/;
	return $var;
			  }
	);    
}

sub get_ensembl_db_collection {
    my ($src_dbs,$dataset) = @_;
    return get_ensembl_db($src_dbs,$dataset,  sub {
	my $var = shift;
	$var =~ s/^(...).*_collection_core_*\d*_[0-9]+_[0-9]+[a-z]*$/$1/;
	return $var;
			  }
	);
}

sub get_ensembl_db {
    my $src_dbs = shift;
    my $dataset = shift;
    my $fn = shift;
    my $ens_db;
    foreach my $src_db (@$src_dbs) {
	my $candidate = $src_db;
	#$candidate =~ s/^(.)?([^_]+_)?([^_]+)_collection_core_*\d*_[0-9]+_[0-9]+[a-z]*$/$1$3/;
	$candidate = $fn->($candidate);
	if($candidate eq $dataset) {
	    $ens_db = $src_db;
	    last;
	}
    }
    $ens_db;
}

# Build the dataset hash
# Assume we are dealing with a Multispecies database at the moment
# not anymore !
# should be workign fine in all cases (tested on both protist and bacterial)
sub build_dataset_href {
    my ($meta_container, $logger) = @_;
    my $dataset_href = {};
    
    my $species_name = $meta_container->db->species;
    my $is_multispecies = $meta_container->db->{_is_multispecies};
    my $src_db = $meta_container->db->{_dbc}->{_dbname};
    
    print STDERR "src_db, $src_db\n";
    
    my $formatted_species_name = undef;
    if (! $is_multispecies) {
	$species_name =~ /^(\w)[^_]+_(.+)/;
	$formatted_species_name = $1 . $2;	
    }
    else {
	if (! defined @{$meta_container->list_value_by_key('species.sql_name')}[0]) {
	    warn "'species.sql_name' meta attribute not defined for species, '$species_name'!\n";
	    die "not allowed for a multispecies database";
       	}
	else {
	    # Todo: Reformat it, by using the proteome_id instead
	    # e.g. 'bac_130'
	    
	    # Was:
	    # $formatted_species_name = @{$meta_container->list_value_by_key('species.sql_name')}[0];
	    
	    # Now: 
	    $src_db =~ /^(\w\w\w).+/;
	    my $db_prefix = $1;
	    $formatted_species_name = $db_prefix . "_" . @{$meta_container->list_value_by_key('species.proteome_id')}[0];
	}
    }
    
    print STDERR "formatted_species_name, $formatted_species_name\n";

    my $species_id = undef;
    if (defined @{$meta_container->list_value_by_key('species.proteome_id')}[0]) {
	# use the proteome_id if possible
	$species_id = @{$meta_container->list_value_by_key('species.proteome_id')}[0];
    }
    else {
	if ($is_multispecies) {
	    print STDERR "species.proteome_id' meta attribute is required in a multispecies database context\n";
	    die "'species.proteome_id' meta attribute not defined for species, '$species_name'!\n";
	}
	else {
	    # set it arbitrarily to 1!
	    $species_id = 1;
	}
    }

    print STDERR "species_id, $species_id\n";
    
    my $baseset = undef;
    if ($is_multispecies) {
	if ($src_db =~ /^(\w)\w+_(\w+)_collection.+$/) {
	    $baseset = $1 . $2;
	}
	else {
	    $src_db =~ /^(\w+)_collection.+$/;
	    $baseset = $1;
	}
    }
    else {
	$src_db =~ /^(\w)\w+_(\w+)_core.+$/;
	$baseset = $1 . $2;
    }
    
    print STDERR "baseset: $baseset\n";
    
    my $version_num = @{$meta_container->list_value_by_key('genebuild.version')}[0];
    if (! defined $version_num) {
        die "'genebuild.version' meta attribute not defined for species, '$species_name'!\n";
    }
    print STDERR "version_num, $version_num\n";
    
    $dataset_href->{formatted_species_name} = $formatted_species_name;
    $dataset_href->{dataset}=$formatted_species_name . "_genomic_sequence";
    my $template_filename = $formatted_species_name . "_genomic_sequence_template.template.xml";
    $dataset_href->{template}=$template_filename;
    $dataset_href->{short_species_name}=$formatted_species_name;
    
    if (defined $logger) {
	$logger->info("dataset name: " . $dataset_href->{dataset});
	$logger->info("template filename, $template_filename");
    }

    ($dataset_href->{baseset}, $dataset_href->{src_db},$dataset_href->{species_id},$dataset_href->{species_name},$dataset_href->{version_num}) = ($baseset,$src_db,$species_id,$species_name,$version_num);
    $dataset_href->{species_uc_name} = $dataset_href->{species_name};
    $dataset_href->{species_uc_name} =~ s/\s+/_/g;
    $dataset_href->{short_name} = get_short_name($dataset_href->{species_name},$dataset_href->{species_id});

    if (defined $logger) {
	$logger->debug(join(',',values(%$dataset_href)));
    }
    
    return $dataset_href;
}


sub get_all_species {
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

1;

