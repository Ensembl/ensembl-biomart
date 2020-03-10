=head1 LICENSE

Copyright [2009-2020] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

# $Source$
# $Revision$
# $Date$
# $Author$
#
# Some common utils for manipulating marts

use warnings;
use strict;
package MartUtils;
use DBI;
use Carp;
use Log::Log4perl qw(:easy);
use DbiUtils qw(get_string query_to_strings);
use Exporter qw/import/;
our @EXPORT_OK = qw(get_ensembl_db get_ensembl_db_collection get_ensembl_db_single_parasite get_dataset_names get_species_name_for_dataset get_sql_name_for_dataset generate_dataset_name_from_db_name get_datasets get_ensembl_db_single);

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
    my $regexp = shift;
    my $suffix = shift;
    print $regexp."\n";
    return get_datasets_regexp ($src_tables, $regexp, $suffix);
}

sub get_datasets_regexp {
    my $src_tables_aref = shift;
    my $regexp = shift;
    my $suffix = shift;
    my @datasets = ();
    foreach my $src_table (@$src_tables_aref) {
	if( $src_table =~ m/^(?!meta)([A-Za-z1-9]*$suffix+)_.*$regexp/ ) {
	    push @datasets,$1 unless grep{$_ eq $1} @datasets;
	}
    }
    return @datasets;
}

#Generate a mart dataset name from a database name
sub generate_dataset_name_from_db_name {
    my ($database) = @_;
    ( my $dataset = $database ) =~ m/^(.)[^_]+_?([a-z0-9])?[a-z0-9]+?_([a-z0-9]+)_[a-z]+_[0-9]+_?[0-9]+?_[0-9]+$/;
    $dataset = defined $2 ? "$1$2$3" : "$1$3";
    return $dataset;
}

#@deprecated
sub get_sequence_datasets {
    my $regexp = "genomic_sequence__dna_chunks__main";
    return get_datasets_regexp (@_, $regexp);
}

sub get_ensembl_db_single {
    my ($src_dbs,$dataset,$release) = @_;
    return get_ensembl_db($src_dbs,$dataset, sub {
	my $var = shift;
    $var = generate_dataset_name_from_db_name($var);
	return $var;
			  }
	);    
}

sub get_ensembl_db_single_parasite {
    my ($src_dbs,$dataset,$release) = @_;
    return get_ensembl_db($src_dbs,$dataset, sub {
        my $var = shift;
        # The database naming convention is different in ParaSite, and the BioMart partition names have been reduced down to 5 letters of the species names + BioProject only otherwise they are too long for MartBuilder
        $var =~ s/^[^_]+_([^_]{1,5})[^_]*_([^_]+)_core_*\d*_($release)_[0-9]+[a-z]*$/$1$2/;
        return $var;
                          }
        );
}

sub get_ensembl_db_collection {
    my ($src_dbs,$dataset,$release) = @_;
    return get_ensembl_db($src_dbs,$dataset,  sub {
	my $var = shift;
	$var =~ s/^(...).*_collection_core_*\d*_($release)_[0-9]+[a-z]*$/$1/;
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
sub build_dataset_href {
    my ($meta_container, $logger, $suffix) = @_;
    my $dataset_href = {};
    $suffix ||= "";
    my $species_name = $meta_container->db->species;
    my $is_multispecies = $meta_container->db->{_is_multispecies};
    my $src_db = $meta_container->db->{_dbc}->{_dbname};
    my $division_value;
    print STDERR "src_db, $src_db\n";
    my $formatted_species_name = undef;
    if (! $is_multispecies) {
        $species_name =~ /^(\w)[^_]+_(.+)/;
	$formatted_species_name = $1 . $2;

	if (defined @{$meta_container->list_value_by_key('species.division')}[0]) {
	    $division_value = @{$meta_container->list_value_by_key('species.division')}[0];
	    # Add a suffix '_eg' to avoid conflicting dataset names in Biomart.org!
	    $formatted_species_name = $formatted_species_name . $suffix;
        }
        # If division is ensembl then use the name column in the dataset_names table
        if ($division_value eq "EnsemblVertebrates") {
            $formatted_species_name = $src_db;
            $formatted_species_name =~ s/^(.)[^_]+_?[a-z0-9]+?_([a-z0-9]+)_[a-z]{4}_[0-9]{2}_[0-9]*$/$1$2/;
        }
    }
    else {
	if (! defined @{$meta_container->list_value_by_key('species.production_name')}[0]) {
	    warn "'species.production_name' meta attribute not defined for species, '$species_name'!\n";
	    die "not allowed for a multispecies database";
       	}
	else {
	    # Todo: Reformat it, by using the proteome_id instead
	    # e.g. 'bac_130'
	    
	    # Was:
	    # $formatted_species_name = @{$meta_container->list_value_by_key('species.production_name')}[0];
	    
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
    }

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
    
    my $version_num;
    if ($division_value ne "EnsemblVertebrates") {
      $version_num = @{$meta_container->list_value_by_key('genebuild.version')}[0];
      if (! defined $version_num) {
          die "'genebuild.version' meta attribute not defined for species, '$species_name'!\n";
      }
    }
    elsif ($species_name eq "homo_sapiens" | $species_name eq "mus_musculus" ){
      $version_num = @{$meta_container->list_value_by_key('assembly.name')}[0];
      if (! defined $version_num) {
          die "'assembly.name' meta attribute not defined for species, '$species_name'!\n";
      }
    }
   else{
     $version_num = @{$meta_container->list_value_by_key('assembly.default')}[0];
      if (! defined $version_num) {
          die "'assembly.default' meta attribute not defined for species, '$species_name'!\n";
      }
   }
    print STDERR "version_num, $version_num\n";
    
    $dataset_href->{formatted_species_name} = $formatted_species_name;
    $dataset_href->{dataset}=$formatted_species_name . "_genomic_sequence";
    my $template_filename = $formatted_species_name . "_genomic_sequence_template.template.xml";
    $dataset_href->{template}=$template_filename;
    $dataset_href->{short_species_name}=$formatted_species_name;
    $dataset_href->{species_id} = $species_id;
    $dataset_href->{species_name} = $species_name;
    
    if (defined $logger) {
	$logger->info("dataset name: " . $dataset_href->{dataset});
	$logger->info("template filename, $template_filename");
	$logger->info("species_name, " . $dataset_href->{species_name});
	#$logger->info("species_id, " . $dataset_href->{species_id});
    }

    ($dataset_href->{baseset}, $dataset_href->{src_db},$dataset_href->{species_id},$dataset_href->{species_name},$dataset_href->{version_num}) = ($baseset,$src_db,$species_id,$species_name,$version_num);
    $dataset_href->{species_uc_name} = $dataset_href->{species_name};
    $dataset_href->{species_uc_name} =~ s/\s+/_/g;

    $dataset_href->{short_name} = get_short_name($dataset_href->{species_name},$dataset_href->{species_id});

    if (defined $logger) {
        $logger->debug(join(',',map { $dataset_href->{$_} // () }keys %$dataset_href));
    }
    
    return $dataset_href;
}


sub get_all_species {

    # the Ensembl division, optional
    my $division = shift;

    my $species_aref = [];
    my %hash;

    foreach my $adap (@{Bio::EnsEMBL::Registry->get_all_DBAdaptors(-group => "core")}){
        if(!defined($hash{$adap->species})){
            if($adap->species =~ /ancestral sequences/i){ # ignore "Ancestral sequences"
                print STDERR "ignoring it!\n";
            }
            else{
		if (defined $division || ($division ne "")) {
		    my $meta_container = Bio::EnsEMBL::Registry->get_adaptor( $adap->species, 'Core', 'MetaContainer' );
		    if (defined @{$meta_container->list_value_by_key('species.division')}[0]) {
			my $species_division = @{$meta_container->list_value_by_key('species.division')}[0];
			if ($division eq $species_division) {
			    push @$species_aref, $adap->species;
			    $hash{$adap->species} = 1;
			}
		    }
		}
		else {
		    push @$species_aref, $adap->species;
		    $hash{$adap->species} = 1;
		}
            }
        }

	$adap->dbc()->disconnect_if_idle();

    }

    return $species_aref;
}

sub contains {
    my $array_ref = shift;
    my $element = shift;

    foreach my $item (@$array_ref) {
	if ($element  =~ /^$item/i) {
	    return 1;
	}
    }

    return 0;
}

1;

