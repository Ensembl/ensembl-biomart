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
    my $regexp = "gene_ensembl__gene__main";
    return get_datasets_regexp (@_, $regexp);
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

sub get_sequence_datasets {
    my $regexp = "genomic_sequence__dna_chunks__main";
    return get_datasets_regexp (@_, $regexp);
}

sub get_ensembl_db {
    my $src_dbs = shift;
    my $dataset = shift;
    my $ens_db;
    foreach my $src_db (@$src_dbs) {
	my $candidate = $src_db;
	$candidate =~ s/^(.)?([^_]+_)?([^_]+)_collection_core_*\d*_51_[0-9]+[a-z]*$/$1$3/;
	if($candidate eq $dataset) {
	    $ens_db = $src_db;
	    last;
	}
    }
    $ens_db;
}

1;


