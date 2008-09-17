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

sub get_datasets {
    my $src_tables = shift;
    my @datasets = ();
    foreach my $src_table (@$src_tables) {
	if( $src_table =~ m/(.*)_gene_ensembl__gene__main/ ) {
	    push @datasets,$1;
	}
    }
    @datasets;
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


