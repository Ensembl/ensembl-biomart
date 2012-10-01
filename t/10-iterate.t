use strict;
use warnings;

use Test::More;
use Bio::EnsEMBL::BioMart::MartService;
use Data::Dumper;

my $srv = Bio::EnsEMBL::BioMart::MartService->new(
					   -URL => 'http://fungi.ensembl.org/biomart/martservice' );

my @marts = @{ $srv->get_marts() };
ok( scalar(@marts) == 4 );

for my $mart (@marts) {
	ok( defined $mart, "Mart defined" );
	for my $dataset ( @{ $mart->datasets() } ) {
		ok( defined $dataset, "Dataset defined for " . $mart->name() );
		my @attributes = @{ $dataset->attributes() };
		ok( scalar(@attributes) > 0,
			"Attributes defined for dataset " . $dataset->name() );
		my @filters = @{ $dataset->filters() };
		ok( scalar(@filters) > 0,
			"Filters defined for dataset " . $dataset->name() );
	}
}

done_testing;
