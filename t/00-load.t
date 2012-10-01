#!perl
use Test::More;

BEGIN {
	use_ok( 'Bio::EnsEMBL::BioMart::MartService' );
	use_ok( 'Bio::EnsEMBL::BioMart::Mart' );
	use_ok( 'Bio::EnsEMBL::BioMart::DataSet' );
	use_ok( 'Bio::EnsEMBL::BioMart::Attribute' );
	use_ok( 'Bio::EnsEMBL::BioMart::Filter' );
}

diag( "Testing BioMart Perl $], $^X" );
done_testing;