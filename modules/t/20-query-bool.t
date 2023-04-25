# Copyright [2009-2023] EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use strict;
use warnings;
use Test::More;
use Bio::EnsEMBL::BioMart::MartService;
use Data::Dumper;

my $srv = Bio::EnsEMBL::BioMart::MartService->new(
					   -URL => 'https://fungi.ensembl.org/biomart/martservice' );
ok( defined $srv, "Service found" );

my $mart;

# print Dumper($srv->get_marts());
for my $m ( @{ $srv->get_marts() } ) {
	print $m->name()."\n";
	if ( $m->name() =~ m/fungi_mart+/ ) {
		$mart = $m;
		last;
	}
}
ok( defined $mart, "Test mart found" );
# print Dumper($mart);
my $dataset = $mart->get_dataset_by_name('spombe_eg_gene');
ok( defined $dataset, "Test dataset found" );
my $filter = $dataset->get_filter_by_name('with_uniprotswissprot');
ok( defined $filter, "Test filter found" );
is( $filter->name(), "with_uniprotswissprot", "Checking filter name" );
my $attribute = $dataset->get_attribute_by_name('ensembl_gene_id');
ok( defined $attribute, "Test attr 1 found" );
is( $attribute->name(), "ensembl_gene_id", "Checking attribute 1 name" );
my $attribute2 = $dataset->get_attribute_by_name('ensembl_transcript_id');
ok( defined $attribute2, "Test attr 2 found" );
is( $attribute2->name(), "ensembl_transcript_id", "Checking attribute 2 name" )
  ;
my $data =
  $srv->do_query( $mart, $dataset,
				  [ $attribute, $attribute2 ],
				  [ { filter => $filter } ] );
ok( defined $data, "Data found found" );

done_testing();
