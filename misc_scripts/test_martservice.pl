use warnings;
use strict;
use Bio::EnsEMBL::BioMart::MartService;
use Data::Dumper;
use XML::Simple;

my $srv = Bio::EnsEMBL::BioMart::MartService->new(
					   -URL => 'http://fungi.ensembl.org/biomart/martservice' );

my $mart = $srv->get_mart_by_name('fungi_mart_15');
my $dataset = $mart->get_dataset_by_name('spombe_eg_gene');
my $filter = $dataset->get_filter_by_name('chromosome_name');
my $attribute = $dataset->get_attribute_by_name('ensembl_gene_id');
my $attribute2 = $dataset->get_attribute_by_name('ensembl_transcript_id');
my $data = $srv->do_query($mart,$dataset,[$attribute, $attribute2],{$filter->name()=>'AB325691'});
print $data;