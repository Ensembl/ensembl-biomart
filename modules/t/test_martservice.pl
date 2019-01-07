# Copyright [2009-2019] EMBL-European Bioinformatics Institute
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

use warnings;
use strict;
use Bio::EnsEMBL::BioMart::MartService;

my $srv = Bio::EnsEMBL::BioMart::MartService->new(
					   -URL => 'http://fungi.ensembl.org/biomart/martservice' );

my $mart = $srv->get_mart_by_name('fungi_mart_15');
my $dataset = $mart->get_dataset_by_name('spombe_eg_gene');
my $filter = $dataset->get_filter_by_name('chromosome_name');
my $attribute = $dataset->get_attribute_by_name('ensembl_gene_id');
my $attribute2 = $dataset->get_attribute_by_name('ensembl_transcript_id');
for my $row (@{my $data = $srv->do_query($mart,$dataset,[$attribute, $attribute2],{$filter->name()=>'AB325691'})}) {
	print join (",", @$row) . "\n";
}
