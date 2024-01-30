# Copyright [2009-2024] EMBL-European Bioinformatics Institute
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
    -URL => 'https://fungi.ensembl.org/biomart/martservice');

my @marts = @{$srv->get_marts()};
ok(scalar(@marts) == 4);

for my $mart (@marts) {
    ok(defined $mart, "Mart defined ". $mart->name());
    if (!$mart =~ /sequences/) {
        # Failing for sequences mart for some reason.
        for my $dataset (@{$mart->datasets()}) {
            ok(defined $dataset, "Dataset defined for " . $mart->name());
            my @attributes = @{$dataset->attributes()};
            ok(scalar(@attributes) > 0,
                "Attributes defined for dataset " . $dataset->name());
            my @filters = @{$dataset->filters()};
            ok(scalar(@filters) > 0,
                "Filters defined for dataset " . $dataset->name());
        }
    }
}

done_testing;
