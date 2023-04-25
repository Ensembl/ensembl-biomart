#!/usr/bin/perl
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
use Data::Dumper;
use File::Slurp;

use Bio::EnsEMBL::BioMart::Mart qw(genome_to_include);

my @divisions = ('vertebrates');

for my $division (@divisions) {
    my $species = genome_to_include($division,  $ENV{'BASE_DIR'});
    ok(scalar($species) > 0, "List Species not empty");
    ok(ref($species) eq 'ARRAY', "Species is an Array");
}

done_testing();

