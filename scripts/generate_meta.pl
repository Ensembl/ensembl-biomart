#!/usr/bin/env perl

=head1 LICENSE

Copyright [2016] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=head1 CONTACT

  Please email comments or questions to the public Ensembl
  developers list at <dev@ensembl.org>.

  Questions may also be sent to the Ensembl help desk at
  <helpdesk@ensembl.org>.
  
=head1 SYNOPSIS

generate_meta.pl [arguments]

  --user=user                      username for the BioMart database

  --pass=pass                      password for the BioMart database

  --host=host                      server for the BioMart database

  --port=port                      port for the BioMart database

  --dbname=name                      BioMart database name

  --template=file                    template file to load

  --template_name=name               name of the template

  --ds_basename=name                 mart dataset base name

  --max_dropdown=number              number of maximun allowed items in a filter dropdown

  --genomic_features_dbname          genomic_features_mart database name

  --verbose			     show debug info

  --help                              print help (this message)

=head1 DESCRIPTION

This script is used to populate the metatables of the supplied biomart database

=head1 AUTHOR

Dan Staines

=cut

use warnings;
use strict;
use XML::Simple;
use Data::Dumper;
use Carp;
use File::Slurp;
use Log::Log4perl qw(:easy);

use Bio::EnsEMBL::Utils::CliHelper;
use Bio::EnsEMBL::BioMart::MetaBuilder;

my $cli_helper = Bio::EnsEMBL::Utils::CliHelper->new();

my $optsd = $cli_helper->get_dba_opts();
# add the print option
push( @{$optsd}, "template_name:s" );
push( @{$optsd}, "ds_basename:s" );
push( @{$optsd}, "template:s" );
push( @{$optsd}, "genomic_features_dbname:s" );
push( @{$optsd}, "max_dropdown:i" );
push( @{$optsd}, "verbose" );

# process the command line with the supplied options plus a help subroutine
my $opts = $cli_helper->process_args( $optsd, \&pod2usage );
$opts->{template_name} ||= 'genes';
$opts->{ds_basename}   ||= 'gene';
$opts->{max_dropdown}  ||= 256;
$opts->{genomic_features_dbname} ||= '';
if ( $opts->{verbose} ) {
  Log::Log4perl->easy_init($DEBUG);
}
else {
  Log::Log4perl->easy_init($INFO);
}
my $logger = get_logger();
$logger->info( "Reading " . $opts->{template_name} . " template XML from " .
               $opts->{template} );
# load in template
my $template = read_file( $opts->{template} );
my $templ = XMLin( $template, KeepRoot => 1, KeyAttr => [] );

$logger->info("Opening connection to mart database");
my ($dba) = @{ $cli_helper->get_dbas_for_opts($opts) };

# for EG, there are specific sets of attributes/filters that we need to delete from the interface
my $delete = {};
if ( $dba->dbc()->dbname() !~ 'fungi' ) {
  $delete->{pbo}           = 1;
  $delete->{so}            = 1;
  $delete->{mod}           = 1;
  $delete->{fypo}          = 1;
  $delete->{pbo_closure}   = 1;
  $delete->{so_closure}    = 1;
  $delete->{mod_closure}   = 1;
  $delete->{fypo_closure}  = 1;
  $delete->{phi_extension} = 1;
}
if ( $dba->dbc()->dbname() !~ 'protists' ) {
  $delete->{phi_extension} = 1;
}
if ( $dba->dbc()->dbname() !~ 'plants' ) {
  $delete->{po}             = 1;
  $delete->{eo}             = 1;
  $delete->{to}             = 1;
  $delete->{gro}            = 1;
  $delete->{gr_tax}         = 1;
  $delete->{po_closure}     = 1;
  $delete->{eo_closure}     = 1;
  $delete->{to_closure}     = 1;
  $delete->{gro_closure}    = 1;
  $delete->{gr_tax_closure} = 1;
}

# build
my $builder =
  Bio::EnsEMBL::BioMart::MetaBuilder->new( -DBC    => $dba->dbc(),
                                           -DELETE => $delete,
                                           -BASENAME => $opts->{ds_basename},
                                           -MAX_DROPDOWN =>  $opts->{max_dropdown} );

$builder->build( $opts->{template_name}, $templ, $opts->{genomic_features_dbname} );

