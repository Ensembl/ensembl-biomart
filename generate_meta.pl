#!/usr/bin/env perl
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
push( @{$optsd}, "template:s" );
push( @{$optsd}, "verbose" );

# process the command line with the supplied options plus a help subroutine
my $opts = $cli_helper->process_args( $optsd, \&pod2usage );
$opts->{template_name} ||= 'genes';
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

# for EG, there are specific sets of attributes/filters that we need to unhide in the interface
my $unhide = {};
if ( $dba->dbc()->dbname() =~ 'fungi' ) {
  $unhide->{pbo}           = 1;
  $unhide->{so}            = 1;
  $unhide->{mod}           = 1;
  $unhide->{fypo}          = 1;
  $unhide->{pbo_closure}   = 1;
  $unhide->{so_closure}    = 1;
  $unhide->{mod_closure}   = 1;
  $unhide->{fypo_closure}  = 1;
  $unhide->{phi_extension} = 1;
}
elsif ( $dba->dbc()->dbname() =~ 'protists' ) {
  $unhide->{phi_extension} = 1;
}
elsif ( $dba->dbc()->dbname() =~ 'plants' ) {
  $unhide->{po}             = 1;
  $unhide->{eo}             = 1;
  $unhide->{to}             = 1;
  $unhide->{gro}            = 1;
  $unhide->{gr_tax}         = 1;
  $unhide->{po_closure}     = 1;
  $unhide->{eo_closure}     = 1;
  $unhide->{to_closure}     = 1;
  $unhide->{gro_closure}    = 1;
  $unhide->{gr_tax_closure} = 1;
}

# build
my $builder =
  Bio::EnsEMBL::BioMart::MetaBuilder->new( -DBC    => $dba->dbc(),
                                           -UNHIDE => $unhide );

$builder->build( $opts->{template_name}, $templ );

