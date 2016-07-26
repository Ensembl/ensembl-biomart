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
push(@{$optsd}, "template_name:s");
push(@{$optsd}, "template:s");
push(@{$optsd}, "verbose");

# process the command line with the supplied options plus a help subroutine
my $opts = $cli_helper->process_args($optsd, \&pod2usage);
$opts->{template_name} ||= 'genes';
if ( $opts->{verbose} ) {
  Log::Log4perl->easy_init($DEBUG);
}
else {
  Log::Log4perl->easy_init($INFO);
}
my $logger = get_logger();

$logger->info("Reading ".$opts->{template_name}." template XML from ".$opts->{template});
# load in template
my $template = read_file($opts->{template});
my $templ    = XMLin( $template, KeepRoot => 1, KeyAttr => [] );

$logger->info("Opening connection to mart database");
my ($dba) = @{$cli_helper->get_dbas_for_opts($opts)};

# build
my $builder = Bio::EnsEMBL::BioMart::MetaBuilder->new(-DBC=>$dba->dbc());

$builder->build($opts->{template_name},$templ);

