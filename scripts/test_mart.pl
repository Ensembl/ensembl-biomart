#!/bin/env perl

=pod
=head1 LICENSE

  Copyright (c) 1999-2011 The European Bioinformatics Institute and
  Genome Research Limited.  All rights reserved.

  This software is distributed under a modified Apache license.
  For license details, please see

    http://www.ensembl.org/info/about/code_licence.html

=head1 CONTACT

  Please email comments or questions to the public Ensembl
  developers list at <dev@ensembl.org>.

  Questions may also be sent to the Ensembl help desk at
  <helpdesk@ensembl.org>.
 
=cut

use warnings;
use strict;
use FindBin;
use lib "$FindBin::Bin/../modules";
use Bio::EnsEMBL::BioMart::MartService;
use Test::More;
use Try::Tiny;
use Data::Dumper;
use Getopt::Long;
use Carp;
use Pod::Usage;

# Get parameters
my $opts = {};
GetOptions($opts,
  'uri=s',
  'server=s',
  'mart=s',
  'dataset=s',
  'filters',
  'attributes',
  'verbose|v',
  'output_file=s'
);
if ( !defined $opts->{filters} && !defined $opts->{attributes} ) {
	$opts->{filters}    = 1;
	$opts->{attributes} = 1;
}

if ( !defined $opts->{uri} ) {
	if ( defined $opts->{server} ) {
		$opts->{uri} = 'http://' . $opts->{server} . "/biomart/martservice";
	} else {
		pod2usage(1);
	}
}

if ( !defined $opts->{verbose} ) {
	Test::More->builder->output( $opts->{output_file} || './test_mart.out' );
}

# Begin testing
diag "Testing server $opts->{uri}";
my $srv = Bio::EnsEMBL::BioMart::MartService->new( -URL => $opts->{uri} );

BAIL("Server $opts->{uri} does not exist") if ( !defined $srv );

my @marts = ();
if ( defined $opts->{mart} ) {
	my $mart = $srv->get_mart_by_name( $opts->{mart} );
	ok( defined $mart, "Checking that mart " . $opts->{mart} . " exists" );
	@marts = ($mart) if $mart;
} else {
	@marts = @{ $srv->get_marts() };
}

for my $mart (@marts) {
	diag( "Testing " . $mart->name() );
	my @datasets = ();
	if ( defined $opts->{dataset} ) {
		my $dataset = $mart->get_dataset_by_name( $opts->{dataset} );
		ok( defined $dataset,
			"Checking that dataset " . $opts->{dataset} . " exists" );
		@datasets = ($dataset) if $dataset;
	} else {
		@datasets = @{ $mart->datasets() };
	}
	for my $dataset (@datasets) {
		try {
			test_dataset($dataset);
		}
		catch {
			diag($_);
			fail( "Testing " . $dataset->name() . " failed unexpectedly" );
		}
	}
}

Test::More->builder->reset_outputs;
done_testing;

sub test_dataset {
	my ($dataset) = @_;
	diag( "Testing " . $dataset->name() );
	my $attributes = $dataset->attributes();
	if ( defined $opts->{attributes} ) {
		for my $attribute (@$attributes) {
			test_attribute( $dataset, $attribute );
		}
	}
	my $filters = $dataset->filters();
	if ( defined $opts->{filters} ) {
		for my $filter (@$filters) {
			test_filter( $dataset, $filter, $attributes->[0] );
		}
	}
	return;
}

sub test_attribute {
	my ( $dataset, $attribute ) = @_;
	if ( defined $attribute->column() ) {
		eval {
			my $res =
			  $srv->do_query( $dataset->mart(), $dataset, [$attribute], [],
							  { limitSize => 1 } );
			ok( defined $res,
				"Attribute "
				  . $attribute->name()
				  . " from dataset "
				  . $dataset->name() . " in "
				  . $dataset->mart()->name() );
		};
		if ($@) {
			fail(   "Could not use attribute "
				  . $attribute->name()
				  . " from dataset "
				  . $dataset->name() . " in "
				  . $dataset->mart()->name()
				  . ":$@" );
		}
	} else {
		diag(   "Skipping attribute "
			  . $attribute->name()
			  . " as it has no column defined" );
	}
	return;
} ## end sub test_attribute

sub test_filter {
	my ( $dataset, $filter, $attribute ) = @_;
	my $val;
	my $options = $filter->options();
	if ( defined $options && scalar(@$options) > 0 ) {
		$val = $options->[0];
	} else {
		$val = '0';
	}
	eval {
		my $res = $srv->do_query( $dataset->mart(),
								  $dataset,
								  [$attribute],
								  [ { filter => $filter, value => $val } ],
								  { limitSize => 1 } );
		ok( defined $res,
			"Filter "
			  . $filter->name()
			  . " from dataset "
			  . $dataset->name() . " in "
			  . $dataset->mart()->name() );
	};
	if ($@) {
		fail(   "Could not use filter "
			  . $filter->name()
			  . " from dataset "
			  . $dataset->name() . " in "
			  . $dataset->mart()->name()
			  . ":$@" );
	}
	return;

} ## end sub test_filter

__END__

=head1 NAME

test_mart.pl

=head1 SYNOPSIS

test_mart.pl -uri http://fungi.ensembl.org/biomart/martservice [-mart fungi_mart_15] [-dataset spombe_eg_gene] [-attributes] [-filters] [-verbose] [-output_file ./test_mart.out]

=head1 OPTIONS

=over 8

=item B<-uri>

Full URI of mart service

=item B<-server>

Hostname of mart server (used to produce a URI if not specified)

=item B<-mart>

Name of mart (all if not specified)

=item B<-dataset>

Name of dataset (all if not specified)

=item B<-filters>

Test only filters

=item B<-attributes>

Test only attributes

=item B<-verbose>

Print output to STDOUT

=item B<-output_file>

Print output to a file. Not activated if -verbose. If neither -verbose not -output_file is provided, print to ./test_mart.out.

=back

=head1 DESCRIPTION

This program uses the martservice interface to exhaustively test all attributes and filters of a mart.

=head1 AUTHOR

dstaines

=head1 MAINTAINER

$Author$

=head1 VERSION

$Revision$

=cut
