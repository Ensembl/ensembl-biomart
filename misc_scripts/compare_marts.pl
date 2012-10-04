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
use Bio::EnsEMBL::BioMart::MartService;
use Test::More;
use Data::Dumper;
use Getopt::Long;
use Carp;
use Pod::Usage;

my $opts = {};
GetOptions( $opts,        'old_uri=s', 'new_uri=s', 'old_mart=s',
			'new_mart=s', 'dataset=s', 'filters',   'attributes' );
if ( !defined $opts->{filters} && !defined $opts->{attributes} ) {
	$opts->{filters}    = 1;
	$opts->{attributes} = 1;
}

if (    !defined $opts->{old_mart}
	 || !defined $opts->{new_mart}
	 || !defined $opts->{old_uri}
	 || !defined $opts->{new_uri} )
{
	pod2usage(1);
}

diag "Creating connection to old service " . $opts->{old_uri};
my $old_srv =
  Bio::EnsEMBL::BioMart::MartService->new( -URL => $opts->{old_uri} );
ok( defined $old_srv, "Checking old service " . $opts->{old_uri} . " exists" );
diag "Creating connection to old service " . $opts->{new_uri};
my $new_srv =
  Bio::EnsEMBL::BioMart::MartService->new( -URL => $opts->{new_uri} );
ok( defined $new_srv, "Checking old service " . $opts->{new_uri} . " exists" );

diag "Retrieving marts";
my $old_mart = $old_srv->get_mart_by_name( $opts->{old_mart} );
ok( defined $old_mart, "Checking old mart " . $opts->{old_mart} . " exists" );
my $new_mart = $old_srv->get_mart_by_name( $opts->{new_mart} );
ok( defined $new_mart, "Checking new mart " . $opts->{old_mart} . " exists" );

diag "Hashing marts";
my $old_mart_hash = hash_mart($old_mart);
ok( defined $old_mart_hash,
	"Checking old mart " . $old_mart->name() . " can be hashed" );
ok( defined $old_mart_hash,
	"Checking old mart " . $old_mart->name() . " can be hashed" );
my $new_mart_hash = hash_mart($new_mart);
ok( defined $new_mart_hash,
	"Checking new mart " . $new_mart->name() . " can be hashed" );

# compare dataset lists
my @old_datasets;
my @new_datasets;
if ( defined $opts->{dataset} ) {
	my $old_dataset = $old_mart->get_dataset_by_name( $opts->{dataset} );
	my $new_dataset = $old_mart->get_dataset_by_name( $opts->{dataset} );
	ok( defined $old_dataset, "Old dataset " . $opts->{dataset} . " exists" );
	ok( defined $new_dataset, "New dataset " . $opts->{dataset} . " exists" );
	@old_datasets = ($old_dataset);
	@new_datasets = ($new_dataset);
} else {
	@old_datasets = @{ $old_mart->datasets() };
	@new_datasets = @{ $new_mart->datasets() };
	cmp_ok( scalar(@new_datasets),
			">=",
			scalar(@old_datasets),
"Checking we have the same number or more of new datasets compared to the old"
	);
}

for my $dataset (@new_datasets) {
	if ( $opts->{filters} == 1 ) {
		diag "Checking filters for new dataset " . $dataset->name();
		my $old_filters = $old_mart_hash->{ $dataset->name() }{filters};
		my $new_filters = $new_mart_hash->{ $dataset->name() }{filters};
		# old vs new
		for my $old_filter ( values %$old_filters ) {
			my $new_filter = $new_filters->{ $old_filter->name() };
			ok( defined $new_filter,
				"Checking for filter "
				  . $old_filter->name()
				  . " in new dataset "
				  . $dataset->name() );
			compare_filters( $new_filter, $old_filter )
			  if defined $new_filter;
		}
		# new vs old
		for my $new_filter ( values %$new_filters ) {
			my $old_filter = $old_filters->{ $new_filter->name() };
			ok( defined $new_filter,
				"Checking new filter "
				  . $new_filter->name()
				  . " in old dataset "
				  . $dataset->name() );
		}
	} ## end if ( $opts->{filters} ...)

	if ( $opts->{attributes} == 1 ) {
		diag "Checking attributes for new dataset " . $dataset->name();
		my $old_attributes = $old_mart_hash->{ $dataset->name() }{attributes};
		my $new_attributes = $new_mart_hash->{ $dataset->name() }{attributes};
		# old vs new
		for my $old_attribute ( values %$old_attributes ) {
			my $new_attribute = $new_attributes->{ $old_attribute->name() };
			ok( defined $new_attribute,
				"Checking for old attribute "
				  . $old_attribute->name()
				  . " in new dataset "
				  . $dataset->name() );
			compare_attributes( $new_attribute, $old_attribute )
			  if defined $new_attribute;
		}
		# new vs old
		for my $new_attribute ( values %$new_attributes ) {
			my $old_attribute = $old_attributes->{ $new_attribute->name() };
			ok( defined $new_attribute,
				"Checking for new attribute "
				  . $old_attribute->name()
				  . " in old dataset "
				  . $dataset->name() );
		}

	} ## end if ( $opts->{attributes...})
} ## end for my $dataset (@new_datasets)
done_testing;

sub hash_mart {
	my ($mart) = @_;
	my $hash;
	for my $dataset ( @{ $mart->datasets() } ) {
		for my $filter ( @{ $dataset->filters() } ) {
			$hash->{ $dataset->name() }{filters}{ $filter->name() } = $filter;
		}
		for my $attribute ( @{ $dataset->attributes() } ) {
			$hash->{ $dataset->name() }{attributes}{ $attribute->name() } =
			  $attribute;
		}
	}
	return $hash;
}

sub compare_filters {
	my ( $f1, $f2 ) = @_;
	compare_queryobjects( 'filter', $f1, $f2 );
	is( $f1->operator(), $f2->operator(),
		    "Checking operator for filter "
		  . $f1->name()
		  . " from dataset "
		  . $f1->dataset()->name() );
	is( $f1->type(), $f2->type(),
		    "Checking type for filter "
		  . $f1->name()
		  . " from dataset "
		  . $f1->dataset()->name() );
	is( scalar( @{ $f1->options() } ),
		scalar( @{ $f2->options() } ),
		"Checking numbers of options for filter "
		  . $f1->name()
		  . " from dataset "
		  . $f1->dataset()->name() );
	return;
}

sub compare_attributes {
	my ( $a1, $a2 ) = @_;
	compare_queryobjects( 'attribute', $a1, $a2 );
	is( $a1->types(), $a2->types(),
		"Checking types for attribute "
		  . $a1->name() . " from dataset " . $a1->dataset()->name() );
	return;
}

sub compare_queryobjects {
	my ( $type, $o1, $o2 ) = @_;
	for my $method (qw(display_name description page table column)) {
		is( $o1->$method(), $o2->$method(),
			    "Checking $method for $type "
			  . $o1->name()
			  . " from dataset "
			  . $o1->dataset()->name() );
	}
	return;
}

__END__

=head1 NAME

compare_marts.pl
=head1 SYNOPSIS

test_mart.pl -uri http://fungi.ensembl.org/biomart/martservice [-mart fungi_mart_15] [-dataset spombe_eg_gene] [-attributes] [-filters]

=head1 OPTIONS

=over 8

=item B<-old_uri>

Full URI of old mart service

=item B<-new_uri>

Full URI of new mart service

=item B<-mart>

Name of mart (all if not specified)

=item B<-dataset>

Name of dataset (all if not specified)

=item B<-filters>

Test only filters

=item B<-attributes>

Test only attributes

=back

=head1 DESCRIPTION



=head1 AUTHOR

dstaines

=head1 MAINTAINER

$Author$

=head1 VERSION

$Revision$

=cut
