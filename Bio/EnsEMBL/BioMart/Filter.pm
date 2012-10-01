#!/usr/bin/env perl
use warnings;
use strict;

package Bio::EnsEMBL::BioMart::Filter;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );

use base qw(Bio::EnsEMBL::BioMart::QueryObject);

sub new {
	my ( $proto, @args ) = @_;
	my $self = $proto->SUPER::new(@args);
	( $self->{operator}, $self->{options} ) =
	  rearrange( [ 'OPERATOR', 'OPTIONS' ], @args );
	return $self;
}

sub operator {
	my ($self) = @_;
	return $self->{operator};
}

sub options {
	my ($self) = @_;
	return $self->{options};
}

1;
