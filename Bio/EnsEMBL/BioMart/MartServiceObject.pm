#!/usr/bin/env perl
use warnings;
use strict;

package Bio::EnsEMBL::BioMart::MartServiceObject;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );

sub new {
	my ( $proto, @args ) = @_;
	my $self = bless {}, $proto;
	(  $self->{service}, $self->{name} )
	  = rearrange( [ 'SERVICE','NAME' ],
				   @args );
	return $self;
}

sub name {
	my ($self) = @_;
	return $self->{name};
}

sub service {
	my ($self) = @_;
	return $self->{service};
}

1;
