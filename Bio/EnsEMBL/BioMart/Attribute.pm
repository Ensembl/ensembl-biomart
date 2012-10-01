#!/usr/bin/env perl
use warnings;
use strict;

package Bio::EnsEMBL::BioMart::Attribute;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );

use base qw(Bio::EnsEMBL::BioMart::QueryObject);

sub new {
	my ( $proto, @args ) = @_;
	    my $self = $proto->SUPER::new(@args);
	( $self->{types} )
	  = rearrange( [ 'TYPES' ],
				   @args );
	return $self;
}

sub types {
	my ($self) = @_;
	return $self->{types};
}

1;
