#!/usr/bin/env perl
use warnings;
use strict;

package Bio::EnsEMBL::BioMart::DataSet;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );

use base qw(Bio::EnsEMBL::BioMart::MartServiceObject);

sub new {
	my ( $proto, @args ) = @_;
	my $self = $proto->SUPER::new(@args);
	(  $self->{description}, $self->{version},
	   $self->{interface},   $self->{mart}
	) = rearrange( [ 'DESCRIPTION', 'VERSION', 'INTERFACE', 'MART' ], @args );
	return $self;
}

sub description {
	my ($self) = @_;
	return $self->{description};
}

sub version {
	my ($self) = @_;
	return $self->{version};
}

sub interface {
	my ($self) = @_;
	return $self->{interface};
}

sub mart {
	my ($self) = @_;
	return $self->{mart};
}

sub filters {
	my ($self) = @_;
	if ( !defined $self->{filters} ) {
		$self->{filters} = $self->service()->get_filters($self);
	}
	return $self->{filters};
}

sub get_filter_by_name {
	my ($self,$name) = @_;
	my $filter;
	for my $f (@{$self->filters()}) {
		if($f->name() eq $name) {
			$filter = $f;
			last;
		}		
	}
	return $filter;
}


sub attributes {
	my ($self) = @_;
	if ( !defined $self->{attributes} ) {
		$self->{attributes} =
		  $self->service()->get_attributes($self);
	}
	return $self->{attributes};
}

sub get_attribute_by_name {
	my ($self,$name) = @_;
	my $attribute;
	for my $a (@{$self->attributes()}) {
		if($a->name() eq $name) {
			$attribute = $a;
			last;
		}		
	}
	return $attribute;
}


sub run_query {
	my ( $self, $filters, $attributes ) = @_;
	return $self->service()->run_query( $self, $filters, $attributes );
}

1;
