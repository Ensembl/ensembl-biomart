#!/usr/bin/env perl
use warnings;
use strict;

package Bio::EnsEMBL::BioMart::QueryObject;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );
use base qw(Bio::EnsEMBL::BioMart::MartServiceObject);

sub new {
	my ( $proto, @args ) = @_;
	    my $self = $proto->SUPER::new(@args);
	(  $self->{display_name},$self->{description},
	   $self->{page}, $self->{table}, $self->{column}, $self->{dataset} )
	  = rearrange( [ 'DISPLAY_NAME', 'DESCRIPTION', 'PAGE',
					 'TABLE','COLUMN', 'DATASET' ],
				   @args );				   
	return $self;
}

sub display_name {
	my ($self) = @_;
	return $self->{display_name};
}

sub description {
	my ($self) = @_;
	return $self->{description};
}

sub page {
	my ($self) = @_;
	return $self->{page};
}

sub table {
	my ($self) = @_;
	return $self->{table};
}
sub column {
	my ($self) = @_;
	return $self->{table};
}

sub dataset {
	my ($self) = @_;
	return $self->{dataset};
}


1;
