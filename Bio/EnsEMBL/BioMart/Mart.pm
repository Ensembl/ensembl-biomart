#!/usr/bin/env perl
use warnings;
use strict;

package Bio::EnsEMBL::BioMart::Mart;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );

use base qw(Bio::EnsEMBL::BioMart::MartServiceObject);

sub new {
	my ( $proto, @args ) = @_;
    my $self = $proto->SUPER::new(@args);
	(  $self->{visible},
	   $self->{virtual_schema},
	   $self->{display_name} )
	  = rearrange( [ 'VISIBLE', 'VIRTUAL_SCHEMA',
					 'DISPLAY_NAME' ],
				   @args );
	return $self;
}

sub visible {
	my ($self) = @_;
	return $self->{visible};
}

sub virtual_schema {
	my ($self) = @_;
	return $self->{virtual_schema};
}

sub display_name {
	my ($self) = @_;
	return $self->{display_name};
}

sub datasets {
	my ($self) = @_;
	if(!defined $self->{datasets}) {
		$self->{datasets} = $self->service()->get_datasets($self);		
	}
	return $self->{datasets};
}

sub get_dataset_by_name {
	my ($self,$name) = @_;
	my $dataset;
	for my $ds (@{$self->datasets()}) {
		if($ds->name() eq $name) {
			$dataset = $ds;
			last;
		}		
	}
	return $dataset;
}

1;
