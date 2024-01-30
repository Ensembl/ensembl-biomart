#!/usr/bin/env perl
=head1 LICENSE

Copyright [2009-2024] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

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
