#!/usr/bin/env perl
=head1 LICENSE

Copyright [2009-2025] EMBL-European Bioinformatics Institute

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
