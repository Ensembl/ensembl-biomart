#!/usr/bin/env perl
=head1 LICENSE

Copyright [2009-2022] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::BioMart::MartService;
use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );
use Bio::EnsEMBL::Utils::Scalar qw(check_ref assert_ref);
use Bio::EnsEMBL::BioMart::Mart;
use Bio::EnsEMBL::BioMart::DataSet;
use Bio::EnsEMBL::BioMart::Filter;
use Bio::EnsEMBL::BioMart::Attribute;
use LWP::Simple;
use LWP::UserAgent;
use XML::Simple;
use Data::Dumper;
use Carp;

sub new {
  my ($proto, @args) = @_;
  my $self = bless {}, $proto;
  ($self->{url}) = rearrange(['URL', 'SERVER'], @args);
  if (!defined $self->{url}) {
	if (defined $self->{server}) {
	  $self->{url} = sprintf('http://%s/biomart/martservice', $self->{server});
	} else {
	  throw "No URL or SERVER defined";
	}
  }
  return $self;
}

sub get_registry {
  my ($self) = @_;
  if (!defined $self->{registry}) {
	# get xml definition
	my $reg_xml = $self->do_get({type => 'registry'});
	# parse into a hash (keys are mart names)
	$self->{registry} = XMLin($reg_xml)->{MartURLLocation};
  }
  return $self->{registry};
}

sub get_marts {
  my ($self) = @_;
  if (!defined $self->{marts}) {
	my $reg = $self->get_registry();
	while (my ($name, $def) = each %$reg) {
	  push @{$self->{marts}},
		Bio::EnsEMBL::BioMart::Mart->new(-SERVICE        => $self,
										 -NAME           => $name,
										 -VISIBLE        => $def->{visible},
										 -VIRTUAL_SCHEMA => $def->{serverVirtualSchema},
										 -DISPLAY_NAME   => $def->{displayName});
	}
  }
  return $self->{marts};
}

sub get_mart_by_name {
  my ($self, $name) = @_;
  my $mart;
  for my $m (@{$self->get_marts()}) {
	if ($name eq $m->name()) {
	  $mart = $m;
	  last;
	}
  }
  return $mart;
}

sub get_datasets {
  my ($self, $mart) = @_;
  assert_ref($mart, 'Bio::EnsEMBL::BioMart::Mart');
  my $response = $self->do_get({type              => 'datasets',
								mart              => $mart->name(),
								virtualSchemaName => $mart->virtual_schema()});
  my $datasets = [];
  open my $fh, '<', \$response or throw "Could not break ";
  while (<$fh>) {
	chomp;
	my ($name, $des, $ass, $template) = (split('\t', $_))[1, 2, 4, 7];
	if (defined $name) {
	  push @$datasets,
		Bio::EnsEMBL::BioMart::DataSet->new(-SERVICE     => $self,
											-NAME        => $name,
											-DESCRIPTION => $des,
											-VERSION     => $ass,
											-INTERFACE   => $template,
											-MART        => $mart);
	}
  }
  close $fh or throw $!;
  return $datasets;
}

sub get_attributes {
  my ($self, $dataset) = @_;
  my $attributes = [];
  assert_ref($dataset, 'Bio::EnsEMBL::BioMart::DataSet');
  my $response = $self->do_get({type          => 'attributes',
								virtualSchema => $dataset->mart()->virtual_schema(),
								mart          => $dataset->mart()->name(),
								dataset       => $dataset->name()});

  open my $fh, '<', \$response or throw "Could not break ";
  while (<$fh>) {
	chomp;
	# ensembl_gene_id	Ensembl Gene ID	Ensembl Stable ID of the Gene	feature_page	html,txt,csv,tsv,xls	spombe_eg_gene__gene__main	stable_id_1023
	throw "$_" if m/does not exist/;
	my ($name, $display_name, $des, $page, $types, $table, $column) = (split('\t', $_))[0, 1, 3, 4, 5, 6];
	if (defined $name) {
	  push @$attributes,
		Bio::EnsEMBL::BioMart::Attribute->new(-SERVICE      => $self,
											  -NAME         => $name,
											  -DISPLAY_NAME => $display_name,
											  -DESCRIPTION  => $des,
											  -PAGE         => $page,
											  -TYPES        => $types,
											  -TABLE        => $table,
											  -COLUMN       => $column,
											  -DATASET      => $dataset);
	}
  }
  return $attributes;
} ## end sub get_attributes

sub get_filters {
  my ($self, $dataset) = @_;
  my $filters = [];
  my $response = $self->do_get({
	 type          => 'filters',
	 mart          => $dataset->mart()->name(),
	 virtualSchema => $dataset->mart()->virtual_schema(),
	 dataset       => $dataset->name()});
  print Dumper("test response", $response);
  open my $fh, '<', \$response or throw "Could not break ";
  while (<$fh>) {
	chomp;
	# chromosome_name	Chromosome name	[AB325691,I,II,III,MT,MTR]		filters	list	=	spombe_eg_gene__gene__main	name_105
	throw "$_" if m/does not exist/;
	my ($name, $display_name, $opt_str, $des, $page, $type, $operator, $table, $column) = split('\t', $_);
	$opt_str ||= '';
	$opt_str =~ s/\[(.*)\]/$1/;
	  print Dumper("opts ", $opt_str);
	my @options = split(',', $opt_str);
	if (defined $name) {
	  push @$filters,
		Bio::EnsEMBL::BioMart::Filter->new(-SERVICE      => $self,
										   -NAME         => $name,
										   -DISPLAY_NAME => $display_name,
										   -DESCRIPTION  => $des,
										   -PAGE         => $page,
										   -TYPE         => $type,
										   -OPERATOR     => $operator,
										   -OPTIONS      => \@options,
										   -TABLE        => $table,
										   -COLUMN       => $column,
										   -DATASET      => $dataset);
	}
  }
	print Dumper("filters", $filters);
  return $filters;
} ## end sub get_filters

sub do_query {
  my ($self, $mart, $dataset, $attributes, $filters, $options) = @_;
  my $text = $self->do_query_text($mart, $dataset, $attributes, $filters, $options);
  my $query_results = [];
  if (defined $text) {
	for my $line (split("\n", $text)) {
	  chomp $line;
	  push @$query_results, [split("\t", $line)];
	}
  }
  return $query_results;
}

sub do_query_text {
  my ($self, $mart, $dataset, $attributes, $filters, $options) = @_;
  $options    ||= {};
  $attributes ||= [];
  $filters    ||= [];
  my $query = {limitStart => $options->{limitSize} || '0',
			   virtualSchemaName        => $mart->virtual_schema(),
			   formatter            => $options->{formatter} || 'TSV',
			   header               => $options->{header} || '0',
			   uniqueRows           => $options->{uniqueRows} || '0',
			   count                => $options->{count} || '0',
			   datasetConfigVersion => '0.6',
			   limitSize            => $options->{limitSize},
			   Dataset              => {
						   name      => $dataset->name(),
						   interface => $dataset->interface(),
						   Attribute => [],
						   Filter    => []}};
  for my $attribute (@{$attributes}) {
	push @{$query->{Dataset}{Attribute}}, {name => $attribute->name()};
  }
  for my $f (@{$filters}) {
	my $filter = $f->{filter};
	$f->{excluded} ||= 0;
	if (!defined $filter->type()) {
	  throw "Type for filter " . $filter->name() . " is not defined";
	} elsif (   $filter->type() eq 'boolean'
			 || $filter->type() eq 'boolean_list')
	{
	  push @{$query->{Dataset}{Filter}}, {name => $filter->name(), excluded => $f->{excluded}};
	} else {
	  push @{$query->{Dataset}{Filter}}, {name => $filter->name(), value => $f->{value}};
	}
  }
  my $xml = XMLout($query, RootName => "Query", NoIndent => 1);
  return $self->do_post({query => $xml});
} ## end sub do_query_text

sub do_post {
  my ($self, $arguments) = @_;
  my $post_str = "";
  while (my ($key, $value) = each %$arguments) {
	$post_str .= $key . '=' . $value . "\n";
  }
  my $request = HTTP::Request->new("POST", $self->{url}, HTTP::Headers->new(), $post_str);
  my $ua = LWP::UserAgent->new();
  my $output;
  my $response = $ua->request($request);
  if ($response->is_success()) {
	$output = $response->decoded_content();
	$self->check_output($output);
  } else {
	croak "Server error: " . $response->status_line;
  }
  return $output;
}

sub do_get {
  my ($self, $arguments) = @_;
  my @args = ();
  while (my ($key, $value) = each %$arguments) {
	push @args, "$key=$value";
  }
  my $uri = $self->{url} . '?' . join('&', @args);
  my $output = get($uri);
  $self->check_output($output);
  return get($uri);
}

sub check_output {
  my ($self, $output) = @_;
  croak $output
	if ($output =~ m/(BioMart::Exception|Validation Error:|Serious Error:)/);
  return;
}

1;
