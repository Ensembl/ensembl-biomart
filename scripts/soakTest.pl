#!/usr/bin/env perl
# Copyright [1999-2013] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


use strict;
use warnings;

use LWP::Simple;
use LWP::UserAgent;

use List::Util qw( min max );
use Scalar::Util qw( looks_like_number );

my $server = $ARGV[0];

local $| = 1;

foreach my $virtualSchema (
  split(
    /\n+/,
    get(
          "http://${server}/biomart/martservice?"
        . "type=listVirtualSchemas"
    ) ) )
{
  printf( "Virtual Schema = '%s'\n", $virtualSchema );

  foreach my $mart (
    split(
      /\n+/,
      get(
            "http://${server}/biomart/martservice?"
          . "type=listMarts&"
          . "virtualSchema=${virtualSchema}"
      ) ) )
  {
    printf( "Mart = '%s'\n", $mart );

    foreach my $dataset (
      split(
        /\n+/,
        get(
              "http://${server}/biomart/martservice?"
            . "type=listDatasets&"
            . "virtualSchema=${virtualSchema}&"
            . "mart=${mart}"
        ) ) )
    {
      foreach my $interface (
        split(
          /\n+/,
          get(
                "http://${server}/biomart/martservice?"
              . "type=listInterfaces&"
              . "virtualSchema=${virtualSchema}&"
              . "mart=${mart}&"
              . "dataset=${dataset}"
          ) ) )
      {
        print "Processing dataset "
          . "$virtualSchema->$mart->$dataset->$interface...\n";
        # Begin test for this interface.

        # Build a list of all possible attribute lumps.
        my @attributeLumps = ();
        foreach my $ap (
          split(
            /\n+/,
            get(
                  "http://${server}/biomart/martservice?"
                . "type=listAttributePages&"
                . "virtualSchema=${virtualSchema}&"
                . "mart=${mart}&"
                . "dataset=${dataset}&"
                . "interface=${interface}"
            ) ) )
        {
          my ( $apn, $apMax ) = split( /\s+/, $ap );
          foreach my $ag (
            split(
              /\n+/,
              get(
                    "http://${server}/biomart/martservice?"
                  . "type=listAttributeGroups&"
                  . "virtualSchema=${virtualSchema}&"
                  . "mart=${mart}&"
                  . "dataset=${dataset}&"
                  . "interface=${interface}&"
                  . "attributePage=${apn}"
              ) ) )
          {
            my ( $agn, $agMax ) = split( /\s+/, $ag );
            foreach my $ac (
              split(
                /\n+/,
                get(
                      "http://${server}/biomart/martservice?"
                    . "type=listAttributeCollections&"
                    . "virtualSchema=${virtualSchema}&"
                    . "mart=${mart}&"
                    . "dataset=${dataset}&"
                    . "interface=${interface}&"
                    . "attributePage=${apn}&"
                    . "attributeGroup=${agn}"
                ) ) )
            {
              my ( $acn, $acMax ) = split( /\s+/, $ac );
              my $maxSelect = 99999999;

              $maxSelect = min( $maxSelect, $apMax )
                if ( looks_like_number($apMax) );
              $maxSelect = min( $maxSelect, $agMax )
                if ( looks_like_number($agMax) );
              $maxSelect = min( $maxSelect, $acMax )
                if ( looks_like_number($acMax) );

              my @attributeLump = ();
              foreach my $a (
                split(
                  /\n+/,
                  get(
                        "http://${server}/biomart/martservice?"
                      . "type=listAttributes&"
                      . "virtualSchema=${virtualSchema}&"
                      . "mart=${mart}&"
                      . "dataset=${dataset}&"
                      . "interface=${interface}&"
                      . "attributePage=${apn}&"
                      . "attributeGroup=${agn}&"
                      . "attributeCollection=${acn}"
                  ) ) )
              {
                # Found an attribute!
                push @attributeLump, $a;
                if ( scalar @attributeLump == $maxSelect ) {
                  my @copy = @attributeLump;
                  push @attributeLumps, \@copy;
                  @attributeLump = ();
                }
              }
              if ( scalar @attributeLump ) {
                push @attributeLumps, \@attributeLump;
              }
            } ## end foreach my $ac ( split( /\n+/...
          } ## end foreach my $ag ( split( /\n+/...
        } ## end foreach my $ap ( split( /\n+/...

        # Build a list of all possible filters.
        my @filters = ();
        foreach my $fp (
          split(
            /\n+/,
            get(
                  "http://${server}/biomart/martservice?"
                . "type=listFilterPages&"
                . "virtualSchema=${virtualSchema}&"
                . "mart=${mart}&"
                . "dataset=${dataset}&"
                . "interface=${interface}"
            ) ) )
        {
          foreach my $fg (
            split(
              /\n+/,
              get(
                    "http://${server}/biomart/martservice?"
                  . "type=listFilterGroups&"
                  . "virtualSchema=${virtualSchema}&"
                  . "mart=${mart}&"
                  . "dataset=${dataset}&"
                  . "interface=${interface}&"
                  . "filterPage=${fp}"
              ) ) )
          {
            foreach my $fc (
              split(
                /\n+/,
                get(
                      "http://${server}/biomart/martservice?"
                    . "type=listFilterCollections&"
                    . "virtualSchema=${virtualSchema}&"
                    . "mart=${mart}&"
                    . "dataset=${dataset}&"
                    . "interface=${interface}&"
                    . "filterPage=${fp}&"
                    . "filterGroup=${fg}"
                ) ) )
            {
              foreach my $f (
                split(
                  /\n+/,
                  get(
                        "http://${server}/biomart/martservice?"
                      . "type=listFilters&"
                      . "virtualSchema=${virtualSchema}&"
                      . "mart=${mart}&"
                      . "dataset=${dataset}&"
                      . "interface=${interface}&"
                      . "filterPage=${fp}&"
                      . "filterGroup=${fg}&"
                      . "filterCollection=${fc}"
                  ) ) )
              {
                my ( $filter, $filterType ) = split( /\s+/, $f );
                my @options = ();
                foreach my $option (
                  split(
                    /\n+/,
                    get(
                          "http://${server}/biomart/martservice?"
                        . "type=listFilterOptions&"
                        . "virtualSchema=${virtualSchema}&"
                        . "mart=${mart}&"
                        . "dataset=${dataset}&"
                        . "interface=${interface}&"
                        . "filterPage=${fp}&"
                        . "filterGroup=${fg}&"
                        . "filterCollection=${fc}&"
                        . "filter=${filter}"
                    ) ) )
                {
                  push @options, $option;
                }
                # Found a filter!
                my @filterDef = ( $filter, $filterType, \@options );
                push @filters, \@filterDef;
              } ## end foreach my $f ( split( /\n+/...
            } ## end foreach my $fc ( split( /\n+/...
          } ## end foreach my $fg ( split( /\n+/...
        } ## end foreach my $fp ( split( /\n+/...

        # Run a query once per attribute lump
        # - if any fails, break it down.
        print "   ...testing attributes ...\n";
        my $stepSize = 100.0/scalar(@attributeLumps);
        my $progress = 0.0;
        foreach my $attributeLump (@attributeLumps) {
          my $xml =
              "<Query limitStart=\"0\" "
            . "limitSize=\"1\" "
            . "virtualSchemaName=\"${virtualSchema}\" "
            . "formatter=\"TSV\" "
            . "header=\"0\" "
            . "uniqueRows=\"0\" "
            . "count=\"\" "
            . "datasetConfigVersion=\"0.6\">"
            . "<Dataset name=\"${dataset}\" "
            . "interface=\"${interface}\">";
          foreach my $attribute (@$attributeLump) {
            $xml .= "<Attribute name=\"${attribute}\"/>";
          }
          $xml .= "</Dataset></Query>";

          # Construct a query.
          my $request =
            HTTP::Request->new( "POST",
            "http://${server}/biomart/martservice",
            HTTP::Headers->new(), 'query=' . $xml . "\n" );
          my $ua = LWP::UserAgent->new;
          my $response;
          $ua->request(
            $request,
            sub {
              my ( $data, $response ) = @_;
              if ( $response->is_success ) {
                if ( $data =~
                  m/BioMart::Exception|Validation Error:|Serious Error:/
                  )
                {
                  # Query failed.
                  print "\n   QUERY FAILED!\n   $xml\n   $data";
                } else {
                  # Success.
                }
              } else {
                print "Problems with the web server: "
                  . $response->status_line;
              }
            },
            1000
          );
          $progress += $stepSize;
          print "\n   ..." . sprintf( "%d", $progress ) . "%      ";
        } ## end foreach my $attributeLump (...
        print "\n   ...100%      ";
        print "\n";

        # Run a query once per filter with the first attribute lump.
        print "   ...testing filters ...\n";
        $stepSize = 100.0/scalar @filters;
        $progress = 0.0;
        foreach my $filterRef (@filters) {
          my ( $filter, $filterType, $options ) = @$filterRef;
          my $opt = @$options[0];
          $opt = "0" unless $opt;
          my $xml =
              "<Query limitStart=\"0\" "
            . "limitSize=\"1\" "
            . "virtualSchemaName=\"${virtualSchema}\" "
            . "formatter=\"TSV\" "
            . "header=\"0\" "
            . "uniqueRows=\"0\" "
            . "count=\"\" "
            . "datasetConfigVersion=\"0.6\">"
            . "<Dataset name=\"${dataset}\" "
            . "interface=\"${interface}\">";
          foreach my $attribute ( @{ $attributeLumps[0] } ) {
            $xml .= "<Attribute name=\"${attribute}\"/>";
          }
          if ( $filterType eq "boolean" ) {
            $xml .= "<Filter name=\"${filter}\" excluded=\"${opt}\"/>"
              . "</Dataset></Query>";
          } elsif ( $filterType eq "boolean_list" ) {
            $xml .= "<Filter name=\"${opt}\" excluded=\"0\"/>"
              . "</Dataset></Query>";
          } elsif ( $filterType eq "id_list" ) {
            $xml .= "<Filter name=\"${opt}\" value=\"0\"/>"
              . "</Dataset></Query>";
          } else {
            $xml .= "<Filter name=\"${filter}\" value=\"${opt}\"/>"
              . "</Dataset></Query>";
          }
          # Construct a query.
          my $request =
            HTTP::Request->new( "POST",
            "http://${server}/biomart/martservice",
            HTTP::Headers->new(), 'query=' . $xml . "\n" );
          my $ua = LWP::UserAgent->new;
          my $response;
          $ua->request(
            $request,
            sub {
              my ( $data, $response ) = @_;
              if ( $response->is_success ) {
                if ( $data =~
                  m/BioMart::Exception|Validation Error:|Serious Error:/
                  )
                {
                  # Query failed.
                  print "\n   QUERY FAILED!\n   $xml\n   $data";
                } else {
                  # Success.
                }
              } else {
                print "Problems with the web server: "
                  . $response->status_line;
              }
            },
            1000
          );
          $progress += $stepSize;
          print "\n   ..." . sprintf( "%d", $progress ) . "%      ";
        } ## end foreach my $filterRef (@filters)
        print "\n   ...100%      ";
        print "\n";

        # End test for this interface.
      } ## end foreach my $interface ( split...
    } ## end foreach my $dataset ( split...
  } ## end foreach my $mart ( split( /\n+/...
} ## end foreach my $virtualSchema (...
