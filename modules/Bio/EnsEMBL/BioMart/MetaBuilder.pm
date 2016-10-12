#!/usr/bin/env perl

=head1 LICENSE

Copyright [2016] EMBL-European Bioinformatics Institute

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

=pod

=head1 CONTACT

  Please email comments or questions to the public Ensembl
  developers list at <http://lists.ensembl.org/mailman/listinfo/dev>.

  Questions may also be sent to the Ensembl help desk at
  <http://www.ensembl.org/Help/Contact>.

=head1 NAME

Bio::EnsEMBL::BioMart::MetaBuilder

=head1 DESCRIPTION

A module which creates and populates the metatables for a biomart database using a supplied template file

=cut

use warnings;
use strict;

package Bio::EnsEMBL::BioMart::MetaBuilder;

use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );
use IO::Compress::Gzip qw(gzip);
use Carp;
use XML::Simple;
use Data::Dumper;
use Sort::Naturally;

use Log::Log4perl qw/get_logger/;

my $logger = get_logger();

=head1 CONSTRUCTOR
=head2 new
 Arg [-DBC] : 
    Bio::EnsEMBL::DBSQL::DBConnection : instance for the target mart (required)
 Arg [-VERSION] :
    Integer : EG/E version (by default the last number in the mart name)
 Arg [-MAX_DROPDOWN] :
    Integer : Maximum number of items to show in a dropdown menu (default 256)
 Arg [-UNHIDE] :
    Hashref : attributes/filters to unhide for this mart (mainly domain specific ontologies)
 Arg [-BASENAME] :
    String : Base name of dataset - default is "gene"
  Example    : $b = Bio::EnsEMBL::BioMart::MetaBuilder->new(...);
  Description: Creates a new builder object
  Returntype : Bio::EnsEMBL::BioMart::MetaBuilder
  Exceptions : none
  Caller     : general
  Status     : Stable

=cut

sub new {
  my ( $proto, @args ) = @_;
  my $self = bless {}, $proto;
  ( $self->{dbc}, $self->{version}, $self->{max_dropdown}, $self->{unhide}, $self->{basename}  ) =
    rearrange( [ 'DBC', 'VERSION', 'MAX_DROPDOWN', 'UNHIDE', 'BASENAME' ],
               @args );

  if ( !defined $self->{version} ) {
    ( $self->{version} = $self->{dbc}->dbname() ) =~ s/.*_([0-9]+)$/$1/;
  }
  $self->{max_dropdown} ||= 256;
  $self->_load_info();
  return $self;
}

=head1 METHODS
=head2 build
  Description: Build metadata for the supplied  mart
  Arg        : name of template (e.g. gene)
  Arg        : template as hashref
  Returntype : none
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut
sub build {
  my ( $self, $template_name, $template, $genomic_features_mart ) = @_;
  # create base metatables
  $self->create_metatables( $template_name, $template );
  # read datasets
  my $datasets = $self->get_datasets();
  my $n        = 1;
  for my $dataset ( @{$datasets} ) {
    $dataset->{species_id} = $n++;
    $self->process_dataset( $dataset, $template_name, $template, $datasets, $genomic_features_mart );
  }
  return;
}

=head2 get_datasets
  Description: Get datasets to process from dataset_names table
  Returntype : arrayref of hashrefs (1 per dataset)
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut
sub get_datasets {
  my ($self) = @_;
  $logger->debug("Fetching dataset details");
  my $datasets =
    $self->{dbc}->sql_helper()->execute(
    -SQL =>
'select name, species_name as display_name, sql_name as production_name, assembly, genebuild from dataset_names',
    -USE_HASHREFS => 1 );
  $logger->debug( "Found " . scalar(@$datasets) . " datasets" );
  return $datasets;
}

=head2 process_dataset
  Description: Process a given dataset
  Arg        : hashref representing a dataset
  Arg        : name of template (e.g. gene)
  Arg        : template as hashref
  Arg        : all datasets (needed for compara) - arrayref of hashrefs (1 per dataset)
  Returntype : none
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut
sub process_dataset {
  my ( $self, $dataset, $template_name, $template, $datasets, $genomic_features_mart ) = @_;
  $logger->info( "Processing " . $dataset->{name} );
  my $templ_in = $template->{DatasetConfig};
  $logger->debug("Building output");
  $dataset->{config} = {};
  $self->write_toplevel( $dataset, $templ_in );
  $self->write_importables( $dataset, $templ_in );
  $self->write_exportables( $dataset, $templ_in );
  $self->write_filters( $dataset, $templ_in, $datasets, $genomic_features_mart );
  $self->write_attributes( $dataset, $templ_in, $datasets );
  # write meta
  $self->write_dataset_metatables( $dataset, $template_name );
  return $dataset;
}

sub write_toplevel {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing toplevel elements for " . $dataset->{name} );
  $dataset->{production_name} =~ s/_/ /g;
  my $is_default = {
                  'hsapiens' => 1,
                  'drerio' => 1,
                  'rnorvegicus' => 1,
                  'mmusculus' => 1,
                  'ggallus' => 1,
                  'athaliana' => 1
  };
  # handle the top level scalars
  # defaultDataSet
  # displayName
  # version
  my $display_name = $dataset->{display_name};
  my $version = $dataset->{assembly};
  my $ds_base = $dataset->{name}.'_'.$self->{basename};
  while ( my ( $key, $value ) = each %{$templ_in} ) {
    if ( !ref($value) ) {
      if ( $key eq 'defaultDataSet' ) {
        if($is_default->{$dataset->{name}}) {
          $value = 'true';
        } else {
          $value = 'false';
        }
      }
      elsif ( $key eq 'displayName' ) {
        $value =~ s/\*species1\*/${display_name}/g;
        $value =~ s/\*version\*/${version}/g;
      }
      elsif ( $key eq 'description' ) {
        $value =~ s/\*species1\*/${display_name}/g;
        $value =~ s/\*version\*/${version}/g;
      }
      elsif ( $key eq 'version' ) {
        $value = $version;
      }
      elsif ( $key eq 'datasetID' ) {
        $value = $dataset->{species_id};
      }
      elsif ( $key eq 'dataset' ) {
        $value = $ds_base;
      }
      elsif ( $key eq 'template' ) {
        $value = $ds_base;
      }
      elsif ( $key eq 'modified' ) {
        $value = scalar(localtime);
      }
      elsif ( $key eq 'optional_parameters' ) {
        $value =~ s/\*base_name\*/${ds_base}/g;
      }      
      $dataset->{config}->{$key} = $value;
    }
  }
 
  # add MainTable
  my $mt = $templ_in->{MainTable};
  if(ref($mt) ne 'ARRAY') {
    $mt = [$mt];
  }
  $dataset->{config}->{MainTable} = [];
  for my $mainTable ( @$mt ) {
    $mainTable =~ s/\*base_name\*/$ds_base/;
    push @{ $dataset->{config}->{MainTable}}, $mainTable;
  }

  # add MainTable
  my $keys = $templ_in->{Key};
  if(ref($keys) ne 'ARRAY') {
    $keys = [$keys];
  }
  $dataset->{config}->{Key} = [];
  for my $key ( @$keys ) {
    push @{ $dataset->{config}->{Key}}, $key;
  }

  return;
} ## end sub write_toplevel

sub write_importables {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing importables for " . $dataset->{name} );

  my $version = $dataset->{name} . "_" . $dataset->{assembly};
  my $ds_name = $dataset->{name} . "_" . $self->{basename};
  # Importable
  for my $impt ( @{ $templ_in->{Importable} } ) {
    my $imp = copy_hash($impt);
    # replace linkVersion.*link_version* with $version
    if ( defined $imp->{linkVersion} ) {
      $imp->{linkVersion} =~ s/\*link_version\*/$version/;
    }
    # replace linkName.*species3*
    if ( defined $imp->{linkName} ) {
      $imp->{linkName} =~ s/\*species3\*/${ds_name}/;
    }
    # replace name.*species3* with ${name}_e
    $imp->{name} =~ s/\*species3\*/${ds_name}/;
    # push onto out stack
    push @{ $dataset->{config}->{Importable} }, $imp;
  }

  return;
} ## end sub write_importables
my %species_exportables = map { $_ => 1 }
  qw/genomic_region gene_exon_intron transcript_exon_intron gene_flank transcript_flank coding_gene_flank coding_transcript_flank 3utr 5utr cdna gene_exon peptide coding/;

sub write_exportables {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing exportables for " . $dataset->{name} );
  my $version = $dataset->{name} . "_" . $dataset->{assembly};
  my $ds_name = $dataset->{name} . "_" . $self->{basename};
  $logger->info("Processing exportables");
  for my $expt ( @{ $templ_in->{Exportable} } ) {
    my $exp = copy_hash($expt);
    # replace linkVersion.*link_version* with $version
    if ( defined $exp->{linkVersion} ) {
      $exp->{linkVersion} =~ s/\*link_version\*/${version}/;
    }
    if ( defined $exp->{linkName} ) {
      # replace linkName.*species3*
      $exp->{linkName} =~ s/\*species3\*/${ds_name}/;
    }
    # replace name.*species3* with ${ds_name}_eg
    $exp->{name} =~ s/\*species3\*/${ds_name}/;
    $exp->{internalName} =~ s/\*species3\*/${ds_name}/;
    $exp->{attributes} =~ s/\*species3\*/${ds_name}/;
    # push onto out stack
    push @{ $dataset->{config}->{Exportable} }, $exp;
  }

  return;
} ## end sub write_exportables

sub write_filters {
  my ( $self, $dataset, $templ_in, $datasets, $genomic_features_mart ) = @_;
  my $ds_name   = $dataset->{name} . '_' . $self->{basename};
  my $templ_out = $dataset->{config};
  $logger->info( "Writing filters for " . $dataset->{name} );
  # FilterPage
  for my $filterPage ( @{ $templ_in->{FilterPage} } ) {
    $logger->debug( "Processing filterPage " . $filterPage->{internalName} );
    # count the number of groups we add
    my $nG = 0;
    normalise( $filterPage, "FilterGroup" );
    my $fpo = copy_hash($filterPage);

    ## FilterGroup
    for my $filterGroup ( @{ $filterPage->{FilterGroup} } ) {
      my $nC = 0;
      normalise( $filterGroup, "FilterCollection" );
      my $fgo = copy_hash($filterGroup);
      ### Filtercollection
      for my $filterCollection ( @{ $filterGroup->{FilterCollection} } ) {
        my $nD = 0;
        normalise( $filterCollection, "FilterDescription" );
        my $fco = copy_hash($filterCollection);
        ### FilterDescription
        for
          my $filterDescription ( @{ $filterCollection->{FilterDescription} } )
        {
          my $fdo = copy_hash($filterDescription);
          #### pointerDataSet *species3*
          $fdo->{pointerDataset} =~ s/\*species3\*/${ds_name}/
            if defined $fdo->{pointerDataset};
          #### SpecificFilterContent - delete
          #### tableConstraint - update
          update_table_keys( $fdo, $dataset, $self->{keys} );
          #### if contains options, treat differently
          #### if its called homolog_filters, add the homologs here
          if ( $fdo->{internalName} eq 'homolog_filters' ) {

            # check for paralogues
            my $table = "${ds_name}__gene__main";
            {
              my $field = "paralog_$dataset->{name}_bool";
              if ( defined $self->{tables}->{$table}->{$field} ) {
                # add in if the column exists
                push @{ $fdo->{Option} }, {
                    displayName  => "Paralogous $dataset->{display_name} Genes",
                    displayType  => "list",
                    field        => $field,
                    hidden       => "false",
                    internalName => "with_$dataset->{name}_paralog",
                    isSelectable => "true",
                    key          => "gene_id_1020_key",
                    legal_qualifiers => "only,excluded",
                    qualifier        => "only",
                    style            => "radio",
                    tableConstraint  => "main",
                    type             => "boolean",
                    Option           => [ {
                                  displayName  => "Only",
                                  hidden       => "false",
                                  internalName => "only",
                                  value        => "only" }, {
                                  displayName  => "Excluded",
                                  hidden       => "false",
                                  internalName => "excluded",
                                  value        => "excluded" } ] };
              } ## end if ( defined $self->{tables...})
            }
            {
              my $field = "homeolog_$dataset->{name}_bool";
              if ( defined $self->{tables}->{$table}->{$field} ) {
                # add in if the column exists
                push @{ $fdo->{Option} }, {
                    displayName => "Homeologous $dataset->{display_name} Genes",
                    displayType => "list",
                    field       => $field,
                    hidden      => "false",
                    internalName     => "with_$dataset->{name}_homeolog",
                    isSelectable     => "true",
                    key              => "gene_id_1020_key",
                    legal_qualifiers => "only,excluded",
                    qualifier        => "only",
                    style            => "radio",
                    tableConstraint  => "main",
                    type             => "boolean",
                    Option           => [ {
                                  displayName  => "Only",
                                  hidden       => "false",
                                  internalName => "only",
                                  value        => "only" }, {
                                  displayName  => "Excluded",
                                  hidden       => "false",
                                  internalName => "excluded",
                                  value        => "excluded" } ] };
              } ## end if ( defined $self->{tables...})
            }

            for my $homo_dataset (@$datasets) {
              my $field = "homolog_$homo_dataset->{name}_bool";
              if ( defined $self->{tables}->{$table}->{$field} ) {
                # add in if the column exists
                push @{ $fdo->{Option} }, {
                    displayName =>
                      "Orthologous $homo_dataset->{display_name} Genes",
                    displayType      => "list",
                    field            => $field,
                    hidden           => "false",
                    internalName     => "with_$homo_dataset->{name}_homolog",
                    isSelectable     => "true",
                    key              => "gene_id_1020_key",
                    legal_qualifiers => "only,excluded",
                    qualifier        => "only",
                    style            => "radio",
                    tableConstraint  => "main",
                    type             => "boolean",
                    Option           => [ {
                                  displayName  => "Only",
                                  hidden       => "false",
                                  internalName => "only",
                                  value        => "only" }, {
                                  displayName  => "Excluded",
                                  hidden       => "false",
                                  internalName => "excluded",
                                  value        => "excluded" } ] };
              } ## end if ( defined $self->{tables...})
            } ## end for my $homo_dataset (@$datasets)
            push @{ $fco->{FilterDescription} }, $fdo;
            $nD++;
          } ## end if ( $fdo->{internalName...})
          elsif ( $fdo->{displayType} && $fdo->{displayType} eq 'container' ) {
            my $nO = 0;
            normalise( $filterDescription, "Option" );
            for my $option ( @{ $filterDescription->{Option} } ) {
              my $opt = copy_hash($option);
              update_table_keys( $opt, $dataset, $self->{keys} );
              if ( defined $self->{tables}->{ $opt->{tableConstraint} } &&
                   defined $self->{tables}->{ $opt->{tableConstraint} }
                   ->{ $opt->{field} } &&
                   (!defined $opt->{key} || defined $self->{tables}->{ $opt->{tableConstraint} }
                    ->{ $opt->{key} }) )
              {
                push @{ $fdo->{Option} }, $opt;
                for my $o ( @{ $option->{Option} } ) {
                  push @{ $opt->{Option} }, $o;
                }
                $nO++;
              }
              else {
                $logger->debug( "Could not find table " .
                            ( $opt->{tableConstraint} || 'undef' ) . " field " .
                            ( $opt->{field}           || 'undef' ) . ", Key " .
                            ( $opt->{key} || 'undef' ) . ", Option " .
                            $opt->{internalName} );
              }
              restore_main( $opt, $ds_name );
            }
            if ( $nO > 0 ) {
              push @{ $fco->{FilterDescription} }, $fdo;
              $nD++;
            }
          } ## end elsif ( $fdo->{displayType... [ if ( $fdo->{internalName...})]})
          else {
            if ( defined $fdo->{tableConstraint} ) {
              #### check tableConstraint and field and key
              if ( defined $self->{tables}->{ $fdo->{tableConstraint} } &&
                   defined $self->{tables}->{ $fdo->{tableConstraint} }
                   ->{ $fdo->{field} } &&
                   (!defined $fdo->{key} || defined $self->{tables}->{ $fdo->{tableConstraint} }
                   ->{ $fdo->{key} }) )
              {
                if ( defined $filterDescription->{SpecificFilterContent} &&
                  ref( $filterDescription->{SpecificFilterContent} ) eq 'HASH'
                  && $filterDescription->{SpecificFilterContent}->{internalName}
                  eq 'replaceMe' )
                {
                  # get contents
                  $logger->info(
                            "Autopopulating dropdown for $fdo->{internalName}");
                  my $max = $self->{max_dropdown} + 1;
                  my %kstart_config=();
                  my %kend_config=();
                  my $vals =
                    $self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>
"select distinct $fdo->{field} from $fdo->{tableConstraint} where $fdo->{field} is not null order by $fdo->{field} limit $max"
                    );
                  if ( scalar(@$vals) <= $self->{max_dropdown} ) {
                    if ($fdo->{internalName} eq "chromosome_name") {
                      # We need to sort the chromosome dropdown to make it more user friendly
                      @$vals = nsort(@$vals);
                      # Retrieving chr band informations
                      my ($chr_bands_kstart,$chr_bands_kend)=generate_chromosome_bands_push_action($self,$dataset->{name},$genomic_features_mart);
                      $fdo->{Option} = [];
                      for my $val (@$vals) {
                      # Creating band start and end configuration for a given chromosome
                      if (defined $chr_bands_kstart->{$val} and defined $chr_bands_kend->{$val}){
                        my %hchr_bands_kstart=%$chr_bands_kstart;
                        foreach my $kstart (@{$hchr_bands_kstart{$val}}){
                          push @{ $kstart_config{$val} }, {
                               internalName => $kstart,
                               displayName  => $kstart,
                               value        => $kstart,
                               isSelectable => 'true',
                               useDefault   => 'true'
                           };
                         }
                         my %hchr_bands_kend=%$chr_bands_kend;
                         foreach my $kend (@{$hchr_bands_kend{$val}}){
                           push @{ $kend_config{$val} }, {
                               internalName => $kend,
                               displayName  => $kend,
                               value        => $kend,
                               isSelectable => 'true',
                               useDefault   => 'true'
                            };
                          }
                        }
                        # If the species has band information, creating chromosome option and associated band start and end push action dropdowns
                        if (defined $kstart_config{$val} and defined $kend_config{$val})
                        {
                          push @{ $fdo->{Option} }, {
                            internalName => $val,
                            displayName  => $val,
                            value        => $val,
                            isSelectable => 'true',
                            useDefault   => 'true',
                            PushAction => [ {
                                   internalName => "band_start_push_$val",
                                   useDefault   => 'true',
                                   ref => 'band_start',
                                   Option => $kstart_config{$val} },
                                   {
                                   internalName => "band_end_push_$val",
                                   useDefault   => 'true',
                                   ref => 'band_end',
                                   Option => $kend_config{$val} } ],
                          };
                        }
                        else {
                          push @{ $fdo->{Option} }, {
                            internalName => $val,
                            displayName  => $val,
                            value        => $val,
                            isSelectable => 'true',
                            useDefault   => 'true'};
                        }
                      }
                    }
                    else {
                      $fdo->{Option} = [];
                      for my $val (@$vals) {
                          push @{ $fdo->{Option} }, {
                            internalName => $val,
                            displayName  => $val,
                            value        => $val,
                            isSelectable => 'true',
                            useDefault   => 'true' };
                      }
                    }
                  }
                  else {
                    $logger->info("Too many dropdowns, changing to text");
                    $fdo->{type}        = "text";
                    $fdo->{displayType} = "text";
                    $fdo->{style}       = undef;
                  }

                } ## end if ( defined $filterDescription...)
                push @{ $fco->{FilterDescription} }, $fdo;
                $nD++;
              } ## end if ( defined $self->{tables...})
              else {
                $logger->debug( "Could not find table " .
                           ( $fdo->{tableConstraint} || 'undef' ) . " field " .
                           ( $fdo->{field}           || 'undef' ) . ", Key " .
                           ( $fdo->{key} || 'undef' ) . ", FilterDescription " .
                           $fdo->{internalName} );
              }
            } ## end if ( defined $fdo->{tableConstraint...})
            else {
              push @{ $fco->{FilterDescription} }, $fdo;
              $nD++;
            }
            #### otherFilters *species4*
            if (defined $fdo->{otherFilters}){
              my $gfm_ds_name=substr($dataset->{name},0,4);
              $fdo->{otherFilters} =~ s/\*species4\*/${gfm_ds_name}/g;
            }
            #### pointerDataSet *species4*
            if (defined $fdo->{pointerDataset}){
              my $gfm_ds_name=substr($dataset->{name},0,4);
              $fdo->{pointerDataset} =~ s/\*species4\*/${gfm_ds_name}/g;
            }
            restore_main( $fdo, $ds_name );
          } ## end else [ if ( $fdo->{internalName...})]
        } ## end for my $filterDescription...
        if ( $nD > 0 ) {
          push @{ $fgo->{FilterCollection} }, $fco;
          $nC++;
        }
      } ## end for my $filterCollection...

      if ( $nC > 0 ) {
        push @{ $fpo->{FilterGroup} }, $fgo;
        $nG++;
        if ( defined $fgo->{hidden} &&
             $fgo->{hidden} eq "true" &&
             defined $self->{unhide}->{ $fgo->{internalName} } )
        {
          $fgo->{hidden} = "false";
        }
      }
    } ## end for my $filterGroup ( @...)
    if ( $nG > 0 ) {
      push @{ $templ_out->{FilterPage} }, $fpo;
    }
  } ## end for my $filterPage ( @{...})
  return;
} ## end sub write_filters

sub write_attributes {
  my ( $self, $dataset, $templ_in, $datasets ) = @_;
  $logger->info( "Writing attributes for " . $dataset->{name} );
  my $ds_name   = $dataset->{name} . '_' . $self->{basename};
  my $templ_out = $dataset->{config};
  # AttributePage
  for my $attributePage ( @{ $templ_in->{AttributePage} } ) {
    $logger->debug( "Processing filterPage " . $attributePage->{internalName} );
    # count the number of groups we add
    my $nG = 0;
    normalise( $attributePage, "AttributeGroup" );
    my $apo = copy_hash($attributePage);

    ## AttributeGroup
    for my $attributeGroup ( @{ $attributePage->{AttributeGroup} } ) {
      my $nC = 0;
      normalise( $attributeGroup, "AttributeCollection" );
      my $ago = copy_hash($attributeGroup);
      #### add the homologs here
      if ( $ago->{internalName} eq 'orthologs' ) {

        for my $dataset (@$datasets) {
          my $table = "${ds_name}__homolog_$dataset->{name}__dm";
          if ( defined $self->{tables}->{$table} ) {
            push @{ $ago->{AttributeCollection} }, {
              displayName          => "$dataset->{display_name} Orthologues",
              internalName         => "homolog_$dataset->{name}",
              AttributeDescription => [ {
                  displayName  => "$dataset->{display_name} gene stable ID",
                  field        => "stable_id_4016_r2",
                  internalName => "$dataset->{name}_gene",
                  key          => "gene_id_1020_key",
                  linkoutURL =>
                    "exturl1|/$dataset->{production_name}/Gene/Summary?g=%s",
                  maxLength       => "128",
                  tableConstraint => $table }, {
                  displayName  => "$dataset->{display_name} protein stable ID",
                  field        => "stable_id_4016_r3",
                  internalName => "$dataset->{name}_homolog_ensembl_peptide",
                  key          => "gene_id_1020_key",
                  maxLength    => "128",
                  tableConstraint => $table }, {
                  displayName => "$dataset->{display_name} chromosome/scaffold",
                  field       => "chr_name_4016_r2",
                  internalName    => "$dataset->{name}_chromosome",
                  key             => "gene_id_1020_key",
                  maxLength       => "40",
                  tableConstraint => $table }, {
                  displayName     => "$dataset->{display_name} start (bp)",
                  field           => "chr_start_4016_r2",
                  internalName    => "$dataset->{name}_chrom_start",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "$dataset->{display_name} end (bp)",
                  field           => "chr_end_4016_r2",
                  internalName    => "$dataset->{name}_chrom_end",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName => "Representative protein or transcript ID",
                  field       => "stable_id_4016_r1",
                  internalName =>
                    "homolog_$dataset->{name}__dm_stable_id_4016_r1",
                  key             => "gene_id_1020_key",
                  maxLength       => "128",
                  tableConstraint => $table }, {
                  displayName     => "Ancestor",
                  field           => "node_name_40192",
                  internalName    => "$dataset->{name}_homolog_ancestor",
                  key             => "gene_id_1020_key",
                  maxLength       => "40",
                  tableConstraint => $table }, {
                  displayName     => "Homology type",
                  field           => "description_4014",
                  internalName    => "$dataset->{name}_orthology_type",
                  key             => "gene_id_1020_key",
                  maxLength       => "25",
                  tableConstraint => $table }, {
                  displayName     => "% identity",
                  field           => "perc_id_4015",
                  internalName    => "$dataset->{name}_homolog_perc_id",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "$dataset->{display_name} % identity",
                  field           => "perc_id_4015_r1",
                  internalName    => "$dataset->{name}_homolog_perc_id_r1",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "dN",
                  field           => "dn_4014",
                  internalName    => "$dataset->{name}_homolog_ds",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "dS",
                  field           => "ds_4014",
                  internalName    => "$dataset->{name}_homolog_dn",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName  => "Orthology confidence [0 low, 1 high]",
                  field        => "is_tree_compliant_4014",
                  internalName => "$dataset->{name}_homolog_is_tree_compliant",
                  key          => "gene_id_1020_key",
                  maxLength    => "10",
                  tableConstraint => $table }, {
                  displayName  => "Bootstrap/Duplication Confidence Score Type",
                  field        => "tag_4060",
                  hidden       => "true",
                  internalName => "homolog_$dataset->{name}__dm_tag_4060",
                  key          => "gene_id_1020_key",
                  maxLength    => "50",
                  tableConstraint => $table }, {
                  displayName     => "Bootstrap/Duplication Confidence Score",
                  field           => "value_4060",
                  hidden          => "true",
                  internalName    => "homolog_$dataset->{name}__dm_value_4060",
                  key             => "gene_id_1020_key",
                  maxLength       => "255",
                  tableConstraint => $table } ] };
            $nC++;
          } ## end if ( defined $self->{tables...})
        } ## end for my $dataset (@$datasets)
      } ## end if ( $ago->{internalName...})
      elsif ( $ago->{internalName} eq 'paralogs' ) {
        my $table = "${ds_name}__paralog_$dataset->{name}__dm";
        if ( defined $self->{tables}->{$table} ) {
          push @{ $ago->{AttributeCollection} }, {

            displayName          => "$dataset->{display_name} Paralogues",
            internalName         => "paralogs_$dataset->{name}",
            AttributeDescription => [ {
                displayName     => "Paralogue gene stable ID",
                field           => "stable_id_4016_r2",
                internalName    => "$dataset->{name}_paralog_gene",
                key             => "gene_id_1020_key",
                linkoutURL      => "exturl1|/*species2*/Gene/Summary?g=%s",
                maxLength       => "140",
                tableConstraint => $table }, {
                displayName => "Paralogue protein stable ID",
                field       => "stable_id_4016_r3",
                internalName =>
                  "$dataset->{name}_paralog_paralog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Paralogue chromosome/scaffold",
                field           => "chr_name_4016_r2",
                internalName    => "$dataset->{name}_paralog_chromosome",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Paralogue start (bp)",
                field           => "chr_start_4016_r2",
                internalName    => "$dataset->{name}_paralog_chrom_start",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogue end (bp)",
                field           => "chr_end_4016_r2",
                internalName    => "$dataset->{name}_paralog_chrom_end",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Representative protein stable ID",
                field           => "stable_id_4016_r1",
                internalName    => "$dataset->{name}_paralog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Ancestor",
                field           => "node_name_40192",
                internalName    => "$dataset->{name}_paralog_ancestor",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Homology type",
                field           => "description_4014",
                internalName    => "$dataset->{name}_paralog_type",
                key             => "gene_id_1020_key",
                maxLength       => "25",
                tableConstraint => $table }, {
                displayName     => "% identity",
                field           => "perc_id_4015",
                internalName    => "paralog_$dataset->{name}_identity",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogue % identity",
                field           => "perc_id_4015_r1",
                internalName    => "paralog_$dataset->{name}_paralog_identity",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "dN",
                field           => "dn_4014",
                internalName    => "$dataset->{name}_paralog_dn",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "dS",
                field           => "ds_4014",
                internalName    => "$dataset->{name}_paralog_ds",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogy confidence [0 low, 1 high]",
                field           => "is_tree_compliant_4014",
                internalName    => "$dataset->{name}_paralog_confidence",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName  => "Bootstrap/Duplication Confidence Score Type",
                field        => "tag_4060",
                internalName => "$dataset->{name}_paralog_bootstrap_conf_type",
                key          => "gene_id_1020_key",
                maxLength    => "50",
                tableConstraint => $table }, {
                displayName  => "Bootstrap/Duplication Confidence Score",
                field        => "value_4060",
                internalName => "$dataset->{name}_paralog_bootstrap_conf_score",
                key          => "gene_id_1020_key",
                maxLength    => "255",
                tableConstraint => $table } ] };
          $nC++;
        } ## end if ( defined $self->{tables...})

      } ## end elsif ( $ago->{internalName... [ if ( $ago->{internalName...})]})
      elsif ( $ago->{internalName} eq 'homeologs' ) {
        my $table = "${ds_name}__homeolog_$dataset->{name}__dm";
        if ( defined $self->{tables}->{$table} ) {
          push @{ $ago->{AttributeCollection} }, {

            displayName          => "$dataset->{display_name} Homoeologues",
            internalName         => "paralogs_$dataset->{name}",
            AttributeDescription => [ {
                displayName     => "Homoeologue gene stable ID",
                field           => "stable_id_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_gene",
                key             => "gene_id_1020_key",
                linkoutURL      => "exturl1|/*species2*/Gene/Summary?g=%s",
                maxLength       => "140",
                tableConstraint => $table }, {
                displayName => "Homoeologue protein stable ID",
                field       => "stable_id_4016_r3",
                internalName =>
                  "$dataset->{name}_homoeolog_homoeolog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Homoeologue chromosome/scaffold",
                field           => "chr_name_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_chromosome",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName => "Homoeologue start (bp)",
                field       => "chr_start_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_chrom_start",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Homoeologue end (bp)",
                field           => "chr_end_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_chrom_end",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Representative protein stable ID",
                field           => "stable_id_4016_r1",
                internalName    => "$dataset->{name}_homoeolog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Ancestor",
                field           => "node_name_40192",
                internalName    => "$dataset->{name}_homoeolog_ancestor",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Homology type",
                field           => "description_4014",
                internalName    => "$dataset->{name}_homoeolog_type",
                key             => "gene_id_1020_key",
                maxLength       => "25",
                tableConstraint => $table }, {
                displayName     => "% identity",
                field           => "perc_id_4015",
                internalName    => "homoeolog_$dataset->{name}_identity",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName  => "Homoeologue % identity",
                field        => "perc_id_4015_r1",
                internalName => "homoeolog_$dataset->{name}_homoeolog_identity",
                key          => "gene_id_1020_key",
                maxLength    => "10",
                tableConstraint => $table }, {
                displayName     => "dN",
                field           => "dn_4014",
                internalName    => "$dataset->{name}_homoeolog_dn",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "dS",
                field           => "ds_4014",
                internalName    => "$dataset->{name}_homoeolog_ds",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Homoeology confidence [0 low, 1 high]",
                field           => "is_tree_compliant_4014",
                internalName    => "$dataset->{name}_homoeolog_confidence",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName => "Bootstrap/Duplication Confidence Score Type",
                field       => "tag_4060",
                internalName =>
                  "$dataset->{name}_homoeolog_bootstrap_conf_type",
                key             => "gene_id_1020_key",
                maxLength       => "50",
                tableConstraint => $table }, {
                displayName => "Bootstrap/Duplication Confidence Score",
                field       => "value_4060",
                internalName =>
                  "$dataset->{name}_homoeolog_bootstrap_conf_score",
                key             => "gene_id_1020_key",
                maxLength       => "255",
                tableConstraint => $table } ] };
          $nC++;
        } ## end if ( defined $self->{tables...})
      } ## end elsif ( $ago->{internalName... [ if ( $ago->{internalName...})]})
      else {

        ### Attributecollection
        for my $attributeCollection (
                                   @{ $attributeGroup->{AttributeCollection} } )
        {
          my $nD = 0;
          normalise( $attributeCollection, "AttributeDescription" );
          my $aco = copy_hash($attributeCollection);
          ### AttributeDescription
          for my $attributeDescription (
                             @{ $attributeCollection->{AttributeDescription} } )
          {
            my $ado = copy_hash($attributeDescription);
            #### pointerDataSet *species3*
            $ado->{pointerDataset} =~ s/\*species3\*/$dataset->{name}/
             if defined $ado->{pointerDataset};
            #### SpecificAttributeContent - delete
            #### tableConstraint - update
            update_table_keys( $ado, $dataset, $self->{keys} );
            #### if contains options, treat differently
            if ( defined $ado->{tableConstraint} ) {
              if ( $ado->{tableConstraint} =~ m/__dm$/ ) {
                $ado->{key} = $self->{keys}->{ $ado->{tableConstraint} } ||
                  $ado->{key};
              }
              #### check tableConstraint and field and key
              if (
                 defined defined $self->{tables}->{ $ado->{tableConstraint} } &&
                 defined $self->{tables}->{ $ado->{tableConstraint} }
                 ->{ $ado->{field} } &&
                 (!defined $ado->{key} || defined $self->{tables}->{ $ado->{tableConstraint} }
                 ->{ $ado->{key} }) )
              {
                push @{ $aco->{AttributeDescription} }, $ado;
                $nD++;
              }
              else {
                $logger->debug( "Could not find table " .
                        ( $ado->{tableConstraint} || 'undef' ) . " field " .
                        ( $ado->{field}           || 'undef' ) . ", Key " .
                        ( $ado->{key} || 'undef' ) . ", AttributeDescription " .
                        $ado->{internalName} );
              }
            }
            else {
              $ado->{pointerDataset} =~ s/\*species3\*/$dataset->{name}/g
                if defined $ado->{pointerDataset};
              push @{ $aco->{AttributeDescription} }, $ado;
              $nD++;
            }
            if ( defined $ado->{linkoutURL} ) {
              if ( $ado->{linkoutURL} =~ m/exturl|\/\*species2\*/ ) {
                # reformat to add URL placeholder
                $ado->{linkoutURL} =~ s/\*species2\*/$dataset->{production_name}/;
              }
            }
            restore_main( $ado, $ds_name );
          } ## end for my $attributeDescription...
          if ( $nD > 0 ) {
            push @{ $ago->{AttributeCollection} }, $aco;
            $nC++;
          }
        } ## end for my $attributeCollection...
      } ## end else [ if ( $ago->{internalName...})]

      if ( defined $ago->{hidden} &&
           $ago->{hidden} eq "true" &&
           defined $self->{unhide}->{ $ago->{internalName} } )
      {
        $ago->{hidden} = "false";
      }

      if ( $nC > 0 ) {
        push @{ $apo->{AttributeGroup} }, $ago;
        $nG++;
      }
    } ## end for my $attributeGroup ...

    if ( $nG > 0 ) {
      $apo->{outFormats} =~ s/,\*mouse_formatter[123]\*//g
        if defined $apo->{outFormats};
      push @{ $templ_out->{AttributePage} }, $apo;
    }
  } ## end for my $attributePage (...)

  return;
} ## end sub write_attributes

sub copy_hash {
  my ($in) = @_;
  my $out = {};
  while ( my ( $k, $v ) = each %$in ) {
    if ( $k eq 'key' || $k eq 'field' || $k eq 'tableConstraint' ) {
      $v = lc $v;
    }
    if ( !ref($v) ) {
      $out->{$k} = $v;
    }
  }
  return $out;
}

sub normalise {
  my ( $hash, $key ) = @_;
  $hash->{$key} = [ $hash->{$key} ] unless ref( $hash->{$key} ) eq 'ARRAY';
  return;
}

sub update_table_keys {
  my ( $obj, $dataset, $keys ) = @_;
  my $ds_name = $dataset->{config}->{dataset};
  if ( defined $obj->{tableConstraint}) {
    if ( $obj->{tableConstraint} eq 'main' ) {
      if(!defined $obj->{key}) {
        ($obj->{tableConstraint}) = @{$dataset->{config}->{MainTable}};
        $obj->{tableConstraint} =~ s/\*base_name\*/${ds_name}/;
      } else {
        # use key to find the correct main table
        if ( $obj->{key} eq 'gene_id_1020_key' ) {
          $obj->{tableConstraint} = "${ds_name}__gene__main";
        }
        elsif ( $obj->{key} eq 'transcript_id_1064_key' ) {
          $obj->{tableConstraint} = "${ds_name}__transcript__main";
        }
        elsif ( $obj->{key} eq 'translation_id_1068_key' ) {
          $obj->{tableConstraint} = "${ds_name}__translation__main";
        }
      }
    }
    else {
      $obj->{tableConstraint} = "${ds_name}__" . $obj->{tableConstraint}
        if ( defined $obj->{tableConstraint} );
    }
    # for dimension tables, correct the key
    if ( defined $keys->{ $obj->{tableConstraint} } &&
         $obj->{tableConstraint} =~ m/.*__dm$/ )
    {
      $obj->{key} = $keys->{ $obj->{tableConstraint} };
    }
  }
  return;
} ## end sub update_table_keys

sub restore_main {
  my ( $obj, $ds_name ) = @_;
  # switch main back to placeholder name after we've checked the real table name
  if ( defined $obj->{tableConstraint} ) {
    if ( $obj->{tableConstraint} =~ m/^${ds_name}__.+__main$/ ||
         $obj->{tableConstraint} =~ m/^${ds_name}__.+__main$/ ||
         $obj->{tableConstraint} =~ m/^${ds_name}__.+__main$/ )
    {
      $obj->{tableConstraint} = 'main';
    }
  }
  return;
}

sub create_metatables {
  my ( $self, $template_name, $template ) = @_;
  $logger->info("Creating meta tables");

  # create tables
  $self->create_metatable( 'meta_version__version__main',
                           ['version varchar(10) default NULL'] );
  $self->{dbc}->sql_helper()
    ->execute_update(
             -SQL => "INSERT INTO meta_version__version__main VALUES ('0.7')" );

  # template tables
  $self->create_metatable( 'meta_template__template__main', [
                             'dataset_id_key int(11) NOT NULL',
                             'template varchar(100) NOT NULL' ] );

  $self->create_metatable( 'meta_template__xml__dm', [
                             'template varchar(100) default NULL',
                             'compressed_xml longblob',
                             'UNIQUE KEY template (template)' ] );

  ## meta_template__xml__dm
  my $template_xml =
    XMLout( { DatasetConfig => $template->{config} }, KeepRoot => 1 );

  if( ! -d "./tmp" ) {
    mkdir "./tmp";
  }
  open my $out, ">", "./tmp/tmp.xml";
  print $out $template_xml;
  close $out;
  my $gzip_template;
  gzip \$template_xml => \$gzip_template;

  $self->{dbc}->sql_helper()->execute_update(
                      -SQL => 'INSERT INTO meta_template__xml__dm VALUES (?,?)',
                      -PARAMS => [ $template_name, $gzip_template ] );
  $self->create_metatable(
    'meta_conf__dataset__main', [
      'dataset_id_key int(11) NOT NULL',
      'dataset varchar(100) default NULL',
      'display_name varchar(200) default NULL',
      'description varchar(200) default NULL',
      'type varchar(20) default NULL',
      'visible int(1) unsigned default NULL',
      'version varchar(128) default NULL',
'modified timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP',
      'UNIQUE KEY dataset_id_key (dataset_id_key)' ] );

  # dataset tables
  $self->create_metatable( 'meta_conf__xml__dm', [
                             'dataset_id_key int(11) NOT NULL',
                             'xml longblob',
                             'compressed_xml longblob',
                             'message_digest blob',
                             'UNIQUE KEY dataset_id_key (dataset_id_key)' ] );

  $self->create_metatable( 'meta_conf__user__dm', [
                             'dataset_id_key int(11) default NULL',
                             'mart_user varchar(100) default NULL',
'UNIQUE KEY dataset_id_key (dataset_id_key,mart_user)' ] );

  $self->create_metatable( 'meta_conf__interface__dm', [
                             'dataset_id_key int(11) default NULL',
                             'interface varchar(100) default NULL',
'UNIQUE KEY dataset_id_key (dataset_id_key,interface)' ] );

  $logger->info("Completed creation of metatables");
  return;
} ## end sub create_metatables

sub write_dataset_metatables {
  my ( $self, $dataset, $template_name ) = @_;

  my $ds_name   = $dataset->{name} . '_' . $self->{basename};
  my $speciesId = $dataset->{species_id};
  my $display_species_name = $dataset->{display_name}.' genes ('.$dataset->{assembly}.')';

  $logger->info("Populating metatables for $ds_name ($speciesId)");

  my $dataset_xml =
    XMLout( { DatasetConfig => $dataset->{config} }, KeepRoot => 1 );

  open my $out, ">", "./tmp/$ds_name.xml";
  print $out $dataset_xml;
  close $out;
  my $gzip_dataset_xml;
  gzip \$dataset_xml => \$gzip_dataset_xml;

  $self->{dbc}->sql_helper()
    ->execute_update( -SQL =>
"INSERT INTO meta_template__template__main VALUES($speciesId,'$template_name')"
    );

  $self->{dbc}->sql_helper()->execute_update(
    -SQL =>
"INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,?,'TableSet',1,?)",
    -PARAMS => [ $speciesId,               $ds_name,
                 $display_species_name, "Ensemmbl $template_name", $dataset->{assembly} ] );

  $self->{dbc}->sql_helper()->execute_update(
              -SQL    => 'INSERT INTO meta_conf__xml__dm VALUES (?,?,?,?)',
              -PARAMS => [ $speciesId, $dataset_xml, $gzip_dataset_xml, 'NULL' ]
  );

  $self->{dbc}->sql_helper()
    ->execute_update(
       -SQL => "INSERT INTO meta_conf__user__dm VALUES($speciesId,'default')" );

  $self->{dbc}->sql_helper()
    ->execute_update( -SQL =>
          "INSERT INTO meta_conf__interface__dm VALUES($speciesId,'default')" );

  $logger->info("Population complete for $ds_name");
  return;
} ## end sub write_dataset_metatables

sub create_metatable {
  my ( $self, $table_name, $cols ) = @_;
  $logger->info("Creating $table_name");
  $self->{dbc}
    ->sql_helper->execute_update( -SQL => "DROP TABLE IF EXISTS $table_name" );
  $self->{dbc}
    ->sql_helper->execute_update( -SQL => "CREATE TABLE $table_name (" .
               join( ',', @$cols ) . ") ENGINE=MyISAM DEFAULT CHARSET=latin1" );
  return;
}

sub _load_info {
  my ($self) = @_;
  $logger->info( "Reading table list for " . $self->{dbc}->dbname() );
  # create hash of tables to columns
  $self->{tables} = {};
  # create lookup of key by table
  $self->{keys} = {};
  $self->{dbc}->sql_helper()->execute_no_return(
    -SQL =>
'select table_name,column_name from information_schema.columns where table_schema=?',
    -PARAMS   => [ $self->{dbc}->dbname() ],
    -CALLBACK => sub {
      my ( $table, $col ) = @{ shift @_ };
      $col = lc $col;
      $self->{tables}->{$table}->{$col} = 1;
      if ( $col =~ m/[a-z]+_id_[0-9]+_key/ ) {
        $self->{keys}->{$table} = $col;
      }
      return;
    } );
  return;
}

sub generate_chromosome_bands_push_action {
my ($self,$dataset_name,$genomic_features_mart)= @_;
my $gfm_ds_name=substr($dataset_name,0,4);
my $chr_bands_kstart;
my $chr_bands_kend;

my $database_tables =$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${genomic_features_mart}'" );
if ($database_tables->[0] > 0) {
  my $empty_ks_table=$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${genomic_features_mart}' and table_name='${gfm_ds_name}_karyotype_start__karyotype__main'" );
  my $empty_ke_table=$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${genomic_features_mart}' and table_name='${gfm_ds_name}_karyotype_start__karyotype__main'" );

  if ($empty_ks_table->[0] > 0 and $empty_ke_table->[0] > 0) {
    $chr_bands_kstart = $self->{dbc}->sql_helper()->execute_into_hash(
      -SQL => "select name_1059, band_1027 from ${genomic_features_mart}.${gfm_ds_name}_karyotype_start__karyotype__main where band_1027 is not null order by band_1027",
      -CALLBACK => sub {
        my ( $row, $value ) = @_;
        $value = [] if !defined $value;
        push($value, $row->[1] );
        return $value;
        }
    );
    $chr_bands_kend = $self->{dbc}->sql_helper()->execute_into_hash(
      -SQL => "select name_1059, band_1027 from ${genomic_features_mart}.${gfm_ds_name}_karyotype_end__karyotype__main where band_1027 is not null order by band_1027",
      -CALLBACK => sub {
        my ( $row, $value ) = @_;
        $value = [] if !defined $value;
        push($value, $row->[1] );
        return $value;
        }
    );
  }
}
return ($chr_bands_kstart,$chr_bands_kend);
}

1;
