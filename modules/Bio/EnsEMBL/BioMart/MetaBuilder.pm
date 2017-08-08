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
use Clone qw/clone/;
use Log::Log4perl qw/get_logger/;
use LWP::UserAgent;
use Config::IniFiles;

my $logger = get_logger();

# properties of
my $template_properties = {
         genes      => { type => 'TableSet',        visible => 1 },
         variations => { type => 'TableSet',        visible => 1 },
         variations_som => { type => 'TableSet',        visible => 1 },
         structural_variations => { type => 'TableSet',        visible => 1 },
         structural_variations_som => { type => 'TableSet',        visible => 1 },
         annotated_features => { type => 'TableSet',        visible => 1 },
         external_features => { type => 'TableSet',        visible => 1 },
         mirna_target_features => { type => 'TableSet',        visible => 1 },
         motif_features => { type => 'TableSet',        visible => 1 },
         regulatory_features => { type => 'TableSet',        visible => 1 },
         sequences  => { type => 'GenomicSequence', visible => 0 },
         encode => { type => 'TableSet',        visible => 0 },
         qtl_feature => { type => 'TableSet',        visible => 0 },
         karyotype_start => { type => 'TableSet',        visible => 0 },
         karyotype_end => { type => 'TableSet',        visible => 0 },
         marker_start => { type => 'TableSet',        visible => 0 },
         marker_end => { type => 'TableSet',        visible => 0 },
         ontology => { type => 'TableSet',        visible => 0 },
         ontology_mini => { type => 'TableSet',        visible => 0 },
         ontology_regulation => { type => 'TableSet',        visible => 0 },
         ontology_motif => { type => 'TableSet',        visible => 0 }, };

=head1 CONSTRUCTOR
=head2 new
 Arg [-DBC] :
    Bio::EnsEMBL::DBSQL::DBConnection : instance for the target mart (required)
 Arg [-VERSION] :
    Integer : EG/E version (by default the last number in the mart name)
 Arg [-MAX_DROPDOWN] :
    Integer : Maximum number of items to show in a dropdown menu (default 256)
 Arg [-DELETE] :
    Hashref : attributes/filters to delete for this mart (mainly domain specific ontologies)
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
  ( $self->{dbc}, $self->{version}, $self->{max_dropdown},
    $self->{delete}, $self->{basename} )
    = rearrange( [ 'DBC', 'VERSION', 'MAX_DROPDOWN', 'DELETE', 'BASENAME' ],
                 @args );

  if ( !defined $self->{version} ) {
    ( $self->{version} = $self->{dbc}->dbname() ) =~ s/.*_([0-9]+)$/$1/;
  }
  $self->_load_info();
  return $self;
}

=head1 METHODS
=head2 build
  Description: Build metadata for the supplied  mart
  Arg        : name of template (e.g. gene)
  Arg        : template as hashref
  Arg        : Genomic features mart database name
  Arg        : ini file GitHub URL used to retrieve xrefs URLs
  Returntype : none
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub build {
  my ( $self, $template_name, $template, $genomic_features_mart, $ini_file, $registry_loaded ) = @_;
  # create base metatables
  $self->create_metatables( $template_name, $template );
  # read datasets
  my $datasets = $self->get_datasets();
  # get latest species_id from the dataset_name table
  my $description="Ensembl $template_name";
  my $offsets = $self->{dbc}->sql_helper()->execute_simple( -SQL =>"select max(dataset_id_key) from meta_conf__dataset__main where description != '${description}'");
  # avoid clashes for multiple template types
  my $n        = $offsets->[0] || 0;
  my $xref_url_list;
  # Getting list of URLs for genes marts only
  # parsing extra ini file from eg-web-common for divisions that are not e!
  # Merge both hashes
  # For do this only for genes marts
  if ($template_name eq "genes"){
     #For ensembl_gene and mouse gene mart use ensembl-webcode defaults.ini
    if ($self->{dbc}->dbname() =~ 'ensembl' or $self->{dbc}->dbname() =~ 'mouse'){
      $logger->info( "Parsing ini file containing xrefs URLs: " . $ini_file );
      $xref_url_list = $self->parse_ini_file($ini_file,"ENSEMBL_EXTERNAL_URLS");
    }
    # Else, parsing the Ensembl genomes division DEFAULT.ini file and extra eg-web-common ini file.
    else{
      $logger->info( "Parsing ini file containing xrefs URLs: " . $ini_file );
      my $xref_division = $self->parse_ini_file($ini_file,"ENSEMBL_EXTERNAL_URLS");
      $logger->info( "Parsing ini file containing xrefs URLs for eg-web-common");
      my $xref_eg_common_list = $self->parse_ini_file("https://raw.githubusercontent.com/EnsemblGenomes/eg-web-common/master/conf/ini-files/DEFAULTS.ini","ENSEMBL_EXTERNAL_URLS");
      $xref_url_list = {%$xref_division,%$xref_eg_common_list};
    }
  }

  for my $dataset ( @{$datasets} ) {
    my $core_dba;
    my $regulation_dba;
    my $variation_dba;
    my $ds_name;
    if ($self->{basename} ne '') {
      $ds_name  = $dataset->{name} . '_' . $self->{basename};
    }
    else {
      $ds_name  = $dataset->{name};
    }
    # Check if dataset has a main table
    if($self->has_main_tables($dataset,$template->{DatasetConfig},$ds_name)==0) {
      $logger->warn("No main tables found for $template_name for ".$dataset->{name});
      next;
    }
    if ($self->{dbc}->dbname() !~ 'ontology') {
      $core_dba = $registry_loaded->get_DBAdaptor( $dataset->{production_name}, 'Core');
      $regulation_dba = $registry_loaded->get_DBAdaptor( $dataset->{production_name}, 'funcgen');
      $variation_dba = $registry_loaded->get_DBAdaptor( $dataset->{production_name}, 'variation');
    }
    $dataset->{species_id} = ++$n;
    $self->process_dataset( $dataset, $template_name, $template, $datasets, $genomic_features_mart, $xref_url_list, $core_dba, $regulation_dba, $variation_dba, $ds_name );
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
'select name, species_name as display_name, sql_name as production_name, assembly, genebuild, src_db from dataset_names order by name',
    -USE_HASHREFS => 1 );
  $logger->debug( "Found " . scalar(@$datasets) . " datasets" );
  #Sorting orthologues, Paralogues and homeologues attribute by dataset display name
  $datasets = [sort { $a->{display_name} cmp $b->{display_name} } @$datasets];
  return $datasets;
}

=head2 process_dataset
  Description: Process a given dataset
  Arg        : hashref representing a dataset
  Arg        : name of template (e.g. gene)
  Arg        : template as hashref
  Arg        : all datasets (needed for compara) - arrayref of hashrefs (1 per dataset)
  Arg        : Genomic features mart database name
  Arg        : ini file GitHub URL used to retrieve xrefs URLs
  Returntype : none
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub process_dataset {
  my ( $self, $dataset, $template_name, $template, $datasets, $genomic_features_mart, $xref_url_list, $core_dba, $regulation_dba, $variation_dba, $ds_name ) = @_;
  $logger->info( "Processing " . $dataset->{name} );
  my $templ_in = $template->{DatasetConfig};
  my $xref_list;
  my $probe_list;
  my $protein_domain_and_feature_list;
  my $exception_xrefs;
  $logger->debug("Building output");
  $dataset->{config} = {};
  $self->write_toplevel( $dataset, $templ_in, $ds_name );
  if ($template_name eq "genes"){
    $xref_list = $self->generate_xrefs_list($dataset, $core_dba);
    $probe_list = $self->generate_probes_list($dataset, $regulation_dba);
    $protein_domain_and_feature_list = $self->generate_protein_domain_and_feature_list($dataset, $core_dba);

    # Hardcoded list of Xrefs using the dbprimary_acc_1074 columns or using both dbprimary_acc_1074 and display_label_1074
    # Each column as an associated name that will be use for filter/attribute name
     $exception_xrefs = {
      hgnc => { columns => { dbprimary_acc_1074 => "ID",  display_label_1074 => "symbol"}},
      mirbase => { columns => { dbprimary_acc_1074 => "accession", display_label_1074 => "ID"}},
      mim_gene => {columns => { dbprimary_acc_1074 => "accession", description_1074 => "description"}},
      mim_morbid => {columns => { dbprimary_acc_1074 => "accession", description_1074 => "description"}},
      dbass3 => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "name"}},
      dbass5 => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "name"}},
      wikigene => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "name", description_1074 => "description"}},
      zfin_id => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "symbol"}},
      mgi => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "symbol", description_1074 => "description"}},
      rgd => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "symbol"}},
      zfin_xpat => {columns => {dbprimary_acc_1074 => "ID", display_label_1074 => "symbol"}},
    };
  }

  ### Write xml data
  $self->write_importables( $dataset, $templ_in, $ds_name );
  $self->write_exportables( $dataset, $templ_in, $datasets, $template_name, $ds_name );
  $self->write_filters( $dataset, $templ_in, $datasets, $genomic_features_mart, $xref_list, $probe_list, $exception_xrefs, $protein_domain_and_feature_list, $ds_name, $core_dba, $regulation_dba, $variation_dba );
  $self->write_attributes( $dataset, $templ_in, $datasets, $xref_list, $probe_list, $xref_url_list, $exception_xrefs, $protein_domain_and_feature_list, $ds_name, $core_dba, $regulation_dba, $variation_dba );
  # write meta
  $self->write_dataset_metatables( $dataset, $template_name, $ds_name );
  return;
}

sub has_main_tables {
  my ($self, $dataset, $templ_in, $ds_name) = @_;
  for my $mainTable (@{elem_as_array(clone($templ_in->{MainTable}))}) {
    $mainTable =~ s/\*base_name\*/$ds_name/;
    if(!defined $self->{tables}->{$mainTable}) {
      $logger->warn("Main table $mainTable not found");
      return 0;
    }
  }
  return 1;
}

sub write_toplevel {
  my ( $self, $dataset, $templ_in, $ds_name ) = @_;
  $logger->info( "Writing toplevel elements for " . $dataset->{name} );
  # List of default species that should be displayed at the top of the species dropdown list
  # on the the mart web interface
  my $is_default = { 'hsapiens'    => 1,
                     'drerio'      => 1,
                     'rnorvegicus' => 1,
                     'mmusculus'   => 1,
                     'ggallus'     => 1,
                     'athaliana'   => 1 };
  # handle the top level scalars
  # defaultDataSet
  # displayName
  # version
  my $display_name = $dataset->{display_name};
  my $version;
  # For Vectorbase, we want to display the genebuild.version meta key
  if ($self->{dbc}->dbname() =~ 'vb_') {
    $version = $dataset->{genebuild};
  }
  else {
    $version = $dataset->{assembly};
  }
  while ( my ( $key, $value ) = each %{$templ_in} ) {
    if ( !ref($value) ) {
      if ( $key eq 'defaultDataset') {
        if ( $is_default->{ $dataset->{name} } ) {
          $value = 'true';
        }
        else {
          $value = 'false';
        }
      }
      elsif ( $key eq 'displayName' ) {
        $value =~ s/\*species1\*/${display_name}/g;
        $value =~ s/\*version\*/${version}/g;
        $dataset->{dataset_display_name} = $value; # record for later use
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
        $value = $ds_name;
      }
      elsif ( $key eq 'template' ) {
        $value = $ds_name;
      }
      elsif ( $key eq 'modified' ) {
        $value = scalar(localtime);
      }
      elsif ( $key eq 'optional_parameters' ) {
        $value =~ s/\*base_name\*/${ds_name}/g;
      }
      $dataset->{config}->{$key} = $value;
    } ## end if ( !ref($value) )
  } ## end while ( my ( $key, $value...))

  # add MainTable
  $dataset->{config}->{MainTable} = [];
  for my $mainTable (@{elem_as_array(clone($templ_in->{MainTable}))}) {
    $mainTable =~ s/\*base_name\*/$ds_name/;
    push @{ $dataset->{config}->{MainTable} }, $mainTable;
  }

  # add Key
  $dataset->{config}->{Key} = [];
  for my $key (@{elem_as_array(clone($templ_in->{Key}))}) {
    push @{ $dataset->{config}->{Key} }, $key;
  }

  return;
} ## end sub write_toplevel

sub write_importables {
  my ( $self, $dataset, $templ_in, $ds_name ) = @_;
  $logger->info( "Writing importables for " . $dataset->{name} );

  my $version = $dataset->{name} . "_" . $dataset->{assembly};
  # Importable
  for my $impt ( @{ elem_as_array($templ_in->{Importable}) } ) {
    my $imp = copy_hash($impt);
    # replace linkVersion.*link_version* with $version
    if ( defined $imp->{linkVersion} ) {
      $imp->{linkVersion} =~ s/\*link_version\*/$version/;
    }
    # replace linkName.*species3*
    if ( defined $imp->{linkName} ) {
      $imp->{linkName} =~ s/\*species3\*/$dataset->{name}/;
    }
    # replace name.*species3* with ${name}_e
    $imp->{name} =~ s/\*species3\*/$dataset->{name}/;
    # push onto out stack
    push @{ $dataset->{config}->{Importable} }, $imp;
  }

  return;
} ## end sub write_importables
my %species_exportables = map { $_ => 1 }
  qw/genomic_region gene_exon_intron transcript_exon_intron gene_flank transcript_flank coding_gene_flank coding_transcript_flank 3utr 5utr cdna gene_exon peptide coding/;


sub write_exportables {
  my ( $self, $dataset, $templ_in, $datasets, $template_name, $ds_name ) = @_;
  $logger->info( "Writing exportables for " . $dataset->{name} );
  my $version = $dataset->{name} . "_" . $dataset->{assembly};
  $logger->info("Processing exportables");
  for my $expt ( @{ elem_as_array($templ_in->{Exportable}) } ) {
    my $exp = copy_hash($expt);
    # replace linkVersion.*link_version* with $version
    if ( defined $exp->{linkVersion} ) {
      $exp->{linkVersion} =~ s/\*link_version\*/${version}/;
    }
    if ( defined $exp->{linkName} ) {
      # replace linkName.*species3*
      $exp->{linkName} =~ s/\*species3\*/$dataset->{name}/;
    }
    #Creating exportable name from dataset name
    my $ontology_exportable=lc($ds_name);
    $ontology_exportable =~ s/(closure)_(\w+)/${2}_${1}/;

    # replace name.*species3* with ${ds_name}_eg
    $exp->{name}         =~ s/\*species3\*/$dataset->{name}/;
    $exp->{internalName} =~ s/\*species3\*/$dataset->{name}/;
    $exp->{attributes}   =~ s/\*species3\*/$dataset->{name}/;
    # Ontology exportables
    $exp->{internalName} =~ s/\*ontology\*/$ontology_exportable/;
    $exp->{linkName} =~ s/\*ontology\*/$ontology_exportable/;
    $exp->{name} =~ s/\*ontology\*/$ontology_exportable/;
    # push onto out stack
    push @{ $dataset->{config}->{Exportable} }, $exp;
  }
  # For gene mart only
  if ($template_name eq "genes") {
    # additional exporter for multiple dataset selection
    foreach my $ds (@$datasets){
      if($ds->{name} ne $dataset->{name}) {
        push @{ $dataset->{config}->{Exportable} }, {
                                                     attributes   => "$ds->{name}"."_homolog_ensembl_gene",
                                                     default      => 1,
                                                     internalName => "$ds->{name}_gene_stable_id",
                                                     name         => "$ds->{name}_gene_stable_id",
                                                     linkName     => "$ds->{name}_gene_stable_id",
                                                   type         => "link" };
    }
    }
  }
  return;
} ## end sub write_exportables

sub write_filters {
  my ( $self, $dataset, $templ_in, $datasets, $genomic_features_mart, $xref_list, $probe_list, $exception_xrefs, $protein_domain_and_feature_list, $ds_name, $core_dba, $regulation_dba, $variation_dba ) = @_;
  my $templ_out = $dataset->{config};
  #Defining Annotation filters
  my $annotations = {
    1 => {display_name => 'Gene stable ID(s)', field =>'stable_id_1023', table => $ds_name.'__gene__main', internal_name => 'gene_id'},
    2 => {display_name => 'Gene stable ID(s) with version', field =>'gene__main_stable_id_version', table => $ds_name.'__gene__main', internal_name => 'gene_id_version'},
    3 => {display_name => 'Transcript stable ID(s)', field => 'stable_id_1066', table => $ds_name.'__transcript__main', internal_name => 'transcript_id' },
    4 => {display_name => 'Transcript stable ID(s) with version', field => 'transcript__main_stable_id_version', table => $ds_name.'__transcript__main', internal_name => 'transcript_id_version' },
    5 => {display_name => 'Protein stable ID(s)', field => 'stable_id_1070', table => $ds_name.'__translation__main', internal_name => 'peptide_id' },
    6 => {display_name => 'Protein stable ID(s) with version', field => 'translation__main_stable_id_version', table => $ds_name.'__translation__main', internal_name => 'peptide_id_version' },
    7 => {display_name => 'Exon ID(s)', field => 'stable_id_1016', table => $ds_name.'__exon_transcript__dm', internal_name => 'exon_id' },
    8 => {display_name => 'Gene Name(s)', field => 'display_label_1074', table => $ds_name.'__gene__main', internal_name => 'external_gene_name' },
    9 => {display_name => 'Transcript Name(s)', field => 'display_label_1074_r1', table => $ds_name.'__transcript__main', internal_name => 'external_transcript_name' },
  };
  # Defining Extrain protein domains
  my $extra_protein_domains = {
    1 => {display_name => 'Interpro', field =>'interpro_ac_1026', table => $ds_name.'__interpro__dm', internal_name => 'interpro'},
    2 => {display_name => 'Ensembl Protein Family', field =>'stable_id_408', table => $ds_name.'__translation__main', internal_name => 'family'},
  };
  $logger->info( "Writing filters for " . $dataset->{name} );
  # FilterPage
  for my $filterPage ( @{ elem_as_array( $templ_in->{FilterPage}) } ) {
    $logger->debug( "Processing filterPage " . $filterPage->{internalName} );
    # count the number of groups we add
    my $nG = 0;
    normalise( $filterPage, "FilterGroup" );
    my $fpo = copy_hash($filterPage);

    ## FilterGroup
    for my $filterGroup ( @{ elem_as_array($filterPage->{FilterGroup}) } ) {
      $logger->debug( "Processing filterGroup " . $filterGroup->{internalName} );
      my $nC = 0;
      normalise( $filterGroup, "FilterCollection" );
      my $fgo = copy_hash($filterGroup);
      ### Filtercollection
      for my $filterCollection ( @{ elem_as_array($filterGroup->{FilterCollection}) } ) {
        $logger->debug( "Processing filterCollection " . $filterCollection->{internalName} );
        my $nD = 0;
        normalise( $filterCollection, "FilterDescription" );
        my $fco = copy_hash($filterCollection);
        ### FilterDescription
        for
          my $filterDescription ( @{ elem_as_array($filterCollection->{FilterDescription}) } )
            {
              $logger->debug( "Processing filterDescription " . $filterDescription->{internalName} );
          my $fdo = copy_hash($filterDescription);
          #### pointerDataSet *species3*
          $fdo->{pointerDataset} =~ s/\*species3\*/$dataset->{name}/
            if defined $fdo->{pointerDataset};
          #### Replacing ontology internal name
          if ($self->{dbc}->dbname() =~ 'ontology') {
            my $ontology_name=lc($ds_name);
            $ontology_name =~ s/(closure)_(\w+)/${2}_${1}/;
            $ontology_name = $2;
            $fdo->{internalName} =~ s/\*ontology_name\*/$ontology_name/;
          }
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
              my $field = "homoeolog_$dataset->{name}_bool";
              if ( defined $self->{tables}->{$table}->{$field} ) {
                # add in if the column exists
                push @{ $fdo->{Option} }, {
                    displayName => "Homoeologous $dataset->{display_name} Genes",
                    displayType => "list",
                    field       => $field,
                    hidden      => "false",
                    internalName     => "with_$dataset->{name}_homoeolog",
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
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end if ( $fdo->{internalName...})
          elsif ( $fdo->{internalName} eq 'id_list_xrefs_filters' ) {
            $logger->info(
                            "Generating data for $fdo->{internalName}");
              foreach my $xref (@{ $xref_list }) {
                my $field = "ox_".$xref->[0]."_bool";
                my $table = $ds_name."__ox_".$xref->[0]."__dm";
                if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                    if ( defined $self->{tables}->{$table}->{$key} ) {
                      my $main_table=${ds_name}."__translation__main";
                      if (defined $self->{tables}->{$main_table}->{$field} ) {
                        # add in if the column exists
                        push @{ $fdo->{Option} }, {
                          displayName  => "With $xref->[1] ID(s)",
                          displayType  => "list",
                          field        => $field,
                          hidden       => "false",
                          internalName => "with_$xref->[0]",
                          isSelectable => "true",
                          key          => $key,
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
                      }
                    }
                }
              } ## end if ( defined $self->{tables...})
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end elsif ( $fdo->{internalName...})
          elsif ( $fdo->{internalName} eq 'id_list_limit_xrefs_filters' ) {
            $logger->info(
                            "Generating data for $fdo->{internalName}");
            # Generating Filters for Gene, Transcript, Protein and exons
            foreach my $annotation (sort keys %{$annotations}) {
                my $field = $annotations->{$annotation}->{'field'};
                my $table = $annotations->{$annotation}->{'table'};
                if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                    if (defined $self->{tables}->{$table}->{$field} ) {
                      my $example = $self->get_example($table,$field);
                      # We need to keep historical internal name. This would make many people in BiomaRt community if the naming change
                      my $internal_name;
                      #For ensembl gene mart we need to use historical prefix.
                      if ($annotations->{$annotation}->{'internal_name'} =~ "external")
                      {
                        $internal_name = $annotations->{$annotation}->{'internal_name'};
                      }
                      else{
                        $internal_name = "ensembl_".$annotations->{$annotation}->{'internal_name'};
                      }
                      # add in if the column exists
                      push @{ $fdo->{Option} }, {
                        displayName  => $annotations->{$annotation}->{'display_name'}." [e.g. $example]",
                        displayType  => "text",
                        description  => "Filter to include genes with supplied list of $annotations->{$annotation}->{'display_name'}",
                        field        => $field,
                        hidden       => "false",
                        internalName => $internal_name,
                        isSelectable => "true",
                        key          => $key,
                        legal_qualifiers => "=,in",
                        multipleValues   => "1",
                        qualifier        => "=",
                        tableConstraint  => $table,
                        type             => "List" };
                    }
                  }
                }
              } ## end if ( defined $self->{tables...})
              # Generation all the other xrefs
              foreach my $xref (@{ $xref_list }) {
                my $table = $ds_name."__ox_".$xref->[0]."__dm";
                if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                    if (exists $exception_xrefs->{$xref->[0]}) {
                      #Checking if the xrefs is part of the execption xrefs hash.
                      #We need to use dbprimary_acc_1074 instead of display_label_1074 or both
                      while ( my ($field, $suffix) = each %{$exception_xrefs->{$xref->[0]}->{columns}}) {
                        # We don't want filters for description field
                        # E.g: MIM gene description(s) [e.g. RING1- AND YY1-BINDING PROTEIN; RYBP;;YY1- AND E4TF1/GABP-ASSOCIATED FACTOR 1; YEAF1]
                        # or MIM morbid description(s) [e.g. MONOCARBOXYLATE TRANSPORTER 1 DEFICIENCY; MCT1D]

                        #Remove from display name string whatever has been defined in the regex below.
                        # At the moment removing Symbol for the HGNC xref and ID for other xrefs like INSDC protein
                        my $xref_display_name = $xref->[1];
                        $xref_display_name =~ s/(\s+[sS]ymbol$)|(\s+[iI][dD]$)//;
                        next if ($field eq "description_1074");
                        my $example = $self->get_example($table,$field);
                        push @{ $fdo->{Option} }, {
                        displayName  => "$xref_display_name $suffix"."(s) [e.g. $example]",
                        displayType  => "text",
                        description  => "Filter to include genes with supplied list of $xref_display_name $suffix"."(s)",
                        field        => $field,
                        hidden       => "false",
                        internalName => $xref->[0]."_".lc($suffix),
                        isSelectable => "true",
                        key          => $key,
                        legal_qualifiers => "=,in",
                        multipleValues   => "1",
                        qualifier        => "=",
                        tableConstraint  => $table,
                        type             => "List" };
                      }
                    }
                    else {
                      #Remove from display name string whatever has been defined in the regex below.
                      # At the moment removing Symbol for the HGNC xref and ID for other xrefs like INSDC protein
                      my $xref_display_name = $xref->[1];
                      $xref_display_name =~ s/(\s+[sS]ymbol$)|(\s+[iI][dD]$)//;
                      #Use dbprimary_acc_1074 column for all the other xrefs
                      my $field = "dbprimary_acc_1074";
                      # add in if the column exists
                      my $example = $self->get_example($table,$field);
                      push @{ $fdo->{Option} }, {
                        displayName  => "$xref_display_name ID(s) [e.g. $example]",
                        displayType  => "text",
                        description  => "Filter to include genes with supplied list of $xref->[1] ID(s)",
                        field        => $field,
                        hidden       => "false",
                        internalName => "$xref->[0]",
                        isSelectable => "true",
                        key          => $key,
                        legal_qualifiers => "=,in",
                        multipleValues   => "1",
                        qualifier        => "=",
                        tableConstraint  => $table,
                        type             => "List" };
                    }
                  }
                }
              } ## end if ( defined $self->{tables...})
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end elsif ( $fdo->{internalName...})
          elsif ( $fdo->{internalName} eq 'id_list_microarray_filters' ) {
            if ( !defined $probe_list) {
              $self->{delete}{$dataset->{name}."_".$fco->{internalName}}=1;
              $logger->info(
                            "No data for $fdo->{internalName}, removing it from the template");
            }
            else {
              $logger->info(
                            "Generating data for $fdo->{internalName}");
                foreach my $probe (@{ $probe_list }) {
                  my $field = "efg_".lc($probe->[1])."_bool";
                  my $table = $ds_name."__efg_".lc($probe->[1])."__dm";
                  if ( defined $self->{tables}->{$table} ) {
                    my $key = "transcript_id_1064_key";
                    if ( defined $self->{tables}->{$table}->{$key} ) {
                      my $main_table=${ds_name}."__transcript__main";
                      if (defined $self->{tables}->{$main_table}->{$field} ) {
                        my $display_name=$probe->[1];
                        $display_name =~ s/_/ /g;
                        # add in if the column exists
                        push @{ $fdo->{Option} }, {
                          displayName  => "With $display_name probe ID(s)",
                          displayType  => "list",
                          field        => $field,
                          hidden       => "false",
                          internalName => "with_".lc($probe->[1]),
                          isSelectable => "true",
                          key          => $key,
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
                      }
                  }
                }
              } ## end if ( defined $self->{tables...})
            }
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end elsif ( $fdo->{internalName...})
          elsif ( $fdo->{internalName} eq 'id_list_limit_microarray_filters' ) {
            #Checking if we have any probes for this species
            if ( !defined $probe_list) {
              $self->{delete}{$dataset->{name}."_".$fco->{internalName}}=1;
              $logger->info(
                            "No data for $fdo->{internalName}, removing it from the template");
              }
            else {
              $logger->info(
                            "Generating data for $fdo->{internalName}");
                foreach my $probe (@{ $probe_list }) {
                  my $field = "display_label_11056";
                  my $table = $ds_name."__efg_".lc($probe->[1])."__dm";
                  if ( defined $self->{tables}->{$table} ) {
                    my $key = "transcript_id_1064_key";
                    if ( defined $self->{tables}->{$table}->{$key} ) {
                      my $display_name=$probe->[1];
                      $display_name =~ s/_/ /g;
                      my $example = $self->get_example($table,$field);
                      # add in if the column exists
                      push @{ $fdo->{Option} }, {
                        displayName  => "$display_name probe ID(s) [e.g. $example]",
                        displayType  => "text",
                        description  => "Filter to include genes with supplied list of $display_name ID(s)",
                        field        => $field,
                        hidden       => "false",
                        internalName => lc($probe->[1]),
                        isSelectable => "true",
                        key          => $key,
                        legal_qualifiers => "=,in",
                        multipleValues   => "1",
                        qualifier        => "=",
                        tableConstraint  => $table,
                        type             => "List" };
                  }
                }
              } ## end if ( defined $self->{tables...})
            }
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end elsif ( $fdo->{internalName...})
          elsif ( $fdo->{internalName} eq 'id_list_protein_domain_and_feature_filters' ) {
            $logger->info(
                            "Generating data for $fdo->{internalName}");
            # Generating Filters for Extra Protein domains
            foreach my $extra_protein_domain (sort keys %{$extra_protein_domains}) {
                #We don't have a boolean filter for Ensembl Protein family
                next if $extra_protein_domain eq "family";
                my $field = $extra_protein_domains->{$extra_protein_domain}->{internal_name}."_bool";
                my $table = $extra_protein_domains->{$extra_protein_domain}->{'table'};
                 if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                    if ( defined $self->{tables}->{$table}->{$key} ) {
                      my $main_table=${ds_name}."__translation__main";
                      if (defined $self->{tables}->{$main_table}->{$field} ) {
                        # add in if the column exists
                        push @{ $fdo->{Option} }, {
                          displayName  => "With $extra_protein_domains->{$extra_protein_domain}->{'display_name'} ID(s)",
                          displayType  => "list",
                          field        => $field,
                          hidden       => "false",
                          internalName => "with_".$extra_protein_domains->{$extra_protein_domain}->{internal_name},
                          isSelectable => "true",
                          key          => $key,
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
                      }
                    }
                  }
              } ## end foreach my $extra_protein_domain
              foreach my $protein_domain_and_feature (@{ $protein_domain_and_feature_list }) {
                my $field = "protein_feature_".$protein_domain_and_feature->[0]."_bool";
                my $table = $ds_name."__protein_feature_".$protein_domain_and_feature->[0]."__dm";
                my $display_name;
                # We only want to display "ID(s)" for Protein domains
                if (defined $protein_domain_and_feature->[2] and $protein_domain_and_feature->[2] ne "{'type' => 'feature'}"){
                  $display_name="With $protein_domain_and_feature->[3] ID(s)";
                }
                elsif (defined $protein_domain_and_feature->[3])
                {
                  $display_name="With $protein_domain_and_feature->[3]";
                } 
                # BlastProdom don't have an analysis_description.
                else {
                  $display_name="With $protein_domain_and_feature->[1]";
                }
                if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                    if ( defined $self->{tables}->{$table}->{$key} ) {
                      my $main_table=${ds_name}."__translation__main";
                      if (defined $self->{tables}->{$main_table}->{$field} ) {
                        # add in if the column exists
                        push @{ $fdo->{Option} }, {
                          displayName  => $display_name,
                          displayType  => "list",
                          field        => $field,
                          hidden       => "false",
                          internalName => "with_".$protein_domain_and_feature->[0],
                          isSelectable => "true",
                          key          => $key,
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
                      }
                    }
                }
              } ## end if ( defined $self->{tables...})
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end elsif ( $fdo->{internalName...})
          elsif ( $fdo->{internalName} eq 'id_list_limit_protein_domain_filters' ) {
            $logger->info(
                            "Generating data for $fdo->{internalName}");
            # Generating Filters for Gene, Transcript, Protein and exons
            foreach my $extra_protein_domain (sort keys %{$extra_protein_domains}) {
                my $field = $extra_protein_domains->{$extra_protein_domain}->{'field'};
                my $table = $extra_protein_domains->{$extra_protein_domain}->{'table'};
                if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                    if (defined $self->{tables}->{$table}->{$field} ) {
                      my $example = $self->get_example($table,$field);
                      # add in if the column exists
                      push @{ $fdo->{Option} }, {
                        displayName  => $extra_protein_domains->{$extra_protein_domain}->{'display_name'}." ID(s) [e.g. $example]",
                        displayType  => "text",
                        description  => "Filter to include genes with supplied list of $extra_protein_domains->{$extra_protein_domain}->{'display_name'}",
                        field        => $field,
                        hidden       => "false",
                        internalName => $extra_protein_domains->{$extra_protein_domain}->{internal_name},
                        isSelectable => "true",
                        key          => $key,
                        legal_qualifiers => "=,in",
                        multipleValues   => "1",
                        qualifier        => "=",
                        tableConstraint  => $table,
                        type             => "List" };
                    }
                  }
                }
              } ## end foreach  my $extra_protein_domain
              # Generation all the other protein domains
              foreach my $protein_domain_and_feature (@{ $protein_domain_and_feature_list }) {
                # Excluding protein features
                next if !defined $protein_domain_and_feature->[2] or $protein_domain_and_feature->[2]  eq "{'type' => 'feature'}";
                my $table = $ds_name."__protein_feature_".$protein_domain_and_feature->[0]."__dm";
                if ( defined $self->{tables}->{$table} ) {
                  my $key = $self->get_table_key($table);
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                      my $field = "hit_name_1048";
                      my $display_name;
                      if (defined $protein_domain_and_feature->[3]) {
                        $display_name=$protein_domain_and_feature->[3];
                      }
                      else{
                        $display_name=$protein_domain_and_feature->[1];
                      }
                      # add in if the column exists
                      my $example = $self->get_example($table,$field);
                      push @{ $fdo->{Option} }, {
                        displayName  => "$display_name ID(s) [e.g. $example]",
                        displayType  => "text",
                        description  => "Filter to include genes with supplied list of $display_name ID(s)",
                        field        => $field,
                        hidden       => "false",
                        internalName => "$protein_domain_and_feature->[0]",
                        isSelectable => "true",
                        key          => $key,
                        legal_qualifiers => "=,in",
                        multipleValues   => "1",
                        qualifier        => "=",
                        tableConstraint  => $table,
                        type             => "List" };
                    }
                  }
              } ## end foreach  my $protein_domain_and_feature
            push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
            $nD++;
          } ## end elsif ( $fdo->{internalName...})
          elsif ( $fdo->{displayType} && $fdo->{displayType} eq 'container') {
            $logger->debug( "Processing options for " . $filterDescription->{internalName} );
            my $nO = 0;
            normalise( $filterDescription, "Option" );
            for my $option ( @{ $filterDescription->{Option} } ) {
              my $opt = copy_hash($option);
              update_table_keys( $opt, $dataset, $self->{keys} );
              $logger->debug( "Checking option " . $opt->{internalName});
              if ( defined $self->{tables}->{ $opt->{tableConstraint} } &&
                   defined $self->{tables}->{ $opt->{tableConstraint} }
                   ->{ $opt->{field} } &&
                   ( !defined $opt->{key} ||
                     defined $self->{tables}->{ $opt->{tableConstraint} }
                     ->{ $opt->{key} } ) )
              {
                $logger->debug( "Found option " . $opt->{internalName});
                push @{ $fdo->{Option} }, $opt;
                for my $o ( @{ $option->{Option} } ) {
                  push @{ $opt->{Option} }, $o;
                }
                $logger->debug(Dumper($opt));
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
            } ## end for my $option ( @{ $filterDescription...})
            if ( $nO > 0 ) {
              $logger->debug("Options found for filter ".$fdo->{internalName});
              push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
              $nD++;
            }
          } ## end elsif ( $fdo->{displayType... [ if ( $fdo->{internalName...})]})
          # Extra code to deal with Boolean filters which are not xrefs or probes
          elsif ( $fdo->{displayType} && $fdo->{displayType} eq 'list' && ($fdo->{type} eq 'boolean' or $fdo->{type} eq 'list') && defined $filterDescription->{Option}){
            my $nO = 0;
            if ( defined $self->{tables}->{ $fdo->{tableConstraint} } &&
                   defined $self->{tables}->{ $fdo->{tableConstraint} }
                   ->{ $fdo->{field} } &&
                   ( !defined $filterDescription->{key} ||
                     defined $self->{tables}->{ $fdo->{tableConstraint} }
                     ->{ $fdo->{key} } ) )
              {
                normalise( $filterDescription, "Option" );
                for my $option ( @{ $filterDescription->{Option} } ) {
                  my $opt = copy_hash($option);
                  push @{ $fdo->{Option} }, $opt;
                  for my $o ( @{ $option->{Option} } ) {
                    push @{ $opt->{Option} }, $o;
                  }
                  $nO++;
                }
                if ( $nO > 0 ) {
                  push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
                  $nD++;
                }
              } else {
                $logger->debug( "Could not find table " .
                                ( $filterDescription->{tableConstraint} || 'undef' ) . " field " .
                                ( $filterDescription->{field}           || 'undef' ) . ", Key " .
                                ( $filterDescription->{key} || 'undef' ) . ", FilterDescription " .
                                $filterDescription->{internalName} );
              }
            restore_main( $fdo, $ds_name );
          }
          ### end elsif ( $fdo->{displayType} && $fdo->{displayType} eq 'list' && $fdo->{type} eq 'boolean' && defined $filterDescription->{Option})
          else {
            if ( defined $fdo->{tableConstraint} ) {
              #### check tableConstraint and field and key
              if ( defined $self->{tables}->{ $fdo->{tableConstraint} } &&
                   defined $self->{tables}->{ $fdo->{tableConstraint} }
                   ->{ $fdo->{field} } &&
                   ( !defined $fdo->{key} ||
                     defined $self->{tables}->{ $fdo->{tableConstraint} }
                     ->{ $fdo->{key} } ) )
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
                  my %qtl_config=();
                  my $vals =
                    $self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>
"select distinct $fdo->{field} from $fdo->{tableConstraint} where $fdo->{field} is not null order by $fdo->{field} limit $max"
                    );
                  # If the dropdown is empty, remove it from the interface.
                  if ( scalar(@$vals) == 0)
                  {
                    $self->{delete}{$dataset->{name}."_".$fco->{internalName}}=1;
                    $self->{delete}{$dataset->{name}."_".$fdo->{internalName}}=1;
                    $logger->info(
                            "No data for $fdo->{internalName}, removing it from the template");
                  }
                  elsif ( scalar(@$vals) <= $self->{max_dropdown} ) {
                    if ($fdo->{internalName} eq "chromosome_name" || $fdo->{internalName} eq "chromosome" || $fdo->{internalName} eq 'chr_name' || $fdo->{internalName} eq 'name_2') {
                      # We need to sort the chromosome dropdown to make it more user friendly
                      @$vals = nsort(@$vals);
                      # Retrieving chr band informations
                      $fdo->{Option} = [];
                      my $chr_bands_kstart;
                      my $chr_bands_kend;
                      my $qtl_features;
                      if(defined $genomic_features_mart and $genomic_features_mart ne '') {
                        if ($fdo->{internalName} eq 'name_2') {
                          $qtl_features=$self->generate_chromosome_qtl_push_action($dataset->{name},$genomic_features_mart);
                        }
                        else {
                          ($chr_bands_kstart,$chr_bands_kend)=$self->generate_chromosome_bands_push_action($dataset->{name},$genomic_features_mart);
                        }
                      }
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
                        if (defined $qtl_features->{$val}){
                          my %hqtl_features=%$qtl_features;
                          foreach my $qtl (@{$hqtl_features{$val}}){
                            push @{ $qtl_config{$val} }, {
                                                             internalName => $qtl,
                                                             displayName  => $qtl,
                                                             value        => $qtl,
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
                        elsif(defined $qtl_config{$val})
                        {
                          push @{ $fdo->{Option} }, {
                            internalName => $val,
                            displayName  => $val,
                            value        => $val,
                            isSelectable => 'true',
                            useDefault   => 'true',
                            PushAction => [ {
                                   internalName => "qtl_region_push_$val",
                                   useDefault   => 'true',
                                   ref => 'qtl_region',
                                   Option => $qtl_config{$val} } ],
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
                push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
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
              push @{ $fco->{FilterDescription} }, $fdo unless exists $self->{delete}{$fdo->{internalName}};
              $nD++;
            }
            #### otherFilters *species3*
            if (defined $fdo->{otherFilters}){
              $fdo->{otherFilters} =~ s/\*species3\*/$dataset->{name}/g;
            }
            #### pointerDataSet *species3*
            if (defined $fdo->{pointerDataset}){
              $fdo->{pointerDataset} =~ s/\*species3\*/$dataset->{name}/g;
            }
            restore_main( $fdo, $ds_name );
          } ## end else [ if ( $fdo->{internalName...})]
        } ## end for my $filterDescription...
        if ( $nD > 0 ) {
          if(defined $fco->{checkPointerDataset}) {
          # check for special checkPointerDataset tag which allows us to remove unneeded PointerDataset filters which only exist as when
          # connecting to other marts using Importables/Exportables
          $fco->{checkPointerDataset} =~ s/\*species3\*/$dataset->{name}/g;
          my $pointer_dataset_table = check_pointer_dataset_table_exist($self,$dataset->{name},$genomic_features_mart,$fco->{checkPointerDataset});
          if(defined $pointer_dataset_table->[0]) {
            if ($pointer_dataset_table->[0] >= 0) {
              delete $fco->{checkPointerDataset};
              push @{ $fgo->{FilterCollection} }, $fco unless (exists $self->{delete}{$fco->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$fco->{internalName}});
              $nC++;
            }
          }
        } else {
            push @{ $fgo->{FilterCollection} }, $fco unless ( exists $self->{delete}{$fco->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$fco->{internalName}});
            $nC++;
          }
        }
      } ## end for my $filterCollection...

      if ( $nC > 0 ) {
        if(defined $fgo->{checkTable}) {
          # check for special checkTable tag which allows us to remove unneeded filters (e.g: ontology, variation filters in gene mart)
          my $table = $ds_name.'__'.$fgo->{checkTable};
          if(exists $self->{tables}->{$table}) {
            delete $fgo->{checkTable};
            push @{ $fpo->{FilterGroup} }, $fgo unless exists $self->{delete}{$fgo->{internalName}};
            $nG++;
          }
        } else {
          push @{ $fpo->{FilterGroup} }, $fgo unless (exists $self->{delete}{$fgo->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$fgo->{internalName}});
          $nG++;
        }
      }
    } ## end for my $filterGroup ( @...)
    if ( $nG > 0 ) {
      push @{ $templ_out->{FilterPage} }, $fpo unless exists $self->{delete}{$fpo->{internalName}};
    }
  } ## end for my $filterPage ( @{...})
  return;
} ## end sub write_filters

sub write_attributes {
  my ( $self, $dataset, $templ_in, $datasets,$xref_list, $probe_list, $xref_url_list, $exception_xrefs, $protein_domain_and_feature_list, $ds_name, $core_dba, $regulation_dba, $variation_dba ) = @_;
  $logger->info( "Writing attributes for " . $dataset->{name} );
  my $templ_out = $dataset->{config};
  # AttributePage
  for my $attributePage ( @{ elem_as_array($templ_in->{AttributePage}) } ) {
    $logger->debug( "Processing filterPage " . $attributePage->{internalName} );
    # Check if the species has variation data, else skip this page.
    if ($attributePage->{internalName} eq "snp") {
      if(defined $attributePage->{checkTable}) {
        # check for special checkTable tag which allows us to remove unneeded sections
        my $table = $ds_name.'__'.$attributePage->{checkTable};
        if(exists $self->{tables}->{$table}) {
          1;
        }
        else {
          $logger->info( "No Variation data for this dataset, removing attributes");
          next;
        }
      }
    }
    elsif ($attributePage->{internalName} eq "snp_somatic"){
      if(defined $attributePage->{checkTable}) {
        # check for special checkTable tag which allows us to remove unneeded sections
        my $table = $ds_name.'__'.$attributePage->{checkTable};
        if(exists $self->{tables}->{$table}) {
          1;
        }
        else {
          $logger->info( "No Variation somatic data for this dataset, removing attributes");
          next;
        }
      }
    }
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
        for my $o_dataset (@$datasets) {
          my $table = "${ds_name}__homolog_$o_dataset->{name}__dm";
          if ( defined $self->{tables}->{$table} ) {
            push @{ $ago->{AttributeCollection} }, {
              displayName          => "$o_dataset->{display_name} Orthologues",
              internalName         => "homolog_$o_dataset->{name}",
              AttributeDescription => [ {
                  displayName  => "$o_dataset->{display_name} gene stable ID",
                  field        => "stable_id_4016_r2",
                  internalName => "$o_dataset->{name}_homolog_ensembl_gene",
                  key          => "gene_id_1020_key",
                  linkoutURL =>
                    "exturl|/$o_dataset->{production_name}/Gene/Summary?g=%s",
                  maxLength       => "128",
                  tableConstraint => $table }, {
                  displayName  => "$o_dataset->{display_name} gene name",
                  field        => "display_label_40273_r1",
                  linkoutURL  => "exturl|/$o_dataset->{production_name}/Gene/Summary?g=%s|$o_dataset->{name}_homolog_ensembl_gene",
                  internalName => "$o_dataset->{name}_homolog_associated_gene_name",
                  key          => "gene_id_1020_key",
                  maxLength    => "128",
                  tableConstraint => $table },{
                  displayName  => "$o_dataset->{display_name} protein or transcript stable ID",
                  field        => "stable_id_4016_r3",
                  internalName => "$o_dataset->{name}_homolog_ensembl_peptide",
                  key          => "gene_id_1020_key",
                  maxLength    => "128",
                  tableConstraint => $table }, {
                  displayName => "$o_dataset->{display_name} chromosome/scaffold name",
                  field       => "chr_name_4016_r2",
                  internalName    => "$o_dataset->{name}_homolog_chromosome",
                  key             => "gene_id_1020_key",
                  maxLength       => "40",
                  tableConstraint => $table }, {
                  displayName     => "$o_dataset->{display_name} chromosome/scaffold start (bp)",
                  field           => "chr_start_4016_r2",
                  internalName    => "$o_dataset->{name}_homolog_chrom_start",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "$o_dataset->{display_name} chromosome/scaffold end (bp)",
                  field           => "chr_end_4016_r2",
                  internalName    => "$o_dataset->{name}_homolog_chrom_end",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName => "Query protein or transcript ID",
                  field       => "stable_id_4016_r1",
                  internalName =>
                    "$o_dataset->{name}_homolog_canonical_transcript_protein",
                  key             => "gene_id_1020_key",
                  maxLength       => "128",
                  tableConstraint => $table }, {
                  displayName     => "Last common ancestor with $o_dataset->{display_name}",
                  field           => "node_name_40192",
                  internalName    => "$o_dataset->{name}_homolog_subtype",
                  key             => "gene_id_1020_key",
                  maxLength       => "40",
                  tableConstraint => $table }, {
                  displayName     => "$o_dataset->{display_name} homology type",
                  field           => "description_4014",
                  internalName    => "$o_dataset->{name}_homolog_orthology_type",
                  key             => "gene_id_1020_key",
                  maxLength       => "25",
                  tableConstraint => $table }, {
                  displayName     => "%id. target $o_dataset->{display_name} gene identical to query gene",
                  field           => "perc_id_4015",
                  internalName    => "$o_dataset->{name}_homolog_perc_id",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "%id. query gene identical to target $o_dataset->{display_name} gene",
                  field           => "perc_id_4015_r1",
                  internalName    => "$o_dataset->{name}_homolog_perc_id_r1",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table },{
                  displayName     => "$o_dataset->{display_name} Gene-order conservation score",
                  field           => "goc_score_4014",
                  internalName    => "$o_dataset->{name}_homolog_goc_score",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table },{
                  displayName     => "$o_dataset->{display_name} Whole-genome alignment coverage",
                  field           => "wga_coverage_4014",
                  internalName    => "$o_dataset->{name}_homolog_wga_coverage",
                  key             => "gene_id_1020_key",
                  maxLength       => "5",
                  tableConstraint => $table }, {
                  displayName     => "dN with $o_dataset->{display_name}",
                  field           => "dn_4014",
                  internalName    => "$o_dataset->{name}_homolog_dn",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName     => "dS with $o_dataset->{display_name}",
                  field           => "ds_4014",
                  internalName    => "$o_dataset->{name}_homolog_ds",
                  key             => "gene_id_1020_key",
                  maxLength       => "10",
                  tableConstraint => $table }, {
                  displayName  => "$o_dataset->{display_name} orthology confidence [0 low, 1 high]",
                  field        => "is_high_confidence_4014",
                  internalName => "$o_dataset->{name}_homolog_orthology_confidence",
                  key          => "gene_id_1020_key",
                  maxLength    => "10",
                  tableConstraint => $table } ] };
            $nC++;
          } ## end if ( defined $self->{tables...})
        } ## end for my $o_dataset (@$datasets)
      } ## end if ( $ago->{internalName...})
      elsif ( $ago->{internalName} eq 'paralogs' ) {
        my $table = "${ds_name}__paralog_$dataset->{name}__dm";
        if ( defined $self->{tables}->{$table} ) {
          push @{ $ago->{AttributeCollection} }, {

            displayName          => "$dataset->{display_name} Paralogues",
            internalName         => "paralogs_$dataset->{name}",
            AttributeDescription => [ {
                displayName     => "$dataset->{display_name} paralogue gene stable ID",
                field           => "stable_id_4016_r2",
                internalName    => "$dataset->{name}_paralog_ensembl_gene",
                key             => "gene_id_1020_key",
                linkoutURL      => "exturl|/$dataset->{production_name}/Gene/Summary?g=%s",
                maxLength       => "140",
                tableConstraint => $table }, {
                displayName => "$dataset->{display_name} paralogue associated gene name",
                field       => "display_label_40273_r1",
                linkoutURL  => "exturl|/$dataset->{production_name}/Gene/Summary?g=%s|$dataset->{name}_paralog_ensembl_gene",
                internalName => "$dataset->{name}_paralog_associated_gene_name",
                key             => "gene_id_1020_key",
                maxLength       => "128",
                tableConstraint => $table }, {
                displayName => "$dataset->{display_name} paralogue protein or transcript ID",
                field       => "stable_id_4016_r3",
                internalName => "$dataset->{name}_paralog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} paralogue chromosome/scaffold name",
                field           => "chr_name_4016_r2",
                internalName    => "$dataset->{name}_paralog_chromosome",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} paralogue chromosome/scaffold start (bp)",
                field           => "chr_start_4016_r2",
                internalName    => "$dataset->{name}_paralog_chrom_start",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} paralogue chromosome/scaffold end (bp)",
                field           => "chr_end_4016_r2",
                internalName    => "$dataset->{name}_paralog_chrom_end",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogue query protein or transcript ID",
                field           => "stable_id_4016_r1",
                internalName    => "$dataset->{name}_paralog_canonical_transcript_protein",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Paralogue last common ancestor with $dataset->{display_name}",
                field           => "node_name_40192",
                internalName    => "$dataset->{name}_paralog_subtype",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} paralogue homology type",
                field           => "description_4014",
                internalName    => "$dataset->{name}_paralog_orthology_type",
                key             => "gene_id_1020_key",
                maxLength       => "25",
                tableConstraint => $table }, {
                displayName     => "Paralogue %id. target $dataset->{display_name} gene identical to query gene",
                field           => "perc_id_4015",
                internalName    => "$dataset->{name}_paralog_perc_id",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogue %id. query gene identical to target $dataset->{display_name} gene",
                field           => "perc_id_4015_r1",
                internalName    => "$dataset->{name}_paralog_perc_id_r1",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogue dN with $dataset->{display_name}",
                field           => "dn_4014",
                internalName    => "$dataset->{name}_paralog_dn",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Paralogue dS with $dataset->{display_name}",
                field           => "ds_4014",
                internalName    => "$dataset->{name}_paralog_ds",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} paralogy confidence [0 low, 1 high]",
                field           => "is_high_confidence_4014",
                internalName    => "$dataset->{name}_paralog_paralogy_confidence",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table } ] };
          $nC++;
        } ## end if ( defined $self->{tables...})

      } ## end elsif ( $ago->{internalName... [ if ( $ago->{internalName...})]})
      elsif ( $ago->{internalName} eq 'homoeologs' ) {
        my $table = "${ds_name}__homoeolog_$dataset->{name}__dm";
        if ( defined $self->{tables}->{$table} ) {
          push @{ $ago->{AttributeCollection} }, {

            displayName          => "$dataset->{display_name} Homoeologues",
            internalName         => "paralogs_$dataset->{name}",
            AttributeDescription => [ {
                displayName     => "$dataset->{display_name} homoeologue gene stable ID",
                field           => "stable_id_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_gene",
                key             => "gene_id_1020_key",
                linkoutURL      => "exturl|/$dataset->{production_name}/Gene/Summary?g=%s",
                maxLength       => "140",
                tableConstraint => $table }, {
                displayName => "$dataset->{display_name} homoeologue protein or transcript stable ID",
                field       => "stable_id_4016_r3",
                internalName =>
                  "$dataset->{name}_homoeolog_homoeolog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} homoeologue chromosome/scaffold name",
                field           => "chr_name_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_chromosome",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} homoeologue chromosome/scaffold start (bp)",
                field           => "chr_start_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_chrom_start",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} homoeologue chromosome/scaffold end (bp)",
                field           => "chr_end_4016_r2",
                internalName    => "$dataset->{name}_homoeolog_chrom_end",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Homoeologue query protein or transcript ID",
                field           => "stable_id_4016_r1",
                internalName    => "$dataset->{name}_homoeolog_ensembl_peptide",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "Homoelogue last common ancestor with $dataset->{display_name}",
                field           => "node_name_40192",
                internalName    => "$dataset->{name}_homoeolog_ancestor",
                key             => "gene_id_1020_key",
                maxLength       => "40",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} homoelogue homology type",
                field           => "description_4014",
                internalName    => "$dataset->{name}_homoeolog_type",
                key             => "gene_id_1020_key",
                maxLength       => "25",
                tableConstraint => $table }, {
                displayName     => "Homoelogue %id. target $dataset->{display_name} gene identical to query gene",
                field           => "perc_id_4015",
                internalName    => "homoeolog_$dataset->{name}_identity",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName  => "Homoeologue %id. query gene identical to target $dataset->{display_name} gene",
                field        => "perc_id_4015_r1",
                internalName => "homoeolog_$dataset->{name}_homoeolog_identity",
                key          => "gene_id_1020_key",
                maxLength    => "10",
                tableConstraint => $table }, {
                displayName     => "Homoeologue dN with $dataset->{display_name}",
                field           => "dn_4014",
                internalName    => "$dataset->{name}_homoeolog_dn",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "Homoeologue dS with $dataset->{display_name}",
                field           => "ds_4014",
                internalName    => "$dataset->{name}_homoeolog_ds",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }, {
                displayName     => "$dataset->{display_name} homoeology confidence [0 low, 1 high]",
                field           => "is_high_confidence_4014",
                internalName    => "$dataset->{name}_homoeolog_confidence",
                key             => "gene_id_1020_key",
                maxLength       => "10",
                tableConstraint => $table }]};
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

          if ( $aco->{internalName} eq 'xrefs' ) {
            $logger->info(
                            "Generating data for $aco->{internalName} attributes");
            foreach my $xref (@{ $xref_list }) {
              #Skipping GO and GOA attributes since they have their own attribute section
              next if ($xref->[0] eq "go" or $xref->[0] eq "goslim_goa");
              my $table = $ds_name."__ox_".$xref->[0]."__dm";
              if ( defined $self->{tables}->{$table} ) {
                my $key = $self->get_table_key($table);
                if ( defined $self->{tables}->{$table}->{$key} ) {
                  my $url = $xref_url_list->{$xref->[0]};
                  # Replacing ###ID### with mart placeholder %s and ###SPECIES### with
                  # species production name
                  if (defined $url) {
                    $url =~ s/###ID###/%s/;
                    $url =~ s/###SPECIES###/$dataset->{production_name}/;
                    $url = "exturl|".$url;
                  }
                  else{
                    $url='';
                  }
                  # Getting exception where we should use dbprimary_acc_1074 instead of display_label_1074 or both
                  if (exists $exception_xrefs->{$xref->[0]}) {
                    while (my ($field,$suffix) = each %{$exception_xrefs->{$xref->[0]}->{columns}}) {
                      #For extra attribute using URL of the main field dbprimary_acc_1074
                      if ($field ne "dbprimary_acc_1074")
                      {
                        $url=$url."|".$xref->[0]."_".lc($exception_xrefs->{$xref->[0]}->{columns}->{"dbprimary_acc_1074"})
                      }
                      #Remove from display name string whatever has been defined in the regex below.
                      # At the moment removing Symbol for the HGNC xref and ID for other xrefs like INSDC protein
                      my $xref_display_name = $xref->[1];
                      $xref_display_name =~ s/(\s+[sS]ymbol$)|(\s+[iI][dD]$)//;
                      push @{ $aco->{AttributeDescription} }, {
                        key             => $key,
                        displayName     => "$xref_display_name $suffix",
                        field           => $field,
                        internalName    => $xref->[0]."_".lc($suffix),
                        linkoutURL      => $url,
                        maxLength       => "512",
                        tableConstraint => $table };
                      $nD++;
                      }
                    }
                  # All the other xrefs
                  else {
                    #Remove from display name string whatever has been defined in the regex below.
                    # At the moment removing Symbol for the HGNC xref and ID for other xrefs like INSDC protein
                    my $xref_display_name = $xref->[1];
                    $xref_display_name =~ s/(\s+[sS]ymbol$)|(\s+[iI][dD]$)//;
                    my $field = "dbprimary_acc_1074";
                    push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$xref_display_name ID",
                      field           => $field,
                      internalName    => "$xref->[0]",
                      linkoutURL      => $url,
                      maxLength       => "512",
                      tableConstraint => $table };
                  $nD++;
                  }
                }
              }
            } ## end if ( defined $self->{tables...})
          } ## end elsif ( $aco->{internalName... [ if ( $aco->{internalName...})]})
          elsif ( $aco->{internalName} eq 'microarray' ) {
            $logger->info(
                            "Generating data for $aco->{internalName} attributes");
            foreach my $probe (@{ $probe_list }) {
              my $field = "display_label_11056";
              my $url;
              # Using is_probeset_array to generate the linkout URL
              # This link is mainly for affy probes
              if ($probe->[2]){
                $url="exturl|/$dataset->{production_name}/Location/Genome?ftype=ProbeFeature;fdb=funcgen;id=%s;ptype=pset;";
              }
              # This link is for other probes like codelink, agilent, illumina...
              else {
                $url="exturl|/$dataset->{production_name}/Location/Genome?ftype=ProbeFeature;fdb=funcgen;id=%s";
              }
              my $table = $ds_name."__efg_".lc($probe->[1])."__dm";
                if ( defined $self->{tables}->{$table} ) {
                  my $key = "transcript_id_1064_key";
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                    my $display_name=$probe->[1];
                    $display_name =~ s/_/ /g;
                    push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name probe",
                      field           => $field,
                      internalName    => lc($probe->[1]),
                      linkoutURL      => $url,
                      maxLength       => "140",
                      tableConstraint => $table };
                  $nD++;
                  }
                }
            } ## end if ( defined $self->{tables...})
          } ## end elsif ( $aco->{internalName... [ if ( $aco->{internalName...})]})
          elsif ( $aco->{internalName} eq 'protein_domain' ) {
            $logger->info(
                            "Generating data for $aco->{internalName} attributes");
            foreach my $protein_domain_and_feature (@{ $protein_domain_and_feature_list }) {
              # Excluding protein features
              next if !defined $protein_domain_and_feature->[2] or $protein_domain_and_feature->[2] eq "{'type' => 'feature'}";
              my $table = $ds_name."__protein_feature_".$protein_domain_and_feature->[0]."__dm";
                if ( defined $self->{tables}->{$table} ) {
                  my $key = "translation_id_1068_key";
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                    # Getting URLs from the parsed Web ini file
                    my $url = $xref_url_list->{lc($protein_domain_and_feature->[1])};
                    # Replacing ###ID### with mart placeholder %s and ###SPECIES### with
                    # species production name
                    if (defined $url) {
                      $url =~ s/###ID###/%s/;
                      $url =~ s/###SPECIES###/$dataset->{production_name}/;
                      $url = "exturl|".$url;
                    }
                    else{
                      $url='';
                    }
                    my $display_name;
                    if (defined $protein_domain_and_feature->[3]) {
                      $display_name=$protein_domain_and_feature->[3];
                    }
                    else{
                      $display_name=$protein_domain_and_feature->[1];
                    }
                    push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name ID",
                      field           => "hit_name_1048",
                      internalName    => $protein_domain_and_feature->[0],
                      linkoutURL      => $url,
                      maxLength       => "40",
                      tableConstraint => $table };
                      push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name start",
                      field           => "seq_start_1048",
                      internalName    => $protein_domain_and_feature->[0]."_start",
                      linkoutURL      => "exturl|/$dataset->{production_name}/Transcript/Domains?db=core;t=%s|ensembl_transcript_id",
                      maxLength       => "10",
                      tableConstraint => $table };
                      push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name end",
                      field           => "seq_end_1048",
                      internalName    => $protein_domain_and_feature->[0]."_end",
                      linkoutURL      => "exturl|/$dataset->{production_name}/Transcript/Domains?db=core;t=%s|ensembl_transcript_id",
                      maxLength       => "10",
                      tableConstraint => $table };
                  $nD++;
                  }
                }
            } ## end if ( defined $self->{tables...})
          } ## end elsif ( $aco->{internalName... [ if ( $aco->{internalName...})]})
          elsif ( $aco->{internalName} eq 'protein_feature' ) {
            $logger->info(
                            "Generating data for $aco->{internalName} attributes");
            foreach my $protein_domain_and_feature (@{ $protein_domain_and_feature_list }) {
              #Excluding protein domains
              next if $protein_domain_and_feature->[2] eq "{'type' => 'domain'}";
              my $table = $ds_name."__protein_feature_".$protein_domain_and_feature->[0]."__dm";
                if ( defined $self->{tables}->{$table} ) {
                  my $key = "translation_id_1068_key";
                  if ( defined $self->{tables}->{$table}->{$key} ) {
                    my $url = $xref_url_list->{lc($protein_domain_and_feature->[1])};
                    # Replacing ###ID### with mart placeholder %s and ###SPECIES### with
                    # species production name
                    if (defined $url) {
                      $url =~ s/###ID###/%s/;
                      $url =~ s/###SPECIES###/$dataset->{production_name}/;
                      $url = "exturl|".$url;
                    }
                    else{
                      $url='';
                    }
                    my $display_name;
                    if (defined $protein_domain_and_feature->[3]) {
                      $display_name=$protein_domain_and_feature->[3];
                    }
                    else{
                      $display_name=$protein_domain_and_feature->[1];
                    }
                    push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name",
                      field           => "hit_name_1048",
                      internalName    => $protein_domain_and_feature->[0],
                      linkoutURL      => $url,
                      maxLength       => "40",
                      tableConstraint => $table };
                      push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name start",
                      field           => "seq_start_1048",
                      internalName    => $protein_domain_and_feature->[0]."_start",
                      linkoutURL      => "exturl|/$dataset->{production_name}/Transcript/Domains?db=core;t=%s|ensembl_transcript_id",
                      maxLength       => "10",
                      tableConstraint => $table };
                      push @{ $aco->{AttributeDescription} }, {
                      key             => $key,
                      displayName     => "$display_name end",
                      field           => "seq_end_1048",
                      internalName    => $protein_domain_and_feature->[0]."_end",
                      linkoutURL      => "exturl|/$dataset->{production_name}/Transcript/Domains?db=core;t=%s|ensembl_transcript_id",
                      maxLength       => "10",
                      tableConstraint => $table };
                  $nD++;
                  }
                }
            } ## foreach my $protein_domain_and_feature
          } ## end elsif ( $aco->{internalName... [ if ( $aco->{internalName...})]})
          ### AttributeDescription
          for my $attributeDescription (
                             @{ $attributeCollection->{AttributeDescription} } )
          {
            my $ado = copy_hash($attributeDescription);
            #### pointerDataSet *species3* and *species4*
            if (defined $ado->{pointerDataset}) {
              $ado->{pointerDataset} =~ s/\*species4\*/$ds_name/;
              $ado->{pointerDataset} =~ s/\*species3\*/$dataset->{name}/;
            }
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
                 ( !defined $ado->{key} ||
                   defined $self->{tables}->{ $ado->{tableConstraint} }
                   ->{ $ado->{key} } ) )
              {
                push @{ $aco->{AttributeDescription} }, $ado unless (exists $self->{delete}{$ado->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$ado->{internalName}});
                $nD++;
              }
              else {
                $logger->debug( "Could not find table " .
                        ( $ado->{tableConstraint} || 'undef' ) . " field " .
                        ( $ado->{field}           || 'undef' ) . ", Key " .
                        ( $ado->{key} || 'undef' ) . ", AttributeDescription " .
                        $ado->{internalName} );
              }
            } ## end if ( defined $ado->{tableConstraint...})
            else {
              if (defined $ado->{pointerDataset}){
                $ado->{pointerDataset} =~ s/\*species4\*/$ds_name/g;
                $ado->{pointerDataset} =~ s/\*species3\*/$dataset->{name}/g;
              }
              push @{ $aco->{AttributeDescription} }, $ado unless (exists $self->{delete}{$ado->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$ado->{internalName}});
              $nD++;
            }
            if ( defined $ado->{linkoutURL} ) {
              if ( $ado->{linkoutURL} =~ m/exturl|\/\*species2\*/ ) {
                # reformat to add URL placeholder
                $ado->{linkoutURL} =~
                  s/\*species2\*/$dataset->{production_name}/;
              }
            }
            restore_main( $ado, $ds_name );
          } ## end for my $attributeDescription...

          if ( $nD > 0 ) {
            push @{ $ago->{AttributeCollection} }, $aco unless (exists $self->{delete}{$aco->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$aco->{internalName}});
            $nC++;
          }
        } ## end for my $attributeCollection...
      } ## end else [ if ( $ago->{internalName...})]

      if ( $nC > 0 ) {
        push @{ $apo->{AttributeGroup} }, $ago unless (exists $self->{delete}{$ago->{internalName}} or exists $self->{delete}{$dataset->{name}."_".$ago->{internalName}});
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
  if(defined $hash->{$key}) {
    $hash->{$key} = [ $hash->{$key} ] unless ref( $hash->{$key} ) eq 'ARRAY';
  }
  return;
}

sub elem_as_array {
  my ($elem) = @_;
  if(!defined $elem) {
    $elem = [];
  } elsif(ref($elem) ne 'ARRAY') {
    $elem = [$elem];
  }
  return $elem;
}

sub update_table_keys {
  my ( $obj, $dataset, $keys ) = @_;
  my $ds_name = $dataset->{config}->{dataset};
  if ( defined $obj->{tableConstraint} ) {
    if ( $obj->{tableConstraint} eq 'main' ) {
      if ( !defined $obj->{key} ) {
        ( $obj->{tableConstraint} ) = @{ $dataset->{config}->{MainTable} };
        $obj->{tableConstraint} =~ s/\*base_name\*/${ds_name}/;
      }
      else {
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
        elsif ( $obj->{key} eq 'variation_id_2025_key' ) {
          $obj->{tableConstraint} = "${ds_name}__variation__main";
        }
        elsif ( $obj->{key} eq 'variation_feature_id_2026_key' ) {
          $obj->{tableConstraint} = "${ds_name}__variation_feature__main";
        }
        elsif ( $obj->{key} eq 'structural_variation_id_2072_key' ) {
          $obj->{tableConstraint} = "${ds_name}__structural_variation__main";
        }
        elsif ( $obj->{key} eq 'structural_variation_feature_id_20104_key' ) {
          $obj->{tableConstraint} = "${ds_name}__structural_variation_feature__main";
        }
        elsif ( $obj->{key} eq 'annotated_feature_id_103_key' ) {
          $obj->{tableConstraint} = "${ds_name}__annotated_feature__main";
        }
        elsif ( $obj->{key} eq 'external_feature_id_1021_key' ) {
          $obj->{tableConstraint} = "${ds_name}__external_feature__main";
        }
        elsif ( $obj->{key} eq 'mirna_target_feature_id_1079_key' ) {
          $obj->{tableConstraint} = "${ds_name}__mirna_target_feature__main";
        }
        elsif ( $obj->{key} eq 'motif_feature_id_1065_key' ) {
          $obj->{tableConstraint} = "${ds_name}__motif_feature__main";
        }
        elsif ( $obj->{key} eq 'regulatory_feature_id_1036_key' ) {
          $obj->{tableConstraint} = "${ds_name}__regulatory_feature__main";
        }
        elsif ( $obj->{key} eq 'misc_feature_id_1037_key' ) {
          $obj->{tableConstraint} = "${ds_name}__misc_feature__main";
        }
        elsif ( $obj->{key} eq 'phenotype_feature_id_2023_key' ) {
          $obj->{tableConstraint} = "${ds_name}__qtl_feature__main";
        }
        elsif ( $obj->{key} eq 'karyotype_id_1027_key' ) {
          $obj->{tableConstraint} = "${ds_name}__karyotype__main";
        }
        elsif ( $obj->{key} eq 'marker_feature_id_1031_key' ) {
          $obj->{tableConstraint} = "${ds_name}__marker_feature__main";
        }
        elsif ( $obj->{key} eq 'closure_id_301_key') {
         $obj->{tableConstraint} = "${ds_name}__closure__main";
        }
        elsif ( $obj->{key} eq 'closure_mini_id_301_key') {
         $obj->{tableConstraint} = "${ds_name}__closure_mini__main";
        }
        elsif ( $obj->{key} eq 'closure_regulation_id_301_key') {
         $obj->{tableConstraint} = "${ds_name}__closure_regulation__main";
        }
        elsif ( $obj->{key} eq 'closure_motif_id_301_key') {
         $obj->{tableConstraint} = "${ds_name}__closure_motif__main";
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
  } ## end if ( defined $obj->{tableConstraint...})
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
  my $rows = $self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select count(version) from meta_version__version__main" );
  # Only add a row if the table is empty
  if ($rows->[0] == 0) {
    $self->{dbc}->sql_helper()
                    ->execute_update(-SQL => "INSERT INTO meta_version__version__main VALUES ('0.7')" );
  }
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

  if ( !-d "./tmp" ) {
    mkdir "./tmp";
  }
  open my $out, ">", "./tmp/tmp.xml";
  print $out $template_xml;
  close $out;
  my $gzip_template;
  gzip \$template_xml => \$gzip_template;

  $self->{dbc}->sql_helper()
    ->execute_update( -SQL =>
                      "DELETE FROM meta_template__xml__dm WHERE template=?",
                      -PARAMS=>[$template_name]
                    );

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
  my ( $self, $dataset, $template_name, $ds_name ) = @_;
  my $speciesId = $dataset->{species_id};

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
                      q/DELETE m,i,u,x,t FROM meta_conf__dataset__main m
left join meta_conf__interface__dm i using (dataset_id_key)
left join meta_conf__user__dm u using (dataset_id_key)
left join meta_conf__xml__dm x using (dataset_id_key)
left join meta_template__template__main t using (dataset_id_key)
 WHERE dataset=?/,
                      -PARAMS=>[$ds_name]
                    );

  $self->{dbc}->sql_helper()
    ->execute_update( -SQL =>
"INSERT INTO meta_template__template__main VALUES($speciesId,'$template_name')"
    );

  $self->{dbc}->sql_helper()->execute_update(
    -SQL =>
"INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,?,?,?,?)",
    -PARAMS => [ $speciesId,
                 $ds_name,
                 $dataset->{dataset_display_name},
                 "Ensembl $template_name",
                 $template_properties->{$template_name}->{type},
                 $template_properties->{$template_name}->{visible},
                 $dataset->{assembly} ] );

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
    ->sql_helper->execute_update( -SQL => "CREATE TABLE IF NOT EXISTS $table_name (" .
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

=head2 generate_chromosome_qtl_push_action
  Description: Retrieve a list of chromosome band_start and band_end for a given dataset
  Arg        : Mart dataset name
  Arg        : Genomic features mart name
  Returntype : 2 Hashrefs (keys are seq_region names, values are associated array of band). On hasref for band_start and one for band_end.
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub generate_chromosome_bands_push_action {
  my ($self,$dataset_name,$genomic_features_mart)= @_;
  my $chr_bands_kstart;
  my $chr_bands_kend;

  my $database_tables =$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${genomic_features_mart}'" );
  if (defined $database_tables->[0]) {
    if ($database_tables->[0] > 0) {
      my $empty_ks_table=$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${genomic_features_mart}' and table_name='${dataset_name}_karyotype_start__karyotype__main'" );
      my $empty_ke_table=$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${genomic_features_mart}' and table_name='${dataset_name}_karyotype_start__karyotype__main'" );
      if(defined $empty_ks_table->[0] and defined $empty_ke_table->[0]) {
        if ($empty_ks_table->[0] > 0 and $empty_ke_table->[0] > 0) {
          $chr_bands_kstart = $self->{dbc}->sql_helper()->execute_into_hash(
            -SQL => "select name_1059, band_1027 from ${genomic_features_mart}.${dataset_name}_karyotype_start__karyotype__main where band_1027 is not null order by band_1027",
            -CALLBACK => sub {
              my ( $row, $value ) = @_;
              $value = [] if !defined $value;
              push($value, $row->[1] );
              return $value;
              }
          );
          $chr_bands_kend = $self->{dbc}->sql_helper()->execute_into_hash(
            -SQL => "select name_1059, band_1027 from ${genomic_features_mart}.${dataset_name}_karyotype_end__karyotype__main where band_1027 is not null order by band_1027",
            -CALLBACK => sub {
              my ( $row, $value ) = @_;
              $value = [] if !defined $value;
              push($value, $row->[1] );
              return $value;
              }
          );
        }
      }
    }
  }
  return ($chr_bands_kstart,$chr_bands_kend);
}

=head2 generate_chromosome_qtl_push_action
  Description: Retrieve a list of chromosome QTLs for a given dataset
  Arg        : Mart dataset name
  Arg        : Genomic features mart name
  Returntype : Hashref (keys are seq_region names, values are associated array of QTL regions)
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub generate_chromosome_qtl_push_action {
  my ($self,$dataset_name,$genomic_features_mart)= @_;
  my $qtl_features;

  my $database_tables =$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${genomic_features_mart}'" );
  if (defined $database_tables->[0]) {
    if ($database_tables->[0] > 0) {
      my $empty_qtl_table=$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${genomic_features_mart}' and table_name='${dataset_name}_qtl_feature__qtl_feature__main'" );
      if(defined $empty_qtl_table->[0]) {
        if ($empty_qtl_table->[0] > 0) {
          $qtl_features = $self->{dbc}->sql_helper()->execute_into_hash(
            -SQL => "select distinct name_2033, qtl_region from ${genomic_features_mart}.${dataset_name}_qtl_feature__qtl_feature__main where qtl_region is not null order by qtl_region",
            -CALLBACK => sub {
              my ( $row, $value ) = @_;
              $value = [] if !defined $value;
              push($value, $row->[1] );
              return $value;
              }
          );
        }
      }
    }
  }
  return $qtl_features;
}

=head2 generate_xrefs_list
  Description: Retrieve a list of xrefs for a given dataset. Subroutine use the information_schema database and Core database external_db table.
  Arg        : Mart dataset name,
  Arg        : Core database adaptor
  Returntype : Hashref (keys are xrefs names, values are associated xref display name)
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub generate_xrefs_list {
  my ($self,$dataset,$core_dba)= @_;
  my $db_dbc = $core_dba->dbc();
  my $database = $db_dbc->dbname;
  my $xrefs_list;
  my $database_tables = $db_dbc->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${database}'" );
    if (defined $database_tables->[0]) {
      # Need to make sure all the db_name are lowercase and don't contain / to match mart table names.
      # Removed "Symbol" from HGNC symbol as this is causing issues in the attribute section
      $xrefs_list = $db_dbc->sql_helper()->execute(
        -SQL => "select distinct(REPLACE(LOWER(db_name),'/','')), db_display_name from ${database}.external_db order by db_display_name");
    }
    else {
      die "$database database is missing from the server\n"
    }
  $db_dbc->disconnect_if_idle();
  return ($xrefs_list);
}

=head2 generate_probes_list
  Description: Retrieve a list of probes for a given dataset. Subroutine use the information_schema database and MTMP_probestuff_helper table.
  Arg        : Mart dataset name
  Arg        : Regulation database adaptor
  Returntype : Hashref (keys are mircroarray names, values are associated microarray vendor and name)
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub generate_probes_list {
  my ($self,$dataset,$regulation_dba)= @_;
  my $probes_list;
  if (defined $regulation_dba){
    my $db_dbc = $regulation_dba->dbc();
    my $regulation_db =$db_dbc->dbname;
    my $database_tables = $db_dbc->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${regulation_db}'" );
      if (defined $database_tables->[0]) {
        if ($database_tables->[0] > 0) {
          my $empty_probe_table=$db_dbc->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${regulation_db}' and table_name='MTMP_probestuff_helper'" );
          if(defined $empty_probe_table->[0]) {
            if ($empty_probe_table->[0] > 0) {
              $probes_list = $db_dbc->sql_helper()->execute(
                -SQL => "select distinct(replace(LOWER(array_name),' ','')), replace(array_vendor_and_name,' ',''), is_probeset_array from ${regulation_db}.MTMP_probestuff_helper order by array_vendor_and_name",
              );
            }
          }
        }
      }
    $db_dbc->disconnect_if_idle();
  }
  return ($probes_list);
}

=head2 generate_protein_domain_and_feature_list
  Description: Retrieve a list of protein domains for a given dataset. Subroutine use the information_schema database, protein_feature, web_data and analysis  table.
  Arg        : Mart dataset name
  Arg        : Core database adaptor
  Returntype : Hashref (keys are logic names, values are associated db, web_data and display_label)
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub generate_protein_domain_and_feature_list {
  my ($self,$dataset, $core_dba)= @_;
  my $db_dbc = $core_dba->dbc();
  my $core_db =$db_dbc->dbname;
  my $protein_domain_and_feature_list;
  my $database_tables = $db_dbc->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${core_db}'" );
  if (defined $database_tables->[0]) {
    if ($database_tables->[0] > 0) {
      $protein_domain_and_feature_list = $db_dbc->sql_helper()->execute(
              -SQL => "select distinct(LOWER(logic_name)), db, web_data, display_label from ${core_db}.protein_feature JOIN ${core_db}.analysis USING(analysis_id) LEFT JOIN ${core_db}.analysis_description USING (analysis_id) order by db",
            );
    }
  }
  $db_dbc->disconnect_if_idle();
  return ($protein_domain_and_feature_list);
}

=head2 get_table_key
  Description: Subroutine to return mart main table key for a given mart dataset and table
  Arg        : Mart database table name
  Returntype : String corresponding of the mart table main key
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub get_table_key {
my ($self,$table)= @_;
my $key;
my $mart=$self->{dbc}->dbname();
my $main_key;
my $database_table = $self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${mart}' and table_name='${table}'" );
  if (defined $database_table->[0]) {
    if ($database_table->[0] > 0) {
      if ($table =~ "main"){
        $table =~ m/.*__(.*)__main$/;
        my $main_key=$1;
        $key = $self->{dbc}->sql_helper()->execute_simple(
            -SQL => "select COLUMN_NAME from information_schema.columns where table_schema='${mart}' and table_name='${table}' and COLUMN_NAME like '${main_key}%key';",
          )->[0];
      }
      else {
          $key = $self->{dbc}->sql_helper()->execute_simple(
            -SQL => "select COLUMN_NAME from information_schema.columns where table_schema='${mart}' and table_name='${table}' and COLUMN_NAME like '%key';",
          )->[0];
        }
    }
  }
return ($key);
}

=head2 get_example
  Description: Subroutine to retrieve the first item of a column for a given table
  Arg        : Mart database table name
  Arg        : Mart database column name
  Returntype : String or interger depending of the data
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub get_example {
  my ($self,$table,$field)= @_;
  my $mart=$self->{dbc}->dbname();
  my $example = $self->{dbc}->sql_helper()->execute_simple(
            -SQL => "select $field from $mart.$table where $field is not null limit 1;",
          )->[0];
  return ($example);
}

=head2 parse_ini_file
  Description: Subroutine parsing an ini file to extract parameters and values of a given section. I am reusing code from https://github.com/Ensembl/ensembl-metadata/blob/master/modules/Bio/EnsEMBL/MetaData/AnnotationAnalyzer.pm
  Arg        : ini file GitHub URL
  Arg        : Name of the section of interested parameters and values
  Returntype : Hash ref (keys are method parameter, values are associated parameter value)
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub parse_ini_file {
  my ($self, $ini_file, $section)= @_ ;
  my $ua = LWP::UserAgent->new();
  my $req = HTTP::Request->new( GET => $ini_file );
  # Pass request to the user agent and get a response back
  my $res = $ua->request($req);
  my $ini;
  # Check the outcome of the response
  if ( $res->is_success ) {
    $ini = $res->content;
  }
  else {
    $logger
      ->debug( "Could not retrieve $ini_file: " . $res->status_line );
  }
  # parse out and look at given section, e.g:
  # [ENSEMBL_EXTERNAL_URLS]
  ## Used by more than one group
  #EPMC_MED                    = http://europepmc.org/abstract/MED/###ID###
  #LRG                         = http://www.lrg-sequence.org/LRG/###ID###
  # then store some or all of this in my output e.g. {xref_name}{xref_url}
  my %parsed_data;
  if ( defined $ini ) {
    my $cfg = Config::IniFiles->new( -file => \$ini );
    if ( defined $cfg ) {
      for my $parameter ( $cfg->Parameters($section) ) {
        my @values = $cfg->val($section,$parameter);
        # Remove any / from parameter name
        $parameter  =~ s/\///g;
        #Because we are lowercasing xrefs, we can end up replacing hash key
        if (exists $parsed_data{lc($parameter)})
        {
          1;
        }
        else{
          # Making sure that the parameter name is lowercase
          # When the file contains multiple URL, the last one seems to be the one displayed on the website
          $parsed_data{lc($parameter)}=$values[-1];
        }
      }
    }
  }
  return \%parsed_data;
}

=head2 check_pointer_dataset_table_exist
  Description: Subroutine used to check if a given dataset has data in a pointer mart eg: For band filter in the gene mart, does the genomic_features_mart has tables for the given databaset
  Arg        : hashref representing a dataset
  Arg        : Name of the pointer mart
  Arg        : Name of the dataset in the pointer mart
  Returntype : integer
  Exceptions : none
  Caller     : general
  Status     : Stable
=cut

sub check_pointer_dataset_table_exist {
  my ($self,$dataset_name,$pointer_mart,$pointer_dataset)= @_;
  my $pointer_dataset_table;

  my $database_tables =$self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select count(table_name) from information_schema.tables where table_schema='${pointer_mart}'" );
  if (defined $database_tables->[0]) {
    if ($database_tables->[0] > 0) {
      $pointer_dataset_table = $self->{dbc}->sql_helper()
                    ->execute_simple( -SQL =>"select TABLE_ROWS from information_schema.tables where table_schema='${pointer_mart}' and table_name like '${pointer_dataset}%'" );
    }
  }
  return ($pointer_dataset_table);
}

1;
