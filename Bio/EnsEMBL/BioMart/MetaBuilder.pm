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

use warnings;
use strict;

package Bio::EnsEMBL::BioMart::MetaBuilder;

use Bio::EnsEMBL::Utils::Exception qw(throw warning);
use Bio::EnsEMBL::Utils::Argument qw( rearrange );
use IO::Compress::Gzip qw(gzip);
use Carp;
use XML::Simple;
use Data::Dumper;

use Log::Log4perl qw/get_logger/;

my $logger = get_logger();

sub new {
  my ( $proto, @args ) = @_;
  my $self = bless {}, $proto;
  ( $self->{dbc}, $self->{version} ) = rearrange( [ 'DBC', 'VERSION' ], @args );

  if ( !defined $self->{version} ) {
    ( $self->{version} = $self->{dbc}->dbname() ) =~ s/.*_([0-9]+)$/$1/;
  }

  $self->_load_info();
  return $self;
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

sub build {
  my ( $self, $template_name, $template ) = @_;
  # create base metatables
  $self->create_metatables( $template_name, $template );
  # read datasets
  for my $dataset ( @{ $self->get_datasets() } ) {
    $self->process_dataset( $dataset, $template_name, $template );
  }
}

sub get_datasets {
  my ($self) = @_;
  $logger->debug("Fetching dataset details");
  my $datasets =
    $self->{dbc}->sql_helper()->execute(
    -SQL =>
'select name, species_name as display_name, sql_name as production_name, version from dataset_names',
    -USE_HASHREFS => 1 );
  $logger->debug( "Found " . scalar(@$datasets) . " datasets" );
  return $datasets;
}

sub process_dataset {
  my ( $self, $dataset, $template_name, $template ) = @_;
  $logger->info( "Processing " . $dataset->{name} );
  my $templ_in = $template->{DatasetConfig};
  $logger->debug("Building output");
  $dataset->{config} = {};
  $self->write_toplevel( $dataset, $templ_in );
  $self->write_importables( $dataset, $templ_in );
  $self->write_exportables( $dataset, $templ_in );
  #$self->write_filters( $dataset, $templ_in );
  #$self->write_attributes( $dataset, $templ_in );
  # write meta
  #$self->write_dataset_metatables( $dataset, $template_name );
  print Dumper($dataset);
  die;
  return $dataset;
}

sub write_toplevel {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing toplevel elements for " . $dataset->{name} );
  # handle the top level scalars
  # defaultDataSet
  # displayName
  # version
  while ( my ( $key, $value ) = each %{$templ_in} ) {
    if ( !ref($value) ) {
      if ( $key eq 'defaultDataSet' ) {
        $value = $dataset->{name};
      }
      elsif ( $key eq 'displayName' ) {
        $value = $dataset->{display_name};
      }
      elsif ( $key eq 'description' ) {
        $value = $dataset->{display_name} . ' Genes';
      }
      elsif ( $key eq 'version' ) {
        $value = $dataset->{version};
      }
      $dataset->{config}->{$key} = $value;
    }
  }

  # add dynamicdataset
  $dataset->{config}->{DynamicDataset} = {
                                        internalName => $dataset->{name},
                                        displayName => $dataset->{display_name},
                                        useDefault  => "true" };
  # add MainTable
  push @{ $dataset->{config}->{MainTable} }, "$dataset->{name}__gene__main";
  push @{ $dataset->{config}->{MainTable} },
    "$dataset->{name}__transcript__main";
  push @{ $dataset->{config}->{MainTable} },
    "$dataset->{name}__translation__main";
  # add Key
  push @{ $dataset->{config}->{Key} }, 'gene_id_1020_key';
  push @{ $dataset->{config}->{Key} }, 'transcript_id_1064_key';
  push @{ $dataset->{config}->{Key} }, 'translation_id_1068_key';

  return;
} ## end sub write_toplevel

sub write_importables {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing importables for " . $dataset->{name} );

  my $version = $dataset->{name} . "_" . $self->{version};
  my $ds_name = $dataset->{name};

  # Importable
  for my $imp ( @{ $templ_in->{Importable} } ) {
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
    if ( $imp->{internalName} eq 'genomic_sequence' ) {
      $imp->{filters} .= ",species_id_key";
    }
    # push onto out stack
    push @{ $dataset->{config}->{Importable} }, $imp;
  }

  return;
} ## end sub write_importables
my %species_exportables = map { $_ => 1 }
  qw/genomic_region gene_exon_intron transcript_exon_intron gene_flank transcript_flank coding_gene_flank coding_transcript_flank 3utr 5utr cdna gene_exon peptide coding/
  ;

sub write_exportables {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing exportables for " . $dataset->{name} );
  my $version = $dataset->{name} . "_" . $self->{version};
  my $ds_name = $dataset->{name};
  $logger->info("Processing exportables");
  for my $exp ( @{ $templ_in->{Exportable} } ) {
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
    if ( $species_exportables{ $exp->{internalName} } ) {
      $exp->{attributes} .= ",species_id_key";
    }
    # push onto out stack
    push @{ $dataset->{config}->{Exportable} }, $exp;
  }

  # additional exporter
  push @{ $dataset->{config}->{Exportable} }, {
      attributes   => "${ds_name}",
      default      => 1,
      internalName => "${ds_name}_stable_id",
      name         => "${ds_name}_stable_id",
      linkName     => "${ds_name}_stable_id",
      type         => "link" };

  return;
} ## end sub write_exportables

sub write_filters {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing filters for " . $dataset->{name} );
  return;
}

sub write_attributes {
  my ( $self, $dataset, $templ_in ) = @_;
  $logger->info( "Writing attributes for " . $dataset->{name} );
  return;
}

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
  my ( $obj, $ds_name, $keys ) = @_;
  if ( defined $obj->{tableConstraint} ) {
    if ( $obj->{tableConstraint} eq 'main' ) {
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
  if ( defined $obj->{tableConstraint} ) {
    if ( $obj->{tableConstraint} eq "${ds_name}__gene__main" ||
         $obj->{tableConstraint} eq "${ds_name}__transcript__main" ||
         $obj->{tableConstraint} eq "${ds_name}__translation__main" )
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

  open my $out, ">", "tmp.xml";
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
  $logger->info("Completed creation of metatables");
  return;
} ## end sub create_metatables

sub write_dataset_metatables {
  # TODO accept array of dataset objects
  my ( $self, $dataset, $template_name ) = @_;

  my $ds_name = $dataset->{name};

  $logger->info("Populating metatables for $ds_name");

  my $dataset_xml =
    XMLout( { DatasetConfig => $dataset->{config} }, KeepRoot => 1 );

  open my $out, ">", "tmp.xml";
  print $out $dataset_xml;
  close $out;
  my $gzip_dataset_xml;
  gzip \$dataset_xml => \$gzip_dataset_xml;

  my $speciesId = $dataset->{species_id};

  $self->{dbc}->sql_helper()
    ->execute_update( -SQL =>
"INSERT INTO meta_template__template__main VALUES($speciesId,'$template_name')"
    );

  $self->{dbc}->sql_helper()->execute_update(
    -SQL => -SQL =>
"INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,?,'TableSet',1,?)",
    -PARAMS => [ $speciesId,               $dataset->{name},
                 $dataset->{display_name}, $dataset->{version} ] );

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
