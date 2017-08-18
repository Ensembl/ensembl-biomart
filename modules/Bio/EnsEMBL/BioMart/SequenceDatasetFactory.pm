use strict;
use warnings;

package Bio::EnsEMBL::BioMart::SequenceDatasetFactory;
use Bio::EnsEMBL::Hive::Utils qw/go_figure_dbc/;
use base ('Bio::EnsEMBL::Hive::RunnableDB::JobFactory')
  ; # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Data::Dumper;
use Bio::EnsEMBL::ApiVersion;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Bio::EnsEMBL::DBSQL::DBConnection;
use Bio::EnsEMBL::Registry;
use Bio::EnsEMBL::ApiVersion qw/software_version/;

sub run {
  my $self    = shift @_;
  my $release = software_version();

  Bio::EnsEMBL::Registry->load_registry_from_db(
                                          -host => $self->param('host'),
                                          -user => $self->param('user'),
                                          -pass => $self->param('pass'),
                                          -port => $self->param('port'),
                                          -db_version => $release );

  my $dbas =
    Bio::EnsEMBL::Registry->get_all_DBAdaptors( -group => 'core' );

  my $mart_dbc =
    Bio::EnsEMBL::DBSQL::DBConnection->new(
                                          -USER => $self->param('user'),
                                          -PASS => $self->param('pass'),
                                          -HOST => $self->param('host'),
                                          -PORT => $self->param('port')
    );

  my $mart = $self->param('mart');
  $mart_dbc->sql_helper()
    ->execute_update( -SQL => qq/create database if not exists $mart/ );

  $mart_dbc->sql_helper()->execute_update(
    -SQL => qq/
create table if not exists $mart.dataset_names (
  name varchar(100),
  src_dataset varchar(100),
  src_db varchar(100),
  species_id varchar(100),
  tax_id int(10),
  species_name varchar(100),
  sql_name varchar(100),
  assembly varchar(100),
  genebuild varchar(100),
  collection varchar(100),
  has_chromosomes tinyint)
/
  );

  my $suffix = $self->param('suffix');

  my $output_ids = [];
  my $division   = $self->param('division');
  my $pId;
  if    ( $division eq 'EnsemblProtists' ) { $pId = 10000; }
  elsif ( $division eq 'EnsemblPlants' )   { $pId = 20000; }
  elsif ( $division eq 'EnsemblMetazoa' ) { $pId = 30000;  }
  elsif ( $division eq 'EnsemblFungi' )    { $pId = 40000;  }
  elsif ( $division eq 'Vectorbase' ) { $pId = 50000; }
  elsif ( $division eq 'Parasite' )   { $pId = 60000 }
  elsif ( $division eq 'Ensembl' ) { $pId = 0;}

  for my $dba ( @{$dbas} ) {

    if ( $dba->dbc()->dbname() =~ m/_collection_/ || 
         (defined $division &&
          $dba->get_MetaContainer()->get_division() ne $division ))
      {
        $dba->dbc()->disconnect_if_idle();
        next;
      }
    
    my $database = $dba->dbc()->dbname();
    my $ds =
      $mart_dbc->sql_helper()
      ->execute_into_hash( -SQL =>
qq/select meta_key,meta_value from ${database}.meta where species_id=1/
      );

    ( my $dataset = $database ) =~
      s/^([a-z])[a-z]+.*_([^_]+)_core.*/$1$2/;
    $dataset = $dataset . $suffix;

    my $assembly  = $ds->{'assembly.name'};
    if ( !defined $ds->{'species.proteome_id'} ||
         !isdigit $ds->{'species.proteome_id'} )
    {
      $ds->{'species.proteome_id'} = ++$pId;
    }
    my $gb_version = $ds->{'genebuild.version'};
    if ( $division eq "Ensembl" ) {
      $gb_version =
        $ds->{'genebuild.last_geneset_update'} ||
        $ds->{'genebuild.start_date'}          ||
        $ds->{'genebuild_version'};
    }

    my $has_chromosomes =
      $mart_dbc->sql_helper()->execute_simple(
"select count(distinct coord_system_id) from ${database}.coord_system join ${database}.seq_region using (coord_system_id) join ${database}.seq_region_attrib using (seq_region_id) join ${database}.attrib_type using (attrib_type_id) where code='karyotype_rank' and species_id=1"
    )->[0];

    $mart_dbc->sql_helper()->execute_update(
               -SQL => qq/delete from $mart.dataset_names where name=?/,
               -PARAMS => [$dataset] );
    $mart_dbc->sql_helper()->execute_update(
      -SQL =>
qq/insert into $mart.dataset_names() values(?,?,?,?,?,?,?,?,?,NULL,?)/,
      -PARAMS => [ $dataset,
                   $dataset,
                   $database,
                   $ds->{'species.proteome_id'},
                   $ds->{'species.taxonomy_id'},
                   $ds->{'species.display_name'},
                   $ds->{'species.production_name'},
                   $assembly,
                   $gb_version,
                   $has_chromosomes ] );
    push @$output_ids, { dataset => $dataset };
    $dba->dbc()->disconnect_if_idle();
  } ## end for my $dba ( @{$dbas} )
  $self->param( 'output_ids', $output_ids );

  return;
} ## end sub run

sub write_output {
  my $self       = shift @_;
  my $output_ids = $self->param('output_ids');
  print "Writing output ids\n";
  $self->dataflow_output_id( $output_ids, 1 );
  $self->dataflow_output_id( {}, 2 );
  return 1;
}

1;
