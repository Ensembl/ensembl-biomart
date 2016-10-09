use strict;
use warnings;
package Bio::EnsEMBL::BioMart::SequenceDatasetFactory;
use Bio::EnsEMBL::Hive::Utils qw/go_figure_dbc/;
use base ('Bio::EnsEMBL::Hive::RunnableDB::JobFactory');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Data::Dumper;
use Bio::EnsEMBL::ApiVersion;
use Bio::EnsEMBL::DBSQL::DBAdaptor;
use Bio::EnsEMBL::DBSQL::DBConnection;

sub run {
    my $self = shift @_;
    my $databases = $self->param('databases');
    if(!defined $databases) {
      my $prod_dba = Bio::EnsEMBL::DBSQL::DBAdaptor->new(
                                                         -USER => $self->param('muser'),
                                                         -PASS => $self->param('mpass'),
                                                         -HOST => $self->param('mhost'),
                                                         -PORT => $self->param('mport'),
                                                         -DBNAME => $self->param('mdbname')    
                                                        );
      
      $databases = $prod_dba->dbc()->sql_helper()->execute_simple(
                                                                 -SQL => q/
select full_db_name from db_list join db using (db_id) 
join species using (species_id) join division_species using (species_id) join division using (division_id) 
where db.is_current=1 and db_type='core' and division.name=? and full_db_name not like '%collection%'/,
                                                                 -PARAMS => [$self->param('division')]
                                                                );
    }

    my $mart_dbc = Bio::EnsEMBL::DBSQL::DBConnection->new(
                                                          -USER => $self->param('user'),
                                                          -PASS => $self->param('pass'),
                                                          -HOST => $self->param('host'),
                                                          -PORT => $self->param('port'),
                                                          -DBNAME => $self->param('mart')    
                                                         );

    $mart_dbc->sql_helper()->execute_update(
                                            -SQL=>q/
create table if not exists dataset_names (
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
    for my $database (@$databases) {

      my $ds = $mart_dbc->sql_helper()->execute_into_hash(
                                                 -SQL => qq/select meta_key,meta_value from ${database}.meta where species_id=1/
                                                );

      (my $dataset = $database) =~ s/^([a-z])[a-z]+_([^_]+)_.*/$1$2/;
      $dataset = $dataset.$suffix;
      
      my $assembly = $assembly = $ds->{'assembly.name'};
      my $genebuild = $ds->{'genebuild.last_geneset_update'} ||
        $ds->{'genebuild.start_date'} ||
          $ds->{'genebuild_version'};
      

      $mart_dbc->sql_helper()->execute_update(-SQL => q/delete from dataset_names where name=?/, -PARAMS=>[$dataset]);
      $mart_dbc->sql_helper()->execute_update(
                                              -SQL=>q/insert into dataset_names() values(?,?,?,?,?,?,?,?,?,NULL,?)/,
                                              -PARAMS=>[
                                                        $dataset,
                                                        $dataset,
                                                        $database,
                                                        1, 
                                                        $ds->{'species.taxonomy_id'},
                                                        $ds->{'species.production_name'},
                                                        $ds->{'species.display_name'},
                                                        $assembly,
                                                        $genebuild,                                                        
                                                        0
                                                       ]
                                              );
      push @$output_ids, {dataset=>$dataset};

    }
    $self->param('output_ids',$output_ids);
    

    return;
}

sub write_output {
    my $self = shift @_;    
    my $output_ids = $self->param('output_ids');
    print "Writing output ids\n";
    $self->dataflow_output_id($output_ids, 1);
    $self->dataflow_output_id({}, 2);
    return 1;
}

1;
