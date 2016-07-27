use strict;
use warnings;

package Bio::EnsEMBL::Mart::Pipeline::DatasetFactory;
use Bio::EnsEMBL::Hive::Utils qw/go_figure_dbc/;
use base ('Bio::EnsEMBL::Hive::RunnableDB::JobFactory')
  ; # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Data::Dumper;
use Bio::EnsEMBL::ApiVersion;

sub run {
  my $self = shift @_;
  # start a new session
  my $mart_dbc =
    Bio::EnsEMBL::DBSQL::DBConnection->new( -USER   => $self->param('user'),
                                            -PASS   => $self->param('pass'),
                                            -PORT   => $self->param('port'),
                                            -HOST   => $self->param('host'),
                                            -DBNAME => $self->param('mart') );

  my $datasets   = $self->param('datasets');
  my $output_ids = [];

  if ( !defined $datasets || scalar( @$datasets ) == 0 ) {
    for my $dataset ( @{$mart_dbc->sql_helper()->execute_simple(
                              -SQL => "select distinct(name) from dataset_names"
                        ) } )
    {
      push @$output_ids, { dataset => $dataset };
    }
  }
  else {
      $output_ids = $datasets;
  }
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
