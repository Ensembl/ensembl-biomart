package Bio::EnsEMBL::BioMart::DatasetFactory;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::RunnableDB::JobFactory');

use Bio::EnsEMBL::Hive::Utils qw/go_figure_dbc/;

sub run {
  my $self = shift @_;

  my $base_name = $self->param_required('base_name');

  my $mart_dbc = Bio::EnsEMBL::DBSQL::DBConnection->new(
    -HOST   => $self->param_required('host'),
    -PORT   => $self->param_required('port'),
    -USER   => $self->param_required('user'),
    -PASS   => $self->param_required('pass'),
    -DBNAME => $self->param_required('mart')
  );
  my $helper = $mart_dbc->sql_helper();

  my $output_ids = [];

  if ( $self->param_is_defined('species') and scalar( @{$self->param('species')} ) ) {
    foreach my $species ( @{$self->param('species')} ) {
      my $dataset_names = $helper->execute(
        -SQL    => 'SELECT name, src_db FROM dataset_names WHERE sql_name = ?',
        -PARAMS => [$species]
      );
      my $dataset = $$dataset_names[0][0];
      my $core = $$dataset_names[0][1];
      my $mart_table_prefix = "$dataset"."_"."$base_name";

      push @$output_ids,
        {
          dataset => $dataset,
          core => $core,
          species => $species,
          mart_table_prefix => $mart_table_prefix,
        };
    }
  } else {
    my $dataset_names = $helper->execute(
      -SQL => 'SELECT name, src_db, sql_name FROM dataset_names'
    );
    foreach my $dataset_name ( @{$dataset_names} ) {
      my ($dataset, $core, $species) = @$dataset_name;
      my $mart_table_prefix = "$dataset"."_"."$base_name";

      if ($self->param('mart') =~ m/mouse_mart/i and ($dataset eq "mmusculus" or $dataset eq "rnorvegicus")) {
        next;
      } elsif ($self->param('mart') =~ m/vb_gene_mart/i and $dataset eq "dmelanogaster_eg") {
        next;
      }

      push @$output_ids, 
        {
          dataset => $dataset,
          core => $core,
          species => $species,
          mart_table_prefix => $mart_table_prefix,
        };
    }
  }

  $self->param('output_ids', $output_ids);
}

1;
