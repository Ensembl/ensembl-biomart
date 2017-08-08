use strict;
use warnings;
package Bio::EnsEMBL::BioMart::DatasetFactory;
use Bio::EnsEMBL::Hive::Utils qw/go_figure_dbc/;
use base ('Bio::EnsEMBL::Hive::RunnableDB::JobFactory');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Data::Dumper;
use Bio::EnsEMBL::ApiVersion;

sub run {
    my $self = shift @_;
    # start a new session
    my $mart_dbc = Bio::EnsEMBL::DBSQL::DBConnection->new(
        -USER=>$self->param('user'),
        -PASS=>$self->param('pass'),
        -PORT=>$self->param('port'),
        -HOST=>$self->param('host'),
        -DBNAME=>$self->param('mart')
        );
    my $output_ids = [];
    for my $dataset_names (@{$mart_dbc->sql_helper()->execute(
        -SQL=>"select distinct(name),src_db from dataset_names"
                             )}) {
        my ($dataset,$core) = @$dataset_names;
        if ($self->param('mart') =~ m/mouse_mart/i and ($dataset eq "mmusculus" or $dataset eq "rnorvegicus")) {
            next;
        }
        elsif ($self->param('mart') =~ m/vb_gene_mart/i and $dataset eq "dmelanogaster_eg") {
            next;
        }
        push @$output_ids, {dataset=>$dataset,core=>$core}
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
