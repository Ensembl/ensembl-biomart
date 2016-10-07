use strict;
use warnings;
package Bio::EnsEMBL::BioMart::BuildSequenceMart;
use Bio::EnsEMBL::Hive::Utils qw/go_figure_dbc/;
use base ('Bio::EnsEMBL::Hive::RunnableDB::JobFactory');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Data::Dumper;
use Bio::EnsEMBL::ApiVersion;

sub run {
    my $self = shift @_;
    my $dataset = $self->param('dataset'); 
    my $mart_dbc = Bio::EnsEMBL::DBSQL::DBConnection->new(
                                                          -USER => $self->param('user'),
                                                          -PASS => $self->param('password'),
                                                          -HOST => $self->param('host'),
                                                          -PORT => $self->param('port'),
                                                          -DBNAME => $self->param('mart')    
                                                         );
    
    my $dbname = $mart_dbc->sql_helper()->execute_single(-SQL=>q/select src_db from dataset_names where name=?/, -PARAMS=>[$dataset]);    

    my $dba = Bio::EnsEMBL::DBSQL::DBAdaptor->new(
                                                  -USER => $self->param('user'),
                                                  -PASS => $self->param('password'),
                                                  -HOST => $self->param('host'),
                                                  -PORT => $self->param('port'),
                                                  -DBNAME => $dbname
                                                 );
    my $sa = $dba->get_SliceAdaptor();

    # create the table
    my $table = "${mart_db}.${dataset}__genomic_sequence__dna_chunks__main";
    $mart_dbc->sql_helper()->execute_update(-SQL=>"drop table if exists $table");
    $mart_dbc->sql_helper()->execute_update(-SQL=>qq/create table $table
(
chunk_key int(10) not null,
chr_name varchar(40) not null default '',
chr_start int(10) not null default '0',
sequence mediumtext
)ENGINE=MyISAM MAX_ROWS=100000 AVG_ROW_LENGTH=100000/
);
    my $row = 0;
    my $slices = $adaptor->fetch_all('toplevel',undef,1,1); #default_version,include references (DR52,DR53),include duplicate (eg HAP and PAR)
    my $chunk_size = 100000;
    while( my $slice = shift @{$slices} ){
      my $chr_name = "\'".$slice->seq_region_name."\'";
      
      my $current_base = 1;
      my $length = $slice->length;
      
      while ($current_base <= $length) {
        
        my $step;
        if ($chunk_size<=$slice->length-$current_base)
          {
            $step=$chunk_size-1;
          } else {
            $step=$slice->length-$current_base;
          }
        
        my $sub_slice = $slice->sub_Slice($current_base,
                                          $current_base+$step);
        
        my $chr_start = "\'".$current_base."\'";
        chunks_to_mart($mart_dbc, $n++, $chr_name, $chr_start, $sub_slice->seq);
        $current_base += $chunk_size;
      }
    }
    return;
}

sub chunks_to_mart{
  my ($dbc, $table, $row, $chr_name, $chr_start, $seq)=@_;

  my $current_base = 0;
  my $length = length($seq);
  
  $dbc->sql_helper()->execute(
                              -SQL=>qq/insert into $table(chunk_key,chr_name,chr_start,sequence) values(?,?,?,?)/,
                              -PARAMS=>[ $row, $chr_name, $chr_start, $seq]
                             );
}


sub write_output {
    my $self = shift @_;    
    $self->dataflow_output_id({}, 1);
    return 1;
}

1;
