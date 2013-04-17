package Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::MergeVarMart;

use strict;
use base ('Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::Base');
use Carp;

sub fetch_input {
    my $self = shift @_;

    my $inputs_aref = [];
    my $sql = "SELECT var_mart_db, create_db_info FROM intermediate_result";
    my $sth = $self->db->dbc()->prepare($sql);
    $sth->execute();
    while (my ($var_mart_db, $create_db_info)=$sth->fetchrow_array()) {
        my $input_href = {
	    'var_mart_db'    => $var_mart_db,
	    'create_db_info' => $create_db_info,
	};
	push (@$inputs_aref, $input_href);
    }
    $sth->finish();
    $self->param('inputs_aref', $inputs_aref);
}

sub run {
    my $self = shift @_;
    
    my $data_dir     = $self->param('data_dir');
    my $db_conn_href = $self->get_db_connection();
    my $final_db_conn_href = $self->param('final_db_conn');
    my $final_snp_mart_db  = $self->param('final_snp_mart_db');

    # array of hash refs
    # two keys per hash: the var_mart_db and the create_db_info
    my $inputs_aref = $self->param('inputs_aref');

    my $mysql_snp_command = "mysql -h " . $final_db_conn_href->{'host'} . " -u " . $final_db_conn_href->{'user'} . " -P " . $final_db_conn_href->{'port'} . " -p" . $final_db_conn_href->{'pass'};

    # 1/ Drop and Create final SNP Mart database

    # Drop if exists
    
    qx/$mysql_snp_command -e "DROP DATABASE IF EXISTS $final_snp_mart_db"/;
    
    qx/$mysql_snp_command -e "CREATE DATABASE $final_snp_mart_db"/;

    # 2/ Merge all sub var mart dbs into the final snp mart for this species

    foreach my $input_href (@$inputs_aref) {

	my $var_mart_db    = $input_href->{'var_mart_db'};
	my $dump_path = $data_dir . "/" . $var_mart_db . ".sql.gz";
	
	print STDERR "Loading dump file, $dump_path, into $mysql_snp_command $final_snp_mart_db\n";
	
	qx/gzip -dc $dump_path | $mysql_snp_command $final_snp_mart_db/;
    }

}

1;

