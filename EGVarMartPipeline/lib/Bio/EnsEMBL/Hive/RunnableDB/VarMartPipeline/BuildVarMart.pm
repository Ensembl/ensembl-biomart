package Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::BuildVarMart;

# Todo: Use DBI to load the sql file, for a better way of capturing errors
# this way we can capture the line it is failing onto

use strict;
use base ('Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::Base');
use Carp;

sub run {
    my $self = shift @_;
    
    my $data_dir   = $self->param('data_dir');
    my $db_conn_href = $self->get_db_connection();
    
    my $input_file  = $self->param('input_file');
    my $var_mart_db    = $self->param('var_mart_db');
    my $create_db_info = $self->param('create_db_info');
    my $enable_keys = $self->param('enable_keys');
    
    my $file_path = $data_dir . '/' . $input_file;
    
    if (! -f $file_path) {
	die "sql file not found, $file_path!\n";
    }
    
    my $mysql_command = "mysql -h " . $db_conn_href->{'host'} . " -u " . $db_conn_href->{'user'} . " -P " . $db_conn_href->{'port'} . " -p" . $db_conn_href->{'pass'};

    my $sql_output = qx/$mysql_command -e "SHOW DATABASES LIKE '$var_mart_db'"/;

    # print STDERR "sql_output, $sql_output\n";

    if (length ($sql_output) > 0) {
	print STDERR "dropping database $var_mart_db\n";
	qx/$mysql_command -e "DROP DATABASE $var_mart_db"/;
    }
    print STDERR "creating database $var_mart_db\n";
    qx/$mysql_command -e "CREATE DATABASE $var_mart_db"/;
    
    print STDERR "transformation into $var_mart_db in progress...\n";
    
    my $loading_results = qx/cat $file_path | $mysql_command $var_mart_db/;

    print STDERR "transformation done\n";
    print STDERR "loading results, $loading_results\n";

    if (length ($loading_results) > 0) {
	    confess ("Failed to load $input_file into $var_mart_db\n" . $! ."\n");
    }

    # Dump it into a file

    my $mysqldump_var_command = "mysqldump --lock_tables=FALSE -h " . $db_conn_href->{'host'} . " -u " . $db_conn_href->{'user'} . " -P " . $db_conn_href->{'port'} . " -p" . $db_conn_href->{'pass'};
    if (!$create_db_info) {
	$mysqldump_var_command .= " --no-create-info";
    }
    if (!$enable_keys) {
	$mysqldump_var_command .= " --skip-opt";
    }
    my $dump_path = $data_dir . "/" . $var_mart_db . ".sql.gz";

    print STDERR "Dumping $var_mart_db into a file, $dump_path...\n";

    eval {
	system("$mysqldump_var_command $var_mart_db | gzip -c > $dump_path");
    };
    if ($@) {
	print STDERR "$mysqldump_var_command $var_mart_db | gzip -c > $dump_path\n";
	warn ($@);
	confess ("Failed to dump $var_mart_db into a file!");
    }

    $self->dataflow_output_id({'var_mart_db' => $var_mart_db, 'create_db_info' => $create_db_info}, 1);

}

1;

