package Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::Base;

use strict;
use Carp;

use base ('Bio::EnsEMBL::Hive::Process');

sub hive_dbh {

  my $self = shift;

  my $dbh = $self->hive_dbc->db_handle();
  confess('Type error!') unless($dbh->isa('DBI::db'));

  return $dbh;
}

sub hive_dbc {
  
  my $self = shift;

  my $dbc = $self->dbc();  
  confess('Type error!') unless($dbc->isa('Bio::EnsEMBL::DBSQL::DBConnection'));

  return $dbc;
}

sub core_dbh {

  my $self = shift;

  my $dbh = $self->core_dbc->db_handle();
  confess('Type error!') unless($dbh->isa('DBI::db'));

  return $dbh;
}

sub core_dbc {
	
  my $self = shift;

  my $dbc = $self->core_dba()->dbc();	
  confess('Type error!') unless($dbc->isa('Bio::EnsEMBL::DBSQL::DBConnection'));

  return $dbc;
}

sub otherfeatures_dbc {
    
  my $self = shift;

  my $dbc = $self->otherfeatures_dba()->dbc();	
  confess('Type error!') unless($dbc->isa('Bio::EnsEMBL::DBSQL::DBConnection'));

  return $dbc;
}


sub core_dba {	

  my $self = shift;

  my $species  = $self->param('species')  || die "'species' is an obligatory parameter";	
  my $dba = Bio::EnsEMBL::Registry->get_adaptor($species, 'core', 'Slice')->db();
  confess('Type error!') unless($dba->isa('Bio::EnsEMBL::DBSQL::DBAdaptor'));
	
  return $dba;
}

=head2 core_database_string_for_user

	Return the name and location of the database in a human readable way.

=cut
sub core_database_string_for_user {
	
  my $self = shift;
  return $self->core_dbc->dbname . " on " . $self->core_dbc->host 
	
}

sub otherfeatures_dba {	

  my $self = shift;

  my $species  = $self->param('species')  || die "'species' is an obligatory parameter";
  my $dba = Bio::EnsEMBL::Registry->get_adaptor($species, 'otherfeatures', 'Slice')->db();
  confess('Type error!') unless($dba->isa('Bio::EnsEMBL::DBSQL::DBAdaptor'));
	
  return $dba;
}

=head2 otherfeatures_database_name

	Return the name of the otherfeatures db
    
=cut
sub otherfeatures_database_name {
    
  my $self = shift;
  return $self->otherfeatures_dbc->dbname;
	
}

=head2 hive_database_string_for_user

  Return the name and location of the database in a human readable way.

=cut
sub hive_database_string_for_user {
  
  my $self = shift;
  return $self->hive_dbc->dbname . " on " . $self->hive_dbc->host 
  
}

=head2 mysql_command_line_connect_do_db
=cut
sub mysql_command_line_connect_do_db {
	
  my $self = shift;

  my $cmd = 
      "mysql"
      . " --host ". $self->core_dbc->host
      . " --port ". $self->core_dbc->port
      . " --user ". $self->core_dbc->username
      . " --pass=". $self->core_dbc->password
      . " ". $self->core_dbc->dbname
  ;

  return $cmd;
	
}

sub get_db_connection {
    my $self = shift;
    
    my $species  = $self->param('species')  || die "'species' is an obligatory parameter";
    my $dba = Bio::EnsEMBL::Registry->get_adaptor($species, 'core', 'Slice')->db();
    
    my $var_host = $dba->dbc->host();
    my $var_port = $dba->dbc->port();
    my $var_user = $dba->dbc->username();
    my $var_pass = $dba->dbc->password();
    
    my $db_conn_href = {
	'host' => $var_host,
	'port' => $var_port,
	'user' => $var_user,
	'pass' => $var_pass,
    };

    return $db_conn_href;
}

1;

