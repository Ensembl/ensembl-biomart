package Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::PrePipelineChecks;

use strict;
use base ('Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::Base');
use Carp;

my $MTMP_tables = ['MTMP_population_genotype','MTMP_transcript_variation','MTMP_variation_set_variation'];
my $MTMP_views = ['MTMP_structural_variation_annotation','MTMP_variation_annotation'];

sub run {
    my $self = shift @_;
    
    my $species            = $self->param('species')            || die "'species' is an obligatory parameter";
    my $data_dir           = $self->param('data_dir');
    my $sql_dir            = $self->param('sql_dir');
    my $var_sql_file       = $self->param('var_sql_file');
    my $var_syn_sql_file   = $self->param('var_syn_sql_file');
    my $structvar_sql_file = $self->param('structvar_sql_file');

    if (! -d $data_dir) {
	# create it
	qx/mkdir -p $data_dir/;
    }

    my $all_checks_passed = 1;
    my @error_message;

    my $entry_ok;
    my $err_msg;
	    
    ($entry_ok, $err_msg) = $self->all_files_present({
	sql_dir            => $sql_dir,
	var_sql_file       => $var_sql_file,
	var_syn_sql_file   => $var_syn_sql_file,
	structvar_sql_file => $structvar_sql_file,
						     });

    if (!$entry_ok) {
	$all_checks_passed = 0;
	push @error_message, $err_msg
    }
    
    # then check the core and variation dbs are in the expected server

    ($entry_ok, $err_msg) = $self->all_databases_present({
	species            => $species,
							 });

    if (!$entry_ok) {
	$all_checks_passed = 0;
	push @error_message, $err_msg
    }

    # then check the MTMP tables and views are in the variatino db
    
    ($entry_ok, $err_msg) = $self->all_tables_present({
	'species'     => $species,
	'mtmp_tables' => $MTMP_tables,
	'mtmp_views'  => $MTMP_views,
							 });

    if (!$entry_ok) {
	$all_checks_passed = 0;
	push @error_message, $err_msg
    }
    
    if (!$all_checks_passed) {

	my $pre_pipeline_check_warning
	    = "\n\n----- Pre pipeline checks failed ! ------\n\n"
	    . (join "\n", @error_message)
	    . "\n\n-----------------------------------------\n\n";

	$self->warning($pre_pipeline_check_warning);
	die($pre_pipeline_check_warning);
    }
}

sub all_files_present {
    my $self = shift;
    my $param = shift;
    
    my $sql_dir            = $param->{sql_dir};
    my $var_sql_file       = $param->{var_sql_file};
    my $var_syn_sql_file   = $param->{var_syn_sql_file};
    my $structvar_sql_file = $param->{structvar_sql_file};

    my $err_msg = "";
    my $entry_ok = 1;

    if (! -d $sql_dir) {
	$entry_ok = 0;
	$err_msg = "no sql directory found, $sql_dir";
	
	return ($entry_ok, $err_msg);
    }
    
    my $sql_files_aref = [$var_sql_file, $var_syn_sql_file, $structvar_sql_file];
    my $missing_sql_files_aref = [];
    foreach my $sql_file (@$sql_files_aref) {
	my $sql_file_path = $sql_dir . "/" . $sql_file;
	if (! -f $sql_file_path) {
	    $entry_ok = 0;
	    $err_msg .= "sql file missing, $sql_file_path. ";
	}
    }
    
    return ($entry_ok, $err_msg);

}

sub all_databases_present {
    
    my $self = shift;
    my $param = shift;
    
    my $species = $param->{species};

    my $err_msg;
    my $entry_ok = 1;
    
    # Check the core database first

    my $analysis_adaptor = Bio::EnsEMBL::Registry->get_adaptor($species, 'core', 'Analysis');
    
    if (!defined $analysis_adaptor) {
	$entry_ok = 0;
	$err_msg = "No core database loaded in the Registry for species, $species\n";
	return ($entry_ok, $err_msg);
    }
    
    # then check the variation database

    my $variation_adaptor = Bio::EnsEMBL::Registry->get_adaptor($species, 'variation', 'Variation');

    if (!defined $variation_adaptor) {
	$entry_ok = 0;
	$err_msg = "No variation database loaded in the Registry for species, $species\n";
    }

    return ($entry_ok, $err_msg);
    
}

sub all_tables_present {
    
    my $self = shift;
    my $param = shift;
    
    my $species           = $param->{'species'};
    my $mtmp_tables_aref  = $param->{'mtmp_tables'};
    my $mtmp_views_aref   = $param->{'mtmp_views'};
    
    my $err_msg = "";
    my $entry_ok = 1;
    
    # Get a database adaptor for the variation database

    my $variation_adaptor = Bio::EnsEMBL::Registry->get_adaptor($species, 'variation', 'Variation');
    
    my $dbh = $variation_adaptor->db()->dbc()->db_handle();

    my $table_sql = "SHOW TABLES like ?";
    my $table_sth = $dbh->prepare($table_sql);
    my $missing_tables_aref = [];
    my $missing_views_aref = [];

    foreach my $table_name (@$mtmp_tables_aref) {
	$table_sth->execute($table_name);
	my ($result) = $table_sth->fetchrow_array();
	
	if (!defined $result) {
	    push(@$missing_tables_aref, $table_name);
	}
    }

    foreach my $table_name (@$mtmp_views_aref) {
	$table_sth->execute($table_name);
	my ($result) = $table_sth->fetchrow_array();

	if (!defined $result) {
	    push(@$missing_views_aref, $table_name);
	}
    }

    $table_sth->finish();

    if (@$missing_tables_aref) {
	$entry_ok = 0;
	$err_msg = "Missing variation tables, " . join (',', @$missing_tables_aref) . ". ";
    }
    if (@$missing_views_aref) {
	$entry_ok = 0;
	$err_msg .= "Missing variations views, " . join (',', @$missing_views_aref);
    }
    
    return ($entry_ok, $err_msg);
    
}

1;

