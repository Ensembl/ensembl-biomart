package Production::TableFiller;


=head1 NAME

Production::TableFiller.pm

=head1 SYNOPSIS

  use Env;
  use DBI;
  use Production::TableFiller;


=head1 DESCRIPTION

An object to help programmers write one of the family of scripts
("table scripts") which are used for filling tables in an ensembl mart
database. The majority of these scripts simply execute a number of SQL
statements which move data from tables in normalised databases to a
table in the mart, denormalised database. All the scripts take the
same arguments and have the same requirements in terms of database
access, communicating with the user, error logging, and SQL
execution. For those situations where the programmer needs more
involved interaction with the database the DBI database handle can be
accessed through the get_dbh() method.

The table scripts are usually called from a driver script and to
facilitate this the error and standard output are reversed. In
addition, by default, the DBI trace method is invoked to generate a
log file of interactions with the database. This includes the SQL
statements being executed. Hence methods are provided for routing the
various output streams appropriately and stopping and restarting
output to the log file.

The log file is created in the current working directory with a name
specified by E<lt>speciesE<gt>_E<lt>datasetE<gt>_E<lt>tablenameE<gt>.log.

As a prelude to running the SQL in the individual table scripts this
module queries the specified mySQL server and determines the most up
to date versions of the various databases present for the specified
species and dataset. This relies on the databases being named to a
consistent convention. It is not possible to mix for instance
homo_sapiens_core_6_30 and homo_sapiens_snp_7_30. Both must be either
6_ or 7_. Also the versioning of data releases must be born in
mind. For example, a second data release may result in the snp
database being called homo_sapiens_snp_7_30a. This module will
correctly use the 7_30a version in preference to the 7_30 version,
however when specifying the release on the command line of the table
scripts the data version letter ie 'a' should be omitted.

By default this module sets the DBI InactiveDestroy attribute to true
so that database handles in a parent process are not destroyed when a
child process exits.

The script emits a commentary, as it runs, for user reassurance.


=head1 AUTHOR

    Damian Keefe - dkeefe@ebi.ac.uk

=head1 COPYRIGHT

GRL/EBI

=head1 CVS
 $Log: TableFiller.pm,v $
 Revision 1.15  2007/12/07 13:20:44  rh4
 Script mods as for 48 release.

 Revision 1.14  2007/06/21 11:00:56  ds5
 changes to move genomic features dataset to a new mart

 Revision 1.13  2005/10/25 16:18:23  dlondon
 added some methods for finding out the host, user, port, and password

 Revision 1.12  2005/01/13 22:04:35  arek
 part2 now handles correctly switches for multiple marts

 Revision 1.11  2005/01/13 18:59:38  arek
 part 1 now works correctly with switches for redirecting tables into multiple
 marts: vega, sequence, snp and ensembl

 Revision 1.10  2004/12/01 17:29:46  arek
 various fixes and changes for 27

 Revision 1.9  2004/11/11 00:43:24  arek
 removed hardcoded go database, now it comes as an extra param

 Revision 1.8  2004/11/10 22:03:56  arek
 fixed for the 'variation' naming convention, removed obsolete stuff

 Revision 1.7  2004/07/27 12:14:16  ds5
 added extra parameter for paralogs.pl and SET SQL_LOG_BIN=0 to stop logs getting too large

 Revision 1.6  2004/05/11 08:59:33  dkeefe
 even less unsolicited output

 Revision 1.5  2004/05/06 10:22:07  dkeefe
 added a debug property. Set to 0 by default. Toggles verbose output.

 Revision 1.4  2004/01/13 12:10:45  dkeefe
 set the DBI InactiveDestroy attribute to true so that database
 handles in a parent process are not destroyed when a child process
 exits.

 Revision 1.3  2004/01/07 11:08:37  dkeefe
 added methods for creating and loading interim tables which record the
 columns in a table

 Revision 1.2  2003/11/20 12:28:42  dkeefe
 corrected some error output

 Revision 1.1  2003/09/03 13:23:59  dkeefe
 moved to mart-build directory in repository

 Revision 1.5  2003/07/24 11:31:16  dkeefe
 improved error messages

 Revision 1.4  2003/06/25 11:09:56  dkeefe
 added method column_exists - for testing presence/absence of named columns

 Revision 1.3  2003/05/13 16:25:32  ds5
 compara_db method added

 Revision 1.2  2003/05/13 10:59:36  ds5
 fix to allow homologs.pl to have a 5th argument (the compara db)

 Revision 1.1  2003/05/02 16:51:46  dlondon
 Moved EnsemblMart::TableFiller module to Production::TableFiller
 to separate production utilities from UI utilities.
 Production::TableNameUtils has a number of utilities used by the production scripts to generate table names, abbreviate species, etc.  It is not object oriented.  It also has old to new and new to old table name mappings.

 Revision 1.15  2003/03/10 09:36:31  dkeefe
 added methods back_out and roll_back and modified fatal() to do a
 rollback in order to facilitate preservation of interim tables.

 Revision 1.14  2003/02/07 16:54:59  dlondon
 added a switch that skips the method to find the latest version of x databases
 when you are building tables internal to mart, eg., meta tables.

 Revision 1.13  2003/02/06 13:35:00  dkeefe
 expression satelite databases can now have the dataset in their name

 Revision 1.12  2003/01/15 09:02:14  dkeefe
 more support for gnf expression database

 Revision 1.11  2002/12/18 15:04:21  dkeefe
 added support for expression databases

 Revision 1.10  2002/12/10 09:34:10  heikki
 POD fixes

 Revision 1.9  2002/09/20 15:00:22  dkeefe
 added an arg to drop_temp_tables method

 Revision 1.8  2002/09/19 08:33:53  dkeefe
 added methods drop_temp_tables() and get_tablenames_like() to
 facilitate automatic clearup

 Revision 1.7  2002/06/27 10:28:56  dkeefe
 added table_is_filled method to allow checking of temp tables

 Revision 1.5  2002/06/14 12:24:22  dkeefe
 added method final_is_filled - checks that there is something in the
 final table

 Revision 1.4  2002/06/12 08:55:18  dkeefe
 added methods to determine names of most up to date databases on the
 server as indicated by letter suffixes on names. Also accessor methods for
 database names

 Revision 1.3  2002/03/20 13:53:11  dkeefe
 added table_exists method to see if a table is already present in the db

 Revision 1.2  2002/03/15 11:03:20  dkeefe
 1. added methods for stopping and (re)-starting DBI trace output to the
    log file
 2. if SQL execution fails, the entire SQL statement is printed to the log file

 Revision 1.1  2002/03/12 10:41:13  arek
 initial Damians commits


=head1 TO DO


 3. more POD

=head1 USAGE

=head2 environment

The following environment variables must be set. The following is an example for the csh.

 setenv ENSMARTUSER ecs1dadmin
 setenv ENSMARTPWD big_secret:)
 setenv ENSMARTHOST localhost
 setenv ENSMARTPORT 3360
 setenv ENSMARTDRIVER mysql
 setenv DBI_TRACE 1=/dev/null

NB. port number can be found by typing show variables at the mysql prompt

=head2 table naming conventions

When creating temporary tables in table scripts, observe the following conventions.

1. Temporary tables - if the table is used in the script but is not needed after the final table(s) for that script has been created ensure the table name contains the word 'temp'. This allows this module to do automatic clearup of unwanted tables.

2. Persistent Tables - some scripts eg update_family.pl, import_pairs.pl create tables which are needed by subsequent scripts but which are not needed in the final database. Such tables should not contain the word 'temp' or they will be removed during automatic clearup. It is recommended that these have the word 'interim' in the name.


=head1 EXAMPLES

 my $this_table = 'karyotype';
 my $tf = Production::TableFiller->new($this_table, @ARGV);

 my $mart_db = $tf->mart_db();
 my $final_table="$mart_db.".$tf->species."_".$tf->dataset."_$this_table";
 my $core_db = $tf->species()."_".$tf->dataset()."_".$tf->release();

 push @array, "drop table if exists $final_table;";
 $tf->exe(@array);

=head1 SEE ALSO

 the pod for the driver script...martmaker.pl

=head1 FUNCTIONS

=cut

use strict;
use Env;
use DBI;
use Carp;

sub new{
    my $class = shift;

    my $self = {};
    bless($self, $class);
    $self->_init(@_); # dies on failure
    return($self);
}


sub _init{
    my $self = shift;
    ($self->{'table_name'} = shift)  or fatal("you must give a table name");

    
    $self->{debug} = 0;

    # get configuration from environment variables
    $self->_config or $self->fatal("failed to get environment variables");

    # get names from command line
    $self->_check_args(@_) or $self->fatal("$self->{'err'}");

    # connect to database server
    my $dsn = "DBI:$self->{'driver'}:".
              "database=$self->{'mart_db'};".
              "host=$self->{'host'};".
              "port=$self->{'port'}";


    print "dsn $dsn user: ",$self->{'user'}, " pass: ", $self->{'password'},"\n";


    $self->{'dbh'} = DBI->connect($dsn,
                                $self->{'user'},
                                $self->{'password'},
                                #{RaiseError => 1},
				{PrintError =>0, # do our own error handling
                                 InactiveDestroy=>1} # permit forking
                                );
    $self->{'dbh'}->do("SET SQL_LOG_BIN=0");
    if ($self->{'dbh'}){
        $self->commentary("connected to $self->{'mart_db'}\n");
    }else{
        $self->fatal("failed to connect to database $self->{'mart_db'}");
    }

    # use trace to put SQL into a log file
    $self->{'log_file'} = "/tmp/".
                          "$self->{'species'}".
                         "_$self->{'dataset'}".
                         "_$self->{'table_name'}".".log";
    $self->commentary("LOGFILE: $self->{'log_file'} \n");
    unlink($self->{'log_file'}); # delete the file if it already exists
    $self->start_log();
    $self->log("creating $self->{'table_name'} table".`date`."\n");
    $self->log("process_id: ".$$);

    # dont worry about getting ensembl_core information if the table is like '^\_\w+', eg a meta table.
    # used by scripts which work on the entire mart, and dont rely on data from outside
    # of the mart.
    unless ($self->{'table_name'} =~ m/^\_\w+/) {
        # get names of most up to date databases for this release ie a or b etc
        $self->_get_db_names();
    }
}


=head2 table_exists

  Arg [1]   : txt - the name of a table in the current database
  Function  : checks if a named table exists in the database
              returns 1 if it does 0 if not
  Returntype: int
  Exceptions: none
  Caller    :
  Example   : if($tf->table_exists($final_table)){&relax();}


=cut

sub table_exists{
    my $self = shift;
    my $table = shift;

    my $query = "select count(*) from $table";
    my $sth;
    my $dbh = $self->{'dbh'};

    unless($sth=$dbh->prepare($query)){
	$self->perr("ERROR: preparation of statement failed");
        $self->log("failed preparing $query");
	$self->fatal("database error message: ".$dbh->errstr);
    }

    unless($sth->execute){
        $sth->finish;
	return(0);
    }
    $sth->finish;
    return(1);
}

=head2 column_exists

  Arg [1]   : txt - the name of a table in the current database
  Arg [2]   : txt - the name of a column
  Function  : checks if a named column exists in the table
              returns 1 if it does 0 if not
  Returntype: int
  Exceptions: none
  Caller    :
  Example   : if($tf->column_exists($final_table,'vega_description')){&relax();}


=cut

sub column_exists{
    my $self = shift;
    my $table = shift;
    my $col = shift;

    my $query = "select $col from $table limit 1";
    my $sth;
    my $dbh = $self->{'dbh'};

    unless($sth=$dbh->prepare($query)){
	$self->perr("ERROR: preparation of statement failed");
        $self->log("failed preparing $query");
	$self->fatal("database error message: ".$dbh->errstr);
    }

    unless($sth->execute){
        $sth->finish;
	return(0);
    }
    $sth->finish;
    return(1);
}

=head2 final_is_filled

  DEPRECATED - slated for removal

  Arg [1]   : none
  Function  : looks at the final table which has been created to see if it contains any data
  Returntype: int - true (number of rows) if table is filled, 0 if table is empty.
  Exceptions: none
  Caller    :
  Example   :
  $tf->final_is_filled || $tf->fatal("ERROR: final table contains no data");



sub final_is_filled{
    my $self = shift;

    my $table = $self->species.'_'.$self->dataset.'_'.$self->{'table_name'};
    my $query = "select count(*) from $table";

    my $rows = $self->{'dbh'}->selectrow_array($query);

    unless(defined $rows){
      $self->perr("selectrow_array method failed in final_is_filled");
      $self->log("failed on $query");
      $self->fatal("database error message: ".$self->{'dbh'}->errstr);
    }

    $rows ? return($rows):return(0);
}

=cut

=head2 table_is_filled

  Arg [1]   : txt - name of a table in the mart database
  Function  : looks at the named table to see if it contains any data
  Returntype: int - true (number of rows) if table is filled, 0 if table is empty.
  Exceptions: none
  Caller    :
  Example   :
  $tf->table_is_filled($final_table) || $tf->fatal("ERROR:final table empty");

=cut

sub table_is_filled{
    my $self = shift;
    my $table = shift;

    my $query = "select count(*) from $table";

    my $rows = $self->{'dbh'}->selectrow_array($query);

    unless(defined $rows){
      $self->perr("selectrow_array method failed in table_is_filled");
      $self->log("failed on $query");
      $self->fatal("database error message: ".$self->{'dbh'}->errstr);
    }

    $rows ? return($rows):return(0);
}


=head2 create_columns_table

  Arg [1]   : string - name of table to be examined
  Arg [2]   : string - name of table to be created
  Function  : examines the table given by Arg[1] and creates table Arg[2] which contains a single column holding the names of the columns in the examined table.
  Returntype: boolean 1 for success 0 for a problem. Examine $TableFiller->errstr for details of the problem
  Exceptions: database errors, missing arguments
  Caller    :
  Example   : 


unless($tf->create_columns_table($final_table,$interim_table)){
    $tf->fatal("ERROR: couldn't create interim columns table $interim_table\n".
               $tf->errstr);
}


=cut



sub create_columns_table{
    my $self = shift;
    my $source = shift;
    my $target = shift;

    my $q = "desc $source";
    my $ref = $self->{dbh}->selectall_arrayref($q);
    unless(defined $ref){
	$self->_err($self->{dbh}->errstr);
        return(0);
    }

    unless(@$ref > 0){
	$self->_err("table $source contains no columns");
	return(0);
    }

    
    my @sql;
    $q = "create table $target ( cols varchar(128) )";
    push @sql , $q;


    foreach my $row_ref (@$ref){
	$q = "insert into $target values('".$row_ref->[0]."')";
	push @sql , $q;
    }

    return($self->_execute(@sql));

}



=head2 load_columns_table

  Arg [1]   : string - name of table to be loaded
  Function  : loads the single column table given by Arg[1]. Usually this table will have been created by $TableFiller->create_columns_table in order to record which columns were produced by the first part of a two part script.
  Arg [2]   : string - 'HASH' or 'ARRAY' the list type to be returned by this method. If HASH then keys are column_names and values are 1.
  Returntype: array of strings if successful; undef for a problem. Examine $TableFiller->errstr for details of the problem
  Exceptions: database errors, missing arguments, illegal type argument, no rows in table
  Caller    :
  Example   : 


 my @cols = $tf->load_columns_table($interim_table,'ARRAY') or $tf->fatal($tf->errstr);
 print "columns\n\n".join("\n",@cols)."\n";



=cut



sub load_columns_table{
    my $self = shift;
    my $table = shift;
    my $type = shift; # 'ARRAY' or 'HASH'

    my $q = "select * from $table";
    my $ref = $self->{dbh}->selectall_arrayref($q);
    
     unless(defined $ref){
	$self->_err($self->{dbh}->errstr);
        return(undef);
    }

    unless(@$ref > 0){
	$self->_err("table $table contains no columns");
	return(undef);
    }

    my @ret;
    foreach my $row_ref (@$ref){
	
	#push @ret, $row_ref->[0];
    }

    if($type eq 'ARRAY'){
        my @ret;
        @ret = map {$_->[0]} @$ref;
        return(@ret);
    }

    if($type eq 'HASH'){
        my %ret;
        foreach my $row_ref (@$ref){
            $ret{$row_ref->[0]} = 1;
	}
	return(%ret);
    }

    $self->_err("USAGE ERROR: illegal or missing type $type");
    return(undef); 

}



=head2 start_log

  Arg [1]   : none
  Function  : starts or restarts the DBI trace function with output to the
              default logfile
  Returntype: none
  Exceptions: none
  Caller    :
  Example   : $tf->start_log();

=cut

sub start_log{
    my $self = shift;

    $self->{'dbh'}->trace( 2,"$self->{'log_file'}");
}


=head2 stop_log

  Arg [1]   : none
  Function  : Stops output to the DBI logfile. Useful if your script has a
              loop containing an SQL statement. If you do not stop output,
              the logfile gets an entry each time the statement is executed
              which can quickly generate an extremely large file.
  Returntype: none
  Exceptions: none
  Example   : $tf->stop_log();

=cut

sub stop_log{
    my $self = shift;

    $self->log("LOG STOPPED\n...\n");
    $self->{'dbh'}->trace( 0,"/dev/null");
}


=head2 back_out

  Arg [1]   : txt - new name of table
  Arg [2]   : txt - original name of table
  Function  : sets the table names for a rollback operation which we want if 
              an error occurs. Some scripts read from an interim table and 
              write to a table with the same name. Before writing to this table
              they make a copy of it called 
              <species>_<dataset>_pre_<script_name>_interim. 
              If something goes wrong during the script then the $tf->fatal 
              method is called and if a backout has been specified the tables 
              are renamed. 
  Returntype: none
  Exceptions: none
  Caller    : 
  Example   : $tf->back_out($pre_table,$interim_table);

=cut

sub back_out{
    my $self = shift;

    unless(@_ == 2){ die "USAGE ERROR: back_out must have two arguments"}
    my $from = shift;
    my $to = shift;
 
    warn "backout will rename $from as $to";

    $self->{'back_from'} = $from;
    $self->{'back_to'} = $to;

}

=head2 fatal

  Arg [1]   : txt - an error message
  Function  : prints the error message via self->perr() and exits with 1.
  Returntype: none
  Exceptions: none
  Caller    : called both internally and as instance method
  Example   : $tf->fatal("thats torn it!");

=cut

sub fatal{
    my $self = shift;
    my $msg = shift;

    $self->perr($msg);

    if(exists $self->{'back_from'}){
        my $q = "alter table ".$self->{'back_from'}.
	        " rename as ".$self->{'back_to'};

	warn "rollback query: $q";

	$self->_execute($q) or $self->perr("ROLLBACK FAILURE");
    }


    exit(1);
}

=head2 rollback

  Arg [1]   : none
  Function  : causes the rollback operation, which was specified using the 
              back_out method, to be executed. Can be used if a script needs to be prematurely terminated but the TableFiller->fatal method will not be invoked.
  Returntype: int - true on success, 0 on failure.
  Exceptions: dies if back_out method hasnt been called first
  Caller    : 
  Example   : unless($tf->rollback){ print "rename by hand"} 

=cut



sub rollback{
    my $self = shift;

    if(exists $self->{'back_from'}){
        my $q = "alter table ".$self->{'back_from'}.
	        " rename as ".$self->{'back_to'};

	if($self->_execute($q)){
	    return 1;
	}else{
            $self->perr("ROLLBACK FAILURE: $q  failed");
            return 0;
        }
    }else{
        die "USAGE ERROR: must call back_out method before rollback()";
    }

}



=head2 log

  Arg [1]   : txt - a string which programmer wants to appear in the log file.
  Function  : prints a string in the DBI log file.
  Returntype: none
  Exceptions: none
  Caller    : called both internally and as instance method
  Example   : $tf->log("KARYOTYPE TABLE PREPARATION ".`date`);

=cut

sub log{
    my $self = shift;
    my $msg = shift;

    $self->{'dbh'}->trace_msg($msg);
}



=head2 perr

  Arg [1]   : txt - a string to appear on the error output ie user will see it
  Function  : prints the error string followed by newline.
  Returntype: none
  Exceptions: none
  Caller    :
  Example   : $tf->perr("Doh!");

=cut

sub perr{
    my $self = shift;
    my $msg = shift;

    print $msg."\n";
}

=head2 commentary

  Arg [1]   : txt - a string to appear on the console ie user will see it
  Function  : prints the string followed by newline.
  Returntype: none
  Exceptions: none
  Caller    :
  Example   : $tf->commentary("Busy working");

=cut

sub commentary{
    my $self = shift;
    my $msg = shift;

    print STDERR $msg;
}


sub _check_args{
    my $s = shift;
    #hack to allow homologs.pl to have the compara db as the last argument
    if ($s->{'table_name'} eq 'homologs' || $s->{'table_name'} eq 'paralogs'){
      unless(scalar (@_) == 5){
        $s->_err("Incorrect number of command line arguments");
        return(0);
    }

      ($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'}, $s->{'compara_db'})=@_;

      return(1);
  }

# go db for xref tables
 if ($s->{'table_name'} eq 'xref_tables' || $s->{'table_name'} eq 'concat_xref')  {
      unless(scalar (@_) == 5){
        $s->_err("Incorrect number of command line arguments");
        return(0);
    }

      ($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'}, $s->{'go_db'})=@_;

      return(1);
  }

 if ($s->{'table_name'} eq 'gene_gc' || $s->{'table_name'} eq 'structure')  {
      unless(scalar (@_) == 5){
 
 print STDERR "scalar ", scalar (@_), "\n";
        

        $s->_err("Incorrect number of command line arguments");
        return(0);
    }

      ($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'}, $s->{'sequence_mart'})=@_;

      return(1);
  }

    if ($s->{'table_name'} eq 'miscfeature' )  {
	unless(scalar (@_) == 5){

	    print STDERR "scalar ", scalar (@_), "\n";


	    $s->_err("Incorrect number of command line arguments");
	    return(0);
	}

	($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'}, $s->{'genomic_features_mart'})=@_;

	return(1);
    }


 if ($s->{'table_name'} eq 'snp')  {
      unless(scalar (@_) == 5){
      
	  print STDERR "scalar ", scalar (@_), "\n";

        $s->_err("Incorrect number of command line arguments");
        return(0);
    }

      ($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'}, $s->{'snp_mart'})=@_;

      return(1);
  }

 if ($s->{'table_name'} eq 'gene_pt2' || $s->{'table_name'} eq 'transcript_pt2')  {
      unless(scalar (@_) == 5){
      
	  print STDERR "scalar ", scalar (@_), "\n";

        $s->_err("Incorrect number of command line arguments");
        return(0);
    }

      ($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'}, $s->{'snp_mart'})=@_;

      return(1);
  }






    # there must be 4 arguments, can use dummy args for meta table creation
    unless(scalar (@_) == 4){

               print STDERR "scalar ", scalar (@_), "\n";

        $s->_err("Incorrect number of command line arguments");
        warn "failing";
        return(0);
    }

    ($s->{'species'}, $s->{'dataset'}, $s->{'release'}, $s->{'mart_db'})=@_;

    return(1);
}



sub species{
    my $self = shift;

    return($self->{'species'});
}

sub dataset{
    my $self = shift;

    return($self->{'dataset'});
}

sub release{
    my $self = shift;

    return($self->{'release'});
}

sub mart_db{
   my $self = shift;

    return($self->{'mart_db'});
}

sub compara_db{
   my $self = shift;

    return($self->{'compara_db'});
}

sub go_db{
   my $self = shift;

    return($self->{'go_db'});
}


sub sequence_mart{
   my $self = shift;

    return($self->{'sequence_mart'});
}

sub snp_mart{
   my $self = shift;

    return($self->{'snp_mart'});
}









sub core_db{
    my $self = shift;
    if (exists $self->{'core_db'}){
        return($self->{'core_db'});
    }else{
        $self->_err("no core db found on server for
                     $self->species $self->release");
	return '';
    }
}

# returns the name of the most up to date species_dataset_X_Yv database
sub dataset_core_db{
    my $self = shift;
    my $db = $self->dataset.'_db';
    if (exists ($self->{$db})){
	
        return($self->{$db});
    }else{
        $self->_err("no $self->dataset core db found on server for
                     $self->species $self->release");
	return '';
    }
}

# returns the name of the most up to date species_core_X_Yv
# where name always contains 'core' even if dataset is actually something else
sub core_core_db{
    my $self = shift;

    return $self->core_db;

}

sub maps_db{
    my $self = shift;
    if (exists $self->{'maps_db'}){
        return($self->{'maps_db'});
    }else{
        $self->_err("no maps db found on server for
                     $self->species $self->release");
	return '';
    }
}


#sub snp_db{
#    my $self = shift;
#    if (exists $self->{'snp_db'}){
#        return($self->{'snp_db'});
#    }else{
#        $self->_err("no snp db found on server for ".
#                     $self->species."  ".$self->release);

#warn ("no snp db found on server for ".
#                     $self->species." ".$self->release);
#
#
#	return '';
#    }
#}

sub snp_db{
    my $self = shift;
    if (exists $self->{'variation_db'}){
        return($self->{'variation_db'});
    }else{
        $self->_err("no variation db found on server for ".
                     $self->species."  ".$self->release);

warn ("no variation db found on server for ".
                     $self->species." ".$self->release);


	return '';
    }
}







sub disease_db{
    my $self = shift;
    if (exists $self->{'disease_db'}){
        return($self->{'disease_db'});
    }else{
        $self->_err("no disease db found on server for
                     $self->species $self->release");
	return '';
    }
}


sub lite_db{
    my $self = shift;
    if (exists $self->{'lite_db'}){
        return($self->{'lite_db'});
    }else{
        $self->_err("no lite db found on server for
                     $self->species $self->release");
	return '';
    }
}


sub expression_est_db{
    my $self = shift;
    if (exists $self->{'expressionest_db'}){
        return($self->{'expressionest_db'});
    }else{
        $self->_err("no expression_est db found on server for 
                     $self->species $self->release");
	return '';
    }
}



sub expression_gnf_db{
    my $self = shift;
    if (exists $self->{'expressiongnf_db'}){
        return($self->{'expressiongnf_db'});
    }else{
        $self->_err("no expression_gnf db found on server for 
                     $self->species $self->release");
	return '';
    }
}



sub _get_db_names{
    my $self = shift;


    my @list = $self->_get_release_tables() or return 0;

    # collect the names of each type of database we might need
    # maps, disease, core, snp
    my %group;
    foreach my $db_type ($self->dataset,'core','disease','variation','expressionest','expressiongnf'){
        my @case = ();
        # collect all the cases of one type
        foreach my $db (@list){
            
	
            my $type_str = $db_type;
            if( $type_str =~ /expression/ ){ # expression dbs include dataset 
                $type_str = $self->dataset.$type_str;
	    }


            if($db_type eq 'core' || $db_type eq $self->dataset){
		$type_str .= '_[\d]+';
	    }

	    $type_str = '_'.$type_str.'_';
            #warn "type string $type_str\n";

            if ($db =~ /$type_str/){
            push @case, $db;
	    }
	}

        if (@case>0){
            # sort the list and assign last element to instance variable
            my @sorted = sort(@case);
            #warn (join(" ",@sorted)."\n");
            $self->commentary( "most recent $db_type = ".$sorted[-1]."\n");
            $self->{$db_type."_db"} = $sorted[-1];
        }
    }
}

sub _get_release_tables{
    my $self = shift;

    my $query = "show databases like \'".
                $self->species.'%'.$self->release.'%'."\'";

    my $arrayref=$self->{dbh}->selectall_arrayref($query);

    my @ret;
    if(defined $arrayref){
        my $rows = scalar(@{$arrayref});
        unless($rows){
	    $self->_err("No databases for $self->species $self->release");
	    return(0);
	}
        foreach my $row (@$arrayref){
            push @ret,  $row->[0];

	}
    }else{
        $self->_err("Undefined result for $query");
	return(0);
    }

    return @ret;
}


=head2 drop_temp_tables

  Arg [1]   : txt - optional - part of a table name
  Function  : drops all tables with the word temp in their name
  Returntype: none
  Exceptions: none
  Caller    : object::methodname or just methodname
  Example   : optional

=cut

sub drop_temp_tables{
    my $self = shift;
    my $stem = shift;

    my $spec = ($stem)? $stem.'_temp':'temp';

    my @list = $self->get_tablenames_like($spec);
    my $query1 = "drop table if exists ";
    my $query;
    my @sql;

    unless(@list>0){return};

    foreach my $table (@list){
        #print "dropping $table\n";
        $query = $query1.$table;
        push @sql, $query;
    }

    $self->exe(@sql);

}


=head2 get_tablenames_like

  Arg [1]   : txt - a word which will be inserted in a 'show tables like' query
  Function  : queries the database and extracts a list of table names
              containing the text in arg[1].
  Returntype: array
  Exceptions: none
  Caller    :
  Example   :

=cut


sub get_tablenames_like{
    my $self = shift;
    my $word = shift;

    my $query = "show tables like \'%".$word."%\'";

    my $arrayref=$self->{dbh}->selectall_arrayref($query);

    my @ret=();
    if(defined $arrayref){
        my $rows = scalar(@{$arrayref});
        unless($rows){
	    $self->_err("No tables like %$word%");
	    return(@ret);
	}
        foreach my $row (@$arrayref){
            push @ret,  $row->[0];

	}
    }else{
        die("Undefined result for $query :".$self->{dbh}->errstr);
    }

    return @ret;

}


# stores a scalar ie error message string as an instance variable
sub _err{
    my $self = shift;

    $self->{'err'} = shift;

}


# alternative for errstr
sub get_err{
    my $self = shift;

    return($self->{'err'});
}


# same as above
sub errstr{
    my $self = shift;

    return($self->{'err'});
}


# reads environment variables into instance variables
sub _config{
    my $self = shift;

    ($self->{'user'} =     $ENV{'ENSMARTUSER'}) or return(0); # ecs1dadmin
    ($self->{'password'} = $ENV{'ENSMARTPWD'}) or return(0); #TyhRv
    ($self->{'host'}   =   $ENV{'ENSMARTHOST'}) or return(0); #localhost
    ($self->{'port'} =     $ENV{'ENSMARTPORT'}) or return(0); #3360
    ($self->{'driver'}  =  $ENV{'ENSMARTDRIVER'}) or return(0); #mysql

    return(1);
}

sub host {
  my $self = shift;

  return $self->{'host'};
}

sub password {
  my $self = shift;
  return $self->{'password'};
}

sub user {
  my $self = shift;
  return $self->{'user'};
}

sub port {
  my $self = shift;
  return $self->{'port'};
}

sub set_password{
    my $self = shift;

    $self->{'password'} = shift;

}



=head2 get_dbh

  Arg [1]   : none
  Function  : returns the contents of the objects dbh instance variable.
  Returntype: DBI database handle
  Exceptions: none
  Example   : optional

=cut

sub get_dbh{
    my $self = shift;

    return $self->{'dbh'};
}




sub exe{
    my $self = shift;

    unless($self->_execute(@_)){
        # even if logging is turned off we put the offending SQL in the logfile
	$self->start_log();
        $self->log("ERROR:Execution of SQL failed\n$self->{'err'}");
        $self->fatal("ERROR:Execution of SQL failed: $self->{'err'}");
    }

}


# _execute
#
#  Arg [1]   : array of scalars containing lines of text in the format of SQL statements
#  Function  : submits each element of Arg1 to the $dbh->prepare and
#              $dbh->execute methods and checks for successful execution.
#              Returns 0 on failure, 1 on success. Emits error messages
#              via &err.
#  Returntype: int
#  Exceptions: none
#  Example   : execute($dbh, @array);

sub _execute{
    my $self = shift;
    my (@array)=@_;

    my $sth;
    my $dbh = $self->{'dbh'};

    $self->commentary("\nprocessing SQL...") if $self->{debug};

    foreach my $query(@array){
	
	#$self->commentary(".$query\n\n");
	$self->commentary(".") if $self->{debug};

	unless($sth=$dbh->prepare($query)){
            $self->perr("ERROR: preparation of statement failed");
            $self->perr("database error message: ".$dbh->errstr);
            $self->_err("failed on :-\n $query");
            return(0);
	}

        unless($sth->execute){ # returns true on success
            $self->perr("ERROR: statement execution failed");
            $self->perr("database error message: ".$dbh->errstr);
            $self->perr("statement handle error message: ".$sth->errstr);
            $self->_err("failed on :-\n $query");
            return(0);
	}
    }
    $self->commentary("\n") if $self->{debug};
    return(1);
}



=head2 bye

  Arg [1]   : none
  Function  : disconnects the DBI database handle and writes the finishing
              time to the log file.
  Returntype: none
  Exceptions: none
  Caller    :
  Example   : $tf->bye();

=cut

sub bye{
    my $self = shift;

    $self->log("FINISHED: ".`date`."\n\n");
    $self->{'dbh'}->disconnect;
}



=head2 debug

  Arg [1]   : optional int 1 or 0 

  Function  : getter/setter for a debugging flag which causes the
              generation of lots of commentary
  Returntype: int
  Exceptions: too many arguments
  Caller    : production scripts
  Example   : 

    $tf->debug(0); #commentary off
    $tf->debug(1); #commentary on
    print $tf->debug()."\n"; # prints 1

=cut



sub debug{
    my $self = shift;

    if( @_ == 1){
	$self->{debug} = shift;
    }

    if( @_ > 1){
	$self->fatal("USAGE ERROR: only supply one argument to debug()");
    }

    return $self->{debug};
}



#sub DESTROY{
#    my $self = shift;
#
#    $self->{'dbh'}->disconnect;
#}

1;
