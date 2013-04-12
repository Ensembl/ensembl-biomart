use strict;
use warnings;

use Getopt::Std;

use Bio::EnsEMBL::Registry;

use EGVar::egv_utils;


sub load_db
{
    my $database = shift;
    my $base_load_cmd = shift;
    my $dump_file = shift;

    print "Loading $dump_file to $database\n";

    my $load_cmd = undef;

    if( $dump_file =~ /.gz$/ ){
	$load_cmd = "gunzip < $dump_file | $base_load_cmd $database";
    }
    else{
	$load_cmd = "$base_load_cmd $database < $dump_file";
    }

#    print "$load_cmd\n"; 
    system($load_cmd);
}

sub disable_db_keys
{
    my $dbc = shift;
    my $db = shift;

    my @db_tables_to_enable = ();

    my $sql = "SHOW TABLES FROM $db";
    my $sth = $dbc->prepare($sql);

    $sth->execute();

    my $table;

    while (($table) = $sth->fetchrow_array())
    {
        push @db_tables_to_enable, $table;
    }

    $sth->finish();

    foreach $table (@db_tables_to_enable)
    {
        $sql = "ALTER TABLE $db.$table DISABLE KEYS";
        $sth = $dbc->prepare($sql);

        $sth->execute();

        $sth->finish();
    }

    return(\@db_tables_to_enable);
}

sub enable_db_keys
{
    my $dbc = shift;
    my $db_tables_to_enable = shift;

    my $sql;
    my $sth;

    foreach my $db_table (@$db_tables_to_enable)
    {
        $sql = "ALTER TABLE $db_table ENABLE KEYS";
        $sth = $dbc->prepare($sql);

        $sth->execute();

        $sth->finish();
    }
}


my @opt_details =
    (['registry_file', 'r', 's', 'r'],
    ['species', 's', 's', 'r'],
    ['dumps_file', 'f', 's', 'r'],
    ['database', 'd', 's', 'r'],
    ['database_dump_file', 'u', 's', 'o'],
    ['parallel_by_table', 'p', 's', 'o', 'n'], # ??? 'y'],
    ['bsub_queue_name', 'q', 's', 'o', 'production-rh6'],
    ['bsub_wait_status', 'w', 's', 'o', 'ended']);

my %prog_args;

EGVar::egv_utils::get_long_prog_args_from_opt(\@opt_details, \%prog_args);


print "\n" . EGVar::egv_utils::get_started_str() . "\n\n";


# Connect to API using registry

Bio::EnsEMBL::Registry->load_all($prog_args{registry_file});

my $varDB = EGVar::egv_utils::get_DBAdaptor_from_Registry(
    $prog_args{species}, 'variation');
my $varDBC = $varDB->dbc(); # ???


print "\n";

my $db_tables_to_enable;

my $base_cmd = EGVar::egv_utils::get_base_mysql_cmd_from_dbc($varDBC);
my $base_load_cmd = "mysql $base_cmd ";

my $dump_file;
my $num_dumps = 0;

open DUMPS, $prog_args{dumps_file};

my $by_tbl_dumps_file;

if ($prog_args{parallel_by_table} eq 'y')
{
    $by_tbl_dumps_file = $prog_args{dumps_file} . '.by_tbl';

    open DUMPS_BY_TBL, '>' . $by_tbl_dumps_file;
}

foreach $dump_file (<DUMPS>)
{
    chomp $dump_file;
    $num_dumps++;

    if ($dump_file !~ /^#/)
    {
        if ($num_dumps == 1 || $prog_args{parallel_by_table} ne 'y')
        {
            load_db($prog_args{database}, $base_load_cmd, $dump_file);
        }
        else
        {
            print DUMPS_BY_TBL "$dump_file\n";
        }

        if ($num_dumps == 1)
        {
            disable_db_keys($varDBC, $prog_args{database}); # ???
        }
    }
}

enable_db_keys($varDBC, $db_tables_to_enable); # ???

if ($prog_args{parallel_by_table} eq 'y')
{
    close DUMPS_BY_TBL;

    my $tables_to_load = 
        EGVar::egv_utils::get_tables_in_db($varDBC, $prog_args{database});

    my $base_load_by_tbl_cmd = "mysql $base_cmd";
    my $load_by_tbl_cmd;
    my $base_bsub_cmd = "bsub -q$prog_args{bsub_queue_name}";
    my $bsub_cmd;

    foreach my $table_to_load (sort keys %$tables_to_load)
    {
        $load_by_tbl_cmd = "$base_load_by_tbl_cmd";

        $bsub_cmd = $base_bsub_cmd . 
            " -J$prog_args{database}.$table_to_load -o$by_tbl_dumps_file.$table_to_load.out";

        system("$bsub_cmd perl load_dumps_into_db_by_table.pl " .
            "-m \"$load_by_tbl_cmd\" -d $prog_args{database} -t $table_to_load " .
            "-u $by_tbl_dumps_file");
    }

    $bsub_cmd = "bsub -K -w '$prog_args{bsub_wait_status}" . 
        "($prog_args{database}*)" . "' -q$prog_args{bsub_queue_name} -J$prog_args{database}" . ".wait -o$by_tbl_dumps_file.wait.out sleep 1";
    system($bsub_cmd);
}

close DUMPS;


my $base_dump_cmd = "mysqldump $base_cmd ";
my $dump_cmd;

if ($prog_args{database_dump_file})
{
    print "\nDumping $prog_args{database} to $prog_args{database_dump_file}\n";
    $dump_cmd = $base_dump_cmd . 
        " $prog_args{database} > $prog_args{database_dump_file}";
    # print "$dump_cmd\n"; 
    system($dump_cmd);
}


print "\n\n" . EGVar::egv_utils::get_ended_str() . "\n\n";


exit;

