use strict;
use warnings;

use Bio::EnsEMBL::Registry;

use EGVar::egv_utils;


sub get_dump_name
{
    my $work_dir = shift;
    my $sub_var_mart_db = shift;

    my $dump_name = "$work_dir\/$sub_var_mart_db.db.sql.gz";

    return($dump_name);
}

sub get_dump_cmd
{
    my $base_dump_cmd = shift;
    my $db_num = shift;
    my $sub_var_mart_db = shift;
    my $sub_var_mart_dump = shift;
    my $dont_dump_table = shift;

    my $dump_cmd = $base_dump_cmd;

    if ($db_num > 1)
    {
        $dump_cmd .= ' --no-create-info';
    }

    if ($dont_dump_table)
    {
        $dump_cmd .= ' --ignore-table ' . "$sub_var_mart_db.$dont_dump_table";
    }

    $dump_cmd .= " $sub_var_mart_db | gzip -c > $sub_var_mart_dump";

    return($dump_cmd);
}

sub load_db
{
    my $var_mart_db = shift;
    my $base_load_cmd = shift;
    my $sub_var_mart_dump = shift;

    print "Loading $sub_var_mart_dump to $var_mart_db\n";

    my $load_cmd = $base_load_cmd . ' < ' . $sub_var_mart_dump;
    system($load_cmd);
}

sub dump_and_load_db
{
    my $sub_var_mart_db = shift;
    my $work_dir = shift;
    my $show_db = shift;
    my $base_dump_cmd = shift;
    my $db_num = shift;
    my $var_mart_db = shift;
    my $base_load_cmd = shift;

    my $sub_var_mart_dump = get_dump_name($work_dir, $sub_var_mart_db);

    my $dump_str = ($show_db ? '' : "\t") . "Dumping " . 
        ($show_db ? "$sub_var_mart_db " : '') . 
        "to $sub_var_mart_dump\n";
    print $dump_str;

    my $dump_cmd = get_dump_cmd(
        $base_dump_cmd, $db_num, $sub_var_mart_db, $sub_var_mart_dump);
    system($dump_cmd);

    load_db($var_mart_db, $base_load_cmd, $sub_var_mart_dump);
}

sub get_job_out_file
{
    my $work_dir = shift;
    my $job_out_file_id = shift;

    my $job_out_file = '';

    if ($work_dir)
    {
        $job_out_file = "$work_dir/$job_out_file_id.out";
    }

    return($job_out_file);
}

sub create_var_mart_db
{
    my $varDBC = shift;
    my $var_mart_db = shift;
    my $log_sql = shift;

    EGVar::egv_utils::create_db($varDBC, $var_mart_db);

    if ($log_sql eq 'y')
    {
        my $sql = "CREATE TABLE $var_mart_db.sql_log " .
            '(name VARCHAR(255) NOT NULL, datetime DATETIME NOT NULL)';

        my $sth = $varDBC->prepare($sql);

        $sth->execute();

        $sth->finish();
    }
}


my @opt_details =
    (['registry_file', 'r', 's', 'r'],
    ['process_file', 'p', 's', 'r'],
    ['work_dir', 'w', 's', 'r'],
    ['bsub_queue_name', 'q', 's', 'o', 
        EGVar::egv_utils::get_default_bsub_queue_name()],
    ['bsub_wait_status', 's', 's', 'o', 
        EGVar::egv_utils::get_default_bsub_wait_status()]);

my %prog_args;

EGVar::egv_utils::get_long_prog_args_from_opt(\@opt_details, \%prog_args);


EGVar::egv_utils::is_readable_file($prog_args{registry_file}, 't');

EGVar::egv_utils::is_readable_file($prog_args{process_file}, 't');


my $bsub_queue_name = $prog_args{bsub_queue_name};
my $bsub_wait_status = $prog_args{bsub_wait_status};

my $species = '';
my $log_sql = 'n';
my $var_mart_db = '';

my $process_key;
my %process;

open PROCESS, $prog_args{process_file};

foreach my $process_line (<PROCESS>)
{
    print $process_line;
    chomp $process_line;

    if ($process_line !~ /^#/ && $process_line ne '')
    {
        my @process_values = split "\t", $process_line;

        $process_key = shift @process_values;

        if ($process_key eq 'SPECIES')
        {
            $species = $process_values[0];
        }
        elsif ($process_key eq 'VAR_MART_DB')
        {
            $var_mart_db = $process_values[0];
        }
        elsif ($process_key eq 'LOG_SQL')
        {
            $log_sql = $process_values[0];
        }
        elsif ($process_key =~ /^SUB_VAR_MART/)
        {
            $process{$process_key} = \@process_values;
        }
    }
}

close PROCESS;


# Connect to API using registry

print "\n";

Bio::EnsEMBL::Registry->load_all($prog_args{registry_file});

my $varDB = 
    EGVar::egv_utils::get_DBAdaptor_from_Registry($species, 'variation');
my $varDBC = $varDB->dbc();


my $source_ind_gen_table = 'tmp_individual_genotype_single_bp';

if (!EGVar::egv_utils::table_exists($varDBC, $source_ind_gen_table))
{
    die("\nTable '$source_ind_gen_table' " .
        "must exist before a var mart can be built!\n\n");
}

my $trans_var_view = 'MTMP_transcript_variation';

if (!EGVar::egv_utils::table_exists($varDBC, $trans_var_view))
{
    die("\nView '$trans_var_view' " .
        "must exist before a var mart can be built!\n\n");
}


print "\n" . EGVar::egv_utils::get_started_str() . "\n\n";


my ($sub_var_mart_sqls, $sub_var_mart_db);
my $sub_var_mart_sql;
my $var_mart_db_dumps_file;
my $base_cmd = EGVar::egv_utils::get_base_mysql_cmd_from_dbc($varDBC);
my $base_build_cmd = 'mysql ' . $base_cmd;
my $base_dump_cmd = 'mysqldump ' . $base_cmd;
my $base_load_cmd = $base_build_cmd . $var_mart_db;
my $build_cmd;
my $sub_var_mart_dump;
my $var_mart_dump;
my $dont_dump_table;
my $dump_cmd;
my $load_cmd;
my $num_processes = 0;
my $num_sub_marts_built = 0;
my @dbs_to_load = ();
my @sql_types = ('var_pre', 'var_syn', 'var_post', 'struct_var');
my $sql_type;
my $job_sql_type;
my $job_id;
my $prev_job_id;
my $job_out_file;
my @job_out_files_to_check;
my $bsub_cmd;

if (EGVar::egv_utils::db_exists($varDBC, $var_mart_db))
{
    print "Database '$var_mart_db' already exists, doing nothing!\n";
}
else
{
    $num_processes = scalar keys %process;

    if ($num_processes > 1)
    {
        create_var_mart_db($varDBC, $var_mart_db, $log_sql);
    }

    foreach $process_key (sort keys %process)
    {
        $sub_var_mart_sqls = $process{$process_key}[0];
        $sub_var_mart_db = $process{$process_key}[1];

        if (EGVar::egv_utils::db_exists($varDBC, $sub_var_mart_db))
        {
            print "Database '$sub_var_mart_db' already exists, skipping!\n";
        }
        else
        {
            print "Building $sub_var_mart_db from $sub_var_mart_sqls\n";

            create_var_mart_db($varDBC, $sub_var_mart_db, $log_sql);

            $prev_job_id = '';
            $num_sub_marts_built++;

            foreach $sub_var_mart_sql (split ',', $sub_var_mart_sqls)
            {
                if (EGVar::egv_utils::is_readable_file($sub_var_mart_sql))
                {
                    $build_cmd = $base_build_cmd . 
                        "$sub_var_mart_db < $sub_var_mart_sql";

                    if ($bsub_queue_name eq '')
                    {
                        system($build_cmd);
                    }
                    else
                    {
                        $job_sql_type = '';

                        foreach $sql_type (@sql_types)
                        {
                            if ($sub_var_mart_sql =~ /\.($sql_type.*)\.sql/)
                            {
                                $job_sql_type = $1;
                            }
                        }

                        $job_id = $sub_var_mart_db;

                        if ($job_sql_type)
                        {
                            $job_id .= ".$job_sql_type";
                        }

                        $job_id .= '.build';

                        $job_out_file = get_job_out_file(
                            $prog_args{work_dir}, $job_id);

                        $bsub_cmd = "bsub -q$bsub_queue_name " .
                            ($prev_job_id ? 
                                "-w '$bsub_wait_status($prev_job_id)' " : '') .
                            "-J$job_id " .
                            ($job_out_file ne '' ? "-o$job_out_file " : '') .
                            "'$build_cmd'";

                        push @job_out_files_to_check, $job_out_file;

                        system($bsub_cmd);

                        $prev_job_id = $job_id;
                    }
                }
                else
                {
                    print "File '$sub_var_mart_sql' doesn't exist, skipping!\n";
                }
            }

            push @dbs_to_load, $sub_var_mart_db;
        }

        if ($log_sql eq 'y')
        {
            $dont_dump_table = 'sql_log';
        }

        if ($bsub_queue_name eq '')
        {
            if ($prog_args{work_dir})
            {
                dump_and_load_db($sub_var_mart_db, $prog_args{work_dir}, 0,
                    $base_dump_cmd, $num_sub_marts_built, 
                    $var_mart_db, $base_load_cmd);
            }
        }
        else
        {
            if ($bsub_wait_status ne '')
            {
                $sub_var_mart_dump = 
                    get_dump_name($prog_args{work_dir}, $sub_var_mart_db);

                print "\tand dumping to $sub_var_mart_dump\n";

                $job_id = "$sub_var_mart_db.dump";
                $job_out_file = get_job_out_file($prog_args{work_dir}, $job_id);

                $bsub_cmd = "bsub " .
                    "-w '$bsub_wait_status($sub_var_mart_db.*build)' " .
                    "-q$bsub_queue_name -J$job_id " .
                    ($job_out_file ? "-o$job_out_file " : '') .
                    "'" . 
                        get_dump_cmd($base_dump_cmd, 
                            $num_sub_marts_built, 
                            $sub_var_mart_db, $sub_var_mart_dump,
                            $dont_dump_table)
                    . "'";

                push @job_out_files_to_check, $job_out_file;

                system($bsub_cmd);
            }
        }
    }

    if ($bsub_queue_name ne '' && $bsub_wait_status ne '')
    {
        $job_id = "$var_mart_db.wait";
        $job_out_file = get_job_out_file($prog_args{work_dir}, $job_id);

        $bsub_cmd = "bsub -K -w '$bsub_wait_status($var_mart_db*)' " . 
            "-q$bsub_queue_name -J$job_id " .
            ($job_out_file ? "-o$job_out_file " : '') .
            ' sleep 1';
        system($bsub_cmd);

        push @job_out_files_to_check, $job_out_file;

        print "\n";

        if ($num_processes > 1)
        {
            if ($prog_args{work_dir} ne '' && (scalar @dbs_to_load > 0))
            {
                $var_mart_db_dumps_file = 
                    "$prog_args{work_dir}/$var_mart_db.dumps";
                open DUMPS, ">$var_mart_db_dumps_file";

                foreach $sub_var_mart_db (@dbs_to_load)
                {
                    print DUMPS get_dump_name(
                        $prog_args{work_dir}, $sub_var_mart_db) . "\n";
                }

                close DUMPS;

                my $cmd = 'perl load_dumps_into_db.pl ' .
                    "-r $prog_args{registry_file} -s $species " .
                    "-d $var_mart_db -f $var_mart_db_dumps_file -u " . 
                    get_dump_name($prog_args{work_dir}, $var_mart_db) ;
                system($cmd);
            }
        }
    }
    else
    {
        if (scalar @dbs_to_load > 0)
        {
            print "DBs to dump and load:\n";

            foreach $sub_var_mart_db (@dbs_to_load)
            {
                print "\t$sub_var_mart_db\n";
            }
        }
    }

    my $check_cmd;
    my $check_status;

    foreach $job_out_file (@job_out_files_to_check)
    {
        $check_cmd = "grep -B2 'Resource usage summary' $job_out_file | " .
            'tail -n3 | head -n1';

        $check_status = `$check_cmd`;
        chomp($check_status);

        if ($check_status ne 'Successfully completed.')
        {
            print "Check $job_out_file for possible failure!\n";
        }
    }
}


print "\n\n" . EGVar::egv_utils::get_ended_str() . "\n\n";


exit;

