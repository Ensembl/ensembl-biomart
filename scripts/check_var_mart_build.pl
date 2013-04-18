use strict;
use warnings;

use Bio::EnsEMBL::Registry;

use EGVar::egv_utils;


sub get_table_counts_for_db
{
    my $dbc = shift;
    my $db_name = shift;
    my $table_counts_for_dbs = shift;

    $table_counts_for_dbs->{$db_name} = {};

    my $tables_sql = "SHOW TABLES FROM $db_name";
    my $tables_sth = $dbc->prepare($tables_sql);

    $tables_sth->execute();

    my $base_count_sql = 'SELECT COUNT(*) FROM ';
    my $count_sth;
    my $table_count;

    while (my ($table_name) = $tables_sth->fetchrow_array())
    {
        $count_sth = $dbc->prepare($base_count_sql . " $db_name.$table_name");

        $count_sth->execute();

        ($table_count) = $count_sth->fetchrow_array();

        $table_counts_for_dbs->{$db_name}->{$table_name} = $table_count;

        $count_sth->finish();
    }

    $tables_sth->finish();
}

sub get_key_col_name
{
    my $dbc = shift;
    my $table_name = shift;
    my $col_name = shift;

    my $col_key_like = "$col_name" . '_%_key';
    my $sql = "SHOW FIELDS FROM $table_name LIKE '$col_key_like'";
    my $sth = $dbc->prepare($sql);

    $sth->execute();

    my ($key_col_name) = $sth->fetchrow_array();

    $sth->finish();

    print "\tKey: $col_name -> $key_col_name\n";

    return($key_col_name);
}

sub check_value
{
    my $var_value = shift;
    my $mart_value = shift;
    my $value_name = shift;
    my $matches = shift;

    if ($var_value && $mart_value != $var_value)
    {
        print "\t$value_name (var = $var_value, mart = $mart_value) " .
            "doesn't match\n";

        $$matches = 0;
    }
}


my @opt_details =
    (['registry_file', 'r', 's', 'r'],
    ['process_file', 'p', 's', 'r']);

my %prog_args;

EGVar::egv_utils::get_long_prog_args_from_opt(\@opt_details, \%prog_args);


EGVar::egv_utils::is_readable_file($prog_args{registry_file}, 't');

EGVar::egv_utils::is_readable_file($prog_args{process_file}, 't');


print "\n" . EGVar::egv_utils::get_started_str() . "\n\n";


print "\n";

my $species = '';
my ($count, $min_var_id, $max_var_id);
my $var_mart_db = '';

my $process_key;
my %sub_var_marts;

open PROCESS, $prog_args{process_file};

foreach my $process_line (<PROCESS>)
{
    print $process_line;
    chomp $process_line;

    if ($process_line !~ /^#/ && $process_line ne '') # LIMIT ???
    {
        my @process_values = split "\t", $process_line;

        $process_key = shift @process_values;

        if ($process_key eq 'SPECIES')
        {
            $species = $process_values[0];
        }
        elsif ($process_key eq 'COUNT')
        {
            $count = $process_values[0];
        }
        elsif ($process_key eq 'MIN_VAR_ID')
        {
            $min_var_id = $process_values[0];
        }
        elsif ($process_key eq 'MAX_VAR_ID')
        {
            $max_var_id = $process_values[0];
        }
        elsif ($process_key eq 'VAR_MART_DB')
        {
            $var_mart_db = $process_values[0];
        }
        elsif ($process_key =~ /^SUB_VAR_MART/)
        {
            $sub_var_marts{$process_key} = $process_values[1];
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


my %var_mart_table_counts;
get_table_counts_for_db($varDBC, $var_mart_db, \%var_mart_table_counts);

my %sub_var_marts_table_counts;
foreach my $sub_var_mart (sort keys %sub_var_marts)
{
    if ($sub_var_marts{$sub_var_mart} ne $var_mart_db)
    {
        get_table_counts_for_db($varDBC, $sub_var_marts{$sub_var_mart}, 
            \%sub_var_marts_table_counts);
    }
}


my ($db_name, $table_name);

print "\nTable\tTotal # in Mart";

my $num_sub_var_marts = scalar keys %sub_var_marts_table_counts;

my $pattern = '^' . $var_mart_db . '_(.*)$';

foreach $db_name (sort keys %sub_var_marts_table_counts)
{
    if ($db_name ne $var_mart_db)
    {
        print "\t# in ";

        if ($db_name =~ /$pattern/)
        {
            print "Sub Mart $1";
        }
        else
        {
            print $db_name;
        }
    }
}

if ($num_sub_var_marts > 0)
{
    print "\tTotal # in Sub Marts";
}

print "\n";

my $db_table_counts = $var_mart_table_counts{$var_mart_db};
my $dbs_are_consistent = 1;
my $total;

my @main_tables;

foreach $table_name (sort keys %$db_table_counts)
{
    print "$table_name\t$db_table_counts->{$table_name}";

    if ($table_name =~ /__main$/)
    {
        push @main_tables, $table_name;
    }

    $total = 0;

    foreach $db_name (sort keys %sub_var_marts_table_counts)
    {
        if (exists $sub_var_marts_table_counts{$db_name}{$table_name})
        {
            print "\t$sub_var_marts_table_counts{$db_name}{$table_name}";

            $total += $sub_var_marts_table_counts{$db_name}{$table_name};
        }
        else
        {
            if ($table_name !~ /__structural_variation__main/)
            {
                print "\t?";

                $dbs_are_consistent = 0;
            }
            else
            {
                print "\t0";
            }
        }
    }

    if ($num_sub_var_marts > 0)
    {
        print "\t$total";

        if ($db_table_counts->{$table_name} != $total)
        {
            print "\tTotals don't match!";

            $dbs_are_consistent = 0;
        }
    }

    print "\n";
}


print "\n\n";

my $mart_table_name;
my $key_col_name = '';
my $sql;
my $sth;
my @col_names;
my $col_name;
my $count_nulls_sql;
my $count_nulls_sth;
my $nulls_count;

my ($is_feature, $is_struct_var);
my $proc_count;
my ($mart_count, $mart_distinct_key_count, $mart_min_var_id, $mart_max_var_id);

foreach $table_name (@main_tables)
{
    $is_feature = ($table_name =~ /__variation_feature__/);
    $is_struct_var = ($table_name =~ /__structural_variation__/);
    $mart_table_name = "$var_mart_db.$table_name";

    print "Checking $mart_table_name\n";

    $key_col_name = get_key_col_name($varDBC, $mart_table_name, 
        ($is_struct_var ? 'structural_variation_id' : 'variation_id'));

    $sql = "SELECT COUNT(*), COUNT(DISTINCT $key_col_name), " .
        "MIN($key_col_name), MAX($key_col_name) FROM $mart_table_name";
    $sth = $varDBC->prepare($sql);

    $sth->execute();

    ($mart_count, $mart_distinct_key_count, 
        $mart_min_var_id, $mart_max_var_id) = $sth->fetchrow_array();

    $sth->finish();

    if (!$is_struct_var)
    {
        check_value($count, $mart_distinct_key_count, 'Count', 
            \$dbs_are_consistent);

        check_value($min_var_id, $mart_min_var_id, 'Min var ID', 
            \$dbs_are_consistent);

        check_value($max_var_id, $mart_max_var_id, 'Max var ID',
            \$dbs_are_consistent);
    }

    $sql = "DESC $mart_table_name";
    $sth = $varDBC->prepare($sql);

    $sth->execute();

    @col_names = ();

    while (($col_name) = $sth->fetchrow_array())
    {
        push @col_names, $col_name;
    }

    foreach $col_name (sort @col_names)
    {
        if ($col_name =~ /_bool$/)
        {
            $count_nulls_sql = "SELECT COUNT(*) FROM $mart_table_name " .
                "WHERE $col_name IS NULL";
            $count_nulls_sth = $varDBC->prepare($count_nulls_sql);

            $count_nulls_sth->execute();

            ($nulls_count) = $count_nulls_sth->fetchrow_array();

            if ($mart_count == $nulls_count)
            {
                print "\tAll $col_name are NULL!\n";
            }

            $count_nulls_sth->finish();
        }
    }

    $sth->finish();
    
    print "\n";
}


print "\nDatabase(s) are" . 
    ($dbs_are_consistent ? '' : "n't") . " consistent!\n";


print "\n\n" . EGVar::egv_utils::get_ended_str() . "\n\n";


exit;

