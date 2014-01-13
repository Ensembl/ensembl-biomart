# Copyright [2009-2014] EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use strict;
use warnings;

use Bio::EnsEMBL::Registry;

use EGVar::egv_utils;


sub populate_genotype_table
{
    my $varDBC = shift;
    my $genotype_table = shift;
    my $species = shift;
    my $work_dir = shift;
    my $ref_strain = shift;
    my $sql_file = shift;
    my $bsub_queue_name = shift;
    my $bsub_wait_status = shift;

    print "\nPopulating '$genotype_table'\n";


    # create table first
    
    my $run_num = 0;
    my $base_cmd = "perl get_gtype_poly.pl -species $species " .
        "-tmpdir $work_dir -tmpfile g_g_p.$run_num.tmp";
    my $cmd;

    $cmd = $base_cmd . 
        ' -create_gtype_table -table_only -ref_strain ' . $ref_strain .
        ' -sql_file ' . $sql_file;

    system($cmd);

    if (!EGVar::egv_utils::table_exists($varDBC, $genotype_table))
    {
        die("Failed to create table $genotype_table - cannot continue");
    }


    # then process seq_regions in parallel using bsub

    $cmd = "bsub -Jg_g_p.r$run_num -q$bsub_queue_name $base_cmd " .
        '-seq_region_id ';

    my $sql = 'SELECT seq_region_id FROM seq_region';
    my $sth = $varDBC->prepare($sql);
    my $old_run_num;

    $sth->execute();

    while (my ($seq_region_id) = $sth->fetchrow_array())
    {
        $old_run_num = $run_num;
        $run_num++;

        $cmd =~ s/\.r$old_run_num/\.r$run_num/g;

        system($cmd . $seq_region_id);
    }

    $sth->finish();


    # wait for jobs to finish
    $cmd = "bsub -K -w '$bsub_wait_status(g_g_p.r*)'" .
        " -q$bsub_queue_name -Jg_g_p.wait sleep 1";

    system($cmd);


    $sql = "SELECT COUNT(*) FROM $genotype_table";
    $sth = $varDBC->prepare($sql);

    $sth->execute();

    my ($num_rows) = $sth->fetchrow_array();

    print "\t$num_rows rows inserted\n";

    $sth->finish();
}

sub write_out_sql_line
{
    my $out_sql_fh = shift;
    my $sql_type = shift;
    my $out_sql_line = shift;
    my $log_sql = shift;
    my $sql_line_num = shift;

    if ($out_sql_line ne '')
    {
        print $out_sql_fh $out_sql_line . "\n";
    }

    if ($log_sql eq 'y' && $out_sql_line !~ /^#/)
    {
        print $out_sql_fh 
            "INSERT INTO sql_log VALUES ('$sql_type-$$sql_line_num', now());\n";

        $$sql_line_num++;
    }
}

sub write_out_sql_file
{
    my $out_sql_file = shift;
    my $template_sql_by_type = shift; # ???
    my $sql_type = shift;
    my $value_by_tag = shift;
    my $ind_gen_var_key_between_clauses = shift;
    my $log_sql = shift;

    my $sql_line_num = 0;

#print "\n$out_sql_file\n\n";
    open my $out_sql_fh, ">$out_sql_file";

    write_out_sql_line($out_sql_fh, $sql_type, '', $log_sql, \$sql_line_num);

    foreach my $template_sql_line (@{$template_sql_by_type->{$sql_type}})
    {
        if ($template_sql_line =~ /DISTINCT_POLY/)
        {
            $value_by_tag->{DISTINCT_POLY} = ''; # ???
            $value_by_tag->{CREATE_THEN_INSERT} = 'create table';

            if ($ind_gen_var_key_between_clauses)
            {
                foreach my $ind_gen_var_key_between_clause 
                    (@$ind_gen_var_key_between_clauses)
                {
                    $value_by_tag->{VAR_KEY_COND} = 
                        $ind_gen_var_key_between_clause;

                    write_out_sql_line($out_sql_fh, $sql_type,
                        EGVar::egv_utils::tag_to_value(
                            $value_by_tag, $template_sql_line), 
                        $log_sql, \$sql_line_num);

                    $value_by_tag->{CREATE_THEN_INSERT} = 'insert into';
                }
            }
            else # ???
            {
                $value_by_tag->{VAR_KEY_COND} = '';
                $value_by_tag->{where} = ';'; # ???

                write_out_sql_line($out_sql_fh, $sql_type,
                    EGVar::egv_utils::tag_to_value(
                        $value_by_tag, $template_sql_line), 
                    $log_sql, \$sql_line_num);

                delete $value_by_tag->{where}; # ???
            }
        }
        else
        {
            write_out_sql_line($out_sql_fh, $sql_type,
                EGVar::egv_utils::tag_to_value(
                    $value_by_tag, $template_sql_line), 
                $log_sql, \$sql_line_num);
        }
    }

    close $out_sql_fh;
}

sub get_src_ids_by_name
{
    my $varDBC = shift;
    my $exclude_source_without_synonyms = shift;

    my $sql = 'SELECT s.source_id, s.name, COUNT(vs.variation_synonym_id) ' .
        'FROM source s LEFT JOIN variation_synonym vs ' .
        'ON (s.source_id = vs.source_id) ' .
        'GROUP BY s.source_id ORDER BY s.name';
    my $sth = $varDBC->prepare($sql);

    $sth->execute();

    my %src_ids_by_name = ();

    print "\n\tSources:\n";

    while (my ($src_id, $src_name, $src_vs_count) = $sth->fetchrow_array())
    {
        $src_name =~ tr/\/\_\- //d;
        $src_name = lc($src_name);

        print "\t\t$src_name -> $src_id ($src_vs_count)\n";

        if ($src_vs_count > 0 || $exclude_source_without_synonyms eq 'n')
        {
            $src_ids_by_name{$src_name} = $src_id;
        }
    }

    print "\n";

    $sth->finish();

    return(\%src_ids_by_name);
}

sub get_ind_gen_count
{
    my $varDBC = shift;
    my $between_clause = shift;
    my $single_or_multiple = shift;
    my $ind_gen_counts = shift;
    my $key_in = shift;

    my $sql = 'SELECT COUNT(*) FROM ' .
        ($single_or_multiple eq 's' ?
            'tmp_individual_genotype_single_bp' :
            'individual_genotype_multiple_bp') .
        ' WHERE ' . $between_clause;
    my $sth = $varDBC->prepare($sql);

    $sth->execute();

    my $key = ($key_in ? $key_in : $single_or_multiple);

    ($ind_gen_counts->{$key}) = $sth->fetchrow_array();

    $sth->finish();
}

sub get_ind_gen_var_key_between_clauses
{
    my $varDBC = shift;
    my $value_by_tag = shift;
    my $ind_gen_count = shift;
    my $max_ind_gens_to_process = shift;
    my $variation_key = shift;
    my $min_var_id = shift;
    my $max_var_id = shift;
    my $var_id_between_clause = shift;

    my @ind_gen_var_key_between_clauses = ();

    $value_by_tag->{'DISTINCT_POLY '} = '';

    my $num_distinct_poly_sets = 
        $ind_gen_count / $max_ind_gens_to_process;

    my $int_num_distinct_poly_sets = 
        int($num_distinct_poly_sets);

    if ($num_distinct_poly_sets > 
        $int_num_distinct_poly_sets)
    {
        $num_distinct_poly_sets = 
            $int_num_distinct_poly_sets + 1;
    }
    else
    {
        $num_distinct_poly_sets = 
            $int_num_distinct_poly_sets;
    }

    my $min_ind_gen_var_id;
    my $max_ind_gen_var_id;
    my $ind_gen_var_id_step;
    my $sub_set_num;
    my $total;
    my $ind_gen_between_clause;
    my %poly_set_ind_gen_counts;

    if ($num_distinct_poly_sets > 1)
    {
        $ind_gen_var_id_step = 
            ($max_var_id - $min_var_id + 1) / 
            ($num_distinct_poly_sets * 10);

        $ind_gen_var_id_step = 
            int($ind_gen_var_id_step);

        $min_ind_gen_var_id = $min_var_id;
        $max_ind_gen_var_id = 
            $min_var_id + $ind_gen_var_id_step;

        $sub_set_num = 0;
        $total = 0;

        while ($min_ind_gen_var_id < $max_var_id)
        {
            $sub_set_num++;

            $ind_gen_between_clause = 
                'variation_id between ' .
                $min_ind_gen_var_id . ' and ' .
                $max_ind_gen_var_id;

            get_ind_gen_count($varDBC, 
                $ind_gen_between_clause, 
                's', \%poly_set_ind_gen_counts, 
                $sub_set_num);
print "$sub_set_num ($min_ind_gen_var_id - $max_ind_gen_var_id) = " . $poly_set_ind_gen_counts{$sub_set_num} . "\n";
$total += $poly_set_ind_gen_counts{$sub_set_num};

            $min_ind_gen_var_id = 
                $max_ind_gen_var_id + 1;
            $max_ind_gen_var_id = 
                $min_ind_gen_var_id + $ind_gen_var_id_step;

            if ($max_ind_gen_var_id > $max_var_id)
            {
                $max_ind_gen_var_id = $max_var_id;
            }
        }

        my $num_sub_sets = $sub_set_num;
print "total = $total\n\n";

        $min_ind_gen_var_id = $min_var_id;
        $max_ind_gen_var_id = 
            $min_var_id + $ind_gen_var_id_step;
                                
        my $num_ind_gens_in_set = 0;
        $total = 0;

        foreach $sub_set_num 
            (sort {$a <=> $b} keys %poly_set_ind_gen_counts)
        {
            $num_ind_gens_in_set += $poly_set_ind_gen_counts{$sub_set_num};
            $max_ind_gen_var_id += $ind_gen_var_id_step;

            if ($max_ind_gen_var_id > $max_var_id)
            {
                $max_ind_gen_var_id = $max_var_id;
            }

            if ($num_ind_gens_in_set > $max_ind_gens_to_process || $sub_set_num == $num_sub_sets)
            {
                print "$num_ind_gens_in_set ($min_ind_gen_var_id - $max_ind_gen_var_id)\n";

                $total += $num_ind_gens_in_set;

                push @ind_gen_var_key_between_clauses, 
                    "$variation_key between $min_ind_gen_var_id and $max_ind_gen_var_id;";

                $min_ind_gen_var_id = $max_ind_gen_var_id + 1;
                $num_ind_gens_in_set = 0;
            }
        }
print "total = $total\n\n";
    }
    else
    {
        my $var_key_between_clause = $var_id_between_clause;

        $var_key_between_clause =~ s/variation_id/$variation_key/;

        push @ind_gen_var_key_between_clauses, 
            $var_key_between_clause . ';';
    }

    return(\@ind_gen_var_key_between_clauses);
}


my @opt_details =
    (['registry_file', 'r', 's', 'r'],
    ['species', 's', 's', 'r'],
    ['template_sql_file', 't', 's', 'r'],
    ['out_sql_file_prefix', 'o', 's', 'r'],
    ['template_source_sql_file', 'u', 's', 'r'],
    ['var_mart_db_tag', 'm', 's', 'o', 'VAR_MART_DB'],
    ['var_mart_db_value', 'n', 's', 'r'],
    ['var_db_tag', 'v', 's', 'o', 'VAR_DB'],
    ['core_db_tag', 'c', 's', 'o', 'CORE_DB'],
    ['species_abbrev_tag', 'a', 's', 'o', 'SPECIES_ABBREV'],
    ['species_abbrev_value', 'b', 's', 'r'],
    ['var_id_cond_tag', 'i', 's', 'o', 'VAR_ID_COND'],
    ['limit', 'l', 'i', 'o'],
    ['rows_per_sub_mart', 'f', 'i', 'o', '1000000'],
    ['src_idx_tag', 'g', 's', 'o', 'I_'],
    ['process_file', 'p', 's', 'r'],
    ['work_dir', 'w', 's', 'r'],
    ['populate_genotype_table', 'y', 's', 'o', 'n'], # ???
    ['default_ref_strain', 'e', 's', 'o', 'Ref'], # ???
    ['exclude_source_without_synonyms', 'x', 's', 'o', 'y'],
    ['template_struct_var_sql_file', 'h', 's', 'o'],
    ['log_sql', 'q', 's', 'o', 'n'],
    ['bsub_queue_name', 'd', 's', 'o', 
        EGVar::egv_utils::get_default_bsub_queue_name()],
    ['bsub_wait_status', 'z', 's', 'o', 
        EGVar::egv_utils::get_default_bsub_wait_status()]);

my %prog_args;

EGVar::egv_utils::get_long_prog_args_from_opt(\@opt_details, \%prog_args);


EGVar::egv_utils::is_readable_file($prog_args{registry_file}, 't');

if ($prog_args{template_sql_file})
{
    EGVar::egv_utils::is_readable_file(
        $prog_args{template_sql_file}, 't');
}

if ($prog_args{template_source_sql_file})
{
    EGVar::egv_utils::is_readable_file(
        $prog_args{template_source_sql_file}, 't');
}

if ($prog_args{template_struct_var_sql_file})
{
    EGVar::egv_utils::is_readable_file(
        $prog_args{template_struct_var_sql_file}, 't');
}


if ($prog_args{populate_genotype_table} eq 'y' && $prog_args{work_dir} eq '')
{
    die("work_dir must be specified if populate_genotype_table is!");
}


# Connect to API using registry

Bio::EnsEMBL::Registry->load_all($prog_args{registry_file});

my $varDB = EGVar::egv_utils::get_DBAdaptor_from_Registry(
    $prog_args{species}, 'variation');
my $varDBC = $varDB->dbc();

my $coreDB = EGVar::egv_utils::get_DBAdaptor_from_Registry(
    $prog_args{species}, 'core');
my $coreDBC = $coreDB->dbc();

if ($varDBC->host() ne $coreDBC->host())
{
    die('var and core DBs must be on same host ' .
        '(var = \'' . $varDBC->host . '\', core = \'' . $coreDBC->host . '\')');
}


print "\n" . EGVar::egv_utils::get_started_str() . "\n\n";


my $sql = 
    'SELECT COUNT(*), MIN(variation_id), MAX(variation_id) FROM variation';
my $sth = $varDBC->prepare($sql);

$sth->execute();

my ($count, $min, $max) = $sth->fetchrow_array();

$sth->finish();

print "\n$count variations, min id = $min, max = $max\n";


if ($count > 0)
{
    my %value_by_tag = 
        ($prog_args{var_db_tag} => $varDBC->dbname(),
        $prog_args{core_db_tag} => $coreDBC->dbname(),
        $prog_args{species_abbrev_tag} => $prog_args{species_abbrev_value});

    my %template_sql_by_type = ();

    my @template_sql_lines;
    my $template_sql_line;
    my $modified_template_sql_line;

    my $template_sql_line_num = 0;
    my $pre_vfm_sql_line_num = -1;
    my $src_ids_by_name;

    my $distinct_poly = 0;
    my $distinct_mpoly = 0;
    my $variation_key = '';

    open TEMPLATE_SQL, $prog_args{template_sql_file};

    foreach $template_sql_line (<TEMPLATE_SQL>)
    {
        chomp $template_sql_line;

        if ($template_sql_line =~ /DISTINCT_POLY/ && !$distinct_poly)
        {
            $distinct_poly = 1;
        }

        if ($template_sql_line =~ /DISTINCT_MPOLY/ && !$distinct_mpoly)
        {
            $distinct_mpoly = 1;
        }

        if ($template_sql_line =~ /(variation_id_\d+_key)/ && 
            !$variation_key)
        {
            $variation_key = $1;
        }

        if ($prog_args{template_source_sql_file} && 
            $template_sql_line =~ /PRE_VFM/)
        {
            $pre_vfm_sql_line_num = $template_sql_line_num;
        }
        else
        {
            push @template_sql_lines, EGVar::egv_utils::tag_to_value(
                \%value_by_tag, $template_sql_line);
        }

        $template_sql_line_num++;
    }

    close TEMPLATE_SQL;

    $template_sql_by_type{var_all} = [@template_sql_lines];

    if ($pre_vfm_sql_line_num != -1)
    {
        $template_sql_by_type{var_pre} = 
            [@template_sql_lines[0 .. ($pre_vfm_sql_line_num - 1)]];
        $template_sql_by_type{var_post} = [@template_sql_lines[
            $pre_vfm_sql_line_num .. ($template_sql_line_num - 2)]];

        @template_sql_lines = ();

        open TEMPLATE_SQL, $prog_args{template_source_sql_file};

        foreach $template_sql_line (<TEMPLATE_SQL>)
        {
            chomp $template_sql_line;

            if ($template_sql_line =~ 
                /(index )($prog_args{src_idx_tag}\w*)( on)/)
            {
                $modified_template_sql_line = "$`$1$2_SRC_NAME$3$'";
            }
            else
            {
                $modified_template_sql_line = $template_sql_line;
            }

            push @template_sql_lines, EGVar::egv_utils::tag_to_value(
                \%value_by_tag, $modified_template_sql_line);
        }

        close TEMPLATE_SQL;

        $template_sql_by_type{var_syn} = [@template_sql_lines];

        $src_ids_by_name = get_src_ids_by_name($varDBC, 
            $prog_args{exclude_source_without_synonyms});
    }

    if ($prog_args{template_struct_var_sql_file})
    {
        @template_sql_lines = ();

        open TEMPLATE_SQL, $prog_args{template_struct_var_sql_file};

        foreach $template_sql_line (<TEMPLATE_SQL>)
        {
            chomp $template_sql_line;

            push @template_sql_lines, EGVar::egv_utils::tag_to_value(
                \%value_by_tag, $template_sql_line);
        }

        close TEMPLATE_SQL;

        $template_sql_by_type{struct_var} = [@template_sql_lines];
    }

    my $out_sql_file;
    my $separator;
    my $out_sql_files;
    my $min_var_id = $min;
    my $max_var_id = $min_var_id + $prog_args{rows_per_sub_mart} - 1;
    my $sub_mart_num = 1;
    my @sql_types;
    my $sql_type;
    my ($src_name, $prev_src_name);
    my $between_clause;
    my %ind_gen_counts;
    my $single_or_multiple;
    my $count_separator;
    my $max_ind_gens_to_process = 10000000;
    my $may_run_very_slowly;

    open PROCESS, ">$prog_args{process_file}";

    print PROCESS "SPECIES\t$prog_args{species}\n";
    print PROCESS "HOST\t" . $varDBC->host() . "\n";
    print PROCESS "VAR_DB\t" . $varDBC->dbname() . "\n";
    print PROCESS "COUNT\t$count\n";
    print PROCESS "MIN_VAR_ID\t$min\n";
    print PROCESS "MAX_VAR_ID\t$max\n";
    print PROCESS "CORE_DB\t" . $coreDBC->dbname() . "\n";
    print PROCESS "LOG_SQL\t" . $prog_args{log_sql} . "\n";

    if ($prog_args{limit})
    {
        print PROCESS "LIMIT\t" . $prog_args{limit} . "\n";
    }

    print PROCESS "VAR_MART_DB\t$prog_args{var_mart_db_value}\n";

    while ($min_var_id < $max)
    {
        if ($max_var_id > $max)
        {
            $max_var_id = $max;
        }

        $between_clause = "variation_id between $min_var_id and " .
            (!$prog_args{limit} ? 
                $max_var_id : ($min_var_id + $prog_args{limit} - 1));

        %value_by_tag =
            ($prog_args{var_mart_db_tag} => $prog_args{var_mart_db_value} . 
                ($count > $prog_args{rows_per_sub_mart} ? 
                    '_' . $sub_mart_num : ''),
            $prog_args{var_id_cond_tag} . '_a' => 
                ' a.' . $between_clause . ';',
            $prog_args{var_id_cond_tag} . '_b' => 
                ' b.' . $between_clause . ';');

        get_ind_gen_count($varDBC, $between_clause, 's', \%ind_gen_counts);
        get_ind_gen_count($varDBC, $between_clause, 'm', \%ind_gen_counts);

        @sql_types = ();

        if (scalar keys %$src_ids_by_name > 0)
        {
            @sql_types = ('var_pre', 'var_syn', 'var_post');
        }

        if (@sql_types)
        {
            $separator = '';
            $out_sql_files = '';

            foreach $sql_type (@sql_types)
            {
                $out_sql_file = $prog_args{out_sql_file_prefix};

                if ($count > $prog_args{rows_per_sub_mart})
                {
                    $out_sql_file .= "_$sub_mart_num";
                }

                $out_sql_file .= ".$sql_type";

                if ($sql_type eq 'var_syn')
                {
                    $prev_src_name = '';

                    foreach $src_name (keys %$src_ids_by_name)
                    {
                        if (!$prev_src_name)
                        {
                            $out_sql_file .= "-$src_name.sql";
                        }
                        else
                        {
                            $out_sql_file =~ s/$prev_src_name/$src_name/;
                        }

                        $prev_src_name = $src_name;

                        $value_by_tag{SRC_NAME} = $src_name;
                        $value_by_tag{SRC_ID} = $src_ids_by_name->{$src_name};

                        write_out_sql_file($out_sql_file, 
                            \%template_sql_by_type, $sql_type, \%value_by_tag,
                            undef, $prog_args{log_sql});

                        $out_sql_files .= $separator . $out_sql_file;
                        $separator = ',';
                    }
                }
                else
                {
                    my $ind_gen_var_key_between_clauses;

                    if ($sql_type eq 'var_pre')
                    {
                        if ($distinct_poly)
                        {
                            $ind_gen_var_key_between_clauses = 
                                get_ind_gen_var_key_between_clauses(
                                    $varDBC, \%value_by_tag, $ind_gen_counts{s},
                                    $max_ind_gens_to_process, $variation_key, 
                                    $min_var_id, $max_var_id, $between_clause);
                        }

                        if ($distinct_mpoly)
                        {
                            $ind_gen_var_key_between_clauses = 
                                get_ind_gen_var_key_between_clauses(
                                    $varDBC, \%value_by_tag, $ind_gen_counts{m},
                                    $max_ind_gens_to_process, $variation_key, 
                                    $min_var_id, $max_var_id, $between_clause);
                        }
                    }

                    $out_sql_file .= ".sql";

                    write_out_sql_file($out_sql_file, 
                        \%template_sql_by_type, $sql_type, \%value_by_tag,
                        $ind_gen_var_key_between_clauses, $prog_args{log_sql});

                    $out_sql_files .= $separator . $out_sql_file;
                    $separator = ',';
                }
            }
        }
        else
        {
            $out_sql_file = $prog_args{out_sql_file_prefix} . '_' . 
                $sub_mart_num . '.sql';

            write_out_sql_file($out_sql_file, \%template_sql_by_type, 
                'var_all', 
                \%value_by_tag, undef, $prog_args{log_sql});

            $out_sql_files = $out_sql_file;
        }

        print "\t$sub_mart_num: $min_var_id - $max_var_id -> $out_sql_files\n";
        print "\t\tind gens: ";

        $count_separator = '';
        $may_run_very_slowly = 0;

        foreach $single_or_multiple (sort keys %ind_gen_counts)
        {
            print "$count_separator$single_or_multiple=" .
                "$ind_gen_counts{$single_or_multiple}";

            if ($ind_gen_counts{$single_or_multiple} > 
                    $max_ind_gens_to_process && 
                $may_run_very_slowly == 0)
            {
                $may_run_very_slowly = 1;
            }

            $count_separator = ', ';
        }

        if ($may_run_very_slowly)
        {
            print ' (may run very slowly)';
        }

        print "\n";

        if ($sub_mart_num == 1 && $prog_args{template_struct_var_sql_file})
        {
            $out_sql_file = $prog_args{out_sql_file_prefix} . '_' . 
                $sub_mart_num . '.struct_var.sql';

            write_out_sql_file($out_sql_file, \%template_sql_by_type, 
                'struct_var', 
                \%value_by_tag, undef, $prog_args{log_sql});

            $out_sql_files .= $separator . $out_sql_file;
        }

        print PROCESS "SUB_VAR_MART_$sub_mart_num\t$out_sql_files\t" .
            "$value_by_tag{$prog_args{var_mart_db_tag}}\n";

        $min_var_id += $prog_args{rows_per_sub_mart};
        $max_var_id += $prog_args{rows_per_sub_mart};

        $sub_mart_num++;
    }

    close PROCESS;

    my $trans_var_view = 'MTMP_transcript_variation';
    if (!EGVar::egv_utils::table_exists($varDBC, $trans_var_view))
    {
        print "\nView $trans_var_view must exist in var db " .
            "before mart can be built\n";
    }

    my $source_ind_gen_table = 'tmp_individual_genotype_single_bp';
    my $source_ind_gen_table_exists = 
        EGVar::egv_utils::table_exists($varDBC, $source_ind_gen_table);

    if ($prog_args{populate_genotype_table} eq 'y') # ???
    {
        my $genotype_table = 'strain_gtype_poly';

        if (EGVar::egv_utils::table_exists($varDBC, $genotype_table))
        {
            print "\nGenotypes table '$genotype_table' already exists, " .
                "so not populating it!\n\n";
        }
        elsif (!$source_ind_gen_table_exists)
        {
            print "\nCan't populate genotype table, " .
                "as source table '$source_ind_gen_table' doesn't exist\n\n";
        }
        else
        {
            populate_genotype_table($varDBC, $genotype_table, 
                $prog_args{species}, $prog_args{work_dir}, 
                $prog_args{ref_strain}, 
                "$prog_args{out_sql_file_prefix}.genotype_table.sql",
                $prog_args{bsub_queue_name}, $prog_args{bsub_wait_status});
        }
    }
    elsif (!$source_ind_gen_table_exists)
    {
        print "\nTable '$source_ind_gen_table' " .
            "must be created before var mart can be built!\n\n";
    }
}
else
{
    print "No variations so nothing to do!\n";
}


print "\n\n" . EGVar::egv_utils::get_ended_str() . "\n\n";


exit;

