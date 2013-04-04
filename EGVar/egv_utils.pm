#
# EnsemblGenomes Variation Utilities
#
# pderwent, 24/7/09
#

use strict;
use warnings;

package EGVar::egv_utils;

use Getopt::Std; # ???
use Getopt::Long;

use Bio::EnsEMBL::Utils::Exception qw(warning throw);
use Bio::EnsEMBL::Variation::Utils::Sequence qw(ambiguity_code unambiguity_code);

use DBI;


sub load 
{
    my $db = shift;
    my $table_file = shift;
    my $table_name = shift;
    my $col_names = shift;

    my $sql = qq{LOAD DATA LOCAL INFILE "$table_file" INTO TABLE $table_name};
    debug('sql', $sql);

    if ($col_names)
    {
        $sql .= "($col_names)";
    }

    $db->do($sql);
}


# debugging

sub debug_env
{
    if ($ENV{'EGV_DEBUG'})
    {
        print 'EGV_DEBUG = \'' . $ENV{'EGV_DEBUG'} . "'\n";
    }
}

sub do_debug
{
    my $type = shift;

    my $env_debug = $ENV{'EGV_DEBUG'};
    my $do_debug = ($env_debug && ($env_debug eq 'ALL' || $env_debug =~ $type));

    return($do_debug);
}

sub debug
{
    my $type = shift;
    my $message = shift;

    my $do_debug = do_debug($type);

    if ($do_debug)
    {
        print 'DEBUG: ' . $type . ' - ' . $message . "\n";
    }

    return($do_debug);
}

sub debug_perl_ver
{
    debug('perl_ver', sprintf("%vd", $^V));
}

sub debug_VariationFeature # ???
{
    if (do_debug('api_object'))
    {
        my $var_feat = shift;

        my $debug_str = 
            "\n" . 'VariationFeature ->' . "\n" .
            "\t" . 'slice = \'' . $var_feat->slice()->name() . "'\n" .
            "\t" . 'strand = ' . $var_feat->strand() . "\n" .
            "\t" . 'start = ' . $var_feat->start() . "\n" .
            "\t" . 'end = ' . $var_feat->end() . "\n" .
            "\t" . 'variation_name = \'' . $var_feat->variation_name() . "'\n" .
            "\t" . 'map_weight = ' . $var_feat->map_weight() . "\n" .
            "\t" . 'allele_string = \'' .
                ($var_feat->allele_string() ? $var_feat->allele_string() : '') . 
                "'\n";

        $debug_str .= "\t" . 'Variation ->' . "\n" .
            "\t\t" . 'name = \'' . $var_feat->variation()->name() . "'\n" .
            "\t\t" . 'source = \'' . $var_feat->variation()->source() . "'\n" .
            "\t\t" . 'ancestral_allele = \'' .
                ($var_feat->variation()->ancestral_allele() ? 
                    $var_feat->variation()->ancestral_allele() : '') . "'\n" .
            "\t\t" . '5\' = \'' .
                ($var_feat->variation()->five_prime_flanking_seq() ?
                    $var_feat->variation()->five_prime_flanking_seq() : '') 
                . "'\n" .
            "\t\t" . '3\' = \'' .
                ($var_feat->variation()->three_prime_flanking_seq() ?
                    $var_feat->variation()->three_prime_flanking_seq() : '')
                . "'\n";

        foreach my $allele (@{$var_feat->variation()->get_all_Alleles()})
        {
            $debug_str .= "\t\t" . 'Allele ->' . "\n" .
                "\t\t\t" . 'allele = \'' . $allele->allele() . "'\n";

            if ($allele->frequency())
            {
                $debug_str .= "\t\t\t" . 'frequency = ' . 
                    $allele->frequency() . "\n";
            }
        }

        debug('api_object', $debug_str);
    }
}

sub debug_PopulationGenotype # NOT USED
{
    if (do_debug('api_object'))
    {
        my $pop_gen = shift;

        my $debug_str = "\n" . 'PopulationGenotype ->' . "\n" .
            "\t" . 'allele1 = \'' . $pop_gen->allele1() . "'\n" .
            "\t" . 'allele2 = \'' . $pop_gen->allele2() . "'\n";

        if ($pop_gen->frequency())
        {
            $debug_str .= "\t" . 'frequency = ' . $pop_gen->frequency() . "\n";
        }

        $debug_str .= "\t" . 'Population ->' . "\n" .
            "\t\t" . 'name = \'' .
                $pop_gen->population()->name() . "'\n" .
            "\t" . 'Variation ->' . "\n" .
            "\t\t" . 'name = \'' . $pop_gen->variation()->name() . "'\n" .
            "\t\t" . 'source = \'' . $pop_gen->variation()->source() . "'\n" .
            "\t\t" . 'ancestral_allele = \'' .
                ($pop_gen->variation()->ancestral_allele() ?
                    $pop_gen->variation()->ancestral_allele() : '') . "'\n";

        debug('api_object', $debug_str);
    }
}

sub debug_IndividualGenotype # NOT USED
{
    if (do_debug('api_object'))
    {
        my $ind_gen = shift;

        debug('api_object', "\n" . 'IndividualGenotype ->' . "\n" .
            "\t" . 'allele1 = \'' . $ind_gen->allele1() . "'\n" .
            "\t" . 'allele2 = \'' . $ind_gen->allele2() . "'\n" .
            "\t" . 'Individual ->' . "\n" .
            "\t\t" . 'name = \'' .
                $ind_gen->individual()->name() . "'\n" .
            "\t" . 'Variation ->' . "\n" .
            "\t\t" . 'name = \'' . $ind_gen->variation()->name() . "'\n" .
            "\t\t" . 'source = \'' . $ind_gen->variation()->source() . "'\n" .
            "\t\t" . 'ancestral_allele = \'' . 
                ($ind_gen->variation()->ancestral_allele() ?
                    $ind_gen->variation()->ancestral_allele() : '') . "'\n");
    }
}

sub debug_variation_object # ???
{
    my $var_obj = shift;

    if (do_debug('api_object'))
    {
        if ($var_obj.isa('EnsEMBL::Variation::VariationFeature'))
        {
            debug_VariationFeature($var_obj);
        }
        elsif ($var_obj.isa('EnsEMBL::Variation::IndividualGenotype'))
        {
            debug_IndividualGenotype($var_obj);
        }
        elsif ($var_obj.isa('EnsEMBL::Variation::PopulationGenotype'))
        {
            debug_PopulationGenotype($var_obj);
        }
    }
}


# date / time

sub get_date_and_time_str
{
    my @local_time = localtime();

    my $date_and_time_str =
        $local_time[3] . '/' . ($local_time[4] + 1) . '/' . ($local_time[5] + 1900);

    $date_and_time_str .= ' ' . sprintf("%02d:%02d:%02d",
        $local_time[2], $local_time[1], $local_time[0]);

    return($date_and_time_str);
}

sub get_datetime_str # NOT USED
{
    my @local_time = localtime();

    my $datetime_str = sprintf("%4d-%02d-%02d %02d:%02d:%02d",
        ($local_time[5] + 1900), ($local_time[4] + 1), $local_time[3],
        $local_time[2], $local_time[1], $local_time[0]);

    return($datetime_str);
}


# start and end

sub get_started_str
{
    my $started_str = 'Started ' . get_date_and_time_str();

    return($started_str);
}

sub get_ended_str
{
    my $ended_str = 'Ended ' . get_date_and_time_str();

    return($ended_str);
}


# float

sub round_float
{
    my $float = shift;
    my $num_dec_place = shift;

    if (!$num_dec_place)
    {
        $num_dec_place = 5;
    }

    my $rounded_float = sprintf('%.' . $num_dec_place . 'f', $float);

    return($rounded_float);
}


# files

sub _throw_or_print_on_error
{
    my $on_error = shift;
    my $error_msg = shift;

    if ($on_error eq 't')
    {
        throw($error_msg);
    }
    else
    {
        print "$error_msg\n";
    }
}

sub is_readable_file
{
    my $file_name = shift;
    my $on_error = shift;

    my $is_readable = 1;

    if (!(-f $file_name && -r $file_name && -s $file_name))
    {
        $is_readable = 0;

        if ($on_error)
        {
            _throw_or_print_on_error($on_error, 
                "File '$file_name' doesn't exist, isn't readable or is empty");
        }
    }

    return($is_readable);
}

sub is_writable_dir
{
    my $dir_name = shift;
    my $on_error = shift;

    my $is_writable = 1;

    if (!(-d $dir_name && -w $dir_name))
    {
        $is_writable = 0;

        if ($on_error)
        {
            _throw_or_print_on_error($on_error, 
                "Directory '$dir_name' doesn't exist, or isn't writable");
        }
    }

    return($is_writable);
}

sub _get_file_id # ???
{
    my $host_name = `hostname`;
    chomp $host_name;

    my $file_id = $host_name . '.' . $$;
}


# nucleotide

sub is_ambiguous # NOT USED
{
    my $nucleotide = shift;

    return($nucleotide !~ /[ACGT]/);
}


# sequence

sub get_sequence_start_and_end
{
    my $pos = shift;
    my $slice = shift;
    my $five_or_three = shift;
    my $sequence_len = shift;
    my $start = shift;
    my $end = shift;

    if ($five_or_three == 5)
    {
        $$start = $pos - $sequence_len;
        $$end = $pos - 1;
    }
    else
    {
        $$start = $pos + 1;
        $$end = $pos + $sequence_len;
    }

    if ($$start < $slice->start())
    {
        $$start = $slice->start();
    }

    if ($$end > $slice->end())
    {
        $$end = $slice->end();
    }

    if ($$start == $pos)
    {
        $$start = 0;
    }

    if ($$end == $pos)
    {
        $$end = 0;
    }
}

sub get_chr_seq
{
    my $slice_adaptor = shift;
    my $chr = shift;
    my $start = shift;
    my $end = shift;

    my $locus_slice = $slice_adaptor->fetch_by_region( # ???
        'Chromosome', $chr, $start, $end);
    my $seq = $locus_slice->seq();

    return($seq);
}

sub get_flanking_sequence_by_seq_region # NOT USED
{
    my $slice_adaptor = shift;
    my $seq_region_id = shift;
    my $start = shift;
    my $end = shift;

    my $flanking_slice = $slice_adaptor->fetch_by_seq_region_id(
        $seq_region_id, $start, $end);

    return($flanking_slice->seq());
}

sub get_flanking_sequence_by_chromosome # NOT USED
{
    my $slice_adaptor = shift;
    my $chr = shift;
    my $start = shift;
    my $end = shift;

    my $flanking_slice = $slice_adaptor->fetch_by_region(
        'Chromosome', $chr, $start, $end);

    return($flanking_slice->seq());
}

sub get_flanking_sequence # For backwards compatability
{
    my $slice_adaptor = shift;
    my $chr = shift;
    my $start = shift;
    my $end = shift;

    my $flanking_slice = 
        get_flanking_sequence_by_chromosome($slice_adaptor, $chr, $start, $end);

    return($flanking_slice->seq());
}


# Registry

sub get_DBAdaptor_from_Registry
{
    my $species = shift;
    my $group = shift;

    my $db = Bio::EnsEMBL::Registry->get_DBAdaptor($species, $group);

    if (! $db)
    {
        throw('Failed to get DBAdaptor for \'' . 
            $species . '\' and \'' . $group . '\' from Registry');
    }

    return($db);
}


# API

sub get_Individual_for_strain # NOT USED
{
    my $dbVariation = shift;
    my $strain_name = shift;

    my $ind_arr = $dbVariation->get_IndividualAdaptor->
        fetch_all_by_name($strain_name);
    my $ind = $ind_arr->[0]; # should only be 1

    if (! $ind)
    {
        $ind = Bio::EnsEMBL::Variation::Individual->new(
            -name => $strain_name);

        store_variation_object_to_db($ind, $dbVariation);
    }

    return($ind);
}

sub get_Population
{
    my $dbVariation = shift;
    my $population_name = shift;

    my $pop = $dbVariation->get_PopulationAdaptor->
        fetch_by_name($population_name);

    if (! $pop)
    {
        $pop = Bio::EnsEMBL::Variation::Population->new(
            -name => $population_name);
    }

    return($pop);
}

sub set_flanking_seqs_on_Variation # NOT USED
{
    my $var = shift;
    my $chr = shift;
    my $pos = shift;
    my $flanking_seq_len = shift;
    my $slice_adaptor = shift;

    my ($start, $end);

    if ($flanking_seq_len > 0)
    {
        get_sequence_start_and_end($pos, 5, $flanking_seq_len,
            \$start, \$end);

        $var->five_prime_flanking_seq(
            get_flanking_sequence(
                $slice_adaptor, $chr, $start, $end));

        get_sequence_start_and_end($pos, 3, $flanking_seq_len,
            \$start, \$end);

        $var->three_prime_flanking_seq(
            get_flanking_sequence(
                $slice_adaptor, $chr, $start, $end));
    }
    else
    {
        $var->five_prime_flanking_seq('');
        $var->three_prime_flanking_seq('');
    }
}

sub get_indel_type
{
    my $start = shift;
    my $end = shift;

    my $indel_type;

    if ($end == ($start - 1))
    {
        $indel_type = 'd';
    }
    else
    {
        $indel_type = 'i';
    }

    return($indel_type);
}

sub get_allele_string # NOT USED
{
    my $type = shift;
    my $ref = shift;

    my $allele_string;

    my %unique_alleles = ();

    foreach my $allele (@_)
    {
        $unique_alleles{$allele} = $allele;
    }

    $allele_string = join('/', sort keys %unique_alleles);

    if ($type eq 's' || $type eq 'd')
    {
        $allele_string = $ref . '/' . $allele_string;
    }
    elsif ($type eq 'i')
    {
        $allele_string .= '/' . $ref;
    }

    return($allele_string);
}

sub set_allele_string
{
    my $type = shift;
    my $allele_string = shift;
    my $ref = shift;

    my %unique_alleles = ();

    my $allele;

    foreach $allele (@_)
    {
        if ($allele ne $ref)
        {
            $unique_alleles{$allele} = $allele;
        }
    }

    my $allele_separator = '/';

    # don't 'lose' non-ref alleles that are already in allele_string
    if ($$allele_string)
    {
        my @old_alleles = split($allele_separator, $$allele_string);

        foreach $allele (@old_alleles)
        {
            if ($allele ne $ref)
            {
                $unique_alleles{$allele} = $allele;
            }
        }
    }

    $$allele_string = join($allele_separator, sort keys %unique_alleles);

    if ($type eq 's' || $type eq 'd' || $type eq 't')
    {
        $$allele_string = $ref . $allele_separator . $$allele_string;
    }
    elsif ($type eq 'i')
    {
        $$allele_string .= $allele_separator . $ref;
    }
}

sub get_ordered_genotype
{
    my $type = shift;
    my $genotype = shift;

    $genotype = uc($genotype);
    my $genotype_len = length($genotype);

    my $ordered_genotype;

    if ($type eq 's')
    {
        if ($genotype_len == 1)
        {
            $ordered_genotype = $genotype x 2;
        }
        elsif ($genotype_len == 2)
        {
            my $allele_1 = substr($genotype, 0, 1);
            my $allele_2 = substr($genotype, 1, 1);

            if ($allele_1 le $allele_2)
            {
                $ordered_genotype = $allele_1 . $allele_2;
            }
            else
            {
                $ordered_genotype = $allele_2 . $allele_1;
            }
        }
        else
        {
            throw('Genotype \'' . $genotype . '\' must have 1 or 2 alleles');
        }
    }
    else
    {
        $ordered_genotype = $genotype;
    }

    return($ordered_genotype);
}

sub fetch_VariationFeature_by_name
{
    my $variation_name = shift;
    my $chr_slice = shift;
    my $pos = shift;
    my $v_adaptor = shift;
    my $vf_adaptor = shift;
    
    my $var = $v_adaptor->fetch_by_name($variation_name);
    my $var_feats;
    my $var_feat;

    my $matched = 0;

    if ($var)
    {
        $var_feats = $vf_adaptor->fetch_all_by_Variation($var);

        do
        {
            $var_feat = shift @{$var_feats};

            if ($var_feat &&
                $var_feat->slice->dbID() == $chr_slice->dbID() &&
                $var_feat->seq_region_start() == $pos &&
                $var_feat->seq_region_end() == $pos)
            {
                $matched = 1;
            }
        } while(!$matched && $var_feat);

        if (!$matched)
        {
            $var_feat = undef;
        }
    }

    return($var_feat);
}

sub fetch_VariationFeature_by_variation_id
{
    my $variation_id = shift;
    my $v_adaptor = shift;
    my $vf_adaptor = shift;
    
    my $var = $v_adaptor->fetch_by_dbID($variation_id);
    my $var_feats;
    my $var_feat;

    if ($var)
    {
        $var_feats = $vf_adaptor->fetch_all_by_Variation($var);

        if (scalar @{$var_feats} > 1)
        {
            warning('More than 1 VariationFeature for variation_id = ' . 
                $variation_id);
        }

        $var_feat = shift @{$var_feats}; # Take first, hopefully only
    }

    return($var_feat);
}

sub select_variation_by_id
{
    my $db_args = shift;
    my $var_id = shift;

    if (!$db_args->{'var_sth'})
    {
        my $var_sql = 'SELECT v.variation_id, v.name, v.source_id, ' .
            'v.ancestral_allele, vf.variation_name, vf.map_weight, ' . 
            'vf.seq_region_start, vf.seq_region_end, vf.seq_region_strand, ' .
            'vf.allele_string, vf.seq_region_id ' .
            'FROM variation v ' .
            'JOIN variation_feature vf ON (v.variation_id = vf.variation_id) ' .
            'WHERE v.variation_id = ?';

        $db_args->{'var_sth'} = $db_args->{'dbc'}->prepare($var_sql);
    }

    $db_args->{'var_sth'}->bind_param(1, $var_id);

    $db_args->{'var_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $db_args->{'var_sth'}, 'select variation', 2);

    my $var_hash;

    if (my $var_values = $db_args->{'var_sth'}->fetchrow_arrayref())
    {
        $var_hash = {};

        $var_hash->{'v.variation_id'} = $var_values->[0];
        $var_hash->{'v.name'} = $var_values->[1];
        $var_hash->{'v.source_id'} = $var_values->[2];
        $var_hash->{'v.ancestral_allele'} = $var_values->[3];
        $var_hash->{'vf.variation_name'} = $var_values->[4];
        $var_hash->{'vf.map_weight'} = $var_values->[5];
        $var_hash->{'vf.seq_region_start'} = $var_values->[6];
        $var_hash->{'vf.seq_region_end'} = $var_values->[7];
        $var_hash->{'vf.seq_region_strand'} = $var_values->[8];
        $var_hash->{'vf.allele_string'} = $var_values->[9];
        $var_hash->{'vf.seq_region_id'} = $var_values->[10];
    }
    else
    {
        $var_hash = undef;
    }

    return($var_hash);
}

sub select_variation_set_by_id
{
    my $db_args = shift;
    my $var_set_id = shift;

    if (!$db_args->{'var_set_sth'})
    {
        my $var_set_sql = 'SELECT variation_set_id, name, description ' .
            'FROM variation_set ' .
            'WHERE variation_set_id = ?';

        $db_args->{'var_set_sth'} = $db_args->{'dbc'}->prepare($var_set_sql);
    }

    $db_args->{'var_set_sth'}->bind_param(1, $var_set_id);

    $db_args->{'var_set_sth'}->execute();

    my $var_set_hash;

    if (my $var_set_values = $db_args->{'var_set_sth'}->fetchrow_arrayref())
    {
        $var_set_hash = {};

        $var_set_hash->{'variation_set_id'} = $var_set_values->[0];
        $var_set_hash->{'name'} = $var_set_values->[1];
        $var_set_hash->{'description'} = $var_set_values->[2];
    }
    else
    {
        $var_set_hash = undef;
    }

    return($var_set_hash);
}

sub fetch_var_set_id_for_name
{
    my $dbc = shift;
    my $name = shift;

    my $sql = "SELECT variation_set_id FROM variation_set WHERE name = '$name'";
    my $sth = $dbc->prepare($sql);

    $sth->execute();

    my $variation_set_id;

    my @var_set_values = $sth->fetchrow_array();

    $sth->finish();

    if (@var_set_values)
    {
        $variation_set_id = $var_set_values[0];
    }

    return($variation_set_id);
}

sub select_alleles_for_variation
{
    my $db_args = shift;
    my $var = shift;

    my $num_alleles = 0;

    if (!$db_args->{'all_sth'})
    {
        my $all_sql = 'SELECT allele, frequency, sample_id, count ' .
            'FROM allele ' .
            'WHERE variation_id = ?';

        $db_args->{'all_sth'} = $db_args->{'dbc'}->prepare($all_sql);
    }

    $db_args->{'all_sth'}->bind_param(1, $var->{'v.variation_id'});

    $db_args->{'all_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw($db_args->{all_sth}, 
            'select allele', 2);

    while (my $all_values = $db_args->{'all_sth'}->fetchrow_arrayref())
    {
        $num_alleles++;

        my $all_hash = {};

        $all_hash->{'allele'} = $all_values->[0];
        $all_hash->{'frequency'} = $all_values->[1];
        $all_hash->{'pop_id'} = $all_values->[2];
        $all_hash->{'count'} = $all_values->[3];

        push(@{$var->{'alleles'}}, $all_hash);
    }

    return($num_alleles);
}

sub select_population_genotypes_for_variation
{
    my $db_args = shift;
    my $var = shift;

    my $num_population_genotypes = 0;

    if (!$db_args->{'pop_gen_sth'})
    {
        my $pop_gen_sql = 'SELECT allele_1, allele_2, frequency, sample_id, ' .
            'count ' .
            'FROM population_genotype ' .
            'WHERE variation_id = ?';

        $db_args->{'pop_gen_sth'} = $db_args->{'dbc'}->prepare($pop_gen_sql);
    }

    $db_args->{'pop_gen_sth'}->bind_param(1, $var->{'v.variation_id'});

    $db_args->{'pop_gen_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw($db_args->{pop_gen_sth}, 
            'select pop-gen', 2);

    while (my $pop_gen_values = $db_args->{'pop_gen_sth'}->fetchrow_arrayref())
    {
        $num_population_genotypes++;

        my $pop_gen_hash = {};

        $pop_gen_hash->{'allele_1'} = $pop_gen_values->[0];
        $pop_gen_hash->{'allele_2'} = $pop_gen_values->[1];
        $pop_gen_hash->{'frequency'} = $pop_gen_values->[2];
        $pop_gen_hash->{'pop_id'} = $pop_gen_values->[3];
        $pop_gen_hash->{'count'} = $pop_gen_values->[4];

        push(@{$var->{'population_genotypes'}}, $pop_gen_hash);
    }

    return($num_population_genotypes);
}

sub select_individual_genotypes_for_variation
{
    my $db_args = shift;
    my $var = shift;

    my $num_individual_genotypes = 0;

    if (!$db_args->{'ind_gen_sth'})
    {
        my $ind_gen_sql = 'SELECT allele_1, allele_2, sample_id FROM ' .
            ($db_args->{'type'} eq 's' ?
                'tmp_individual_genotype_single_bp' :
                'individual_genotype_multiple_bp') .
            ' WHERE variation_id = ?';

        $db_args->{'ind_gen_sth'} = $db_args->{'dbc'}->prepare($ind_gen_sql);
    }

    $db_args->{'ind_gen_sth'}->bind_param(1, $var->{'v.variation_id'});

    $db_args->{'ind_gen_sth'}->execute();

    while (my $ind_gen_values = $db_args->{'ind_gen_sth'}->fetchrow_arrayref())
    {
        $num_individual_genotypes++;

        my $ind_gen_hash = {};

        $ind_gen_hash->{'allele_1'} = $ind_gen_values->[0];
        $ind_gen_hash->{'allele_2'} = $ind_gen_values->[1];
        $ind_gen_hash->{'ind_id'} = $ind_gen_values->[2];

        push(@{$var->{'individual_genotypes'}}, $ind_gen_hash);
    }

    return($num_individual_genotypes);
}

sub select_read_individuals_for_variation # NOT USED
{
    my $db_args = shift;
    my $var = shift;

    my $num_individuals_read = 0;

    if (!$db_args->{'read_ind_sth'})
    {
        my $read_ind_sql = 'SELECT sample_id FROM read_coverage' .
            ' WHERE seq_region_id = ?' .
            ' AND seq_region_start <= ? AND seq_region_end >= ?';

        $db_args->{'read_ind_sth'} = $db_args->{'dbc'}->prepare($read_ind_sql);
    }

    $db_args->{'read_ind_sth'}->bind_param(1, $var->{'vf.seq_region_id'});
    $db_args->{'read_ind_sth'}->bind_param(2, $var->{'vf.seq_region_start'});
    $db_args->{'read_ind_sth'}->bind_param(3, $var->{'vf.seq_region_start'});

    $db_args->{'read_ind_sth'}->execute();

    while (my $ind_values = $db_args->{'read_ind_sth'}->fetchrow_arrayref())
    {
        $num_individuals_read++;

        push(@{$var->{'read_individuals'}}, $ind_values->[0]);
    }

    return($num_individuals_read);
}

sub select_variation_annotation
{
    my $db_args = shift;
    my $var_id = shift;
    my $pheno_id = shift;
    my $source_id = shift;
    my $study = shift;
    my $associated_gene = shift;

    if (!$db_args->{'var_anno_sth'})
    {
        my $var_anno_sql = 'SELECT va.variation_id, va.phenotype_id, ' .
            'va.source_id, va.study, va.study_type, va.associated_gene, ' .
            'va.p_value ' .
            'FROM variation_annotation va ' .
            'WHERE va.variation_id = ? ' .
            'AND va.phenotype_id = ? ' .
            'AND va.source_id = ? ' .
            'AND va.study = ? ' .
            'AND va.associated_gene = ?';

        $db_args->{'var_anno_sth'} = $db_args->{'dbc'}->prepare($var_anno_sql);
    }

    $db_args->{'var_anno_sth'}->bind_param(1, $var_id);
    $db_args->{'var_anno_sth'}->bind_param(2, $pheno_id);
    $db_args->{'var_anno_sth'}->bind_param(3, $source_id);
    $db_args->{'var_anno_sth'}->bind_param(4, $study);
    $db_args->{'var_anno_sth'}->bind_param(5, $associated_gene);

    $db_args->{'var_anno_sth'}->execute();

    my $var_anno_hash;

    if (my $var_anno_values = $db_args->{'var_anno_sth'}->fetchrow_arrayref())
    {
        $var_anno_hash = {};

        $var_anno_hash->{'variation_id'} = $var_anno_values->[0];
        $var_anno_hash->{'phenotype_id'} = $var_anno_values->[1];
        $var_anno_hash->{'source_id'} = $var_anno_values->[2];
        $var_anno_hash->{'study'} = $var_anno_values->[3];
        $var_anno_hash->{'study_type'} = $var_anno_values->[4];
        $var_anno_hash->{'associated_gene'} = $var_anno_values->[5];
        $var_anno_hash->{'p_value'} = $var_anno_values->[6];
    }
    else
    {
        $var_anno_hash = undef;
    }

    return($var_anno_hash);
}

sub fetch_VariationFeatures_by_chr_pos # SNPs NOT USED
{
    my $slice_adaptor = shift;
    my $vf_adaptor = shift;
    my $chr = shift;
    my $pos = shift;
    
    my $snp_slice = $slice_adaptor->fetch_by_region(
        'Chromosome', $chr, $pos, $pos);

    my @chr_var_feats;
    my $chr_var_feat;

    foreach my $var_feat (@{$vf_adaptor->fetch_all_by_Slice($snp_slice)})
    {
        $chr_var_feat = $var_feat->transform('Chromosome');

        debug_VariationFeature($chr_var_feat);

        push @chr_var_feats, $chr_var_feat;
    }

    return(\@chr_var_feats);
}

sub fetch_VariationFeature_by_chr_pos_and_source # SNPs NOT USED
{
    my $slice_adaptor = shift;
    my $vf_adaptor = shift;
    my $chr = shift;
    my $pos = shift;
    my $source_name = shift;

    my @var_feats = @{fetch_VariationFeatures_by_chr_pos(
        $slice_adaptor, $vf_adaptor, $chr, $pos)};

    my $var_feat_for_source = undef;

    foreach my $var_feat (@var_feats)
    {
        if ($var_feat->source() eq $source_name &&
            $var_feat->variation->ancestral_allele() &&
            $var_feat->variation->ancestral_allele() ne '')
        {
            $var_feat_for_source = $var_feat;
        }
    }

    return($var_feat_for_source);
}

sub _select_var_by_chr_start_end_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $start = shift;
    my $end = shift;
    my $source_id = shift; # ???
    my $var_type = shift;

    my $var_class = ($var_type eq 'subst' ? 't' : substr($var_type, 0, 1));

    my $with_flank_seq = 
        ($db_args->{'with_flank_seq'} && $db_args->{'with_flank_seq'} eq 'y');

    my $sth_name = $var_type . '_sth';
    my $var_hash;

    if (!$db_args->{$sth_name})
    {
        my $var_sql = 'SELECT v.variation_id, v.name, v.source_id, v.class, ' .
            'vf.variation_feature_id, vf.variation_name, ' .
            'vf.map_weight, vf.seq_region_start, vf.seq_region_end, ' .
            'vf.seq_region_strand, vf.allele_string ';

        if ($with_flank_seq)
        {
            $var_sql .= ', fs.seq_region_id, fs.seq_region_strand, ' .
                'fs.up_seq, fs.up_seq_region_start, fs.up_seq_region_end, ' .
                'fs.down_seq, fs.down_seq_region_start, fs.down_seq_region_end';
        }

        $var_sql .= ' FROM variation v ' .
            'JOIN variation_feature vf ON (v.variation_id = vf.variation_id)';

        if ($with_flank_seq)
        {
            $var_sql .= ' JOIN flanking_sequence fs' .
                ' ON (v.variation_id = fs.variation_id)';
        }

        $var_sql .=
            ' WHERE vf.seq_region_id = ? AND vf.seq_region_start = ? AND ' .
            'vf.seq_region_end = ? AND v.source_id = ? AND ' .
            'v.class  = ?';

        $var_sql .= ' ORDER BY v.variation_id'; # Use var with lowest ID ???

        $db_args->{$sth_name} = $db_args->{'dbc'}->prepare($var_sql);
    }

    $db_args->{$sth_name}->bind_param(1, $chr_seq_region_id);
    $db_args->{$sth_name}->bind_param(2, $start);
    $db_args->{$sth_name}->bind_param(3, $end);
    $db_args->{$sth_name}->bind_param(4, $source_id); # ???
    $db_args->{$sth_name}->bind_param(5, $var_class); # ???

    $db_args->{$sth_name}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $db_args->{$sth_name}, 'select ' . $var_type, 2);

    if (my $var_values = $db_args->{$sth_name}->fetchrow_arrayref())
    {
        $var_hash = {};

        $var_hash->{'v.variation_id'} = $var_values->[0];
        $var_hash->{'v.name'} = $var_values->[1];
        $var_hash->{'v.source_id'} = $var_values->[2];
        $var_hash->{'v.class'} = $var_values->[3];
        $var_hash->{'vf.variation_feature_id'} = $var_values->[4];
        $var_hash->{'vf.variation_name'} = $var_values->[5];
        $var_hash->{'vf.map_weight'} = $var_values->[6];
        $var_hash->{'vf.seq_region_start'} = $var_values->[7];
        $var_hash->{'vf.seq_region_end'} = $var_values->[8];
        $var_hash->{'vf.seq_region_strand'} = $var_values->[9];
        $var_hash->{'vf.allele_string'} = $var_values->[10];

        if ($with_flank_seq)
        {
            $var_hash->{'fs.seq_region_id'} = $var_values->[11];
            $var_hash->{'fs.seq_region_strand'} = $var_values->[12];
            $var_hash->{'fs.up_seq'} = $var_values->[13];
            $var_hash->{'fs.up_seq_region_start'} = $var_values->[14];
            $var_hash->{'fs.up_seq_region_end'} = $var_values->[15];
            $var_hash->{'fs.down_seq'} = $var_values->[16];
            $var_hash->{'fs.down_seq_region_start'} = $var_values->[17];
            $var_hash->{'fs.down_seq_region_end'} = $var_values->[18];
        }
    }
    else
    {
        $var_hash = undef;
    }

    return($var_hash);
}

sub select_snp_by_chr_pos_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $pos = shift;
    my $source_id = shift;

    my $snp_hash = _select_var_by_chr_start_end_and_source(
        $db_args, $chr_seq_region_id, $pos, $pos, $source_id, 'snp');

    return($snp_hash);
}

sub OLD_select_snp_by_chr_pos_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $pos = shift;
    my $source_id = shift;

    my $with_flank_seq = 
        ($db_args->{'with_flank_seq'} && $db_args->{'with_flank_seq'} eq 'y');

    my $snp_hash;

    if (!$db_args->{'snp_sth'})
    {
        my $snp_sql = 'SELECT v.variation_id, v.name, v.source_id, ' .
            'v.ancestral_allele, vf.variation_feature_id, vf.variation_name, ' .
            'vf.map_weight, vf.seq_region_start, vf.seq_region_end, ' .
            'vf.seq_region_strand, vf.allele_string ';

        if ($with_flank_seq)
        {
            $snp_sql .= ', fs.seq_region_id, fs.seq_region_strand, ' .
                'fs.up_seq, fs.up_seq_region_start, fs.up_seq_region_end, ' .
                'fs.down_seq, fs.down_seq_region_start, fs.down_seq_region_end';
        }

        $snp_sql .= ' FROM variation v ' .
            'JOIN variation_feature vf ON (v.variation_id = vf.variation_id)';

        if ($with_flank_seq)
        {
            $snp_sql .= ' JOIN flanking_sequence fs' .
                ' ON (v.variation_id = fs.variation_id)';
        }

        $snp_sql .=
            ' WHERE vf.seq_region_id = ? AND vf.seq_region_start = ? AND ' .
            'vf.seq_region_end = ? AND v.source_id = ? AND ' .
            'vf.allele_string <> \'\'';
            # ??? 'v.ancestral_allele IS NOT NULL';

        $snp_sql .= ' ORDER BY v.variation_id'; # Use var with lowest ID

        $db_args->{'snp_sth'} = $db_args->{'dbc'}->prepare($snp_sql);
    }

    $db_args->{'snp_sth'}->bind_param(1, $chr_seq_region_id);
    $db_args->{'snp_sth'}->bind_param(2, $pos);
    $db_args->{'snp_sth'}->bind_param(3, $pos);
    $db_args->{'snp_sth'}->bind_param(4, $source_id);

    $db_args->{'snp_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $db_args->{'snp_sth'}, 'select snp', 2);

    if (my $snp_values = $db_args->{'snp_sth'}->fetchrow_arrayref())
    {
        $snp_hash = {};

        $snp_hash->{'v.variation_id'} = $snp_values->[0];
        $snp_hash->{'v.name'} = $snp_values->[1];
        $snp_hash->{'v.source_id'} = $snp_values->[2];
        $snp_hash->{'v.ancestral_allele'} = $snp_values->[3];
        $snp_hash->{'vf.variation_feature_id'} = $snp_values->[4];
        $snp_hash->{'vf.variation_name'} = $snp_values->[5];
        $snp_hash->{'vf.map_weight'} = $snp_values->[6];
        $snp_hash->{'vf.seq_region_start'} = $snp_values->[7];
        $snp_hash->{'vf.seq_region_end'} = $snp_values->[8];
        $snp_hash->{'vf.seq_region_strand'} = $snp_values->[9];
        $snp_hash->{'vf.allele_string'} = $snp_values->[10];

        if ($with_flank_seq)
        {
            $snp_hash->{'fs.seq_region_id'} = $snp_values->[11];
            $snp_hash->{'fs.seq_region_strand'} = $snp_values->[12];
            $snp_hash->{'fs.up_seq'} = $snp_values->[13];
            $snp_hash->{'fs.up_seq_region_start'} = $snp_values->[14];
            $snp_hash->{'fs.up_seq_region_end'} = $snp_values->[15];
            $snp_hash->{'fs.down_seq'} = $snp_values->[16];
            $snp_hash->{'fs.down_seq_region_start'} = $snp_values->[17];
            $snp_hash->{'fs.down_seq_region_end'} = $snp_values->[18];
        }
    }
    else
    {
        $snp_hash = undef;
    }

    return($snp_hash);
}

sub fetch_VariationFeatures_by_chr_start_and_end # InDels NOT USED
{
    my $slice_adaptor = shift;
    my $vf_adaptor = shift;
    my $chr = shift;
    my $start = shift;
    my $end = shift;
    
    my $snp_slice = ($start < $end ?
        $slice_adaptor->fetch_by_region('Chromosome', $chr, $start, $end) :
        $slice_adaptor->fetch_by_region('Chromosome', $chr, $end, $start));

    my @chr_var_feats;
    my $chr_var_feat;

    foreach my $var_feat (@{$vf_adaptor->fetch_all_by_Slice($snp_slice)})
    {
        $chr_var_feat = $var_feat->transform('Chromosome');

        debug_VariationFeature($chr_var_feat);

        push @chr_var_feats, $chr_var_feat;
    }

    return(\@chr_var_feats);
}

sub fetch_VariationFeature_by_chr_start_end_and_source # InDels NOT USED
{
    my $slice_adaptor = shift;
    my $vf_adaptor = shift;
    my $chr = shift;
    my $start = shift;
    my $end = shift;
    my $source_name = shift;

    my @var_feats = @{fetch_VariationFeatures_by_chr_start_and_end(
        $slice_adaptor, $vf_adaptor, $chr, $start, $end)};

    my $var_feat_for_source = undef;

    foreach my $var_feat (@var_feats)
    {
        if ($var_feat->source() eq $source_name &&
            $var_feat->seq_region_start() == $start &&
            $var_feat->seq_region_end() == $end &&
            (!$var_feat->variation->ancestral_allele() ||
            $var_feat->variation->ancestral_allele() eq ''))
        {
            $var_feat_for_source = $var_feat;
        }
    }

    return($var_feat_for_source);
}

sub select_indel_by_chr_start_end_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $start = shift;
    my $end = shift;
    my $source_id = shift;

    my $indel_hash = _select_var_by_chr_start_end_and_source(
        $db_args, $chr_seq_region_id, $start, $end, $source_id, 'indel');

    return($indel_hash);
}

sub OLD_select_indel_by_chr_start_end_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $start = shift;
    my $end = shift;
    my $source_id = shift;

    my $indel_hash;

    my $with_flank_seq = 
        ($db_args->{'with_flank_seq'} && $db_args->{'with_flank_seq'} eq 'y');

    if (!$db_args->{'indel_sth'})
    {
        my $indel_sql = 'SELECT v.variation_id, v.name, v.source_id, ' .
            'vf.variation_feature_id, vf.variation_name, vf.map_weight, ' . 
            'vf.seq_region_start, vf.seq_region_end, vf.seq_region_strand, ' .
            'vf.allele_string';

        if ($with_flank_seq)
        {
            $indel_sql .= ', fs.seq_region_id, fs.seq_region_strand, ' .
                'fs.up_seq, fs.up_seq_region_start, fs.up_seq_region_end, ' .
                'fs.down_seq, fs.down_seq_region_start, fs.down_seq_region_end';
        }

        $indel_sql .= ' FROM variation v JOIN variation_feature vf ' .
            'ON (v.variation_id = vf.variation_id) ';

        if ($with_flank_seq)
        {
            $indel_sql .= 'JOIN flanking_sequence fs' .
                ' ON (v.variation_id = fs.variation_id) ';
        }

        $indel_sql .= 'WHERE vf.seq_region_id = ? AND vf.seq_region_start = ? AND ' .
            'vf.seq_region_end = ? AND v.source_id = ? AND ' .
            # ??? 'v.ancestral_allele IS NULL';
            'vf.allele_string = \'\'';# ???

        $indel_sql .= ' ORDER BY v.variation_id'; # Use var with lowest ID

        $db_args->{'indel_sth'} = $db_args->{'dbc'}->prepare($indel_sql);
    }

    $db_args->{'indel_sth'}->bind_param(1, $chr_seq_region_id);
    $db_args->{'indel_sth'}->bind_param(2, $start);
    $db_args->{'indel_sth'}->bind_param(3, $end);
    $db_args->{'indel_sth'}->bind_param(4, $source_id);

    $db_args->{'indel_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $db_args->{'indel_sth'}, 'select indel', 2);

    if (my $indel_values = $db_args->{'indel_sth'}->fetchrow_arrayref())
    {
        $indel_hash = {};

        $indel_hash->{'v.variation_id'} = $indel_values->[0];
        $indel_hash->{'v.name'} = $indel_values->[1];
        $indel_hash->{'v.source_id'} = $indel_values->[2];
        $indel_hash->{'vf.variation_feature_id'} = $indel_values->[3];
        $indel_hash->{'vf.variation_name'} = $indel_values->[4];
        $indel_hash->{'vf.map_weight'} = $indel_values->[5];
        $indel_hash->{'vf.seq_region_start'} = $indel_values->[6];
        $indel_hash->{'vf.seq_region_end'} = $indel_values->[7];
        $indel_hash->{'vf.seq_region_strand'} = $indel_values->[8];
        $indel_hash->{'vf.allele_string'} = $indel_values->[9];

        if ($with_flank_seq)
        {
            $indel_hash->{'fs.seq_region_id'} = $indel_values->[10];
            $indel_hash->{'fs.seq_region_strand'} = $indel_values->[11];
            $indel_hash->{'fs.up_seq'} = $indel_values->[12];
            $indel_hash->{'fs.up_seq_region_start'} = $indel_values->[13];
            $indel_hash->{'fs.up_seq_region_end'} = $indel_values->[14];
            $indel_hash->{'fs.down_seq'} = $indel_values->[15];
            $indel_hash->{'fs.down_seq_region_start'} = $indel_values->[16];
            $indel_hash->{'fs.down_seq_region_end'} = $indel_values->[17];
        }
    }
    else
    {
        $indel_hash = undef;
    }

    return($indel_hash);
}

sub select_subst_by_chr_start_end_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $start = shift;
    my $end = shift;
    my $source_id = shift;

    my $subst_hash = _select_var_by_chr_start_end_and_source(
        $db_args, $chr_seq_region_id, $start, $end, $source_id, 'subst');

    return($subst_hash);
}

sub OLD_select_subst_by_chr_start_end_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $start = shift;
    my $end = shift;
    my $source_id = shift;

    my $subst_hash;

    my $with_flank_seq = 
        ($db_args->{'with_flank_seq'} && $db_args->{'with_flank_seq'} eq 'y');

    if (!$db_args->{'subst_sth'})
    {
        my $subst_sql = 'SELECT v.variation_id, v.name, v.source_id, ' .
            'vf.variation_feature_id, vf.variation_name, vf.map_weight, ' . 
            'vf.seq_region_start, vf.seq_region_end, vf.seq_region_strand, ' .
            'vf.allele_string';

        if ($with_flank_seq)
        {
            $subst_sql .= ', fs.seq_region_id, fs.seq_region_strand, ' .
                'fs.up_seq, fs.up_seq_region_start, fs.up_seq_region_end, ' .
                'fs.down_seq, fs.down_seq_region_start, fs.down_seq_region_end';
        }

        $subst_sql .= ' FROM variation v JOIN variation_feature vf ' .
            'ON (v.variation_id = vf.variation_id) ';

        if ($with_flank_seq)
        {
            $subst_sql .= 'JOIN flanking_sequence fs' .
                ' ON (v.variation_id = fs.variation_id) ';
        }

        $subst_sql .= 'WHERE vf.seq_region_id = ? AND vf.seq_region_start = ? AND ' .
            'vf.seq_region_end = ? AND v.source_id = ? AND ' .
            # ??? 'v.ancestral_allele IS NULL';
            'vf.allele_string <> \'\'';# ???

        $subst_sql .= ' ORDER BY v.variation_id'; # Use var with lowest ID

        $db_args->{'subst_sth'} = $db_args->{'dbc'}->prepare($subst_sql);
    }

    $db_args->{'subst_sth'}->bind_param(1, $chr_seq_region_id);
    $db_args->{'subst_sth'}->bind_param(2, $start);
    $db_args->{'subst_sth'}->bind_param(3, $end);
    $db_args->{'subst_sth'}->bind_param(4, $source_id);

    $db_args->{'subst_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $db_args->{'subst_sth'}, 'select subst', 2);

    if (my $subst_values = $db_args->{'subst_sth'}->fetchrow_arrayref())
    {
        $subst_hash = {};

        $subst_hash->{'v.variation_id'} = $subst_values->[0];
        $subst_hash->{'v.name'} = $subst_values->[1];
        $subst_hash->{'v.source_id'} = $subst_values->[2];
        $subst_hash->{'vf.variation_feature_id'} = $subst_values->[3];
        $subst_hash->{'vf.variation_name'} = $subst_values->[4];
        $subst_hash->{'vf.map_weight'} = $subst_values->[5];
        $subst_hash->{'vf.seq_region_start'} = $subst_values->[6];
        $subst_hash->{'vf.seq_region_end'} = $subst_values->[7];
        $subst_hash->{'vf.seq_region_strand'} = $subst_values->[8];
        $subst_hash->{'vf.allele_string'} = $subst_values->[9];

        if ($with_flank_seq)
        {
            $subst_hash->{'fs.seq_region_id'} = $subst_values->[10];
            $subst_hash->{'fs.seq_region_strand'} = $subst_values->[11];
            $subst_hash->{'fs.up_seq'} = $subst_values->[12];
            $subst_hash->{'fs.up_seq_region_start'} = $subst_values->[13];
            $subst_hash->{'fs.up_seq_region_end'} = $subst_values->[14];
            $subst_hash->{'fs.down_seq'} = $subst_values->[15];
            $subst_hash->{'fs.down_seq_region_start'} = $subst_values->[16];
            $subst_hash->{'fs.down_seq_region_end'} = $subst_values->[17];
        }
    }
    else
    {
        $subst_hash = undef;
    }

    return($subst_hash);
}

sub select_struct_var_by_chr_start_end_and_source # non-API approach
{
    my $db_args = shift;
    my $chr_seq_region_id = shift;
    my $start = shift;
    my $end = shift;
    my $source_id = shift;

    my $struct_var_hash;

    if (!$db_args->{'struct_var_sth'})
    {
        my $struct_var_sql = 
            'SELECT structural_variation_id, variation_name, ' .
            'source_id, seq_region_id, seq_region_start, seq_region_end, ' .
            'seq_region_strand, class, bound_start, bound_end ' .
            'FROM structural_variation ' .
            'WHERE seq_region_id = ? AND seq_region_start = ? AND ' .
            'seq_region_end = ? AND source_id = ?';

        $struct_var_sql .= ' ORDER BY structural_variation_id'; # Use var with lowest ID

        $db_args->{'struct_var_sth'} = 
            $db_args->{'dbc'}->prepare($struct_var_sql);
    }

    $db_args->{'struct_var_sth'}->bind_param(1, $chr_seq_region_id);
    $db_args->{'struct_var_sth'}->bind_param(2, $start);
    $db_args->{'struct_var_sth'}->bind_param(3, $end);
    $db_args->{'struct_var_sth'}->bind_param(4, $source_id);

    # class, bound_start, bound_end ???

    $db_args->{'struct_var_sth'}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $db_args->{'struct_var_sth'}, 'select struct_var', 2);

    if (my $struct_var_values = 
        $db_args->{'struct_var_sth'}->fetchrow_arrayref())
    {
        $struct_var_hash = {};

        $struct_var_hash->{'structural_variation_id'} = $struct_var_values->[0];
        $struct_var_hash->{'variation_name'} = $struct_var_values->[1];
        $struct_var_hash->{'source_id'} = $struct_var_values->[2];
        $struct_var_hash->{'seq_region_id'} = $struct_var_values->[3];
        $struct_var_hash->{'seq_region_start'} = $struct_var_values->[4];
        $struct_var_hash->{'seq_region_end'} = $struct_var_values->[5];
        $struct_var_hash->{'seq_region_strand'} = $struct_var_values->[6];
        $struct_var_hash->{'class'} = $struct_var_values->[7];
        $struct_var_hash->{'bound_start'} = $struct_var_values->[8];
        $struct_var_hash->{'bound_end'} = $struct_var_values->[9];
    }
    else
    {
        $struct_var_hash = undef;
    }

    return($struct_var_hash);
}

sub update_var_feat_allele_string_old
{
    my $store_args = shift;
    my $var = shift;

    if (!$store_args->{'upd_var_feat_all_str_sth'})
    {
        my $sql = 'UPDATE variation_feature SET allele_string = ? ' .
            'WHERE variation_id = ?';
        my $sth = $store_args->{'db'}->dbc()->prepare($sql);

        $store_args->{'upd_var_feat_all_str_sth'} = $sth;
    }

    $store_args->{'upd_var_feat_all_str_sth'}->bind_param(
        1, $var->{'vf.allele_string'});
    $store_args->{'upd_var_feat_all_str_sth'}->bind_param(
        2, $var->{'v.variation_id'});

    $store_args->{'upd_var_feat_all_str_sth'}->execute() or
        sth_execute_throw($store_args->{'upd_var_feat_all_str_sth'},
            'update variation_feature.allele_string', 2);
}

sub update_var_feat_allele_string
{
    my $db_args = shift;
    my $var = shift;

    if (!$db_args->{'upd_var_feat_all_str_sth'})
    {
        my $sql = 'UPDATE variation_feature SET allele_string = ? ' .
            'WHERE variation_id = ?';
        my $sth = $db_args->{'dbc'}->prepare($sql);

        $db_args->{'upd_var_feat_all_str_sth'} = $sth;
    }

    $db_args->{'upd_var_feat_all_str_sth'}->bind_param(
        1, $var->{'vf.allele_string'});
    $db_args->{'upd_var_feat_all_str_sth'}->bind_param(
        2, $var->{'v.variation_id'});

    $db_args->{'upd_var_feat_all_str_sth'}->execute() or
        sth_execute_throw($db_args->{'upd_var_feat_all_str_sth'},
            'update variation_feature.allele_string', 2);
}


# verification

sub verify_value
{
    my $verified = shift;
    my $label = shift;
    my $object_value = shift;
    my $expected_value = shift;
    my $value_type = shift;
    my $out_str = shift;

    if (!$value_type)
    {
        $value_type = '';
    }

    my $quote = ($value_type eq 's' ? '\'' : '');

    if ($value_type eq 's') # ???
    {
        if (!defined $object_value)
        {
            $object_value = '';
        }

        if (!defined $expected_value)
        {
            $expected_value = '';
        }
    }
    elsif ($value_type eq 'f')
    {
        $object_value = substr(round_float($object_value), 0, 5);
        $expected_value = substr(round_float($expected_value), 0, 5);
    }

    my $is_verified = 1;

    if (($value_type eq '' && $object_value != $expected_value) || 
        ($object_value ne $expected_value))
    {
        my $verified_str = $label . ' = ' . $quote . $object_value . $quote .
            ' not ' . $quote . $expected_value  . $quote . "\n";

        if (!$$out_str)
        {
            print $verified_str;
        }
        else
        {
            $$out_str .= $verified_str;
        }

        $is_verified = 0;
    }

    if (!$is_verified && $$verified)
    {
        $$verified = 0;
    }
}

sub print_failed_verification
{
    my $out_str = shift;
    my $num_tabs = shift;

    my $out_line_num = 0;

    foreach my $out_line (split /\n/, $out_str)
    {
        $out_line_num++;

        if ($out_line_num == 1 && $out_line =~ /^	#/)
        {
            print "$out_line\n" . 
                ("\t" x ($num_tabs - 1)) . "Failed verification!\n";
        }
        else
        {
            print "\t" x $num_tabs . "$out_line\n";
        }
    }
}


# validation

sub is_valid_chr
{
    my $coord_system = shift;
    my $chr = shift;
    my $prev_chr = shift;
    my $chr_slices = shift;
    my $chr_slice = shift;
    my $slice_adaptor = shift;
    my $chr_seq_region_id = shift;
    my $invalid_str = shift;
    my $num_invalid_chr = shift;

    my $is_valid = 1;

    if ($$chr =~ /^Chr(.*)$/) # ???
    {
        $$chr = $1;
    }

    if ($$chr)
    {
        if ($$chr ne $prev_chr)
        {
            if (exists $chr_slices->{$$chr})
            {
                $$chr_slice = $chr_slices->{$$chr};
            }
            else
            {
                $$chr_slice =
                    $slice_adaptor->fetch_by_region($coord_system, $$chr);

                if (! $$chr_slice)
                {
                    $is_valid = 0;
                }
                else
                {
                    $chr_slices->{$$chr} = $$chr_slice; # cached in API anyway ?

                    $$chr_seq_region_id = 
                        $slice_adaptor->get_seq_region_id($$chr_slice);
                }
            }

            if ($is_valid)
            {
                print 'Chromosome ' . $$chr . ' - ' .
                    $$chr_slice->start() . ' -> ' . $$chr_slice->end() . "\n";
            }
        }
    }
    else
    {
        $is_valid = 0;
    }

    if (! $is_valid)
    {
        if ($invalid_str)
        {
            $$invalid_str .= ' - chromosome is invalid!';
        }

        if ($num_invalid_chr)
        {
            $$num_invalid_chr++;
        }
    }

    return($is_valid);
}

sub is_valid_pos_on_chr
{
    my $pos = shift;
    my $chr_slice = shift;
    my $invalid_str = shift;
    my $num_invalid_pos = shift;

    my $is_valid = 1;

    if ($pos < $chr_slice->start() || $pos > $chr_slice->end())
    {
        if ($invalid_str)
        {
            $$invalid_str .= ' - invalid pos!';
        }

        if ($num_invalid_pos)
        {
            $$num_invalid_pos++;
        }

        $is_valid = 0;
    }

    return($is_valid);
}


# DB

sub fetch_source_id_for_name
{
    my $dbc = shift;
    my $source_name = shift;

    my $source_id;

    my $sql = 'SELECT source_id FROM source WHERE name = \'' . 
        $source_name . '\'';
    debug('sql', $sql);

    my $sth = $dbc->prepare($sql);
    $sth->execute() or 
        EGVar::egv_utils::sth_execute_throw($sth, 'select source', 2);

    my @row = $sth->fetchrow_array();

    if (@row)
    {
        $source_id = $row[0];
    }

    debug('source_id', $source_id);

    $sth->finish();

    return($source_id);
}

sub link_individual_to_population # NOT USED
{
    my $dbVariation = shift;
    my $individual = shift;
    my $population = shift;

    my $dbc = $dbVariation->dbc();

    my $sql = 'INSERT INTO individual_population' .
        ' SET individual_sample_id = ' . $individual->dbID() . 
        ', population_sample_id = ' . $population->dbID();

    my $sth = $dbc->prepare($sql);

    $sth->execute();

    $sth->finish();
}

sub insert_variation_synonym
{
    my $dbVariation = shift;
    my $variation_id = shift;
    my $source_id = shift;
    my $name = shift;

    my $dbc = $dbVariation->dbc();

    my $sql = 'INSERT INTO variation_synonym SET name = \'' . $name . '\'' .
        ', variation_id = ' . $variation_id .
        ', source_id = ' . $source_id;
    debug('sql', $sql);

    my $sth = $dbc->prepare($sql);

    my $variation_synonym_id = 0;

    if ($sth->execute())
    {
        $variation_synonym_id = $sth->{'mysql_insertid'};
    }
    else
    {
        print 'DB failure - ' . $sth->errstr() . "\n";
    }

    $sth->finish();

    return($variation_synonym_id);
}

sub insert_meta_coord
{
    my $dbc = shift;
    my $table_name = shift;
    my $coord_system_id = shift;

    my $sql = 'SELECT * FROM meta_coord WHERE table_name = \'' . 
        $table_name . '\'';
    debug('sql', $sql);
   
    my $sth = $dbc->prepare($sql);

    $sth->execute() or 
        EGVar::egv_utils::sth_execute_throw($sth, 'select meta_coord', 2);

    my @row = $sth->fetchrow_array();

    $sth->finish();

    if (!@row)
    {
        $sql = 'INSERT INTO meta_coord' .
            ' SET table_name = \'' . $table_name . '\' ,' .
            ' coord_system_id = ' . $coord_system_id;
        debug('sql', $sql);

        $sth = $dbc->prepare($sql);
        $sth->execute() or
            EGVar::egv_utils::sth_execute_throw($sth, 'insert meta_coord', 2);

        $sth->finish();
    }
}

sub get_pos_str
{
    my $type = shift;

    my $pos_str = shift;

    if ($type ne 's')
    {
        $pos_str .= '-' . shift;
    }

    return($pos_str);
}

sub read_var_ids_by_pos_into_hash
{
    my $type = shift;
    my $ids_file_name = shift;

    my $var_ids_by_pos = {};

    my @id_values;
    my ($id, $pos_str);

    open IDS, $ids_file_name;

    foreach my $id_line (<IDS>)
    {
        chomp $id_line;

        @id_values = split "\t", $id_line;

        $id = shift @id_values;

        $pos_str = get_pos_str($type, @id_values);

        $var_ids_by_pos->{$pos_str} = $id;
    }

    close IDS;

    return($var_ids_by_pos);
}

sub select_existing_phenotypes
{
    my $dbVariation = shift;

    my %existing_phenotypes = ();

    my $sql = 'SELECT phenotype_id, name FROM phenotype';
    my $sth = $dbVariation->dbc->prepare($sql);

    $sth->execute();

    while(my ($phenotype_id, $name) = $sth->fetchrow_array())
    {
        $existing_phenotypes{$name} = $phenotype_id;
    }

    $sth->finish();

    return(\%existing_phenotypes);
}


# database processing

sub db_exists
{
    my $dbc = shift;
    my $db_name = shift;

    my $sql = "SHOW DATABASES LIKE '$db_name'";
    my $sth = $dbc->prepare($sql);

    $sth->execute();

    my $db_exists = $sth->fetchrow_array();

    $sth->finish();

    return($db_exists);
}

sub create_db
{
    my $dbc = shift;
    my $db_name = shift;

    my $sql = "CREATE DATABASE $db_name";
    my $sth = $dbc->prepare($sql);

    $sth->execute();

    $sth->finish();
}

sub table_exists
{
    my $dbc = shift;
    my $table_name = shift;

    my $sql;

    if ($table_name =~ /\./)
    {
        $sql = "DESC $table_name";
    }
    else
    {
        $sql = "SHOW TABLES LIKE '$table_name'";
    }

    my $sth = $dbc->prepare($sql);

    $sth->execute();

    my $table_exists = ($sth->fetchrow_array());

    $sth->finish();

    return($table_exists);
}

sub truncate_table
{
    my $dbc = shift;
    my $table_name = shift;

    my $sql = 'TRUNCATE TABLE ' . $table_name;
    my $sth = $dbc->prepare($sql);

    $sth->execute();

    $sth->finish();
}

sub truncate_tables
{
    my $dbc = shift;
    my $tables_to_truncate = shift;

    foreach my $table_to_truncate (@$tables_to_truncate)
    {
        truncate_table($dbc, $table_to_truncate);
    }
}

sub get_tables_in_db
{
    my $dbc = shift;
    my $database = shift;

    my $sql = 'SHOW TABLES';

    if ($database)
    {
        $sql .= " FROM $database";
    }

    my $sth = $dbc->prepare($sql);

    my %tables_in_db = ();
    my @row;

    $sth->execute();

    while(@row = $sth->fetchrow_array())
    {
        $tables_in_db{$row[0]} = $row[0];
    }

    return(\%tables_in_db);
}

sub sth_execute_throw
{
    my $sth = shift;
    my $info = shift;
    my $level = shift;
    my $values = shift;

    if (!$level)
    {
        $level = 1;
    }

    my ($package, $file, $line, $sub) = caller($level);

    my $info_str = ($values ? sprintf($info, $values) : $info);

    throw("Failed to execute $info_str in $file:$sub - " . $sth->errstr);
}

sub finish_db_args_sth
{
    my $db_args = shift;

    foreach my $key (keys %$db_args)
    {
        if ($key =~ /_sth$/)
        {
            $db_args->{$key}->finish();
        }
    }
}


# mysql

sub get_base_mysql_cmd_from_dbc
{
    my $dbc = shift;

    my $base_cmd = '-h' . $dbc->host() . ' -P' . $dbc->port() . 
        ' -u' . $dbc->username() . ' -p' . $dbc->password() . ' ';

    return($base_cmd);
}


# program arguments

sub get_prog_args_from_opt
{
    my $opt_details = shift;
    my $prog_args = shift;
    my $additional_usage = shift;

    my $opts_str = '';
    my $num_opts = 0;

    foreach my $opt_detail (@$opt_details)
    {
        $opts_str .= $opt_detail->[0];

        $num_opts++;
    }

    my %opts = ();

    getopt($opts_str, \%opts);

    my $prog_args_str = "\n$0:\n";
    my $usage_str = "usage: $0";
    my $usage_sub_str;

    my $idx = 0;
    my $delimiter;
    my ($opt_key, $opt_value);
    my $prog_args_ok = 1;

    while($idx < $num_opts)
    {
        $delimiter = ($opt_details->[$idx]->[2] eq 's' ? "'" : '');

        $opt_key = $opt_details->[$idx]->[0];
        $opt_value = (exists $opts{$opt_key} ? $opts{$opt_key} : '');
        $opt_key = $opt_details->[$idx]->[1];

        if (!$opt_value && $opt_details->[$idx]->[3] eq 'o' && 
            defined $opt_details->[$idx]->[4])
        {
            $opt_value = $opt_details->[$idx]->[4];
        }

        $prog_args_str .= "\t$opt_details->[$idx]->[1] = " .
            "$delimiter$opt_value$delimiter\n";

        $usage_sub_str = 
            "-$opt_details->[$idx]->[0] <$opt_details->[$idx]->[1]>";

        if ($opt_details->[$idx]->[3] eq 'o' &&
            $opt_details->[$idx]->[4])
        {
            $usage_sub_str .= "|$opt_details->[$idx]->[4]";
        }

        if ($opt_details->[$idx]->[3] eq 'o')
        {
            $usage_sub_str = "[$usage_sub_str]";
        }

        $usage_str .= " $usage_sub_str";

        $prog_args->{$opt_key} = $opt_value;

        if ($prog_args_ok && 
            $opt_details->[$idx]->[3] eq 'r' &&
            (!$opt_value || $opt_value eq ''))
        {
            $prog_args_ok = 0;
        }

        $idx++;
    }

    if ($prog_args_ok)
    {
        print "$prog_args_str\n";
    }
    else
    {
        if ($additional_usage)
        {
            $usage_str .= "\n$additional_usage";
        }

        die($usage_str);
    }
}

sub get_long_prog_args_from_opt
{
    my $opt_details = shift;
    my $prog_args = shift;
    my $additional_usage = shift;

    my $opt_str;
    my @opts_arr = ();
    my $num_opts = 0;

    my %used_name_counts;
    my %used_alt_name_counts;

    foreach my $opt_detail (@$opt_details)
    {
        $used_name_counts{$opt_detail->[0]}++;
        $used_alt_name_counts{$opt_detail->[1]}++;

        $opt_str = $opt_detail->[0] . 
            ($opt_detail->[1] ne '' ? 
                "|$opt_detail->[1]" : '') . 
            '=' . $opt_detail->[2]; # = ???

        push @opts_arr, $opt_str;

        $num_opts++;
    }

    my $duplicated_arg = 0;

    foreach my $name (sort keys %used_name_counts)
    {
        if ($used_name_counts{$name} > 1)
        {
            print "name $name is used $used_name_counts{$name} times\n";

            $duplicated_arg = 1;
        }
    }

    foreach my $name (sort keys %used_alt_name_counts) # ???
    {
        if ($used_alt_name_counts{$name} > 1)
        {
            print "alt name $name is used $used_alt_name_counts{$name} times\n";

            $duplicated_arg = 1;
        }
    }

    if ($duplicated_arg)
    {
        my @used_alt_names_arr = sort keys %used_alt_name_counts;
        my $used_alt_names_str = "@used_alt_names_arr";
        die("Defined arguments are invalid - used alt names are -> '$used_alt_names_str'");
    }

    my %opts = ();

    GetOptions(\%opts, @opts_arr);

    my $prog_args_str = "\n$0:\n";
    my $usage_str = "usage: $0";
    my $usage_sub_str;

    my $idx = 0;
    my $delimiter;
    my ($opt_key, $opt_value, $full_opt_key);
    my $prog_args_ok = 1;

    while($idx < $num_opts)
    {
        $delimiter = ($opt_details->[$idx]->[2] eq 's' ? "'" : '');

        $opt_key = $opt_details->[$idx]->[0];
        $opt_value = (exists $opts{$opt_key} ? $opts{$opt_key} : '');
        $full_opt_key = '-' . $opt_key . ($opt_details->[$idx]->[1] ne '' ? 
            "|$opt_details->[$idx]->[1]" : '');

        if (!$opt_value && $opt_details->[$idx]->[3] eq 'o' && 
            defined $opt_details->[$idx]->[4])
        {
            $opt_value = $opt_details->[$idx]->[4];
        }

        $prog_args_str .= "\t$full_opt_key = " .
            "$delimiter$opt_value$delimiter\n";

        my %opt_types = (s => 'str', i => 'int');

        $usage_sub_str = 
            "$full_opt_key <$opt_types{$opt_details->[$idx]->[2]}";

        if ($opt_details->[$idx]->[3] eq 'o' &&
            $opt_details->[$idx]->[4])
        {
            $usage_sub_str .= "|$opt_details->[$idx]->[4]";
        }

        $usage_sub_str .= '>';

        if ($opt_details->[$idx]->[3] eq 'o')
        {
            $usage_sub_str = "[$usage_sub_str]";
        }

        $usage_str .= " $usage_sub_str";

        $prog_args->{$opt_key} = $opt_value;

        if ($prog_args_ok && 
            $opt_details->[$idx]->[3] eq 'r' &&
            (!$opt_value || $opt_value eq ''))
        {
            $prog_args_ok = 0;
        }

        $idx++;
    }

    if ($prog_args_ok)
    {
        print "$prog_args_str\n";
    }
    else
    {
        if ($additional_usage)
        {
            $usage_str .= "\n$additional_usage";
        }

        die("\n$usage_str\n\n");
    }
}

sub get_default_bsub_queue_name
{
    my $default_bsub_queue_name = 'production-rh6';

    return($default_bsub_queue_name);
}

sub get_default_bsub_wait_status
{
    my $default_bsub_wait_status = 'ended';

    return($default_bsub_wait_status);
}


# print counts

sub print_counts
{
    my $counts = shift;
    my $ordered_keys = shift;
    my $count_strs = shift;

    my $prev_count_str = ' ';

    print "\n";

    foreach my $count_key (@$ordered_keys)
    {
        if (exists $counts->{$count_key})
        {
            if ($prev_count_str !~ / $/)
            {
                print "\t";
            }

            print "$counts->{$count_key} $count_strs->{$count_key}";

            $prev_count_str = $count_strs->{$count_key};
        }
    }

    print "\n";
}


# frequency related

sub calc_frequency
{
    my $numerator = shift;
    my $denominator = shift;

    my $freq = ($denominator > 0 ? ($numerator / $denominator) : 0);

    return($freq);
}

sub is_valid_frequency
{
    my $frequency = shift;
    my $num_with_invalid_frequency = shift;
    my $num_with_bad_frequency = shift;
    my $out_str = shift;

    if ($frequency != 0.0 && $frequency < 0.01)
    {
        $num_with_invalid_frequency++;

        $out_str .=  "\t$frequency != 0.0 and $frequency < 0.01\n";
    }
    elsif ($frequency < 0.0 || $frequency > 1.0)
    {
        $num_with_bad_frequency++;

        $out_str .=  "\t$frequency < 0.0 or > 1.0\n";
    }
}


# string conversion

sub tag_to_value
{
    my $value_by_tag = shift;
    my $template_line = shift;

    my $out_line = $template_line;

#if (!$template_line)
#{
    #print "BLANK IN!\n";
#}
#else
#{
    #print "'$template_line'\n";
#}
    foreach my $tag (keys %$value_by_tag)
    {
        while ($out_line =~ /$tag/)
        {
            $out_line = $` . $value_by_tag->{$tag} . $';
        }
    }
#if (!$out_line)
#{
    #print "BLANK OUT!\n";
#}
#else
#{
    #print "'$out_line'\n\n";
#}

    return($out_line);
}


# next_id

sub get_next_id
{
    my $next_id_db_args = shift;

    if (!$next_id_db_args->{interval})
    {
        $next_id_db_args->{interval} = 1;
    }

    if (!exists $next_id_db_args->{dbc})
    {
        throw('get_next_id requires next_id_db_args->{dbc}');
    }

    my $sql;

    if (!exists $next_id_db_args->{sel_sth})
    {
        $sql = 'SELECT id FROM tmp_next_id WHERE name = ? FOR UPDATE';
        $next_id_db_args->{sel_sth} = $next_id_db_args->{dbc}->prepare($sql);
    }

    if (!exists $next_id_db_args->{upd_sth})
    {
        $sql = 'UPDATE tmp_next_id SET id = id + ? WHERE name = ?';
        $next_id_db_args->{upd_sth} = $next_id_db_args->{dbc}->prepare($sql);
    }

    $next_id_db_args->{dbc}->db_handle->begin_work();

    $next_id_db_args->{sel_sth}->bind_param(1, $next_id_db_args->{id_name});
    $next_id_db_args->{sel_sth}->execute() or
        EGVar::egv_utils::sth_execute_throw(
            $next_id_db_args->{sel_sth}, 'select id');

    my ($next_id) = $next_id_db_args->{sel_sth}->fetchrow_array();

    if ($next_id)
    {
        $next_id_db_args->{upd_sth}->bind_param(1, 
            $next_id_db_args->{interval});
        $next_id_db_args->{upd_sth}->bind_param(2, $next_id_db_args->{id_name});
        $next_id_db_args->{upd_sth}->execute() or
            EGVar::egv_utils::sth_execute_throw(
                $next_id_db_args->{upd_sth}, 'update id');

        $next_id_db_args->{dbc}->db_handle->commit();
    }
    else
    {
        $next_id_db_args->{dbc}->db_handle->rollback();
        throw("Failed to read id for '$next_id_db_args->{id_name}'");
    }

    return($next_id);
}

sub get_variation_name
{
    my $locus_id_db_args = shift;

    my $var_name_id;

    if ($locus_id_db_args && $locus_id_db_args->{locus_id_group_size})
    {
        if ($locus_id_db_args->{locus_num} && 
            $locus_id_db_args->{locus_num} < 
                $locus_id_db_args->{max_allocated_locus_num})
        {
            $locus_id_db_args->{locus_num}++;
        }
        else
        {
            $locus_id_db_args->{locus_num} = 
                EGVar::egv_utils::get_next_id($locus_id_db_args);

            $locus_id_db_args->{max_allocated_locus_num} = 
                $locus_id_db_args->{locus_num} + 
                $locus_id_db_args->{locus_id_group_size} - 1;
        }

        $var_name_id = $locus_id_db_args->{locus_num};
    }
    else
    {
        throw('Cannot get next id');
    }

    my $variation_name = 
        sprintf("%s%08d", ($locus_id_db_args->{var_name_prefix}, $var_name_id));

    return($variation_name);
}

1;

