=head1 LICENSE

Copyright [2009-2014] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

package Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::InitVarMart;

use strict;
use base ('Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::Base');
use Carp;
use FileHandle;
use POSIX;

sub run {
    my $self = shift @_;
    
    my $species            = $self->param('species')            || die "'species' is an obligatory parameter";
    my $short_species_name = $self->param('short_species_name');
    my $release            = $self->param('release');
    my $eg_release         = $self->param('eg_release');
    my $sql_dir            = $self->param('sql_dir');
    my $var_sql_file       = $self->param('var_sql_file');
    my $var_syn_sql_file   = $self->param('var_syn_sql_file');
    my $structvar_sql_file = $self->param('structvar_sql_file');
    my $data_dir           = $self->param('data_dir');
    my $nb_variations_per_run = $self->param('nb_variations_per_run');
    
    # Replace on the fly the following placeholders:
    # * VAR_MART_DB
    # * SPECIES_ABBREV
    # * CORE_DB
    # * VAR_DB
    
    my $species_abbrev = $short_species_name;
    
    my $variation_adaptor = Bio::EnsEMBL::Registry->get_adaptor($species, 'variation', 'Variation');
    my $variation_db = $variation_adaptor->db()->dbc()->dbname();
    my $core_db = $variation_db;
    $core_db =~ s/variation/core/;
    
    # 1/ var mart

    # Get the number of variations for this dataset

    my $dbh = $variation_adaptor->db()->dbc()->db_handle();
    my $variations_sql = "SELECT count(1) from variation";
    my $variations_sth = $dbh->prepare($variations_sql);
    $variations_sth->execute();
    my ($nb_variations) = $variations_sth->fetchrow_array();

    print STDERR "nb_variations, $nb_variations\n";
    
    my $variations_files = [];
    my $splitting = 0;
    my $nb_files = 1;
    
    if ($nb_variations > $nb_variations_per_run) {
	# need to split the dataset

	print STDERR "splitting is required\n";
	
	$splitting = 1;
	$nb_files = ceil ($nb_variations / $nb_variations_per_run);
	
	print STDERR "nb_files, $nb_files\n";

	for (my $i=1; $i<=$nb_files; $i++) {
	    my $var_mart_db = $short_species_name . "_var_mart_" . $eg_release . "_" . $i;
	    push (@$variations_files, $var_mart_db);
	}
	
    }
    else {
	
	print STDERR "splitting is not required\n";
	
	my $var_mart_db = $short_species_name . "_var_mart_" . $eg_release;
	
	push (@$variations_files, $var_mart_db);
    }

    my $min = 1;
    my $max = $nb_variations;
    if ($splitting) {
	$max = $nb_variations_per_run;
    }
    
    my $file_index = 1;
    foreach my $var_mart_db (@$variations_files) {
	
	my $create_db_info = 0;
	my $enable_keys = 0;

	if ($file_index == 1) {
	    $create_db_info = 1;
	}
	elsif ($file_index == @$variations_files) {
	    $enable_keys = 1;
	}

	my $place_holders_href = {
	    VAR_MART_DB    => $var_mart_db,
	    SPECIES_ABBREV => $species_abbrev,
	    CORE_DB        => $core_db,
	    VAR_DB         => $variation_db,
	};
	
	# Add the between clause
	# to an intermediary sql file
	
	my $in_varmart_file_fh = new FileHandle;
	my $out_varmart_file_fh = new FileHandle;
	
	my $in_varmart_path = $sql_dir . '/' . $var_sql_file;
	my $out_varmart_path = $data_dir . '/with_between_clause_' . $var_sql_file;
	$in_varmart_file_fh->open("<$in_varmart_path") or die "can't open template var file!\n";
	$out_varmart_file_fh->open (">$out_varmart_path") or die "can't open intermediary var file!\n";
	
	print STDERR "processing file, $in_varmart_path\n";

	while (<$in_varmart_file_fh>) {
	    my $line = $_;
	    
	    if ($splitting && ($line =~ /from var_db.variation/i)) {
		my $new_line = add_between_clause ($line, $min, $max);
		print $out_varmart_file_fh "$new_line";
	    }
	    else {
		print $out_varmart_file_fh "$line";
	    }
	}
	
	$in_varmart_file_fh->close();
	$out_varmart_file_fh->close();

	print STDERR "processing file, $var_sql_file, done\n";
	
	# Create the final sql file
	# and replace the place holders on the fly

	my $sub_var_sql_file = $var_sql_file;
        $sub_var_sql_file =~ s/.sql//;
	$sub_var_sql_file .= "_$file_index.sql";
	
	$in_varmart_path = $data_dir . '/with_between_clause_' . $var_sql_file;
	$out_varmart_path = $data_dir . '/' . $sub_var_sql_file;
	$in_varmart_file_fh->open("<$in_varmart_path") or die "can't open template var file!\n";
	$out_varmart_file_fh->open (">$out_varmart_path") or die "can't open output var file!\n";
	
	print STDERR "processing file, $in_varmart_path\n";
	
	while (<$in_varmart_file_fh>) {
	    my $line = $_;
	    my $new_line = replace_place_holders ($place_holders_href, $line);
	    print $out_varmart_file_fh "$new_line";
	}
	
	$in_varmart_file_fh->close();
	$out_varmart_file_fh->close();
	
	print STDERR "processing file, $in_varmart_path, done\n";
    
	$self->dataflow_output_id({'input_file' => $sub_var_sql_file, 'var_mart_db' => $var_mart_db, 'create_db_info' => $create_db_info, 'enable_keys' => $enable_keys, 'file_index' => $file_index});

	$file_index++;
	$min += $nb_variations_per_run;
	$max += $nb_variations_per_run;

    }

    # 2/ the syn sql file - NOT NEEDED apparently! So skip it

    my $synvar_mart_db = $short_species_name . "_synvar_mart_" . $eg_release;
    
    # Todo: Actually need more place holders

    my $src_id = undef;
    my $src_name = undef;

    my $syn_place_holders_href = {
	VAR_MART_DB    => $synvar_mart_db,
	SPECIES_ABBREV => $species_abbrev,
	CORE_DB        => $core_db,
	VAR_DB         => $variation_db,
	SRC_ID         => $src_id,
	SRC_NAME       => $src_name,
	
    };
    
    # Create the synvar_mart_db
    
    my $in_syn_varmart_file_fh = new FileHandle;
    my $out_syn_varmart_file_fh = new FileHandle;
    
    my $in_syn_varmart_path = $sql_dir . '/' . $var_syn_sql_file;
    my $out_syn_varmart_path = $data_dir . '/' . $var_syn_sql_file;
    $in_syn_varmart_file_fh->open("<$in_syn_varmart_path") or die "can't open template var file!\n";
    $out_syn_varmart_file_fh->open (">$out_syn_varmart_path") or die "can't open output var file!\n";
    
    # Replace the place holders on the fly

    print STDERR "processing file, $in_syn_varmart_path\n";
    
    while (<$in_syn_varmart_file_fh>) {
	my $line = $_;
	my $new_line = replace_place_holders ($syn_place_holders_href, $line);
	print $out_syn_varmart_file_fh "$new_line";
    }
    
    $in_syn_varmart_file_fh->close();
    $out_syn_varmart_file_fh->close();
    
    print STDERR "processing file, $in_syn_varmart_path, done\n";

    # so not added
    # $self->dataflow_output_id({'input_file' => $var_syn_sql_file, 'var_mart_db' => $synvar_mart_db, 'create_db_info' => 1});

    # 3/ structvar mart

    my $structvar_mart_db = $short_species_name . "_structvar_mart_" . $eg_release;
    
    my $struct_place_holders_href = {
	VAR_MART_DB    => $structvar_mart_db,
	SPECIES_ABBREV => $species_abbrev,
	CORE_DB        => $core_db,
	VAR_DB         => $variation_db,
    };
    
    my $in_struct_varmart_file_fh = new FileHandle;
    my $out_struct_varmart_file_fh = new FileHandle;

    my $in_struct_varmart_path = $sql_dir . '/' . $structvar_sql_file;
    my $out_struct_varmart_path = $data_dir . '/' . $structvar_sql_file;
    $in_struct_varmart_file_fh->open("<$in_struct_varmart_path") or die "can't open template structvar file!\n";
    $out_struct_varmart_file_fh->open (">$out_struct_varmart_path") or die "can't open output structvar file!\n";

    # Replace the place holders on the fly

    print STDERR "processing file, $in_struct_varmart_path\n";

    while (<$in_struct_varmart_file_fh>) {
	my $line = $_;
	my $new_line = replace_place_holders ($struct_place_holders_href, $line);
	print $out_struct_varmart_file_fh "$new_line";
    }
    
    $in_struct_varmart_file_fh->close();
    $out_struct_varmart_file_fh->close();

    print STDERR "processing file, $in_struct_varmart_path, done\n";

    $self->dataflow_output_id({'input_file' => $structvar_sql_file, 'var_mart_db' => $structvar_mart_db, 'create_db_info' => 1, 'enable_keys' => 1, 'file_index' => $file_index});
    
}

sub add_between_clause {
    my $line = shift;
    my $min = shift;
    my $max = shift;

    my $new_line = $line;

    chomp $new_line;
    $new_line =~ s/;$//;
    $new_line .= " AND a.variation_id >= $min AND a.variation_id <= $max;\n";

    return $new_line;
}

sub replace_place_holders {
    my $place_holders_href = shift;
    my $line = shift;
    
    my $new_line = $line;
    foreach my $place_holder (keys (%$place_holders_href)) {
	my $value = $place_holders_href->{$place_holder};

	$new_line =~ s/$place_holder/$value/g;
    }

    return $new_line;
}

1;

