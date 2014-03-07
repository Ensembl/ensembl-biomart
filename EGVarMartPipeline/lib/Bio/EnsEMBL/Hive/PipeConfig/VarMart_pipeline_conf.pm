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

package Bio::EnsEMBL::Hive::PipeConfig::VarMart_pipeline_conf;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');

sub default_options {
    my ($self) = @_;

    #my $species = $self->o('species');
    my $nb_variations_per_run = 1000000;

    return {
      # inherit other stuff from the base class
      #
      %{ $self->SUPER::default_options() },

      # name used by the beekeeper to prefix job names on the farm
      # and to build the pipeline_dir parameter.
      #
      pipeline_name => 'VarMart_pipeline',   
     
      #short_species_name => '',

      nb_variations_per_run => $nb_variations_per_run,
      
      pipeline_data_dir => $self->o('pipeline_dir'),

      species => $self->o('species'),


      # If set, the pipeline_dir directory will be deleted at the Cleanup
      # stage. 
      #
      delete_pipeline_dir_at_cleanup => 0,

      'pipeline_db' => {  
        -host   => $self->o('hive_host'),
        -port   => $self->o('hive_port'),
        -user   => $self->o('hive_user'),
        -pass   => $self->o('hive_password'),
        -dbname => $self->o('hive_dbname'),
	-driver => $self->o('hive_driver'),
      },
    };
}

sub pipeline_wide_parameters {
  my ($self) = @_;

  my $species = $self->o('species');
  print STDERR "species in pipeline_wide_parameters, $species\n";
  $species =~ /^(\w)[^_]+_(w+)/;
  my $short_species_name = "$1$2";
  
  print STDERR "short_species_name in pipeline_wide_parameters, $short_species_name\n";
  
  return {
    # Use in Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::Base from which all
    # analysis modules of this pipeline inherit so must be set.
    #
    species       => $self->o('species'),
    #short_species_name => $short_species_name,
    release       => $self->o('release'),
    eg_release    => $self->o('eg_release'),
    nb_variations_per_run => $self->o('nb_variations_per_run'),
    %{$self->SUPER::pipeline_wide_parameters()}
  };
}

sub pipeline_create_commands {
    my ($self) = @_;
    return [
      @{$self->SUPER::pipeline_create_commands},  # inheriting database and hive tables' creation
      'mkdir -p '.$self->o('pipeline_data_dir'),
#      $self->db_cmd('pipeline_db', 'CREATE TABLE intermediate_result (var_mart_db char(50) NOT NULL, create_db_info tinyint NOT NULL, enable_keys tinyint NULL, file_index SMALLINT NOT NULL, PRIMARY KEY (var_mart_db))'),
      $self->db_cmd( 'CREATE TABLE intermediate_result (var_mart_db char(50) NOT NULL, create_db_info tinyint NOT NULL, enable_keys tinyint NULL, file_index SMALLINT NOT NULL, PRIMARY KEY (var_mart_db))'),
    ];
}

sub beekeeper_extra_cmdline_options {
  my ($self) = @_;
  return 
      ' -reg_conf ' . $self->o('registry')
  ;
}

sub pipeline_analyses {
    my ($self) = @_;

    my $species       = $self->o('species');
    print STDERR "species, $species\n";
    my $release       = $self->o('release');
    my $eg_release    = $self->o('eg_release');

    print STDERR "release, eg_release, $release, $eg_release\n";

    my $data_dir      = $self->o('pipeline_dir');
    my $sql_dir       = $self->o('EG_repo_root_dir') . "/eg-biomart/var_mart/ensembl";
    my $var_sql_file  = "var_mart_$release.var.sql";
#    my $var_syn_sql_file   = "var_mart_$release.var_syn.sql";
    my $structvar_sql_file = "var_mart_$release.struct_var.sql";
    my $nb_variations_per_run = $self->o('nb_variations_per_run');
    
    my $final_var_host = $self->o('final_host');
    my $final_var_port = $self->o('final_port');
    my $final_var_user = $self->o('final_user');
    my $final_var_pass = $self->o('final_pass');
    
    my $final_db_conn_href = {
	'host' => $final_var_host,
	'port' => $final_var_port,
	'user' => $final_var_user,
	'pass' => $final_var_pass,
    };
    
    my $short_species_name = "";
    my $final_snp_mart_db = "";
    
    if ($species !~ /^#.+#$/) {
	$species =~ /^(\w)[^_]+_(\w+)/;
	$short_species_name = "$1$2";
	
	print STDERR "short_species_name, $short_species_name\n";
	
	$final_snp_mart_db = $short_species_name . "_snp_mart_" . $eg_release;
    }

    return [
        {   -logic_name => 'PrePipelineChecks',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::PrePipelineChecks',
            -hive_capacity => -1,
            -max_retry_count => 1,
	    -parameters    => {
		data_dir     => $data_dir,
		sql_dir      => $sql_dir,
		var_sql_file => $var_sql_file,
#		var_syn_sql_file   => $var_syn_sql_file,
		structvar_sql_file => $structvar_sql_file,
	     },
            -input_ids => [{}],
            -meadow_type    => 'LOCAL',
        },

	{  -logic_name => 'InitVarMart',
	   -module     => 'Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::InitVarMart',
	   -hive_capacity => -1,
	   -max_retry_count => 1,
	   -wait_for => [ 'PrePipelineChecks' ],
	   -parameters    => {
	       'short_species_name' => $short_species_name,
	       'data_dir'     => $data_dir,
	       'sql_dir'      => $sql_dir,
	       'var_sql_file' => $var_sql_file,
#	       'var_syn_sql_file'   => $var_syn_sql_file,
	       'structvar_sql_file' => $structvar_sql_file,
	   },
	   -input_ids => [{}],
	   -flow_into => {
	       1 => [ 'BuildVarMart' ],
	   },
	   -meadow_type    => 'LOCAL',
        },
	
        {   -logic_name => 'BuildVarMart',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::BuildVarMart',
	    -batch_size => 1,
            -hive_capacity => 15,
	    -max_retry_count => 1,
            -priority => 5,
            -wait_for => [ 'InitVarMart' ],
            -parameters    => { 
		'data_dir' => $data_dir,
            },
	    -flow_into => {
                1 => [ 'mysql:////intermediate_result' ],
            },
            -rc_name => '2Gb',
        },

	{   -logic_name    => 'MergeVarMart',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::VarMartPipeline::MergeVarMart',
            -parameters    => {
                'data_dir' => $data_dir,
		'final_snp_mart_db' => $final_snp_mart_db,
		'final_db_conn' => $final_db_conn_href,
            },
            -hive_capacity => 10,
	    -max_retry_count => 1,
            -batch_size    => 2,
            -priority => 20,
	    -wait_for => [ 'BuildVarMart' ],
            -rc_name => '2Gb',
	    -input_ids => [{}],
        },

    ];
}

sub resource_classes {
  my ($self) = @_;
  return {
    'default' => { 'LSF' => '-q production-rh6' },
    '2Gb'     => { 'LSF' => '-q production-rh6 -M 2048 -R "rusage[mem=2048]"' },
    '6Gb'     => { 'LSF' => '-q production-rh6 -M 6048 -R "rusage[mem=6048]"' },
  };
}

1;
