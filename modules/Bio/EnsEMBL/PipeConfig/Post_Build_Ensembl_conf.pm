=pod 
=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION  

=head1 LICENSE
    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
         http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.
=head1 CONTACT
    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates
=cut


package Bio::EnsEMBL::PipeConfig::Post_Build_Ensembl_conf;

use strict;
use warnings;
use Data::Dumper;
use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Bio::EnsEMBL::ApiVersion;
use Cwd;

sub resource_classes {
    my ($self) = @_;
    return {'normal' => {'LSF' => '-q long -M1500 -R"select[mem>1500] rusage[mem=1500]"'},
            'mem'    => {'LSF' => '-q long -M4000 -R"select[mem>4000] rusage[mem=4000]"'}
    };
}

sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
        'user'=>undef,
        'pass'=>undef,
        'port'=>undef,
        'host'=>undef,
        'mart'=>undef,
        'compara'=>undef,
        'release'=>software_version(),
        'registry'=>undef,
        'eg_release'=>undef,
        'base_dir'=>getcwd,
        'division'=>'ensembl',
        'output_directory'=>undef,
        'pipeline_name' => 'ensembl_gene_mart_post_build_'.$self->o('release'),
        'registry'      => $self->o('registry'),
        'genomic_features_mart' => 'genomic_features_mart_'.$self->o('release'),
        'max_dropdown' => '20000',
        'template_name' => 'genes',
        'base_name' => 'gene_ensembl'
    }
}

=head2 pipeline_wide_parameters
=cut

sub pipeline_wide_parameters {
    my ($self) = @_;
    return {
        %{$self->SUPER::pipeline_wide_parameters}          # here we inherit anything from the base class, then add our own stuff
    };
}

sub pipeline_create_commands {
    my ($self) = @_;
    return [
      # inheriting database and hive tables' creation
      @{$self->SUPER::pipeline_create_commands},
      'mkdir -p '.$self->o('output_directory'),
    ];
}

=head2 pipeline_analyses
=cut
    
    sub pipeline_analyses {
        my ($self) = @_;
    my $analyses = [
        {
            -logic_name => 'generate_names',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #base_dir#/ensembl-biomart/scripts/generate_names.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -name gene_ensembl -div #division# -registry #registry#',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'registry' => $self->o('registry'),
                'base_dir' => $self->o('base_dir'),
                'eg_release' => $self->o('eg_release'),
                'division'  => $self->o('division'),
            },                    
                    -input_ids=>[{}],              
                    -analysis_capacity => 1,
                    -meadow_type => 'LOCAL',
                    -flow_into => {1 => 'dataset_factory'},
        },
        {   
            -logic_name => 'dataset_factory',
            -module => 'Bio::EnsEMBL::BioMart::DatasetFactory',
            -wait_for => 'generate_names',
            -parameters    => {
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'registry' => $self->o('registry'),
                'base_dir' => $self->o('base_dir')
            },
            -flow_into => {
                1 => ['add_compara','calculate_sequence', 'add_slims'],
                2 => ['tidy_tables','optimize']
            },
            -meadow_type => 'LOCAL'
        },        
        {
            -logic_name    => 'add_compara',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl
                #base_dir#/ensembl-biomart/scripts/add_compara.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -compara #compara# -dataset #dataset# -name gene_ensembl',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'compara' => $self->o('compara'),
                'base_dir' => $self->o('base_dir')
            },
            -analysis_capacity => 10
        },
        {
            -logic_name    => 'tidy_tables',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -wait_for => ['add_compara','calculate_sequence', 'add_slims'],
            -parameters    => {
                'cmd'        => 'perl #base_dir#/ensembl-biomart/scripts/tidy_tables.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart#',  
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'base_dir' => $self->o('base_dir')
            },
            -input_ids=>[{}],
            -analysis_capacity => 1
        },
        {
            -logic_name    => 'calculate_sequence',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #base_dir#/ensembl-biomart/scripts/calculate_sequence_data.pl -host #host# -port #port# -user #user# -pass #pass# -mart #mart# -dataset #dataset# -dataset_basename #base_name# -registry #registry#',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'registry' => $self->o('registry'),
                'base_dir' => $self->o('base_dir'),
                'base_name' => $self->o('base_name')
            },
            -rc_name          => 'mem',
            -analysis_capacity => 10
        },
        {   
            -logic_name    => 'add_slims',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #base_dir#/ensembl-biomart/scripts/generate_slim.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -name gene_ensembl -registry #registry# -release #release#',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'release' => $self->o('release'),
                'registry' => $self->o('registry'),
                'eg_release' => $self->o('eg_release'),
                'base_dir' => $self->o('base_dir')
            },
            -rc_name          => 'mem',
            -analysis_capacity => 10,
        },
        {
            -logic_name    => 'optimize',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -wait_for => ['calculate_sequence','add_slims', 'add_compara'],
            -parameters    => {
                'cmd'        => 'mysqlcheck -h#host# -u#user# -p#pass# -P#port# --optimize "#mart#"',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'base_dir' => $self->o('base_dir')
            },
            -analysis_capacity => 1
        },
        ];
    return $analyses;
}
1;
