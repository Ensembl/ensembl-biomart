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


package Bio::EnsEMBL::PipeConfig::BuildMart_conf;

use strict;
use warnings;
use Data::Dumper;
use base ('Bio::EnsEMBL::Hive::PipeConfig::HiveGeneric_conf');  # All Hive databases configuration files should inherit from HiveGeneric, directly or indirectly
use Bio::EnsEMBL::ApiVersion;
use Cwd;

sub resource_classes {
    my ($self) = @_;
    return { 'default' => { 'LSF' => '-q production-rh7' }
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
        'datasets'=>undef,
        'compara'=>undef,
        'release'=>software_version(),
        'eg_release'=>undef,
        'script_dir'=>getcwd
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


=head2 pipeline_analyses
=cut
    
    sub pipeline_analyses {
        my ($self) = @_;
    my $anal = [
        {   
            -logic_name => 'dataset_factory',
            -module => 'Bio::EnsEMBL::Mart::Pipeline::DatasetFactory',
            -parameters    => {
                'datasets' => $self->o('datasets'),
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'script_dir' => $self->o('script_dir')
            },
                    -input_ids=>[{}],              
            -flow_into => {
                1 => ['calculate_sequence','add_compara','add_xrefs', 'add_slims'],
                2 => 'tidy_tables'
            },
            -meadow_type => 'LOCAL'
        },        
        {   
            -logic_name    => 'calculate_sequence',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #script_dir#/calculate_sequence_data.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -release #release# -dataset_basename gene',
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'release' => $self->o('release'),
                'eg_release' => $self->o('eg_release'),
                'script_dir' => $self->o('script_dir')
            },
            -analysis_capacity => 10
        },
        {   
            -logic_name    => 'add_compara',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #script_dir#/add_compara.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -compara #compara# -dataset #dataset#',  
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'compara' => $self->o('compara'),
                'script_dir' => $self->o('script_dir')
            },
            -analysis_capacity => 10
        },
        {   
            -logic_name    => 'tidy_tables',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -wait_for => ['add_compara','calculate_sequence'],
            -parameters    => {
                'cmd'        => 'perl #script_dir#/tidy_tables.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart#',  
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'script_dir' => $self->o('script_dir')
            },
            -input_ids=>[{}],              
            -analysis_capacity => 1
        },
        {   
            -logic_name    => 'add_xrefs',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #script_dir#/generate_ontology_extension.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset#',  
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'release' => $self->o('release'),
                'eg_release' => $self->o('eg_release'),
                'script_dir' => $self->o('script_dir')
            },
            -analysis_capacity => 10,
            -wait_for => ['tidy_tables'],
        },        
        {   
            -logic_name    => 'add_slims',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -parameters    => {
                'cmd'        => 'perl #script_dir#/generate_slim.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset#',  
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'release' => $self->o('release'),
                'eg_release' => $self->o('eg_release'),
                'script_dir' => $self->o('script_dir')
            },
            -analysis_capacity => 10,
            -wait_for => ['tidy_tables'],
        },        
        {   
            -logic_name    => 'generate_template',
            -module        => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -meadow_type => 'LSF',
            -wait_for => ['add_xrefs','add_slims'],
            -parameters    => {
                'cmd'        => 'perl #script_dir#/generate_template.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -release #eg_release#',  
                'mart' => $self->o('mart'),
                'user' => $self->o('user'),
                'pass' => $self->o('pass'),
                'host' => $self->o('host'),
                'port' => $self->o('port'),
                'release' => $self->o('release'),
                'eg_release' => $self->o('eg_release'),
                'script_dir' => $self->o('script_dir')
            },
                    -input_ids=>[{}],              
            -analysis_capacity => 1
        }    
         
        ];
    return $anal;
}
1;
