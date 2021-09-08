=pod
=head1 NAME

=head1 SYNOPSIS

=head1 DESCRIPTION  

=head1 LICENSE
    Copyright [1999-2021] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
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

use base ('Bio::EnsEMBL::PipeConfig::GenericMart_conf');


sub default_options {
    my ($self) = @_;
    return {
        %{$self->SUPER::default_options},
        'template_name'  => 'genes',
        'max_dropdown'   => '256',
        'tables_dir'     => $self->o('base_dir') . '/ensembl-biomart/gene_mart/tables',
        'partition_size' => 1000,
        'grch37'         => 0,
        'concat_columns' => {
            'gene__main'        => [ 'stable_id_1023', 'version_1020' ],
            'transcript__main'  => [ 'stable_id_1066', 'version_1064' ],
            'translation__main' => [ 'stable_id_1070', 'version_1068' ],
        },
        'snp_tables'     => [ '_mart_transcript_variation__dm' ],
        'snp_som_tables' => [ '_mart_transcript_variation_som__dm' ],
    };
}

sub beekeeper_extra_cmdline_options {
    my $self = shift;
    return "-reg_conf " . $self->o("registry");
}

sub hive_meta_table {
    my ($self) = @_;
    return {
        %{$self->SUPER::hive_meta_table},
        'hive_use_param_stack' => 1,
    };
}

sub pipeline_wide_parameters {
    my ($self) = @_;
    return {
        %{$self->SUPER::pipeline_wide_parameters},
        mart         => $self->o('mart'),
        host         => $self->o('host'),
        port         => $self->o('port'),
        user         => $self->o('user'),
        pass         => $self->o('pass'),
        base_dir     => $self->o('base_dir'),
        registry     => $self->o('registry'),
        division     => $self->o('division'),
        species      => $self->o('species'),
        base_name    => $self->o('base_name'),
        mart_host    => $self->o('host'),
        mart_port    => $self->o('port'),
        mart_user    => $self->o('user'),
        mart_pass    => $self->o('pass'),
        mart_db_name => $self->o('mart'),
        tables_dir   => $self->o('tables_dir')

    };
}

sub pipeline_analyses {
    my ($self) = @_;
    my $analyses = [
        {
            -logic_name => 'generate_names',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -input_ids  => [ {} ],
            -parameters => {
                cmd => 'perl #base_dir#/ensembl-biomart/scripts/generate_names.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -div #division# -registry #registry#',
            },
            -flow_into  => {
                1 => 'dataset_factory'
            },
        },
        {
            -logic_name => 'dataset_factory',
            -module     => 'Bio::EnsEMBL::BioMart::DatasetFactory',
            -parameters => {},
            -flow_into  => {
                '2->A' => [
                    'calculate_sequence',
                    'add_compara',
                    'add_xrefs',
                    'add_slims',
                    'add_external_synonyms',
                    'AddExtraMartIndexesGene',
                    'AddExtraMartIndexesTranscript',
                    'AddExtraMartIndexesTranslation',
                    'ConcatStableIDColumns',
                    'SpeciesFactory'
                ],
                'A->1' => 'tidy_tables'
            },
        },
        {
            -logic_name        => 'calculate_sequence',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters        => {
                cmd => 'perl #base_dir#/ensembl-biomart/scripts/calculate_sequence_data.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -dataset_basename #base_name# -registry #registry#',
            },
            -analysis_capacity => 10,
            -rc_name           => 'low',
        },
        {
            -logic_name        => 'add_compara',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters        => {
                cmd     => 'perl #base_dir#/ensembl-biomart/scripts/add_compara.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -compara #compara# -dataset #dataset# -name #base_name#',
                compara => $self->o('compara'),
            },
            -analysis_capacity => 10
        },
        {
            -logic_name        => 'add_xrefs',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters        => {
                cmd => 'perl #base_dir#/ensembl-biomart/scripts/generate_ontology_extension.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset#',
            },
            -analysis_capacity => 10,
        },
        {
            -logic_name        => 'add_slims',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters        => {
                cmd => 'perl #base_dir#/ensembl-biomart/scripts/generate_slim.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -registry #registry# -name #base_name#',
            },
            -analysis_capacity => 10,
        },
        {
            -logic_name        => 'add_external_synonyms',
            -module            => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters        => {
                cmd => 'perl #base_dir#/ensembl-biomart/scripts/generate_external_synonym.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart# -dataset #dataset# -reg_file #registry# -basename #base_name#',
            },
            -analysis_capacity => 10,
        },
        {
            -logic_name        => 'AddExtraMartIndexesGene',
            -module            => 'Bio::EnsEMBL::VariationMart::CreateMartIndexes',
            -parameters        => {
                table => 'gene__main',
            },
            -max_retry_count   => 0,
            -analysis_capacity => 10,
        },
        {
            -logic_name        => 'AddExtraMartIndexesTranscript',
            -module            => 'Bio::EnsEMBL::VariationMart::CreateMartIndexes',
            -parameters        => {
                table => 'transcript__main',
            },
            -max_retry_count   => 0,
            -analysis_capacity => 10,
        },
        {
            -logic_name        => 'AddExtraMartIndexesTranslation',
            -module            => 'Bio::EnsEMBL::VariationMart::CreateMartIndexes',
            -parameters        => {
                table => 'translation__main',
            },
            -max_retry_count   => 0,
            -analysis_capacity => 10,
        },
        {
            -logic_name        => 'ConcatStableIDColumns',
            -module            => 'Bio::EnsEMBL::BioMart::ConcatColumns',
            -parameters        => {
                concat_columns => $self->o('concat_columns'),
            },
            -max_retry_count   => 0,
            -analysis_capacity => 10,
        },
        {
            -logic_name      => 'SpeciesFactory',
            -module          => 'Bio::EnsEMBL::Production::Pipeline::Common::SpeciesFactory',
            -parameters      => {
                species  => $self->o('species'),
                division => [ $self->o('division') ],
            },
            -max_retry_count => 0,
            -flow_into       => {
                '4' => 'CreateMartTranscriptVariationTable',
            }
        },
        {
            -logic_name        => 'CreateMartTranscriptVariationTable',
            -module            => 'Bio::EnsEMBL::VariationMart::CreateMartTables',
            -parameters        => {
                snp_tables        => $self->o('snp_tables'),
                snp_som_tables    => $self->o('snp_som_tables'),
                variation_feature => 1,
                consequences      => { '_mart_transcript_variation__dm' => 'consequence_types_2076' },
            },
            -max_retry_count   => 0,
            -analysis_capacity => 10,
            -flow_into         => {
                '1->A' => [ 'PartitionTables' ],
                'A->1' => [ 'CreateMartIndexes' ],
            },
        },
        {
            -logic_name        => 'PartitionTables',
            -module            => 'Bio::EnsEMBL::VariationMart::PartitionTables',
            -parameters        => {
                partition_size => $self->o('partition_size'),
            },
            -max_retry_count   => 0,
            -analysis_capacity => 10,
            -flow_into         => [ 'PopulateMart' ],
        },
        {
            -logic_name        => 'PopulateMart',
            -module            => 'Bio::EnsEMBL::VariationMart::PopulateMart',
            -parameters        => {},
            -max_retry_count   => 2,
            -analysis_capacity => 20,
            -rc_name           => 'himem',
        },
        {
            -logic_name        => 'CreateMartIndexes',
            -module            => 'Bio::EnsEMBL::VariationMart::CreateMartIndexes',
            -parameters        => {},
            -max_retry_count   => 2,
            -analysis_capacity => 10,
        },
        {
            -logic_name => 'tidy_tables',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters => {
                cmd => 'perl #base_dir#/ensembl-biomart/scripts/tidy_tables.pl -user #user# -pass #pass# -port #port# -host #host# -mart #mart#',
            },
            -flow_into  => [ 'optimize' ],
        },
        {
            -logic_name => 'optimize',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters => {
                cmd => 'mysqlcheck -h#host# -u#user# -p#pass# -P#port# --optimize "#mart#"',
            },
            -flow_into  => [ 'generate_meta' ],
        },
        {
            -logic_name => 'generate_meta',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters => {
                cmd                   => 'perl #base_dir#/ensembl-biomart/scripts/generate_meta.pl -user #user# -pass #pass# -port #port# -host #host# -dbname #mart# -template #template# -ds_basename #base_name# -template_name #template_name# -genomic_features_dbname #genomic_features_mart# -max_dropdown #max_dropdown# -scratch_dir #scratch_dir#',
                template              => $self->o('template'),
                template_name         => $self->o('template_name'),
                max_dropdown          => $self->o('max_dropdown'),
                genomic_features_mart => $self->o('genomic_features_mart'),
                scratch_dir           => $self->o('scratch_dir'),
            },
            -rc_name    => 'low',
            -flow_into  => [ 'run_tests' ],
        },
        {
            -logic_name => 'run_tests',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters => {
                cmd         => 'cd #test_dir#;perl #base_dir#/ensembl-biomart/scripts/pre_configuration_mart_healthcheck.pl -newuser #user# -newpass #pass# -newport #port# -newhost #host# -olduser #olduser# -oldport #oldport# -oldhost #oldhost# -new_dbname #mart# -old_dbname #old_mart# -old_rel #old_release# -new_rel #new_release# -empty_column 1 -grch37 #grch37# -mart ensembl_mart',
                oldhost     => $self->o('oldhost'),
                oldport     => $self->o('oldport'),
                olduser     => $self->o('olduser'),
                old_mart    => $self->o('old_mart'),
                old_release => $self->o('old_release'),
                new_release => $self->o('new_release'),
                grch37      => $self->o('grch37'),
                test_dir    => $self->o('test_dir'),
            },
            -flow_into  => [ 'check_tests' ],
        },
        {
            -logic_name => 'check_tests',
            -module     => 'Bio::EnsEMBL::Hive::RunnableDB::SystemCmd',
            -parameters => {
                cmd      => 'EXIT_CODE=0;failed_tests=`find #test_dir#/#old_mart#_#oldhost#_vs_#mart#_#host#.* -type f ! -empty -print`;if [ -n "$failed_tests" ]; then >&2 echo "Some tests have failed please check ${failed_tests}";EXIT_CODE=1;fi;exit $EXIT_CODE',
                oldhost  => $self->o('oldhost'),
                old_mart => $self->o('old_mart'),
                test_dir => $self->o('test_dir'),
            },
        }
    ];

    return $analyses;
}

1;
