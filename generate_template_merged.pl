#!/usr/bin/env perl
use warnings;
use strict;
use XML::Simple;
use Data::Dumper;
use DbiUtils;
use Carp;
use IO::Compress::Gzip qw(gzip);
use File::Slurp;
use Getopt::Long;
use Log::Log4perl qw(:easy);

my $mart_db = "merged_protists_mart_27";
my $db_host = "127.0.0.1";
my $db_port = 4238;
my $db_user = "ensrw";
my $db_pwd = "writ3rp1";
my $template_file = "./templates/eg_template_template.xml";
my $name = "protists";
my $verbose = 1;
my $options_okay = GetOptions (
    "host=s"=>\$db_host,
    "port=i"=>\$db_port,
    "user=s"=>\$db_user,
    "pass=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "name=s"=>\$name,
    "verbose"=>\$verbose
    );

if(defined $verbose) {
    Log::Log4perl->easy_init($DEBUG);
} else {
    Log::Log4perl->easy_init($INFO);
}
my $logger = get_logger();

my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle =
  DBI->connect($mart_string, $db_user, $db_pwd, { RaiseError => 1 })
  or croak "Could not connect to $mart_string";
my $ds_name = "${name}_eg_gene";

my $unhide = {};
if($name eq 'fungi') {
    $unhide->{pdo} = 1;
    $unhide->{so} = 1;
    $unhide->{mod} = 1;
    $unhide->{fypo} = 1;
    $unhide->{phi_extension} = 1;
} elsif($name eq 'protists') {
    $unhide->{phi_extension} = 1;
}

(my $version = $mart_db) =~ s/.*_mart_([0-9]+)/$1/;

$logger->info("Reading table list for $mart_db");
# create hash of tables to columns
my $tables = {};
# create lookup of key by table
my $keys = {};
my $sth = $mart_handle->prepare("select table_name,column_name from information_schema.columns where table_schema=?");
$sth->execute($mart_db);
while(my ($table,$col) = $sth->fetchrow_array()) {
    $col = lc $col;
    $tables->{$table}->{$col} = 1;
    if($col =~ m/[a-z]+_id_[0-9]+_key/) {
        $keys->{$table} = $col;
    }
}

my $datasets = [];
$sth = $mart_handle->prepare("select species_id_1010_key, display_name_1010, production_name_1010 from ${ds_name}__species_meta__dm");
$sth->execute();
while(my ($name,$display,$prod_name) = $sth->fetchrow_array()) {
    push @$datasets, {
        name=>$name,
        display_name=>$display,
        production_name=>$prod_name
    };
}
$datasets = [sort {$a->{display_name} cmp $b->{display_name}} @$datasets];

$logger->info("Reading template XML from $template_file");
# load in template
my $template = read_file($template_file);
my $templ = XMLin($template, KeepRoot => 1, KeyAttr => []);
my $templ_in = $templ->{DatasetConfig};

$logger->info("Building output");
my $templ_out = {};
# handle the top level scalars
# defaultDataSet
# displayName
# version
while(my ($key,$value) = each %{$templ_in}) {
    if(!ref($value)) {
        if($key eq 'defaultDataSet') {
            $value = $ds_name;
        } elsif($key eq 'displayName') {
            $value = $ds_name;
        } elsif($key eq 'description') {
            $value = 'Ensembl '.ucfirst($name).' Genes'; 
        } elsif($key eq 'version') {
            $value = $version;
        } 
        $templ_out->{$key} = $value;
    }
}

# add dynamicdataset
$templ_out->{DynamicDataset} = {
   internalName=>$ds_name,
   displayName=>"All Genomes",   
   useDefault=>"true"       
};
# add MainTable
push @{$templ_out->{MainTable}},"${ds_name}__gene__main";
push @{$templ_out->{MainTable}},"${ds_name}__transcript__main";
push @{$templ_out->{MainTable}},"${ds_name}__translation__main";
# add Key
push @{$templ_out->{Key}},'gene_id_1020_key';
push @{$templ_out->{Key}},'transcript_id_1064_key';
push @{$templ_out->{Key}},'translation_id_1068_key';

$logger->info("Processing importables");

# Importable
for my $imp (@{$templ_in->{Importable}}) {
    # replace linkVersion.*link_version* with $version
    if(defined $imp->{linkVersion}) {
        $imp->{linkVersion} =~ s/\*link_version\*/$version/;
    }
    # replace linkName.*species3*
    if(defined $imp->{linkName}) { 
        $imp->{linkName} =~ s/\*species3\*/${name}_eg/;
    } 
    # replace name.*species3* with ${name}_e
    $imp->{name} =~ s/\*species3\*/${name}_eg/;
    if($imp->{internalName} eq 'genomic_sequence') {
        $imp->{filters} .= ",species_id_key";
    }
    # push onto out stack
    push @{$templ_out->{Importable}}, $imp;
}

# Exportables

# list where we need to add species_id_key
my %species_exportables = map {$_=>1} qw/gene_exon_intron transcript_exon_intron gene_flank transcript_flank coding_gene_flank coding_transcript_flank 3utr 5utr cdna gene_exon peptide coding/;

$logger->info("Processing exportables");
for my $exp (@{$templ_in->{Exportable}}) {
    # replace linkVersion.*link_version* with $version
    if(defined $exp->{linkVersion}) {
        $exp->{linkVersion} =~ s/\*link_version\*/${version}/;
    }
    if(defined $exp->{linkName}) {
        # replace linkName.*species3*
        $exp->{linkName} =~ s/\*species3\*/${name}_eg/;
    }
    # replace name.*species3* with ${name}_eg
    $exp->{name} =~ s/\*species3\*/${name}_eg/;
    if($species_exportables{$exp->{internalName}}) {
        $exp->{attributes} .= ",species_id_key";
    }
    # push onto out stack
    push @{$templ_out->{Exportable}}, $exp;
}

# additional exporter
push @{$templ_out->{Exportable}}, 
{
    attributes=>"${ds_name}",
    default=>1,
    internalName=>"${ds_name}_stable_id",
    name=>"${ds_name}_stable_id",
    linkName=>"${ds_name}_stable_id", 
    type=>"link"
};

$logger->info("Processing filters");
# FilterPage
for my $filterPage (@{$templ_in->{FilterPage}}) {
    $logger->debug("Processing filterPage ".$filterPage->{internalName});    
    # count the number of groups we add
    my $nG = 0;
    normalise($filterPage,"FilterGroup");
    my $fpo = copy_hash($filterPage);

    if($fpo->{internalName} eq 'filters') {
        my $species_opts = [];        
        for my $dataset (@{$datasets}) {
            push @$species_opts, {
                displayName=>$dataset->{display_name},
                value=>$dataset->{production_name},
                isSelectable=>"true",
                useDefault=>"true",
                internalName=>$dataset->{name}
            };
        }
        push @{$fpo->{FilterGroup}}, {
            displayName=>"GENOME", 
            internalName=>"genome_group", 
            useDefault=>"true",
            "FilterCollection"=>[
                {
                    displayName=>"Genome", 
                    internalName=>"species", 
                    useDefault=>"true", 
                    maxSelect=>"1",
                    FilterDescription=>[
                        { 
                            displayName=>"Genome", 
                            type=>"text",
                            displayType=>"list",
                            multipleValues=>"1", 
                            style=>"taxon", 
                            field=>"species_name_1010", 
                            internalName=>"species_id_1010", 
                            key=>"species_id_1010_key", 
                            legal_qualifiers=>"=", 
                            qualifier=>"=", 
                            tableConstraint=>"main", 
                            useDefault=>"true",
                            Option=>$species_opts
                        }
                        ]
                }
                ]
        };        
        $nG++;
    }
    
    ## FilterGroup
    for my $filterGroup (@{$filterPage->{FilterGroup}}) {
        my $nC = 0;
        normalise($filterGroup,"FilterCollection");
        my $fgo = copy_hash($filterGroup);
        ### Filtercollection
        for my $filterCollection (@{$filterGroup->{FilterCollection}}) {
            my $nD = 0;
            normalise($filterCollection,"FilterDescription");
            my $fco = copy_hash($filterCollection);
            ### FilterDescription
            for my $filterDescription (@{$filterCollection->{FilterDescription}}){ 
                my $fdo = copy_hash($filterDescription);
                #### pointerDataSet *species3*
                $fdo->{pointerDataset} =~ s/\*species3\*/${name}_eg/ if defined $fdo->{pointerDataSet};
                #### SpecificFilterContent - delete
                #### tableConstraint - update          
                update_table_keys($fdo,$ds_name,$keys);
                #### if contains options, treat differently
                #### if its called homolog_filters, add the homologs here                
                if($fdo->{internalName} eq 'homolog_filters') {
                    for my $dataset (@$datasets) {
                        # <Option displayName="Orthologous $dataset->{species_name} Genes" displayType="list" field="homolog_$dataset->{dataset}_bool" hidden="false" internalName="with_$dataset->{dataset}_homolog" isSelectable="true" key="gene_id_1020_key" legal_qualifiers="only,excluded" qualifier="only" style="radio" tableConstraint="main" type="boolean"><Option displayName="Only" hidden="false" internalName="only" value="only" /><Option displayName="Excluded" hidden="false" internalName="excluded" value="excluded" /></Option>                        
                        my $field = "homolog_$dataset->{name}_bool";
                        my $table = "main";
                        if(defined $tables->{$table}->{$field}) {
                            # add in if the column exists
                            push @{$fdo->{Option}}, {
                                displayName=>"Orthologous $dataset->{display_name} Genes", 
                                displayType=>"list", 
                                field=>$field,
                                hidden=>"false", 
                                internalName=>"with_$dataset->{name}_homolog", 
                                isSelectable=>"true", 
                                key=>"gene_id_1020_key", 
                                legal_qualifiers=>"only,excluded", 
                                qualifier=>"only", 
                                style=>"radio", 
                                tableConstraint=>$table,
                                type=>"boolean", 
                                Option=>[
                                    {
                                        displayName=>"Only", hidden=>"false", internalName=>"only", value=>"only"
                                    },
                                    {
                                        displayName=>"Excluded", 
                                        hidden=>"false", 
                                        internalName=>"excluded", 
                                        value=>"excluded"
                                    }
                                    ]
                            };
                        }
                    }
                    # NOTE: ignore paralogs as we don't have a bool for it
                    push @{$fco->{FilterDescription}}, $fdo;
                    $nD++;         
                } elsif($fdo->{displayType} && $fdo->{displayType} eq 'container') {
                    my $nO = 0;
                    normalise($filterDescription,"Option");
                    for my $option (@{$filterDescription->{Option}}) {
                        my $opt = copy_hash($option);
                        update_table_keys($opt, $ds_name, $keys);                        
                        if(defined $tables->{$opt->{tableConstraint}} &&
                           defined $tables->{$opt->{tableConstraint}}->{$opt->{field}} &&
                           defined $tables->{$opt->{tableConstraint}}->{$opt->{key}}) {                            
                            push @{$fdo->{Option}}, $opt;
                            for my $o (@{$option->{Option}}) {
                                push @{$opt->{Option}}, $o;
                            }
                            $nO++;                        
                        } else {
                            $logger->info("Could not find table ".($opt->{tableConstraint}||'undef')." field ".($opt->{field}||'undef').", Key ".($opt->{key}||'undef').", Option ".$opt->{internalName});
                        }           
                        restore_main($opt,$ds_name);             
                    }
                    if($nO>0) {
                        push @{$fco->{FilterDescription}}, $fdo;
                        $nD++;     
                    }                                                               
                } else {         
                    if(defined $fdo->{tableConstraint}) {                 
                        #### check tableConstraint and field and key                        
                        if(defined $tables->{$fdo->{tableConstraint}} &&
                           defined $tables->{$fdo->{tableConstraint}}->{$fdo->{field}} &&
                           defined $tables->{$fdo->{tableConstraint}}->{$fdo->{key}}) {
                            if(defined $filterDescription->{SpecificFilterContent} && 
                               ref($filterDescription->{SpecificFilterContent}) eq 'HASH' &&
                               $filterDescription->{SpecificFilterContent}->{internalName} eq 'replaceMe') {                                
                                # get contents
                                $sth = $mart_handle->prepare("select distinct $fdo->{field} from $fdo->{tableConstraint} where $fdo->{field} is not null order by $fdo->{field}");
                                $sth->execute();
                                my $vals = [];
                                my $n = 0;
                                $logger->info("Autopopulating dropdown for $fdo->{internalName}");
                                while(my ($val) = $sth->fetchrow_array()) {
                                    push @$vals, {
                                        internalName=>$val,
                                        displayName=>$val,
                                        value=>$val,
                                        isSelectable=>'true',
                                        useDefault=>'true'
                                    };
                                    if(++$n>100) {
                                        last;
                                    }
                                }                                
                                if($n>100) {
                                    $logger->info("Too many dropdowns, changing to text");
                                    $fdo->{type} = "text";
                                    $fdo->{displayType} = "text";
                                    $fdo->{style} = undef;
                                } else {
                                    $fdo->{Option} = $vals;
                                }
                            }
                            push @{$fco->{FilterDescription}}, $fdo;
                            $nD++;                        
                        } else {
                            $logger->info("Could not find table ".($fdo->{tableConstraint}||'undef')." field ".($fdo->{field}||'undef').", Key ".($fdo->{key}||'undef').", FilterDescription ".$fdo->{internalName});
                        }
                    } else {                        
                        push @{$fco->{FilterDescription}}, $fdo;
                        $nD++;                        
                    }
                    restore_main($fdo,$ds_name);
                }
            }             
            if($nD>0) {
                push @{$fgo->{FilterCollection}}, $fco;
                $nC++;
            }
        }

        if($nC>0) {
            push @{$fpo->{FilterGroup}},$fgo;
            $nG++;
            if(defined $fgo->{hidden} && $fgo->{hidden} eq "true" && defined $unhide->{$fgo->{internalName}}) {
                $fgo->{hidden} = "false";                
            }
        }
    }
    if($nG>0) {
        push @{$templ_out->{FilterPage}},$fpo;
    }
}        

$logger->info("Processing attributes");
# AttributePage
for my $attributePage (@{$templ_in->{AttributePage}}) {
    $logger->debug("Processing filterPage ".$attributePage->{internalName});    
    # count the number of groups we add
    my $nG = 0;
    normalise($attributePage,"AttributeGroup");
    my $apo = copy_hash($attributePage);

    if($apo->{internalName} eq 'feature_page') {
        push @{$apo->{AttributeGroup}}, 
            {
                displayName=>"GENOME",                
                internalName=>"genome_attributes",
                useDefault=>"true",
                AttributeCollection=>[
                    {
                        displayName=>"Species", 
                        internalName=>"species", 
                        useDefault=>"true",
                        AttributeDescription=>[
                            {
                                displayName=>"Internal Name", 
                                field=>"species_id_1010_key", 
                                internalName=>"species_id_key", 
                                key=>"species_id_1010_key", 
                                tableConstraint=>"main", 
                                useDefault=>"true", 
                                hideDisplay=>"true"
                            },
                            {
                                displayName=>"Production Name", 
                                field=>"species_name_1010", 
                                internalName=>"species_name_1010", 
                                key=>"species_id_1010_key", 
                                tableConstraint=>"main", 
                                useDefault=>"true", 
                                hideDisplay=>"true"
                            },
                            {
                                displayName=>"Genome name", 
                                field=>"display_name_1010", 
                                internalName=>"display_name_1010", 
                                key=>"species_id_1010_key",
                                maxLength=>"100", 
                                tableConstraint=>"${ds_name}__species_meta__dm", 
                                useDefault=>"true", 
                                linkoutURL=>"exturl|/%s|species_name_1010",
                                default=>"true"
                            },
                            {
                                displayName=>"Taxonomy ID", 
                                field=>"taxonomy_id_1010", 
                                internalName=>"taxonomy_id_1010", 
                                key=>"species_id_1010_key", 
                                maxLength=>"11", 
                                tableConstraint=>"${ds_name}__species_meta__dm", 
                                useDefault=>"true",
                                linkoutURL=>"http://www.uniprot.org/taxonomy/%s",
                            },
                            {
                                displayName=>"Assembly accession", 
                                field=>"assembly_accession_1010", 
                                internalName=>"assembly_accession_1010", 
                                key=>"species_id_1010_key",
                                maxLength=>"20", 
                                tableConstraint=>"${ds_name}__species_meta__dm", 
                                useDefault=>"true", 
                                linkoutURL=>"http://www.ebi.ac.uk/ena/data/view/%s"
                            }                            
                            ]
                    }
                    ]            
        };
        $nG++;                
    }
        
    ## AttributeGroup
    for my $attributeGroup (@{$attributePage->{AttributeGroup}}) {
        my $nC = 0;
        normalise($attributeGroup,"AttributeCollection");
        my $ago = copy_hash($attributeGroup);
        #### add the homologs here
        if($ago->{internalName} eq 'orthologs') {

            for my $dataset (@$datasets) {
                my $table = "${ds_name}__homolog_$dataset->{name}__dm";
                if(defined $tables->{$table}) {
                    push @{$ago->{AttributeCollection}}, {
                        displayName=>"$dataset->{display_name} Orthologues",
                        internalName=>"homolog_$dataset->{name}",
                        AttributeDescription=>[
                            {
                                displayName=>"$dataset->{display_name} gene stable ID",
                                field=>"stable_id_4016_r2",
                                internalName=>"$dataset->{name}_gene",
                                key=>"gene_id_1020_key",
                                linkoutURL=>"exturl1|/$dataset->{production_name}/Gene/Summary?g=%s",
                                maxLength=>"128",
                                tableConstraint=>$table
                            },                                    
                            {
                                displayName=>"$dataset->{display_name} protein stable ID",
                                field=>"stable_id_4016_r3",
                                internalName=>"$dataset->{name}_homolog_ensembl_peptide",
                                key=>"gene_id_1020_key",
                                maxLength=>"128",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"$dataset->{display_name} chromosome/scaffold",
                                field=>"chr_name_4016_r2",
                                internalName=>"$dataset->{name}_chromosome",
                                key=>"gene_id_1020_key",
                                maxLength=>"40",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"$dataset->{display_name} start (bp)",
                                field=>"chr_start_4016_r2",
                                internalName=>"$dataset->{name}_chrom_start",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"$dataset->{display_name} end (bp)",
                                field=>"chr_end_4016_r2",
                                internalName=>"$dataset->{name}_chrom_end",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"Representative protein or transcript ID",
                                field=>"stable_id_4016_r1",
                                internalName=>"homolog_$dataset->{name}__dm_stable_id_4016_r1",
                                key=>"gene_id_1020_key",
                                maxLength=>"128",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"Ancestor",
                                field=>"node_name_40192",
                                internalName=>"$dataset->{name}_homolog_ancestor",
                                key=>"gene_id_1020_key",
                                maxLength=>"40",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"Homology type",
                                field=>"description_4014",
                                internalName=>"$dataset->{name}_orthology_type",
                                key=>"gene_id_1020_key",
                                maxLength=>"25",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"% identity",
                                field=>"perc_id_4015",
                                internalName=>"$dataset->{name}_homolog_perc_id",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"$dataset->{display_name} % identity",
                                field=>"perc_id_4015_r1",
                                internalName=>"$dataset->{name}_homolog_perc_id_r1",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"dN",
                                field=>"dn_4014",
                                internalName=>"$dataset->{name}_homolog_ds",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"dS",
                                field=>"ds_4014",
                                internalName=>"$dataset->{name}_homolog_dn",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"Orthology confidence [0 low, 1 high]",
                                field=>"is_tree_compliant_4014",
                                internalName=>"$dataset->{name}_homolog_is_tree_compliant",
                                key=>"gene_id_1020_key",
                                maxLength=>"10",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"Bootstrap/Duplication Confidence Score Type",
                                field=>"tag_4060",
                                hidden=>"true",
                                internalName=>"homolog_$dataset->{name}__dm_tag_4060",
                                key=>"gene_id_1020_key",
                                maxLength=>"50",
                                tableConstraint=>$table
                            },
                            {
                                displayName=>"Bootstrap/Duplication Confidence Score",
                                field=>"value_4060",
                                hidden=>"true",
                                internalName=>"homolog_$dataset->{name}__dm_value_4060",
                                key=>"gene_id_1020_key",
                                maxLength=>"255",
                                tableConstraint=>$table
                            }
                            ]
                            };
                    $nC++;                                                
                }
            }
        }elsif($ago->{internalName} eq 'paralogs') {
            ## TODO            
            #push @{$aco->{AttributeDescription}}, $ado;
            #$nD++;                                                
        }elsif($ago->{internalName} eq 'homeologs') {                    
            # ignore for the moment
            #push @{$aco->{AttributeDescription}}, $ado;
            #$nD++;                                                
        } else {         
            
            ### Attributecollection
            for my $attributeCollection (@{$attributeGroup->{AttributeCollection}}) {
                my $nD = 0;
                normalise($attributeCollection,"AttributeDescription");
                my $aco = copy_hash($attributeCollection);
                ### AttributeDescription
                for my $attributeDescription (@{$attributeCollection->{AttributeDescription}}){ 
                    my $ado = copy_hash($attributeDescription);
                    #### pointerDataSet *species3*
                    $ado->{pointerDataset} =~ s/\*species3\*/${name}_eg/ if defined $ado->{pointerDataSet};
                    #### SpecificAttributeContent - delete
                    #### tableConstraint - update          
                    update_table_keys($ado,$ds_name,$keys);
                    #### if contains options, treat differently
                    if(defined $ado->{tableConstraint}) {                                      
                        if($ado->{tableConstraint} =~ m/__dm$/) {
                            $ado->{key} = $keys->{$ado->{tableConstraint}}||$ado->{key};
                        }
                         #### check tableConstraint and field and key                        
                         if(defined defined $tables->{$ado->{tableConstraint}} &&
                            defined $tables->{$ado->{tableConstraint}}->{$ado->{field}} &&
                            defined $tables->{$ado->{tableConstraint}}->{$ado->{key}}) {
                             push @{$aco->{AttributeDescription}}, $ado;
                             $nD++;                        
                         } else {
                             $logger->info("Could not find table ".($ado->{tableConstraint}||'undef')." field ".($ado->{field}||'undef').", Key ".($ado->{key}||'undef').", AttributeDescription ".$ado->{internalName});
                         }
                     } else {                       
                         $ado->{pointerDataset} =~ s/\*species3\*/${name}_eg/g if defined $ado->{pointerDataset};
                         push @{$aco->{AttributeDescription}}, $ado;
                         $nD++;                        
                     }
                     if(defined $ado->{linkoutURL}) {
                         if($ado->{linkoutURL} =~ m/exturl|\/\*species2\*/) {
                             # reformat to add URL placeholder
                             $ado->{linkoutURL} =~ s/\*species2\*/%s/;
                             $ado->{linkoutURL} .= '|species_name_1010|'.$ado->{internalName};
                         }
                     }
                     restore_main($ado,$ds_name);                   
                 }            
                 if($nD>0) {
                     push @{$ago->{AttributeCollection}}, $aco;
                     $nC++;
                 }
             }
         }
         if($nC>0) {
             push @{$apo->{AttributeGroup}},$ago;
             $nG++;
         }        
     }

     if($apo->{internalName} eq 'homologs') {

         unshift @{$apo->{AttributeGroup}}, {
             displayName=>"SPECIES", 
             internalName=>"homologs_species_attributes", 
             useDefault=>"true",
             AttributeCollection=>[
                 {
                     displayName=>"Species",
                     internalName=>"homologs_species",
                     useDefault=>"true",
                     AttributeDescription=>[
                         {internalName=>"homologs_production_name_1010", pointerAttribute=>"production_name_1010", pointerDataset=>"${ds_name}", pointerInterface=>"default"},
                         {internalName=>"homologs_display_name_1010", pointerAttribute=>"display_name_1010", pointerDataset=>"${ds_name}", pointerInterface=>"default"},
                         {internalName=>"homologs_taxonomy_id_1010", pointerAttribute=>"taxonomy_id_1010", pointerDataset=>"${ds_name}", pointerInterface=>"default"},
                         {internalName=>"homologs_assembly_accession_1010", pointerAttribute=>"assembly_accession_1010", pointerDataset=>"${ds_name}", pointerInterface=>"default"},
                         {internalName=>"homologs_species_name_1010", pointerAttribute=>"species_name_1010", pointerDataset=>"${ds_name}", pointerInterface=>"default"},
                         {internalName=>"homologs_nematode_clade_1010", pointerAttribute=>"nematode_clade_1010", pointerDataset=>"${ds_name}", pointerInterface=>"default"}
                         ]
                 }
                 ]
         };
         $nG++;
     }

     if($nG>0) {
         $apo->{outFormats} =~ s/,\*mouse_formatter[123]\*//g if defined $apo->{outFormats};
         push @{$templ_out->{AttributePage}},$apo;
     }
 }        

 my $dataset_xml = XMLout({DatasetConfig=>$templ_out}, KeepRoot=>1);

 open my $out, ">", "tmp.xml";
 print $out $dataset_xml;
 close $out;

 $logger->info("Writing XML to database");
 # populate metatables
 write_metatables($mart_handle, $ds_name, $template, $dataset_xml);
 $logger->info("Completed writing XML to database");

 sub copy_hash {
     my ($in) = @_;
     my $out = {};
     while(my ($k,$v) = each %$in) {
         if($k eq 'key' || $k eq 'field' || $k eq 'tableConstraint') {
             $v = lc $v;
         }
         if(!ref($v)) {
             $out->{$k} = $v;            
         }
     }
     return $out;
 }

sub normalise {
    my ($hash,$key)  = @_;
    $hash->{$key} = [$hash->{$key}] unless ref($hash->{$key}) eq 'ARRAY';
    return;
}

sub update_table_keys {
    my ($obj,$ds_name,$keys) = @_;
    if(defined $obj->{tableConstraint}) {
        if($obj->{tableConstraint} eq 'main') {
            if($obj->{key} eq 'gene_id_1020_key') {
                $obj->{tableConstraint} = "${ds_name}__gene__main";
            } elsif($obj->{key} eq 'transcript_id_1064_key') {
                $obj->{tableConstraint} = "${ds_name}__transcript__main";
            } elsif($obj->{key} eq 'translation_id_1068_key') {
                $obj->{tableConstraint} = "${ds_name}__translation__main";
            }
        } else {
            $obj->{tableConstraint} = "${ds_name}__".$obj->{tableConstraint} if(defined $obj->{tableConstraint});
        }
        # for dimension tables, correct the key
        if(defined $keys->{$obj->{tableConstraint}} && $obj->{tableConstraint} =~ m/.*__dm$/) {
            $obj->{key} = $keys->{$obj->{tableConstraint}};
        }                    
    }
    return;
}

sub restore_main {
    my ($obj,$ds_name) = @_;
    if(defined $obj->{tableConstraint}) {
        if($obj->{tableConstraint} eq "${ds_name}__gene__main" || $obj->{tableConstraint} eq "${ds_name}__transcript__main" || $obj->{tableConstraint} eq "${ds_name}__translation__main") {
            $obj->{tableConstraint} = 'main';
        }
    }
    return;
}

sub write_metatables {
    my ($mart_handle, $ds_name, $template_xml, $dataset_xml, $outdir) = @_;
    my $template_name = "gene";
    my $speciesId = 1;

    $logger->info("Creating meta tables");

    # create tables
    create_metatable($mart_handle,'meta_version__version__main',
		     ['version varchar(10) default NULL']);
    $mart_handle->do("INSERT INTO meta_version__version__main VALUES ('0.7')");

    # template tables
    create_metatable($mart_handle,'meta_template__template__main',
		     ['dataset_id_key int(11) NOT NULL',
		      'template varchar(100) NOT NULL']);

    $mart_handle->do("INSERT INTO meta_template__template__main VALUES($speciesId,'$template_name')");

    create_metatable($mart_handle,'meta_template__xml__dm',
		     ['template varchar(100) default NULL',
		      'compressed_xml longblob',
		      'UNIQUE KEY template (template)']);

    ## meta_template__xml__dm
    my $gzip_template;
    gzip \$template_xml => \$gzip_template;

    $sth = $mart_handle->prepare('INSERT INTO meta_template__xml__dm VALUES (?,?)');
    $sth->execute($template_name, $gzip_template) 
		  or croak "Could not load XML into meta_template__xml__dm";
    $sth->finish();

    create_metatable($mart_handle,'meta_conf__dataset__main',[ 
			 'dataset_id_key int(11) NOT NULL',
			 'dataset varchar(100) default NULL',
			 'display_name varchar(200) default NULL',
			 'description varchar(200) default NULL',
			 'type varchar(20) default NULL',
			 'visible int(1) unsigned default NULL',
			 'version varchar(128) default NULL',
			 'modified timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP',
			 'UNIQUE KEY dataset_id_key (dataset_id_key)']);
    
    $sth = $mart_handle->prepare("INSERT INTO meta_conf__dataset__main(dataset_id_key,dataset,display_name,description,type,visible,version) VALUES(?,?,?,?,'TableSet',1,?)");
    $sth->execute(        $speciesId,
        $ds_name,
        'Ensembl '.ucfirst($name)." $version",
        'Ensembl '.ucfirst($name)." $version",
        $version);
    $sth->finish();

    # dataset tables
    create_metatable($mart_handle,'meta_conf__xml__dm',
		     ['dataset_id_key int(11) NOT NULL',
		      'xml longblob',
		      'compressed_xml longblob',
		      'message_digest blob',
		      'UNIQUE KEY dataset_id_key (dataset_id_key)']);
    $sth = $mart_handle->prepare('INSERT INTO meta_conf__xml__dm VALUES (?,?,?,?)');
    my $gzip_dataset_xml;
    gzip \$dataset_xml => \$gzip_dataset_xml;
    
    $sth->execute($speciesId,
                  $dataset_xml,
                  $gzip_dataset_xml,
                  'NULL'
        ) or croak "Could not update meta_conf__xml__dm";
    $sth->finish();
 
    create_metatable($mart_handle,'meta_conf__user__dm',
		     ['dataset_id_key int(11) default NULL',
		      'mart_user varchar(100) default NULL',
		      'UNIQUE KEY dataset_id_key (dataset_id_key,mart_user)']);
    $mart_handle->do("INSERT INTO meta_conf__user__dm VALUES($speciesId,'default')");

    create_metatable($mart_handle,'meta_conf__interface__dm',
		     ['dataset_id_key int(11) default NULL',
		      'interface varchar(100) default NULL',
		      'UNIQUE KEY dataset_id_key (dataset_id_key,interface)']);
    $mart_handle->do("INSERT INTO meta_conf__interface__dm VALUES($speciesId,'default')");

    $logger->info("Population complete");
}

sub create_metatable {
    my ($db_handle,$table_name,$cols) = @_;
    drop_and_create_table($db_handle,$table_name,$cols,"ENGINE=MyISAM DEFAULT CHARSET=latin1");
}
