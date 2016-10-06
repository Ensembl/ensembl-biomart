package EnsemblMart::TableNameUtils;

=head1 NAME

 EnsemblMart::TableNameUtils

=head1 SYNOPSIS

  use EnsemblMart::TableNameUtils

=head1 DESCRIPTION 

A collection of small, exportable functions to share among scripts to facilitate the naming of database tables and fields.

=head1 Canonicalized Mart Table Names

This is subject to change, but the upshot is that changes to this module should propogate changes seamlessly to all production scripts,

central               <species><focus>_main                          : A main table.  All dimension tables hang off a main table
central_satellite     <species><focus><satellite_type>_dm            : A dimension table off of a main table
lookup                <species><type>_lookup                         : A lookup table, which facilitates the API
map                   <species><focus><satellite_type>_map           : A table that maps a dimension table to one or more satellite_type support tables
support               <species><satellite_type><extrainfo>_support   : A support table with some extrainfo. Note, satellite_type is optional
meta                  _meta_<type>                                   : A meta table
interim               <species><focus>_interim                       : A table which is used through a full production build, and then deleted

=head1 Function Names

Because exportable functions can cause confusion, the functions in this library should be named in a consistent manner, 
using utils_{name}, so that client code can easily document that all 'utils' functions emanate from this module.

=head1 AUTHOR

  Darin London - dlondon@ebi.ac.uk

=head1 COPYRIGHT

GRL/EBI

=head1 CVS

 $Log: TableNameUtils.pm,v $
 Revision 1.14  2006/11/23 16:18:16  rh4
 Updated to 42.

 Revision 1.13  2006/09/21 14:38:21  arek
 configuration for 41

 Revision 1.12  2006/08/28 15:41:34  bb2
 changed naming of dmmain tables into _dmmain - then it be processed  by table_rename_ben.pl

 Revision 1.11  2006/08/28 14:09:11  bb2
 added a new subroutine for naming a _dm table a _main
  'utils_gen_dimensionmain_table_name'
 used by homologs_ben.pl - example :
 dataset 1 :human_main
 dataset 2: human_homologs_mouse_main (instead of dm)

 Revision 1.10  2006/07/24 13:27:26  arek
 added new species

 Revision 1.9  2006/07/23 07:40:42  arek
 added savignyi

 Revision 1.8  2006/05/24 09:45:21  arek
 fugu -> takifugu

 Revision 1.7  2005/11/22 21:09:29  arek
 changes for 36

 Revision 1.6  2005/10/25 16:18:54  dlondon
 new species monodelphis

 Revision 1.5  2005/04/19 13:21:56  arek
 ciona fix

 Revision 1.4  2005/04/19 12:01:47  arek
 changes for the release 31

 Revision 1.3  2005/02/15 17:49:26  arek
 yeast

 Revision 1.2  2005/01/17 13:32:48  arek
 updates for 28, added xenopus

 Revision 1.1  2005/01/16 23:34:59  arek
 removing dependency on ensembl-mart

 Revision 1.6  2004/12/04 19:08:05  arek
 hey, found yet another place where you need to set species, great! ;-)

 Revision 1.5  2004/09/08 14:33:16  dlondon
 fixes for 24 release, new species

 Revision 1.4  2004/05/10 11:55:50  dkeefe
 added gallus

 Revision 1.3  2004/04/27 12:03:45  dlondon
 added hardcoded ptroglodytes -> pan_troglodytes hash for species name conversion, may need to refactor this to do a db lookup

 Revision 1.2  2003/09/08 12:41:56  dkeefe
 changed package from Production to EnsemblMart

 Revision 1.1  2003/09/08 10:04:35  dkeefe
 put back in API directory cos its used by MartInfo.pm

 Revision 1.1  2003/09/03 13:27:23  dkeefe
 moved to mart-build directory in repository

 Revision 1.2  2003/08/05 12:43:49  dkeefe
 prepended '_' to meta in method utils_gen_meta_table_name()

 Revision 1.1  2003/05/02 16:51:46  dlondon
 Moved EnsemblMart::TableFiller module to Production::TableFiller
 to separate production utilities from UI utilities.
 Production::TableNameUtils has a number of utilities used by the production scripts to generate table names, abbreviate species, etc.  It is not object oriented.  It also has old to new and new to old table name mappings.


=cut

use Exporter;
use strict;
use vars qw(@ISA @EXPORT);
@ISA = qw(Exporter);
@EXPORT = qw(&utils_abbrev_species
             &utils_full_species
             &utils_gen_central_table_name
             &utils_gen_dimension_table_name
	     &utils_gen_dimensionmain_table_name
             &utils_gen_map_table_name
             &utils_gen_lookup_table_name
             &utils_gen_support_table_name
             &utils_gen_meta_table_name
             &utils_gen_interim_table_name
             &utils_get_martfocus_for_dataset
             &utils_get_dataset_for_martfocus
             &utils_get_old_tablename_from_new
             &utils_get_new_tablename_from_old);

=head1 _%dataset_mart

  contains mappings between the ensembl core datasets (core, estgene, vega) to mart central table stubs

=cut

# support hashes
my %_dataset_martfocus = ('core' => ({
                                       'gene' => 'ensemblgene',
                                       'transcript' => 'ensembltranscript'
                                     }), 
                          'estgene' => ({
                                         'gene' => 'estgene',
                                         'transcript' => 'esttranscript'
                                       }), 
                          'vega' => ({
                                         'gene' => 'vegagene',
                                         'transcript' => 'vegatranscript'
									 }),
                          'snp' => ({
                                       # hack, because there is no switching for snp
                                       1 => 'snp'
									})
					      );

my %_martfocus_dataset = ( 'ensemblgene' => 'core_gene',
                           'ensembltranscript' => 'core_transcript',
                           'estgene' => 'estgene_gene',
                           'esttranscript' => 'estgene_transcript',
                           'vegagene' => 'vega_gene',
                           'vegatranscript' => 'vega_transcript',
                           # hack for allowing exploratory calls
                           'snp' => 'snp'
                          );

my %_abbrv_long = ('hsapiens'       => 'homo_sapiens',
                   'mmusculus'      => 'mus_musculus',
                   'rnorvegicus'    => 'rattus_norvegicus',
                   'frubripes'      => 'takifugu_rubripes',
                   'drerio'         => 'danio_rerio',
                   'agambiae'       => 'anopheles_gambiae',
                   'dmelanogaster'  => 'drosophila_melanogaster',
                   'celegans'       => 'caenorhabditis_elegans',
                   'cbriggsae'      => 'caenorhabditis_briggsae',
                   'ggallus'        => 'gallus_gallus',
                   'ptroglodytes'   => 'pan_troglodytes',
                   'amellifera'     => 'apis_mellifera',
                   'tnigroviridis'  => 'tetraodon_nigroviridis',
                   'cfamiliaris'    => 'canis_familiaris',
                   'scerevisiae'    => 'saccharomyces_cerevisiae',
                   'cintestinalis' =>    'ciona_intestinalis',
                   'csavignyi' =>    'ciona_savignyi',
                   'xtropicalis'    => 'xenopus_tropicalis',
                   'mdomestica'   => 'monodelphis_domestica',
		   'mdomestica'   => 'monodelphis_domestica',
                   'aaegypti'    => 'aedes_aegypti',
		   'dnovemcinctus'    => 'dasypus_novemcinctus',
		   'etelfairi'    => 'echinops_telfairi',
		   'gaculeatus'    => 'gasterosteus_aculeatus',
		   'lafricana'    => 'loxodonta_africana',
		   'ocuniculus'    => 'oryctolagus_cuniculus',
                    'olatipes'    => 'oryzias_latipes',
                    'mmulatta'   => 'macaca_mulatta',
                    'oanatinus'   => 'ornithorhynchus anatinus'
);

=head1 utils_abbrev_species

 Arguments: genus_species
 Function:  canonicalizes species names to abbreviatedgenus_species
 returns:   g_species

=cut

sub utils_abbrev_species {
    my $species = shift;
    my ($genus_name, $species_name) = $species =~ m/(\w+)_+(\w+)/;
    return join("", substr($genus_name, 0, 1), $species_name);
}

=head1 utils_full_species

 Arguments: abbreviated_species
 Function: returns the full genus_species name for an abbreviated species
 Returns: genus_species

=cut

sub utils_full_species {
    my $species = shift;
    return $_abbrv_long{$species};
}

=head1 utils_gen_central_table_name

 Arguments: genus_species, dataset, focus
 Functions: creates a canonicalized mart central table name for the given species, and martfocus for the given dataset
             implements switching for gene <-> transcript main tables
 returns: mart_central_table_name

=cut

sub utils_gen_central_table_name {
    my ($species, $dataset, $focus) = @_;
    $focus = 1 if ($dataset eq 'snp'); 
    return join("_", &utils_abbrev_species($species), $_dataset_martfocus{$dataset}->{$focus}, qq(main));
}

=head1 utils_gen_dimension_table_name

 Arguments: genus_species, dataset, dimension
 Function: creates a canonicalized mart dimension table name given the species, dataset, and dimension
 Returns: mart_dimension_table_name

=cut

sub utils_gen_dimension_table_name {
    my ($species, $dataset, $focus, $dimension) = @_;
    $focus = 1 if ($dataset eq 'snp');
    return join("_", &utils_abbrev_species($species), $_dataset_martfocus{$dataset}->{$focus}, $dimension, qq(dm));
}

#---------------ben
=head1 utils_gen_dimensionmain_table_name

 Arguments: genus_species, dataset, dimension
 Function: creates a canonicalized mart main (pseudo dimension) table name given the species, dataset, and dimension
 Returns: mart_dimensionmain_table_name

=cut

sub utils_gen_dimensionmain_table_name {
    my ($species, $dataset, $focus, $dimension) = @_;
    $focus = 1 if ($dataset eq 'snp');
    return join("_", &utils_abbrev_species($species), $_dataset_martfocus{$dataset}->{$focus}, $dimension, qq(dmmain));
}
#----------------ben

=head1 utils_gen_map_table_name

 Arguments: genus_species, dataset, dimension
 Function: generates a map tablename for the given species, dataset, and dimension
 Returns: map_table_name

=cut

sub utils_gen_map_table_name {
    my ($species, $dataset, $focus, $dimension) = @_;
    $focus = 1 if ($dataset eq 'snp');
    return join("_", &utils_abbrev_species($species), $_dataset_martfocus{$dataset}->{$focus}, $dimension, qq(map));
}

=head1 utils_gen_lookup_table_name

 Arguments: species, type
 Function: creates a lookup table name of the given type
           lookup tables are more free form in name than other tables, but watch for abuse of the use of the input array
 Returns: mart_lookup_table_name

=cut

sub utils_gen_lookup_table_name {
    return join("_", @_, q(lookup));
}

=head1 utils_gen_support_table_name

 Arguments: genus_species, dimension, extrainfo
 Function: generates a support table for the given species and otpional dimension for the given extrainfo.
           note, dimension is made optional by placing everything after the species into an array.  There 
           are obviously ways to abuse this.
 Returns: mart_support_table_name

=cut

sub utils_gen_support_table_name {
    my ($species, @extrainfo) = @_;
    return join("_", (&utils_abbrev_species($species), @extrainfo, qq(support)));
}

=head1 utils_gen_meta_table_name

 Arguments: type
 Function: generates a meta table name for the given description parameters
           _meta table names are loosely defined, but watch for abuse of the array splitting
 Returns: mart_meta_table_name

=cut

sub utils_gen_meta_table_name {
    return join("_", q(_meta), @_);
}

=head1 utils_gen_interim_table_name

 Arguments: genus_species, extrainfo
 Function: generates an interim table name with the provided info. 
           If species is defined, the name will contain the canonical species name.
           Note: this can be used (and abused) for many types of interim tables.
 Returns: mart_interim_table

=cut

sub utils_gen_interim_table_name {
    my ($species, @info) = @_;
    if ($species) {
        # want a species specific interim
        return join("_", &utils_abbrev_species($species), @info, qq(interim));
	}
    return join("_", @info, qq(interim));
}

=head1 utils_get_martfocus_for_dataset

 Arguments: dataset, focus
 Function: returns the mart focus name for the given dataset
 Returns: mart_focus

=cut

sub utils_get_martfocus_for_dataset {
    my ($dataset, $focus) = @_;
    $focus = 1 if ($dataset eq 'snp');
    return $_dataset_martfocus{$dataset}->{$focus};
}

=head1 utils_get_dataset_for_martfocus

 Arguments: martfocus
 Function: returns the core dataset for the given martfocus
 returns: dataset

=cut

sub utils_get_dataset_for_martfocus {
    my $martfocus = shift;
    return $_martfocus_dataset{$martfocus};
}

=head1 utils_get_old_tablename_from_new

  Arguments: newtablename
  Function: maps a new mart table name to the old style
  returns: oldtablename

=cut

sub utils_get_old_tablename_from_new {
   my $newtable = shift;
   my $oldtable;

   # special table name issues
   if ($newtable =~ m/meta/o) {
       # the meta remains the same
       $oldtable = $newtable;
   }
   elsif ($newtable =~ m/dna_chunks/o) {
       # sgp_chunks in old
       ($oldtable = $newtable) =~ s{
                                       ([a-z]+) # species
                                        _{1,1}
                                        [a-zA-Z_]+ # satellite type ignored
                                        _{1,1}
                                        support
                                    }{
                                       my $dataset = (split /_/, &utils_get_dataset_for_martfocus($2))[0]; # only want the first part
                                       sprintf("%s_sgp_chunks", &utils_full_species($1));
                                    }ex;
   }
   elsif ($newtable =~ m/assembly/o) {
       # static golden path in old
       ($oldtable = $newtable) =~ s{
                                      ([a-z]+) # species
                                        _{1,1}
                                        [a-zA-Z_]+ # satellite type ignored
                                        _{1,1}
                                        support
                                   }{
                                       my $dataset = (split /_/, &utils_get_dataset_for_martfocus($2))[0]; # only want the first part
                                       sprintf("%s_static_golden_path", &utils_full_species($1));
                                   }ex;
   }
   elsif ($newtable =~ m/marker/o) {
       # location in old
       ($oldtable = $newtable) =~ s{
                                       ([a-z]+) # species
                                        _{1,1}
                                        [a-zA-Z_]+ # satellite type ignored
                                        _{1,1}
                                        lookup
                                   }{
                                       my $dataset = (split /_/, &utils_get_dataset_for_martfocus($2))[0]; # only want the first part
                                       sprintf("%s_location", &utils_full_species($1));
                                   }ex;
   }
   elsif ($newtable =~ m/expression/o) {
       # expression table does not have a clean mapping
       if ($newtable =~ m/dm$/o) {
           ($oldtable = $newtable) =~ s{
                                           ([a-z]+) # species
                                            _{1,1}
                                            ([a-z]+) # focus
                                            _{1,1}
                                            ([a-zA-Z_]+) # satellite type
                                            dm
                                       }{
                                           my $dataset = (split /_/, &utils_get_dataset_for_martfocus($2))[0]; # only want the first part
                                           sprintf("%s_%s_%s", &utils_full_species($1), $dataset, $3);
                                       }ex;
	   }
       elsif ($newtable =~ m/support$/o) {
           warn "expression support tables do not exactly map to old expression tables, as they were copied exactly across datasets in old system.\nReplace DATASET with each dataset for the species.\n";
           ($oldtable = $newtable) =~ s{
                                           ([a-z]+) # species
                                            _{1,1}
                                           ([a-zA-Z_]+) # satellite type
                                           _{1,1}
                                           support
                                       }{
                                           sprintf("%s_%s_%s", &utils_full_species($1), q(DATASET), $2)
                                       }ex;
	   }
       elsif ($newtable =~ m/map$/o) {
           ($oldtable = $newtable) =~ s{
                                           ([a-z]+) # species
                                            _{1,1}
                                            ([a-z]+) # focus
                                            _{1,1}
                                            ([a-zA-Z_]+) # satellite type
                                            _{1,1}
                                            map
                                       }{
                                           my $dataset = (split /_/, &utils_get_dataset_for_martfocus($2))[0]; # only want the first part
                                           sprintf("%s_%s_%s", &utils_full_species($1), $dataset, $3);
                                       }ex;
	   }
       else {
          warn("unknown expression table $newtable\n");
          return undef;
       }
   }
   elsif ($newtable =~ m/homologs/o) {
       # homolog other_species doesnt have a clean mapping
       ($oldtable = $newtable) =~ s{
                                        ([a-z]+) # species
                                        _{1,1}
                                        ([a-z]+) # focus
                                        _{1,1}
                                        homologs
                                        _{1,1}
                                        ([a-zA-Z_]+) # otherspecies
                                        _{1,1}
                                        dm
                                   }{
                                       my $otherspecies = $3;
                                       substr($otherspecies, 1) = "_".substr($otherspecies ,1); # xy to x_y
                                       sprintf("%s_%s_homologs_%s", &utils_full_species($1), &utils_get_dataset_for_martfocus($2), $otherspecies)
                                   }ex;
   }
   elsif ($newtable =~ m/effect/o) {
       # snp effect table doesnt have a clean mapping
       ($oldtable = $newtable) =~ s{
                                        ([a-z]+) # species
                                        _{1,1}
                                        snp
                                        _{1,1}
                                        ([a-z]+) # transcript focus effected
                                        _{1,1}
                                        effect_dm
                                   }{
                                       sprintf("%s_%s_snp_effect", &utils_full_species($1), (split "_", &utils_get_dataset_for_martfocus($2))[0]);
                                   }ex;
   }
   elsif ($newtable =~ m/main$/o) {
       ($oldtable = $newtable) =~ s{
                                        ([a-z]+) # species
                                        _{1,1}
                                        ([a-z]+) # focus
                                        _{1,1}
                                        main
                                   }{
                                       sprintf("%s_%s", &utils_full_species($1), &utils_get_dataset_for_martfocus($2))
                                   }ex;
   }
   elsif ($newtable =~ m/dm$/o) {
       ($oldtable = $newtable) =~ s{
                                        ([a-z]+) # species
                                        _{1,1}
                                        ([a-z]+) # focus
                                        _{1,1}
                                        (\w+) # satellite type
                                        _{1,1}
                                        dm
                                   }{
                                       sprintf("%s_%s_%s", &utils_full_species($1), &utils_get_dataset_for_martfocus($2), $3)
                                   }ex;
   }
   elsif ($newtable =~ m/map$/o) {
       ($oldtable = $newtable) =~ s{
                                        ([a-z]+) # species
                                        _{1,1}
                                        ([a-z]+) # focus
                                        _{1,1}
                                        ([a-zA-Z_]+) # satellite type
                                        _{1,1}
                                        map
                                    }{
                                       sprintf("%s_%s_%s", &utils_full_species($1), &utils_get_dataset_for_martfocus($2), $3)
                                    }ex;
   }
   elsif ($newtable =~ m/support$/o) {
       ($oldtable = $newtable) =~ s{
                                        ([a-z]+) # species
                                        _{1,1}
                                        ([a-z]+) # extrainfo
                                        _{1,1}
                                        support
                                    }{
                                       sprintf("%s_%s", &utils_full_species($1), $2)
                                    }ex;
   }
   elsif ($newtable =~ m/lookup$/o) {
       my ($first, @rest) = split /_/, $newtable;
       pop @rest; # dont want lookup
       # $first may be a valid species
       $first = &utils_full_species($first) || $first;
       $oldtable = join("_", $first, @rest);
   }
   else {
       warn("$newtable doesnt look right to me\n");
       return undef;
   }

   return $oldtable;
}

=head1 utils_get_new_tablename_from_old

  Arguments: oldtablename
  Function: maps old style tablenames to the new style tablenames
  Returns: newtablename

=cut

sub utils_get_new_tablename_from_old {
  my $oldtable = shift;
  my $newtable;

  # first deal with special cases without species
  if ($oldtable =~ m/meta/o) {
      $newtable = $oldtable;
  }
  elsif ($oldtable =~ m/evoc/o) {
      # evoc lookups
      $newtable = "${oldtable}_lookup";
  }
  else {
      # rest are species tables
      my @table = split /_/, $oldtable;
      my $newspecies = &utils_abbrev_species($table[0]."_".$table[1]);

      # species tables with totally different satellite names in new
      if ($table[2]."_".$table[3] eq 'sgp_chunks') {
          # dna_chunks_support in new
          $newtable = sprintf("%s_dna_chunks_support", $newspecies);
	  }
      elsif ($table[2] eq 'location') {
          # marker_lookup in new
          $newtable = sprintf("%s_marker_lookup", $newspecies);
	  }
      elsif (join("_", @table[2..4]) eq 'static_golden_path') {
          # assembly_support in new
          $newtable = sprintf("%s_assembly_support", $newspecies);
	  }
      elsif ($table[3] eq 'expression') {
          # expression table does not have a clean mapping
          my ($dataset, @rest) =  @table[2..$#table];

          if (scalar(@rest) == 2) {
              # description dimension table, or map table
              if ($rest[$#rest] eq 'descriptions') {
                  # description dimension
                  $newtable = sprintf("%s_%s_%s_dm", $newspecies, &utils_get_martfocus_for_dataset($dataset, q(gene)), join("_", @rest));
	          }
              else {
                  # ontology map
                  $newtable = sprintf("%s_%s_%s_map", $newspecies, &utils_get_martfocus_for_dataset($dataset, q(gene)), join("_", @rest));
             }
          }
          else {
              # ontology support table
              $newtable = sprintf("%s_%s_support", $newspecies, join("_", @rest));
          }
	  }
      elsif ($table[$#table] eq 'effect') {
          # snp effect dimension table, needs mapping to dataset, gene
          $newtable = sprintf("%s_snp_%s_effect_dm", $newspecies, &utils_get_martfocus_for_dataset($table[2], 'transcript'));
  	  }
      elsif ($table[$#table] =~ m/(karyotype|qtl|encode)/o) {
          # lookup table
          $newtable = sprintf("%s_%s_lookup", $newspecies, $1);
	  }
      elsif (my $martfocus = &utils_get_martfocus_for_dataset($table[2],$table[3])) {
          if ($table[2] eq 'snp' || $table[3] eq 'snp') {
              # snp tables
              if ($#table eq 2) {
                  # snp main, only 3 long
                  $newtable = sprintf("%s_snp_main", $newspecies);
	    	  }
              else {
                  # normal snp dimension
                  $newtable = sprintf("%s_snp_%s_dm", $newspecies, join("_", @table[3..$#table]));
              }
	      }
          elsif ($#table == 3) {
              # gene/transcript main table
              $newtable = sprintf("%s_%s_main", $newspecies, $martfocus);
		  }
          elsif ($table[4] eq 'homologs') {
              # homolog dimension table doesnt have a clean mapping
              my $otherspecies = $table[5].$table[6]; # x_y to xy
              $newtable = sprintf("%s_%s_homologs_%s_dm", $newspecies, $martfocus, $otherspecies);
		  }
          else {
              # gene/transcript dimension
              $newtable = sprintf("%s_%s_%s_dm", $newspecies, $martfocus, join("_", @table[4..$#table]));
	      }
	  }
      else {
          # problem table
          warn ("This table doesnt look correct $oldtable\n");
          return undef;
      }
  }

  return $newtable;
}
1;

