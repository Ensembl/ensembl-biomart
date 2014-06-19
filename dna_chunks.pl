#!/software/bin/perl -w

=head1 NAME dna_chunks.pl


=head1 SYNOPSIS

    dna_chunks.pl  <genus_species> <dataset> <release> <mart_db> [<have_memory>]

    setting optional entry 'have_memory' to true (eg., 1) means that you are on a machine with lots of memory

=head1 DESCRIPTION

Script which extracts data from the source database given on the command line and creates a table called <species>_dna_chunks_support in the mart db given on the command line. The sgp chunks table contains the dna sequence for the static golden path divided into 0.1 megabase chunks.
NB: species is canonicalized by EnsemblMart::TableNameUtils::utils_abbrev_species

In general this script will be called from a driver script, but can be used standalone as described below.

The script emits a commentary, as it runs, for user reassurance.

This is one of a family of scripts based on Production::TableFiller.pm. For further details see the perldoc for that module. Note this script does not put much info in the logfile because the default trace information is extremely voluminous, however if an SQL statement does fail it will be reported to the logfile.

The script uses a temporary disk file to reduce memory usage.


=head1 AUTHOR

    Damian Keefe - dkeefe@ebi.ac.uk

=head1 CVS

 $Log: dna_chunks.pl,v $
 Revision 1.21  2008/11/24 14:26:31  syed
 largely refactor to use the registry, and be able to deal with multispecies databases. Added an extra parameter, species_name for use of the registry for a given species.

 Revision 1.20  2008/04/24 10:17:58  whs
 Add error handling to &chunks_to_mart call

 Revision 1.19  2008/02/18 13:55:05  rh4
 Changes made for 49.

 Revision 1.18  2007/12/07 13:20:44  rh4
 Script mods as for 48 release.

 Revision 1.17  2007/04/10 12:18:24  rh4
 These are changes required for the 44 release.

 Revision 1.16  2007/03/02 10:19:56  rh4
 Post-43 commit.

 Revision 1.15  2006/12/07 16:59:42  rh4
 Changes made for release 42.

 Revision 1.14  2006/11/22 16:08:37  rh4
 Added missing key columns and redefined existing key column in dna_chunks.

 Revision 1.13  2006/11/14 11:15:31  rh4
 Added chunk_key to genomic_sequence tables.

 Revision 1.12  2006/08/01 22:24:22  arek
 fix for subslice  dying on too long sequence

 Revision 1.11  2006/08/01 16:40:41  arek
 implemented Patrick's subslicing ideas to improve memmory handling for
 species with big chromosomes. Eyeballed a few sequences and appear to be
 identical to the previous ones

 Revision 1.10  2005/10/25 16:25:10  dlondon
 new dna_chunks production regime which allows the system to use LSF to submit dna_chunks for all species if it is available

 Revision 1.9  2005/10/04 15:20:43  dlondon
 now relies on the ensembl API to dump its sequences for chunking

 Revision 1.8  2005/06/02 19:06:53  arek
 fix table name to comply with dataset__content__type. Needed for the
 automated updates

 Revision 1.7  2005/01/15 21:50:50  arek
 sup ->main

 Revision 1.6  2005/01/14 20:18:29  arek
 help fix for a sequence mart

 Revision 1.5  2004/05/10 14:16:57  dkeefe
 uses new naming convention as final table is accessed by API during
 subsequent scripts.

 Revision 1.4  2004/04/14 11:32:44  dkeefe
 final table can now exceed 4Gb

 Revision 1.3  2004/03/04 16:06:29  dkeefe
 updates for latest core schema

 Revision 1.2  2003/12/03 14:18:00  dkeefe
 new seqstore

 Revision 1.1  2003/09/10 09:27:39  dkeefe
 moved from directory scripts/current

 Revision 1.2  2003/09/08 13:22:06  dkeefe
 Production::TableNameUtils changed to EnsemblMart::TableNameUtils

 Revision 1.1  2003/09/03 15:18:01  dkeefe
 mart building scripts now in their own directory

 Revision 1.1  2003/05/02 16:55:37  dlondon
 Produces marts with the new table naming system.

 Refactored the transcript table modification out of gene_snp into transcript_pt2 where it belongs.

 marker.pl replaces location.pl
 assembly.pl replaces static_golden_path.pl
 dna_chunks.pl replaces sgp_chunks.pl


 ### Renamed from sgp_chunks.pl to dna_chunks.pl ###
 Revision 1.9  2003/02/20 13:22:17  dkeefe
 all sequence now stored as upper case

 Revision 1.8  2003/01/18 11:37:41  dkeefe

 now emits error when dna collection fails

 Revision 1.7  2002/10/31 13:45:04  dkeefe
 put back chr_name instead of chromosome_id

 Revision 1.6  2002/10/29 12:12:38  dkeefe
  change to new main trunk schema

 Revision 1.5  2002/09/19 14:53:11  dkeefe
 merge from branch baseline
 added $$ to temporary file name
 improved error handling
 removed dependence on order of columns in static_golden_path table

 Revision 1.4  2002/06/27 08:11:37  dkeefe
 changed shebang line and pod

 Revision 1.3  2002/06/26 08:35:26  jws
 Moved namespace from EnsemblLite to EnsemblMart

 Revision 1.2  2002/06/12 11:49:59  dkeefe
 now uses most up to date databases as indicated by suffix letter

 Revision 1.1  2002/05/14 15:27:01  dkeefe
 creates SGP chunks in mart database


=head1 TO DO

 * experiments to optimize chunk size

=head1 USAGE

Certain environment variables must be set. See perldoc Production::TableFiller.pm for details

When specifying the release do not give the version letter ie use 7_30 not 7_30a

=head1 EXAMPLES

  dna_chunks.pl  mus_musculus core 3_1 mart

=head1 SEE ALSO

pod for:
martmaker.pl
Production::TableFiller
EnsemblMart::TableNameUtils

=cut

use strict;
use DBI;
use Env;
use Production::TableFiller;
use EnsemblMart::TableNameUtils;
use Bio::EnsEMBL::Registry;

=head1 old code
# array indices of sgp table
# dna sequence is in 0
use constant superctg_name => 1;
use constant chr_name => 2;
use constant contig_id => 3;
use constant chr_start => 4;
use constant chr_end => 5;
use constant superctg_start => 6;
use constant superctg_end => 7;
use constant superctg_ori => 8;
use constant contig_start => 9;
use constant contig_end => 10;
use constant contig_ori => 11;
use constant type => 12;
=cut

# compulsory command line arguments are $species, $dataset, $core_release, $mart_db and $species_arg
# in that order
($ARGV[4] || (defined $ARGV[5])) or &helptext("ERROR: insufficient arguments");

my $time = time();
my $chunk_size = 100000;
my $this_table = 'dna_chunks';

# Todo: Proper command line processing

my $have_memory = pop @ARGV if (defined $ARGV[5]); #dont want have_memory in ARGV when its sent to Production::TableFiller
my $species_arg = pop @ARGV;

print STDERR "Species command line argument, $species_arg\n";

my $tf = Production::TableFiller->new($this_table, @ARGV);

# Use of the registry now

my $core_release = $tf->release;
if ($core_release =~ /^(\d+)_\w+$/) {
    $core_release = $1;
    print STDERR "parsed core release, $core_release\n";
}
else {
    die "Failed to parse the core release number from, " . $tf->release . "\n";
}

Bio::EnsEMBL::Registry->load_registry_from_db(
					      -host => $tf->host,
					      -user => $tf->user,
					      -pass => $tf->password,
					      -port => $tf->port,
					      -db_version => $core_release);

# Check if we are dealing with a multipspecies database
# If so, the formatted species name will be given by the 'sql_name' meta attribute
my $formatted_species_name = undef;
my $meta_container =
           Bio::EnsEMBL::Registry->get_adaptor( "$species_arg", 'Core', 'MetaContainer' );
my $multispecies_mode = $meta_container->db->{_is_multispecies};
my @metazoa_db_patterns  = ("culex_","drosophila_","anopheles_","aedes_","caenorhabditis_","ixodes_","pediculus");
my @fungal_db_patterns   = ("schizosaccharomyces_pombe_","saccharomyces_cerevisiae_","aspergillus_","neosartorya_","neurospora_");
my @plant_db_patterns    = ("arabidopsis_","oryza_","vitis_","sorghum","populus","brachypodium");

if (! defined $meta_container) {
    die "meta_container couldn't be instanciated for species, \"$species_arg\"\n";
}
my $db_name = $meta_container->db->dbc->dbname;

if ($multispecies_mode) {
    # Todo: Reformat the dataset name:
    # now: '3 letters prefix' + "_" + $proteome_id
    # e.g. 'bac_123456'

    # Was
    # $formatted_species_name = @{$meta_container->list_value_by_key('species.sql_name')}[0];

    # Now
    $db_name =~ /^(\w\w\w).+/;
    print STDERR "db_name, $db_name\n";
    my $db_prefix = lc($1);
    $formatted_species_name = $db_prefix . "_" . @{$meta_container->list_value_by_key('species.proteome_id')}[0];

    if (! defined $formatted_species_name) {
	die "short species name not defined\n";
    }

}

# Todo: Use 'species.division' meta attribute instead

elsif (defined @{$meta_container->list_value_by_key('species.division')}[0]) {
    my $division_value = @{$meta_container->list_value_by_key('species.division')}[0];
    if ($division_value =~ /ensemblmetazoa|ensemblfungi|ensemblplant|ensemblprotist/i) {

	$db_name =~ /^(\w)[^_]+_([^_]+)_.+/;

	# Add a prefix 'eg_' to avoid conflicting dataset names in Biomart.org!
	$formatted_species_name = $1  . $2 . "_eg";
    }
    else {
	 $db_name =~ /^(\w)[^_]+_([^_]+)_.+/;
	 $formatted_species_name = $1 . $2;
    }
}
elsif (contains (\@metazoa_db_patterns, $meta_container->db->dbc->dbname) ) {

    print STDERR "no 'species.division' meta attribute, so using db_patterns matching instead\n";

    print STDERR "metazoa species\n";

    $db_name =~ /^(\w)[^_]+_([^_]+)_.+/;

    # Add a prefix 'eg_' to avoid conflicting dataset names in Biomart.org!
    $formatted_species_name = $1  . $2 . "_eg";
}
elsif (contains (\@fungal_db_patterns, $meta_container->db->dbc->dbname) ) {

    print STDERR "no 'species.division' meta attribute, so using db_patterns matching instead\n";

    print STDERR "fungal species\n";

    $db_name =~ /^(\w)[^_]+_([^_]+)_.+/;

    # Add a prefix 'eg_' to avoid conflicting dataset names in Biomart.org!
    $formatted_species_name = $1 . $2 . "_eg";
}
elsif (contains (\@plant_db_patterns, $meta_container->db->dbc->dbname) ) {

    print STDERR "no 'species.division' meta attribute, so using db_patterns matching instead\n";

    print STDERR "plant species\n";

    $db_name =~ /^(\w)[^_]+_([^_]+)_.+/;

    # Add a prefix 'eg_' to avoid conflicting dataset names in Biomart.org!
    $formatted_species_name = $1 . $2 . "_eg";
}
else {
    $db_name =~ /^(\w)[^_]+_([^_]+)_.+/;
    $formatted_species_name = $1 . $2;
}

print STDERR "species formatted name: $formatted_species_name\n";

my $logfile = "/tmp/".$formatted_species_name."_dna_chunks.out";

#redirect STDERR to a LOG file
open (STDERR, ">>${logfile}") or die "Could not open $logfile: $!\n";

print STDERR "running   dna_chunks.pl ".join(" ", @ARGV)."\n";

print STDERR "BUILDING DNA_CHUNKS FOR SPECIES $formatted_species_name\n";

# create names of dbs and tables
my $mart_db = $tf->mart_db();

my $final_table = "${mart_db}." . $formatted_species_name . "_genomic_sequence__$this_table" . "__main";

# always use the real core table not one from another dataset
my $sgp_table = "${mart_db}.". $formatted_species_name . '_assembly';

print STDERR "final_table: $final_table\n";
print STDERR "sgp_table: $sgp_table\n";

# we don't need to generate this table unless the dataset is core but if
# its not core then the table may need to exist for other scripts to run
# so we check it exists and let the driver script and/or the user know
unless($tf->dataset() eq 'core'){
    if($tf->table_exists($final_table)){
        print STDERR ("NOTE: table $final_table already exists\n");
        exit(0); # tell driver everything OK
    }
    # if the table doesn't exist then we try to create it
}

# create a new, empty $final_table

my @array =(
"drop table if exists $final_table",
"create table $final_table
(
chunk_key int(10) not null,
chr_name varchar(40) not null default '',
chr_start int(10) not null default '0',
sequence mediumtext
)ENGINE=MyISAM MAX_ROWS=100000 AVG_ROW_LENGTH=100000",
"set \@row=0"
);
$tf->exe(@array);

my $adaptor =  Bio::EnsEMBL::Registry->get_adaptor( "$species_arg", 'Core', 'slice' );
#$adaptor->cache_toplevel_seq_mappings() if ($have_memory);

my $slices = $adaptor->fetch_all('toplevel',undef,1,1); #default_version,include references (DR52,DR53),include duplicate (eg HAP and PAR)
$tf->stop_log();

warn( "Got ", scalar(@$slices) , " toplevel" );


while( my $slice = shift @{$slices} ){
    my $chr_name = "\'".$slice->seq_region_name."\'";

    my $current_base = 1;
    my $length = $slice->length;

    while ($current_base <= $length) {


        my $step;

        if ($chunk_size<=$slice->length-$current_base)
	{
	    $step=$chunk_size-1;
	} else {
	    $step=$slice->length-$current_base;
	}

 	my $sub_slice = $slice->sub_Slice($current_base,
            $current_base+$step);

        my $chr_start = "\'".$current_base."\'";
        eval{ &chunks_to_mart($chr_name, $chr_start, $sub_slice->seq) };
        if( $@ ){ warn $@; next }
        $current_base += $chunk_size;
    }
}

warn( "[INFO] Done adding DNA. Adding indexes" );




#create indices (chr_name,chr_start)
$tf->start_log();
my @sql;
push @sql, "alter table $final_table
               add index (chunk_key)";
push @sql, "alter table $final_table
               add index (chr_name)";
push @sql, "alter table $final_table
               add index (chr_start)";
push @sql, "alter table $final_table
               add index chr_st(chr_name,chr_start)";
$tf->exe(@sql);


unless($tf->table_is_filled($final_table)){
    $tf->fatal("ERROR: final table contains no data");
}




# message sent back to calling script or console
$time = time() - $time;
print STDERR ("SUCCESS: Table $final_table created in $time seconds\n");

exit(0);


########################################################################


# reads over seq in sequential chunks and puts them in the db
sub chunks_to_mart{
my ($chr_name, $chr_start, $seq)=@_;


    my $dbh = $tf->get_dbh();
    my $current_base = 0;
    my $length = length($seq);
    my @sql;


  my $sequence = "\'".$seq."\'";
  $sql[0]= "insert into $final_table (chunk_key,chr_name,chr_start,sequence)
                  values (\@row:=\@row+1,$chr_name,$chr_start,$sequence)";


       $tf->exe(@sql);
}

sub err{
    print STDERR "$_[0]\n";
}

# helptext
# called without arguments, prints the Usage message and exits with 0
# if there is an argument it is a string - error message. This is printed via # &err together with the Usage message and the script exits with 1

sub helptext{
    if($_[0]){
	&err("\n$_[0]\n\n");
    }

    &err("Usage:\n");
    &err("     dna_chunks.pl <genus_species> <dataset> <release> <sequence_mart> <species_name> [<have_memory>] n     setting optional 'have_memory' to true means you are on a large memory machine\n");

    &err("\n");


    if($_[0]){
	exit(1);
    }else{
	exit(0);
    }
}

sub contains {
    my $array_ref = shift;
    my $element = shift;

    foreach my $item (@$array_ref) {
	if ($element  =~ /^$item/i) {
	    return 1;
	}
    }

    return 0;
}
