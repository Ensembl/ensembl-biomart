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
use Getopt::Long;

my $opts ={}; 

GetOptions( $opts, '-help' , '-species=s', '-registry=s', '-mart=s', -h );

&check_opts($opts);

my $time = time();
my $chunk_size = 100000;
my $this_table = 'dna_chunks';

my @tab =  ( $opts->{'species'}, 'core' , 100, $opts->{'mart'} );

my $tf = Production::TableFiller->new( $this_table, @tab  );

# Use of the registry now

Bio::EnsEMBL::Registry->load_all( $opts->{'registry'} );

# Check if we are dealing with a multipspecies database
# If so, the formatted species name will be given by the 'sql_name' meta attribute
my $formatted_species_name = undef;
my $meta_container =
           Bio::EnsEMBL::Registry->get_adaptor( "$opts->{'species'}", 'Core', 'MetaContainer' );
my $multispecies_mode = $meta_container->db->{_is_multispecies};
my @metazoa_db_patterns  = ("culex_","drosophila_","anopheles_","aedes_","caenorhabditis_","ixodes_","pediculus");
my @fungal_db_patterns   = ("schizosaccharomyces_pombe_","saccharomyces_cerevisiae_","aspergillus_","neosartorya_","neurospora_");
my @plant_db_patterns    = ("arabidopsis_","oryza_","vitis_","sorghum","populus","brachypodium");

if (! defined $meta_container) {
    die "meta_container couldn't be instanciated for species, \"$opts->{'species'}\"\n";
}
my $db_name = $meta_container->db->dbc->dbname;
my $mart_db = $tf->mart_db();

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
    elsif($division_value =~ /parasite/) {
      $db_name =~ /^(.*?)_(.*?)_(.*?)_.+/;
      $formatted_species_name = $3 . "_eg";
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

my $adaptor =  Bio::EnsEMBL::Registry->get_adaptor( "$opts->{'species'}", 'Core', 'slice' );
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

sub check_opts{

# compulsory command line arguments are $species, registry $mart_db

   my $opts = shift @_;

   if ( $opts->{'h'} or $opts->{'help'} ){ &usage }

   my $mandatory = [ 'species' , 'registry', 'mart' ];
   
   for my $m ( @$mandatory ){ unless( defined $opts->{$m} ){ &usage("-$m parameter missing") } }

   unless( -f $opts->{'registry'} ){ &usage("could not locate registry file $opts->{'registry'}") }
}

sub usage{

   my $message = shift @_;

   if ( $message ){ print">>>> $message\n\n" }

   print<<EOF;

 usage: dna_chunks.pl -species <string> -registry <file> -mart <string>

Uses -registry file to resolve -species to the relevant Ensembl format core database, and extracts chunked
sequence entrires to the specified -mart database, e.g.



EOF

   if ( $message ){ exit 1 }
   else{ exit 0 }
}



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
