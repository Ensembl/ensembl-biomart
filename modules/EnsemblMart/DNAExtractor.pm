package EnsemblMart::DNAExtractor;

=head1 NAME

EnsemblMart::DNAExtractor.pm

=head1 SYNOPSIS

  use Env;
  use DBI;
  use EnsemblMart::DNAExtractor;


  my $dna = EnsemblMart::DNAExtractor->new($dbh);

  my $seq = $dna->fetch('homo_sapiens','22',30000000,30000100);


=head1 DESCRIPTION

An object to supply fragments of DNA from the static golden path to scripts.

Also contains methods for reverse complementing and translation to amino acids.


=head1 AUTHOR

    Damian Keefe - dkeefe@ebi.ac.uk

=head1 COPYRIGHT

GRL/EBI

=head1 CVS

 $Log: DNAExtractor.pm,v $
 Revision 1.2  2005/08/08 15:09:29  ds5
 DNA table change

 Revision 1.1  2005/01/16 23:34:59  arek
 removing dependency on ensembl-mart

 Revision 1.18  2004/05/04 12:33:29  ds5
 updates to MSD and Proteome for new naming convention

 Revision 1.17  2004/04/29 09:03:37  dkeefe
 new tablename for dna_chunks

 Revision 1.16  2004/03/08 14:11:46  ds5
 gene sequence retrieval added

 Revision 1.15  2003/08/29 10:15:47  ds5
 sequence_support table name change

 Revision 1.14  2003/08/29 10:01:16  ds5
 added proteome_fetch method to retrieve proteome protein sequence

 Revision 1.13  2003/06/06 08:11:34  dkeefe
 removed lies from POD

 Revision 1.12  2003/05/13 08:05:29  dkeefe
 bug fix - missing $self on method call

 Revision 1.11  2003/05/07 13:48:23  dkeefe
 initialised  $self->{'last_chr'} to avoid undef warnings

 Revision 1.10  2003/05/06 13:34:14  dkeefe
 new dna table name

 Revision 1.9  2003/03/21 11:28:26  dkeefe
 code reviewed - ie documentation updated, cruft removed, design checked/improved.

 Revision 1.8  2003/02/24 15:53:37  dkeefe
 backed out changes for reporting SQL

 Revision 1.5  2003/01/05 20:04:30  dlondon
 Extracted all sequence handling to separate Extractor objects.

 Modified MartGeneExtractor/MartBaseExtractor to return arrayref of gene_id, transcript_id for useby GeneSeqExtractor (using transcript table instead of gene table)

 removed old _builder methods/hash from MartAdaptor

 Revision 1.4  2002/12/13 19:09:43  dlondon
 - Refactored the SNP Sequence extraction function of MartAdaptor to use
 a SNPSeqExtractor object to get fasta formated sequences.
 - Changed the way the SNP sequences are returned:
     new BioPerl friendly header format:
 >dbSNP=ID 5'flank=xbp|3'flank=ybp|chr #|bases x to y|strand=x
    new sequence format:
 5'flank
 alleles
 3'flank

 changed some internal method calls to hash calls to improve speed.

 Revision 1.3  2002/12/10 09:34:10  heikki
 POD fixes

 Revision 1.2  2002/06/26 08:35:25  jws
 Moved namespace from EnsemblLite to EnsemblMart

 Revision 1.1  2002/05/28 10:46:38  dkeefe
 single source (mart chunks) version of DNA.pm



=head1 TO DO


 * more POD

=head1 USAGE

=head1 EXAMPLES

 use DBI;
 use EnsemblMart::DNAExtractor;



 my $dsn = "DBI:$driver:database=$core_db;host=$host;port=$port";
 my $dbh = DBI->connect("$dsn","$user",$password, {RaiseError => 1});
 my $dna = EnsemblMart::DNAExtractor->new($dbh);
 # human DNA is in the database called mart in chunks of 1000000 bases
 $dna->init_species_db('homo_sapiens','mart',1000000);
 # mouse DNA is in the core ensemble database as variable length contigs
 $dna->init_species_db('mus_musculus','mus_musculus_core_4_2',0);

 my $seq = $dna->fetch('homo_sapiens','22',30000000,30000100);


 unless ($seq){
     print $dna->get_err()."\n";
 }
 print $seq."\n";




=head1 SEE ALSO

=cut

=head1 FUNCTIONS

=cut

use strict;
use DBI;

use constant DEFAULT_CHUNK_SIZE => 100000;


######################################################################
#
# Code for translation of nucleic acid into protein sequence.
#

my %_aa =     ( "AAA", "K", "AAC", "N", "AAG", "K", "AAT", "N",
                "ACA", "T", "ACC", "T", "ACG", "T", "ACT", "T",
                "AGA", "R", "AGC", "S", "AGG", "R", "AGT", "S",
                "ATA", "I", "ATC", "I", "ATG", "M", "ATT", "I",

                "CAA", "Q", "CAC", "H", "CAG", "Q", "CAT", "H",
                "CCA", "P", "CCC", "P", "CCG", "P", "CCT", "P",
                "CGA", "R", "CGC", "R", "CGG", "R", "CGT", "R",
                "CTA", "L", "CTC", "L", "CTG", "L", "CTT", "L",

                "GAA", "E", "GAC", "D", "GAG", "E", "GAT", "D",
                "GCA", "A", "GCC", "A", "GCG", "A", "GCT", "A",
                "GGA", "G", "GGC", "G", "GGG", "G", "GGT", "G",
                "GTA", "V", "GTC", "V", "GTG", "V", "GTT", "V",

                "TAA", "*", "TAC", "Y", "TAG", "*", "TAT", "Y",
                "TCA", "S", "TCC", "S", "TCG", "S", "TCT", "S",
                "TGA", "*", "TGC", "C", "TGG", "W", "TGT", "C",
                "TTA", "L", "TTC", "F", "TTG", "L", "TTT", "F" ) ;


=head2 aa

  Arg [1]   : txt - a DNA sequence

  Function  : translates the given DNA sequence into an amino acid sequence
              codons containing unrecognised characters ie not AGC or T are
              translated to X
  Returntype: none, txt, int, float, Bio::EnsEMBL::Example
  Exceptions: none
  Caller    : object::methodname or just methodname
  Example   : optional

=cut



sub aa{
    my $self = shift;
    my $seq = shift;
    my $i;
    my $prot;
    my $codon;

    $seq = uc($seq);

    my $len = length($seq);

    # Ignore incomplete codon at the end
    $len -= ($len % 3);

    for ($i = 0; $i < $len ; $i+=3){

        $codon = substr($seq, $i, 3);
        $prot .= ($_aa{$codon} || 'X');
    }

    return( $prot);
}



=head2 new

  Arg [1]   : a DBI database handle

  Function  : creates an EnsemblMart::DNAExtractor object.

  Returntype: EnsemblMart::DNAExtractor object
  Exceptions: dies if more than one argument given or if no DNA source is
              specified. These are programming/setup errors which must be fixed
  Caller    : EnsemblMart::MartAdaptor

=cut


sub new{
    my $class = shift;

    my $self = {};
    bless($self, $class);
    $self->_init(@_); # dies on failure
    return($self);
}


sub _init{
    my $self = shift;

    # get database handle in {'db'}
    unless(scalar (@_)  ){
        die("USAGE ERROR: must initialise with db handle argument");
    }

    $self->{'db'} = shift;

    # allow change in default
    $self->{'chunk_size'}=(scalar (@_))? shift:DEFAULT_CHUNK_SIZE;

    # use trace to put SQL into a log file
    # ONLY DO THIS WHEN DEBUGGING - OUTPUT IS VOLUMINOUS
    $self->{'log_file'} = "DNAExtractor.log";
    unlink($self->{'log_file'}); # delete the file if it already exists
    #$self->start_log(); # uncomment this if debugging

    $self->{'last_chr'} = 'avoid undef warnings';

    return(1);
}



=head2 pre_fetch

  Arg [1]   : txt - species_name
  Arg [1]   : txt - chromosome_name
  Arg [1]   : int - start base
  Arg [1]   : int - end base

  Function  : runs SQL which brings into the DNAExtractor cache enough chunks
              of DNA sequence from the named chromosome to include the given
              start and end bases
  Returntype: none
  Exceptions: none
  Caller    : object::methodname or just methodname
  Example   : optional

=cut



sub pre_fetch{
    my $self=shift;
        my ($species, $chr, $start, $last) = @_;

    my $chunk_size = $self->{'chunk_size'};

    # if required DNA is not from same big seg get a new big seg
    if( ($self->{'last_chr'} ne $chr) or
        ($start < $self->{'big_seg_start'}) or
        ($start > $self->{'big_seg_end'}) or
        ($last > $self->{'big_seg_end'})){


        my $tmp =  $start-1;
        #exact coord of a chunk start
        my $select_start=$tmp - ($tmp%$chunk_size)+1;

        my $sql = "select sequence, chr_start
		 from ${species}__dna_chunks__sup
		 where chr_start between $select_start and $last
		 and chr_name = \'$chr\'";

    #warn("DNASQL:\n$sql\n\n");

        # between is inclusive ie >= and <=
	my $tref = $self->{db}->selectall_arrayref($sql);

	unless (defined($tref)){
	    $self->_err("couldn't get dna table for chromosome $chr");
            warn("couldn't get dna table for chromosome $chr");
	    return('');
	}

	my $rows = (!defined($tref) ? 0 : scalar(@{$tref}));

	#print("rows $rows");

	unless( $rows) {
            warn ("failed to get any DNA for chr=$chr, $start, $last");
	    return (0);
	}

	# stitch the returned chunks into a big segment
        $self->{'big_seg'} ='';
	for(my $i=0;$i<$rows;$i++){
	    $self->{'big_seg'} .= $tref->[$i][0];
	}

        # cache this stuff
	$self->{'last_chr'} = $chr;
        $self->{'big_seg_start'} = $select_start;
        $self->{'big_seg_end'} = $self->{'big_seg_start'} +
	                         length($self->{'big_seg'})-1;

    }


    #$self->fetch(@_); #this is the lazy version. it just ignores the output
}


=head2 fetch

  Arg [1]   : txt - species_name
  Arg [1]   : txt - chromosome_name
  Arg [1]   : int - start base
  Arg [1]   : int - end base

  Function  : returns DNA sequence from the static golden path of the named
              species and chromosome from the given start to end base
  Returntype: txt
  Exceptions: none
  Caller    : EnsemblMart::MartAdaptor
  Example   :

=cut




sub fetch{
    my $self = shift;
    my ($species, $chr, $start, $last) = @_;

    my $chunk_size = $self->{'chunk_size'};

    # if required DNA is not from same big seg get a new big seg
    if( ($self->{'last_chr'} ne $chr) or
        ($start < $self->{'big_seg_start'}) or
        ($start > $self->{'big_seg_end'}) or
        ($last > $self->{'big_seg_end'})){


        my $tmp =  $start-1;
        #exact coord of a chunk start
        my $select_start=$tmp - ($tmp%$chunk_size)+1;

        # between is inclusive ie >= and <=
        my $sql = "select sequence, 
                          chr_start
		           from ${species}_genomic_sequence__dna_chunks__main
		           where chr_start between $select_start and $last
		           and chr_name = \'$chr\'";

        #warn("DNASQL:\n$sql\n\n");

	my $tref = $self->{db}->selectall_arrayref($sql);

	unless (defined($tref)){
	    $self->_err("couldn't get dna table for chromosome $chr");
	    warn("couldn't get dna table for chromosome $chr");
	    return('');
	}

	my $rows = (!defined($tref) ? 0 : scalar(@{$tref}));

	#print("rows $rows");

	unless( $rows) {
	    # no dna available so return a string of Ns
	    # *** this should probably return ''
	    my $len = ($last - $start) +1;
            warn ("failed to get any DNA for chr = $chr $start, $last");
	    return $self->_Npad($len);
	}

	# stitch the returned contigs into a big segment
        $self->{'big_seg'} ='';
	for(my $i=0;$i<$rows;$i++){
	    $self->{'big_seg'} .= $tref->[$i][0];
	}

        # cache this stuff untill next call
	$self->{'last_chr'} = $chr;
        $self->{'big_seg_start'} = $select_start;
        $self->{'big_seg_end'} = $self->{'big_seg_start'} +
	                         length($self->{'big_seg'})-1;

    }


    # cut out the requested section from the big segment
    my $len = ($last - $start) + 1;
    my $skip = $start - $self->{'big_seg_start'}; #$tref->[0][1];
    #print( " $skip, $len, $start, $tref->[0][1]\n");
    return(substr($self->{'big_seg'},$skip,$len));

}

=head2 proteome_fetch

  Arg [1]   : int - protein_id

  Function  : returns Protein sequence from the sequence dm of the proteome mart
  Returntype: txt
  Exceptions: none
  Caller    : EnsemblMart::MartAdaptor
  Example   :

=cut




sub proteome_fetch{
    my $self = shift;
    my ($protein_id) = @_;

    my $chunk_size = $self->{'chunk_size'};
        my $sql = "select seq
		           from uniprot__sequence__dm
		           where protein_id_key = '$protein_id'";

	my $tref = $self->{db}->selectall_arrayref($sql);

	unless (defined($tref)){
	    $self->_err("ERROR for $protein_id");
	    warn("couldn't get sequence for $protein_id");
	    return('');
	}

	my $rows = (!defined($tref) ? 0 : scalar(@{$tref}));

	unless( $rows) {
            warn ("failed to get any sequence for protein_id");
	    return 'NO_SEQ';
	}
        $self->{'big_seg'} ='';
	for(my $i=0;$i<$rows;$i++){
	    $self->{'big_seg'} .= $tref->[$i][0];
	}
    return $self->{'big_seg'};
}


=head2 proteome_gene_fetch

  Arg [1]   : int - protein_id

  Function  : returns gene sequences from the pidn dm of the proteome mart
  Returntype: txt
  Exceptions: none
  Caller    : EnsemblMart::MartAdaptor
  Example   :

=cut




sub proteome_gene_fetch{
    my $self = shift;
    my ($protein_id) = @_;

    my $chunk_size = $self->{'chunk_size'};
        my $sql = "select gene_sequence,pidn
		           from uniprot__gene_sequence__dm
		           where protein_id_key = '$protein_id'";

	my $tref = $self->{db}->selectall_arrayref($sql);

	unless (defined($tref)){
	    $self->_err("ERROR for $protein_id");
	    warn("couldn't get sequence for $protein_id");
	    return('');
	}

	my $rows = (!defined($tref) ? 0 : scalar(@{$tref}));

	#unless( $rows) {
         #   warn ("failed to get any sequence for protein_id");
	  #  return 'NO_SEQ';
	#}

        # there are possibly multiple gene sequences per protein_id
        return ($tref);
        #$self->{'big_seg'} ='';        
	#for(my $i=0;$i<$rows;$i++){
	#    $self->{'big_seg'} .= $tref->[$i][0];
	#}
        #return $self->{'big_seg'};
}


=head2 rc

  Arg [1]   : txt - a DNA sequence

  Function  : returns the reverse complement of the supplied sequence
  Returntype: txt
  Exceptions: none
  Caller    : EnsemblMart::MartAdaptor
  Example   :

=cut



sub rc{
    my $self = shift;
    my $seq = reverse($_[0]);

    $seq =~ tr/YABCDGHKMRSTUVyabcdghkmrstuv/RTVGHCDMKYSAABrtvghcdmkysaab/;

    return $seq;
}



sub _Npad{
    my $self = shift;
    my $num = shift;

    return('N' x $num);
}



=head2 start_log

  Arg [1]   : none
  Function  : starts or restarts the DBI trace function with output to the
              default logfile
  Returntype: none
  Exceptions: none
  Caller    :
  Example   : $tf->start_log();

=cut

sub start_log{
    my $self = shift;

    $self->{'db'}->trace( 1,"$self->{'log_file'}");
}


=head2 stop_log

  Arg [1]   : none
  Function  : Stops output to the DBI logfile. Useful if your script has a
              loop containing an SQL statement. If you do not stop output,
              the logfile gets an entry each time the statement is executed
              which can quickly generate an extremely large file.
  Returntype: none
  Exceptions: none
  Example   : $tf->stop_log();

=cut

sub stop_log{
    my $self = shift;

    $self->log("LOG STOPPED\n...\n");
    $self->{'db'}->trace( 0,"/dev/null");
}



=head2 fatal

  Arg [1]   : txt - an error message
  Function  : prints the error message via self->perr() and exits with 1.
  Returntype: none
  Exceptions: none
  Caller    : called both internally and as instance method
  Example   : $tf->fatal("thats torn it!");

=cut

sub fatal{
    my $self = shift;
    my $msg = shift;

    $self->perr($msg);

    exit(1);
}


=head2 log

  Arg [1]   : txt - a string which programmer wants to appear in the log file.
  Function  : prints a string in the DBI log file.
  Returntype: none
  Exceptions: none
  Caller    : called both internally and as instance method
  Example   : $tf->log("KARYOTYPE TABLE PREPARATION ".`date`);

=cut

sub log{
    my $self = shift;
    my $msg = shift;

    $self->{'db'}->trace_msg($msg);
}



=head2 perr

  Arg [1]   : txt - a string to appear on the error output ie user will see it
  Function  : prints the error string followed by newline.
  Returntype: none
  Exceptions: none
  Caller    :
  Example   : $tf->perr("Doh!");

=cut

sub perr{
    my $self = shift;
    my $msg = shift;

    print $msg."\n";
}

=head2 commentary

  Arg [1]   : txt - a string to appear on the console ie user will see it
  Function  : prints the string followed by newline.
  Returntype: none
  Exceptions: none
  Caller    : table_exists()
  Example   : $tf->commentary("Busy working");

=cut

sub commentary{
    my $self = shift;
    my $msg = shift;

    print STDERR $msg;
}



# stores a scalar ie error message string as an instance variable
sub _err{
    my $self = shift;

    $self->{'errstr'} = shift;

}


sub errstr{
    my $self = shift;
    if( @_ ) {
	my $value = shift;
	$self->{'errstr'} = $value;
    }
    return $self->{'errstr'};

}

# synonym for errstr
sub get_err{
    my $self = shift;

    return($self->{'errstr'});
}



=head2 db

  Arg [1]   : none or DBI database handle
  Function  : sets or returns the contents of the objects dbh property.
  Returntype: DBI database handle
  Exceptions: none
  Example   : optional

=cut


sub db{
   my $obj = shift;
   if( @_ ) {
      my $value = shift;
      $obj->{'db'} = $value;
    }
    return $obj->{'db'};

}


1;
