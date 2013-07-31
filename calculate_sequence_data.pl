#!/bin/env perl
#
# $Source$
# $Revision$
# $Date$
# $Author$
#
# Script for calculating exon coordinates (which mart itself is incapable of doing)

use warnings;
use strict;

use Carp;
use Log::Log4perl qw(:easy);
use List::MoreUtils qw(any);
use Data::Dumper;
use DbiUtils;
use MartUtils;
use Getopt::Long;
use Bio::EnsEMBL::Registry;

Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();

my $verbose = 1;

# db params
my $db_host          = 'mysql-cluster-eg-prod-1.ebi.ac.uk';
my $db_port          = 4238;
my $db_user          = 'ensrw';
my $db_pwd           = 'writ3rp1';
my $mart_db;
my $release;
my $dataset_basename = 'gene';
my $dataset;
my $limit_species;

sub usage {
    print "Usage: $0 [-h <host>] [-port <port>] [-u user <user>] [-p <pwd>] [-mart <mart db>] [-release <e! release>] [-dataset_basename <basename>]\n";
    print "-h <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <mart_db>\n";
    print "-release <e! release>\n";
    print "-dataset_basename <dataset basename> Default is $dataset_basename\n";
    exit 1;
};

my $options_okay = GetOptions (
    "host=s"=>\$db_host,
    "port=i"=>\$db_port,
    "user=s"=>\$db_user,
    "pass=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "release=i"=>\$release,
    "dataset_basename=s"=>\$dataset_basename,
    "dataset=s"=>\$dataset,
    "species=s"=>\$limit_species,
    "help"=>sub {usage()}
    );

if(!$options_okay || !defined $mart_db || !defined $release) {
    usage();
}

# open mart
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle =
     DBI->connect( $mart_string, $db_user, $db_pwd, { RaiseError => 1 } )
  or croak "Could not connect to $mart_string with db_user, $db_user and db_pwd, $db_pwd";

# load registry
Bio::EnsEMBL::Registry->load_registry_from_db(
    -host       => $db_host,
    -user       => $db_user,
    -pass       => $db_pwd,
    -port       => $db_port,
    -db_version => $release);

# get hash of datasets and sql names
my @datasets = get_dataset_names($mart_handle);

if(defined $dataset) {
    @datasets = grep {$_ eq $dataset} @datasets;
}

for my $dataset (@datasets) {

	my $ds_base = $dataset . '_' . $dataset_basename;
	my $species_name = get_sql_name_for_dataset( $mart_handle, $dataset );
	next if (defined $limit_species && $species_name ne $limit_species);

	print "Updating $ds_base ($species_name)\n";

	my $dba =
	  Bio::EnsEMBL::Registry->get_DBAdaptor( $species_name, 'Core',
											 'transcript' );

	my @sql;

	my $transcript_adaptor = $dba->get_TranscriptAdaptor();

	if ( !defined($transcript_adaptor) ) {
		die("Failed to get transcript adaptor for $species_name");
	}

	printf( "Getting all transcripts for species %s\n", $species_name );
	my $transcripts = $transcript_adaptor->fetch_all();
	printf( "==> Got %d transcripts, now running...\n",
			scalar( @{$transcripts} ) );

	@{$transcripts} =
	  sort { $a->dbID() <=> $b->dbID() } @{$transcripts};

	print "Updating ${dataset}_temp\n";

	$mart_handle->do("DROP TABLE IF EXISTS ${dataset}_temp");

	$mart_handle->do(
		qq{CREATE TABLE ${dataset}_temp (
  transcript_id_1064_key    INT(10) UNSIGNED,
  exon_id_1017              INT(10) UNSIGNED,
  five_prime_utr_start      INT(10),
  five_prime_utr_end        INT(10),
  three_prime_utr_start     INT(10),
  three_prime_utr_end       INT(10),
  genomic_coding_start      INT(10),
  genomic_coding_end        INT(10),
  cdna_coding_start         INT(10),
  cdna_coding_end           INT(10),
  cds_start                 INT(10),
  cds_end                   INT(10),
  cds_length                INT(10)
)} );

	#$mart_handle->do("LOCK TABLES ${dataset}_temp WRITE");

	my $t0               = time();
	my $transcript_count = 0;

	local $SIG{ALRM} = sub {
		my $t                = time();
		my $transcript_speed = $transcript_count/( $t - $t0 );

		printf( "--> %d transcripts done, "
				  . "%.3gs per transcript "
				  . "(%.3g transcripts/s)\n",
				$transcript_count, 1/$transcript_speed, $transcript_speed );
		printf( "--> Finishing at '%s'\n",
				scalar( localtime(
								$t + scalar( @{$transcripts} )/$transcript_speed
						) ) );

		alarm(10);
	};
	alarm(10);

	my $insert_sth = $mart_handle->prepare(
			  "INSERT INTO ${dataset}_temp VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");

	while ( my $transcript = shift( @{$transcripts} ) ) {
		  if ( ( ++$transcript_count % 500 ) == 0 ) {
		  		print "Done $transcript_count transcripts for $species_name\n";
}
		#
		#		$out->printf( "INSERT INTO %s_temp VALUES\t-- %s:%d:%d:%+d\n",
		#					  $dataset,
		#					  $transcript->stable_id(),
		#					  $transcript->start(),
		#					  $transcript->end(),
		#					  $transcript->strand() );
		my @exon_data_string;

		my $translation = $transcript->translation();

		my $is_coding = defined($translation);

		my $cds_length;
		## my $cdna_length;

		if ($is_coding) {
			$cds_length =
			  $transcript->cdna_coding_end() -
			  $transcript->cdna_coding_start() + 1;

			## my $utr5 = $transcript->five_prime_utr();
			## my $utr3 = $transcript->three_prime_utr();

			## $cdna_length =
			##   $cds_length +
			##   ( defined($utr5) ? $utr5->length() : 0 ) +
			##   ( defined($utr3) ? $utr3->length() : 0 );
		} else {
			$cds_length = undef;
			## $cdna_length = '\N';
		}

		foreach my $exon ( @{ $transcript->get_all_Exons() } ) {

		    eval {
			## my $cdna_start = $exon->cdna_start($transcript) || '\N';
			## my $cdna_end   = $exon->cdna_end($transcript)   || '\N';

			my $exon_cdna_coding_start = $exon->cdna_coding_start($transcript);
			my $exon_cdna_coding_end   = $exon->cdna_coding_end($transcript);

			my $coding_start = $exon->coding_region_start($transcript)
			  || undef;
			my $coding_end = $exon->coding_region_end($transcript)
			  || undef;

			my ( $utr5_start, $utr5_end );
			if (    $is_coding
				 && $exon->start() < $transcript->coding_region_start() )
			{
				if ( $coding_start ) {
					$utr5_start = $exon->start();
					$utr5_end   = $coding_start - 1;
				} else {
					$utr5_start = $exon->start();
					$utr5_end   = $exon->end();
				}
			} else {
				$utr5_start = undef;
				$utr5_end   = undef;
			}

			my ( $utr3_start, $utr3_end );
			if (    $is_coding
				 && $exon->end() > $transcript->coding_region_end() )
			{
				if ( $coding_end ) {
					$utr3_start = $coding_end + 1;
					$utr3_end   = $exon->end();
				} else {
					$utr3_start = $exon->start();
					$utr3_end   = $exon->end();
				}
			} else {
				$utr3_start = undef;
				$utr3_end   = undef;
			}

			my ( $cds_start, $cds_end );
			if ( defined($exon_cdna_coding_start) ) {
				$cds_start =
				  $exon_cdna_coding_start -
				  $transcript->cdna_coding_start() + 1;
				$cds_end =
				  $exon_cdna_coding_end - $transcript->cdna_coding_start() + 1;
			} else {
				$cds_start = undef;
				$cds_end   = undef;
			}

			if ( $transcript->strand() == -1 ) {
				( $utr5_start, $utr3_start ) = ( $utr3_start, $utr5_start );
				( $utr5_end,   $utr3_end )   = ( $utr3_end,   $utr5_end );
			}

			#			push(
			#				@exon_data_string,
			#				sprintf(
			#					"(%s,%s , %s,%s , %s,%s , %s,%s , %s,%s , %s,%s,%s)",
			$insert_sth->execute(
				$transcript->dbID(),
				$exon->dbID(),
				$utr5_start,
				$utr5_end,
				$utr3_start,
				$utr3_end,
				$coding_start,
				$coding_end,
				$exon_cdna_coding_start || undef,
				$exon_cdna_coding_end   || undef,
				## $cdna_length,
				$cds_start,
				$cds_end,
				$cds_length );
		    };
		    if($@) {
			print "Problem dealing with exon ".$exon->dbID().":".$@."\n";
		    }

		} ## end foreach my $exon ( @{ $transcript...})

		#$out->print( "\t", join( ",\n\t", @exon_data_string ), ";\n" );

	} ## end while ( my $transcript = ...)

	alarm(0);

	#$out->print(<<EOT);

	print "Indexing ${dataset}_temp...\n";

	#UNLOCK TABLES;

	$mart_handle->do(
		qq{ALTER TABLE ${dataset}_temp
  ADD INDEX theindex(transcript_id_1064_key, exon_id_1017)} );

	print "Optimizing ${dataset}_temp...\n";

	$mart_handle->do(qq{OPTIMIZE TABLE ${dataset}_temp});

	eval {
	print "Modifying ${ds_base}__exon_transcript__dm...\n";

	$mart_handle->do(
		qq{ALTER ignore TABLE ${ds_base}__exon_transcript__dm
  ADD COLUMN five_prime_utr_start   INT(10),
  ADD COLUMN five_prime_utr_end     INT(10),
  ADD COLUMN three_prime_utr_start  INT(10),
  ADD COLUMN three_prime_utr_end    INT(10),
  ADD COLUMN genomic_coding_start   INT(10),
  ADD COLUMN genomic_coding_end     INT(10),
  ADD COLUMN cdna_coding_start      INT(10),
  ADD COLUMN cdna_coding_end        INT(10),
  ADD COLUMN cds_start              INT(10),
  ADD COLUMN cds_end                INT(10),
  ADD COLUMN cds_length             INT(10)} );
    };
	if($@) {
      print STDERR "Problems modifying the table".$@;
	}
	print "Updating ${ds_base}__exon_transcript__dm...\n";
	$mart_handle->do(
		qq{UPDATE
  ${ds_base}__exon_transcript__dm dm,
  ${dataset}_temp t
SET dm.five_prime_utr_start     = t.five_prime_utr_start,
    dm.five_prime_utr_end       = t.five_prime_utr_end,
    dm.three_prime_utr_start    = t.three_prime_utr_start,
    dm.three_prime_utr_end      = t.three_prime_utr_end,
    dm.genomic_coding_start     = t.genomic_coding_start,
    dm.genomic_coding_end       = t.genomic_coding_end,
    dm.cdna_coding_start        = t.cdna_coding_start,
    dm.cdna_coding_end          = t.cdna_coding_end,
    dm.cds_start                = t.cds_start,
    dm.cds_end                  = t.cds_end,
    dm.cds_length               = t.cds_length
WHERE   dm.transcript_id_1064_key   = t.transcript_id_1064_key
  AND   dm.exon_id_1017             = t.exon_id_1017} );
	print "Optimizing ${ds_base}__exon_transcript__dm...\n";

	$mart_handle->do(qq{OPTIMIZE TABLE ${ds_base}__exon_transcript__dm});

	print "Dropping  ${dataset}_temp...\n";

	$mart_handle->do(qq{DROP TABLE ${dataset}_temp});
	print "Completed ${dataset}\n";
} ## end for my $dataset (@datasets)

