#!/bin/env perl
# Copyright [2009-2024] EMBL-European Bioinformatics Institute
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
use FindBin;
use lib "$FindBin::Bin/../modules";
use DbiUtils;
use MartUtils qw(get_dataset_names get_sql_name_for_dataset);
use Getopt::Long;
use Bio::EnsEMBL::Registry;

Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();

my $verbose = 1;

# db params
my $db_host;
my $db_port;
my $db_user;
my $db_pwd;
my $mart_db;
my $dataset_basename = 'gene';
my $dataset;
my $limit_species;
my $registry;

sub usage {
    print "Usage: $0 [-host <host>] [-port <port>] [-user user <user>] [-pass <pwd>] [-mart <mart db>] [-release <e! release>] [-dataset_basename <basename>]\n";
    print "-host <host> Default is $db_host\n";
    print "-port <port> Default is $db_port\n";
    print "-user <host> Default is $db_user\n";
    print "-pass <password> Default is top secret unless you know cat\n";
    print "-mart <mart_db>\n";
    print "-dataset_basename <dataset basename> Default is $dataset_basename\n";
    exit 1;
};

my $options_okay = GetOptions (
    "host=s"=>\$db_host,
    "port=i"=>\$db_port,
    "user=s"=>\$db_user,
    "pass=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "registry=s"=>\$registry,
    "dataset_basename=s"=>\$dataset_basename,
    "dataset=s"=>\$dataset,
    "species=s"=>\$limit_species,
    "help"=>sub {usage()}
    );

if(!$options_okay || !defined $mart_db) {
    usage();
}

# open mart
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle =
     DBI->connect( $mart_string, $db_user, $db_pwd, { RaiseError => 1 } )
  or croak "Could not connect to $mart_string with db_user, $db_user and db_pwd, $db_pwd";

# load registry
if(defined $registry) {
  Bio::EnsEMBL::Registry->load_all($registry);
} else {
  Bio::EnsEMBL::Registry->load_registry_from_db(
                                                -host       => $db_host,
                                                -user       => $db_user,
                                                -pass       => $db_pwd,
                                                -port       => $db_port);
}

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
	  Bio::EnsEMBL::Registry->get_DBAdaptor( $species_name, 'Core', 'transcript' );

	my $db_dbc = $dba->dbc();
	# Getting rna seq edits data from the transcript_attrib table
	my $rna_seq_edits= $db_dbc->sql_helper()->execute_into_hash(
              -SQL => "select transcript_id,value from transcript JOIN transcript_attrib USING (transcript_id) JOIN attrib_type USING (attrib_type_id) where code='_rna_edit'",
              -CALLBACK => sub {
              my ( $row, $value ) = @_;
              $value = [] if !defined $value;
              push(@$value, $row->[1] );
              return $value;
              }
            );
	$db_dbc->disconnect_if_idle();

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

	print "Updating MTMP_${dataset}_exon\n";

	$mart_handle->do("DROP TABLE IF EXISTS MTMP_${dataset}_exon");

	$mart_handle->do(
		qq{CREATE TABLE MTMP_${dataset}_exon (
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

	print "Updating MTMP_${dataset}_tr\n";

	$mart_handle->do("DROP TABLE IF EXISTS MTMP_${dataset}_tr");

	$mart_handle->do(
		qq{CREATE TABLE MTMP_${dataset}_tr (
  transcript_id_1064_key    INT(10) UNSIGNED,
  cdna_coding_start         INT(10),
  cdna_coding_end           INT(10),
  transcription_start_site    INT(10),
  transcript_length           INT(10),
  value_1065                  TEXT
)} );

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
			  "INSERT INTO MTMP_${dataset}_exon VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");

	my $insert_sth2 = $mart_handle->prepare(
                          "INSERT INTO MTMP_${dataset}_tr VALUES (?,?,?,?,?,?)");

	while ( my $transcript = shift( @{$transcripts} ) ) {

            eval {
		  if ( ( ++$transcript_count % 500 ) == 0 ) {
		  		print "Done $transcript_count transcripts for $species_name\n";
}

		my @exon_data_string;

		my $translation = $transcript->translation();

		my $is_coding = defined($translation);

		my $cds_length;

		if ($is_coding) {
			$cds_length =
			  $transcript->cdna_coding_end() -
			  $transcript->cdna_coding_start() + 1;

		} else {
			$cds_length = undef;
		}
               # If the transcript is coding and strand is forward add the cdna information and TSS=start
               if ($is_coding and $transcript->strand()==1) {
                   $insert_sth2->execute(
                       $transcript->dbID(),
                       $transcript->cdna_coding_start(),
                       $transcript->cdna_coding_end(),
                       $transcript->start(),
                       $transcript->length(),
                       $rna_seq_edits->{$transcript->dbID()}->[0]);
               }
               # If the transcript is coding and strand is reverse add the cdna information and TSS=end
               elsif ($is_coding and $transcript->strand()==-1) {
                   $insert_sth2->execute(
                       $transcript->dbID(),
                       $transcript->cdna_coding_start(),
                       $transcript->cdna_coding_end(),
                       $transcript->end(),
                       $transcript->length(),
                       $rna_seq_edits->{$transcript->dbID()}->[0]);
               }
               # if Transcript is not coding and strand is forward, add TSS=start
               elsif ($transcript->strand()==1) {
                    $insert_sth2->execute(
                        $transcript->dbID(),
                        undef,
                        undef,
                        $transcript->start(),
                        $transcript->length(),
                        $rna_seq_edits->{$transcript->dbID()}->[0]);
               }
               # if Transcript is not coding and strand is reverse, add TSS=end
               elsif ($transcript->strand()==-1) {
                   $insert_sth2->execute(
                       $transcript->dbID(),
                       undef,
                       undef,
                       $transcript->end(),
                       $transcript->length(),
                       $rna_seq_edits->{$transcript->dbID()}->[0]);
                }

		foreach my $exon ( @{ $transcript->get_all_Exons() } ) {

		    eval {

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
				$cds_start,
				$cds_end,
				$cds_length );
		    };
		    if($@) {
			print "Problem dealing with exon ".$exon->dbID().":".$@."\n";
		    }

		} ## end foreach my $exon ( @{ $transcript...})

		#$out->print( "\t", join( ",\n\t", @exon_data_string ), ";\n" );
            };
            if($@) {
                print "Problem processing transcript ".$transcript->stable_id().": ".$@;
            }

	} ## end while ( my $transcript = ...)

	alarm(0);

	#$out->print(<<EOT);

	print "Indexing MTMP_${dataset}_exon...\n";

	#UNLOCK TABLES;

	$mart_handle->do(
		qq{ALTER TABLE MTMP_${dataset}_exon
  ADD INDEX theindex(transcript_id_1064_key, exon_id_1017)} );

	print "Optimizing MTMP_${dataset}_exon...\n";

	$mart_handle->do(qq{OPTIMIZE TABLE MTMP_${dataset}_exon});

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
  MTMP_${dataset}_exon t
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

	print "Dropping  MTMP_${dataset}_exon...\n";

	$mart_handle->do(qq{DROP TABLE MTMP_${dataset}_exon});

	print "Indexing MTMP_${dataset}_tr...\n";

	$mart_handle->do(
		qq{ALTER TABLE MTMP_${dataset}_tr
  ADD UNIQUE INDEX theindex(transcript_id_1064_key)});

    print "Optimizing MTMP_${dataset}_tr...\n";

	$mart_handle->do(qq{OPTIMIZE TABLE MTMP_${dataset}_tr});

    eval {
	print "Modifying ${ds_base}__transcript__main...\n";

	$mart_handle->do(
		qq{ALTER ignore TABLE ${ds_base}__transcript__main
  ADD COLUMN cdna_coding_start      INT(10),
  ADD COLUMN cdna_coding_end        INT(10),
  ADD COLUMN transcription_start_site INT(10),
  ADD COLUMN transcript_length           INT(10),
  ADD COLUMN value_1065                  TEXT} );
    };
	if($@) {
      print STDERR "Problems modifying the table".$@;
	}

	print "Updating ${ds_base}__transcript__main...\n";
	$mart_handle->do(
		qq{UPDATE
  ${ds_base}__transcript__main main,
  MTMP_${dataset}_tr t
SET main.cdna_coding_start = t.cdna_coding_start,
    main.cdna_coding_end   = t.cdna_coding_end,
    main.transcription_start_site = t.transcription_start_site,
    main.transcript_length = t.transcript_length,
    main.value_1065        = t.value_1065

WHERE   main.transcript_id_1064_key = t.transcript_id_1064_key} );

print "Optimizing ${ds_base}__transcript__main...\n";

	$mart_handle->do(qq{OPTIMIZE TABLE ${ds_base}__transcript__main});
eval {
	print "Modifying ${ds_base}__translation__main...\n";

	$mart_handle->do(
		qq{ALTER ignore TABLE ${ds_base}__translation__main
  ADD COLUMN cdna_coding_start      INT(10),
  ADD COLUMN cdna_coding_end        INT(10),
  ADD COLUMN transcription_start_site INT(10),
  ADD COLUMN transcript_length           INT(10),
  ADD COLUMN value_1065                  TEXT} );
    };
	if($@) {
      print STDERR "Problems modifying the table".$@;
	}

print "Updating ${ds_base}__translation__main...\n";
	$mart_handle->do(
		qq{UPDATE
  ${ds_base}__translation__main main,
  MTMP_${dataset}_tr t
SET main.cdna_coding_start = t.cdna_coding_start,
    main.cdna_coding_end   = t.cdna_coding_end,
    main.transcription_start_site = t.transcription_start_site,
    main.transcript_length = t.transcript_length,
    main.value_1065        = t.value_1065

WHERE   main.transcript_id_1064_key = t.transcript_id_1064_key} );

print "Optimizing ${ds_base}__translation__main...\n";

	$mart_handle->do(qq{OPTIMIZE TABLE ${ds_base}__translation__main});

print "Dropping MTMP_${dataset}_tr...\n";

	$mart_handle->do(qq{DROP TABLE  MTMP_${dataset}_tr});

print "Completed ${dataset}\n";
} ## end for my $dataset (@datasets)

