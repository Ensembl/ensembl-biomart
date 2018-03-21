# Copyright [1999-2013] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
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

use strict;
use Getopt::Long;
use Carp;
use DBI;
use POSIX;


my ( $old_dbname, $olduser,    $oldpass, $oldhost,
     $oldport,    $new_dbname, $newuser, $newpass,
     $newhost,    $newport,    $mart,    $oldrel,
     $newrel,     $dumpdir,    $percent, $empty_column );

$oldhost = 'mysql-ens-general-prod-1';
$newhost = 'mysql-ens-sta-1';
$oldport = '4525';
$newport = '4519';
$olduser = $newuser = 'ensro';

GetOptions( 'old_dbname=s'   => \$old_dbname,
            'olduser=s'      => \$olduser,
            'oldpass=s'      => \$oldpass,
            'oldhost=s'      => \$oldhost,
            'oldport=i'      => \$oldport,
            'new_dbname=s'   => \$new_dbname,
            'newuser=s'      => \$newuser,
            'newpass=s'      => \$newpass,
            'newhost=s'      => \$newhost,
            'newport=i'      => \$newport,
            'new_rel=i'      => \$newrel,
            'old_rel=i'      => \$oldrel,
            'dumpdir=s'      => \$dumpdir,
            'percent=s'      => \$percent,
            'mart=s'         => \$mart,
            'empty_column=s' => \$empty_column );

if ( !defined($oldhost) ) {
  $oldhost = 'mysql-ens-general-prod-1';
}

if ( !defined($newhost) ) {
  $newhost = 'mysql-ens-sta-1';
}

if ( !defined($new_dbname) ) {
  $new_dbname = $mart . "_" . $newrel;
}

if ( !defined($old_dbname) ) {
  $old_dbname = $mart . "_" . $oldrel;
}

my $dir = "./";
if ( defined($dumpdir)) {
  $dir = "$dumpdir/";
}

if ( defined($percent) ) {
  $percent = $percent/100;
} else {
  $percent = 0.1;    # 10%
}

my @marts;
if ( defined($mart) ) {
  @marts = split /,/, $mart;
} else {
  @marts = ( "ensembl_mart",          "regulation_mart",
             "genomic_features_mart", "ontology_mart",
             "sequence_mart",         "snp_mart",
             "vega_mart" );
}

if ( !defined($oldrel) or !defined($newrel) ) {
  die "You have to specify the release number s to compare i.e. -old_rel 61 -new_rel 62\n";
}

foreach my $mart (@marts) {
  my %old_tables;
  my %new_tables;
  my %old_count;    # {species}{table} = count;
  my %new_count;
  my %old_species;
  my %new_species;
  my %old_empty_columns;

  my $diff_file   = $old_dbname . "_" . $oldhost . "_vs_" . $new_dbname . "_" . $newhost . ".DIFF";
  my $table_file  = $old_dbname . "_" . $oldhost . "_vs_" . $new_dbname . "_" . $newhost . ".TABLE";
  my $column_file = $old_dbname . "_" . $oldhost . "_vs_" . $new_dbname . "_" . $newhost . ".COLUMN";

  open(    OUT, ">" . $dir . $diff_file )   || die "Could no open $diff_file file";
  open(  TABLE, ">" . $dir . $table_file )  || die "Could no open $table_file file";
  open( COLUMN, ">" . $dir . $column_file ) || die "Could no open $column_file file";

  # If running the empty column test (time consuming test)
  my $empty_column_file;
  if ( defined($empty_column) ) {
    $empty_column_file = $old_dbname . "_" . $oldhost . "_vs_" . $new_dbname . "_" . $newhost . ".EMPTY_COLUMN";
    open( EMPTY_COLUMN, ">" . $dir . $empty_column_file) || die "Could no open $empty_column_file file";
  }

########################################
# Check tables and columns name the same
########################################

  my $new_dbi = dbi( $newhost, $newport, $newuser, $new_dbname, $newpass );
  my $old_dbi = dbi( $oldhost, $oldport, $olduser, $old_dbname, $oldpass );

  $old_dbi->{PrintError} = 0;
  $new_dbi->{PrintError} = 0;

  #Going throught the new mart
  foreach my $ext ( "_main", "_dm" ) {    # may need to add others
    my $tables_sql = 'show tables like "%_' . $ext . '"';
    my $sth        = $new_dbi->prepare($tables_sql);
    $sth->execute();
    while ( my $table_name = $sth->fetchrow_array() ) {

      my %columns;
      my $try_sth = $old_dbi->prepare("describe $table_name");
      if ( $try_sth->execute ) {
        while ( my $col = $try_sth->fetch ) {
          $columns{ $$col[0] } = 1;
          # print join("\t",@$arse)."\n";
        }
        $try_sth = $new_dbi->prepare("describe $table_name");
        $try_sth->execute;
        while ( my $col = $try_sth->fetch ) {
          # If running the empty column test (time consuming test)
          if ( defined($empty_column) ) {
            # Looking for empty columns in strain gtype poly (empty
            # columns will have blank rows)
            if ( $table_name =~ "strain_gtype_poly" ) {
              my $empty_colums = $new_dbi->prepare(
                 "select $$col[0] from $table_name where $$col[0]!='' limit 1"
              );
              $empty_colums->execute();
              my $res = $empty_colums->fetchrow_array();
              #If a strain is empty then store column name into a hash
              if ( $res eq "" ) {
                $old_empty_columns{$table_name}{ $$col[0] } = "";
              }
            }
            # For the other tables, looking for columns full of nulls
            # and storing them into a hash
            else {
              my $empty_colums = $new_dbi->prepare(
                "select $$col[0] from $table_name where $$col[0] is not null limit 1" );
              $empty_colums->execute();
              my $res = $empty_colums->fetchrow_array();
              if ( $res eq "" ) {
                $old_empty_columns{$table_name}{ $$col[0] } = "";
              }
            }
          }
          # For a new column, if it's in strain_gtype_poly check for
          # columns full of empty rows and for the other check for
          # full of nulls.
          if ( !defined( $columns{ $$col[0] } ) ) {
            print COLUMN "** For table $table_name NEW column " . $$col[0] . "\n";
            # If running the empty column test (time consuming test)
            if ( defined($empty_column) ) {
              if ( $table_name =~ "strain_gtype_poly" ) {
                my $empty_colums = $new_dbi->prepare(
                  "select $$col[0] from $table_name where $$col[0]!='' limit 1" );
                $empty_colums->execute();
                my $res = $empty_colums->fetchrow_array();
                #If a strain is empty then store column name into a hash
                if ( $res eq "" ) {
                  print EMPTY_COLUMN "** For table $table_name new column "
                    . $$col[0]
                    . " is empty\n";
                }
              }
              # For the other tables, looking for columns full of nulls
              else {
                my $empty_colums = $new_dbi->prepare(
                  "select $$col[0] from $table_name where $$col[0] is not null limit 1" );
                $empty_colums->execute();
                my $res = $empty_colums->fetchrow_array();
                if ( $res eq "" ) {
                  print EMPTY_COLUMN "** For table $table_name new column "
                    . $$col[0]
                    . " is null\n";
                }
              }
            } ## end if ( defined($empty_column...))
          } ## end if ( !defined( $columns...))
        } ## end while ( my $col = $try_sth...)
      } else {
        print TABLE "***** New Table $table_name *****\n";
      }
    } ## end while ( my $table_name = ...)
  } ## end foreach my $ext ( "_main", ...)

  #Going throught the old mart
  foreach my $ext ( "_main", "_dm" ) {    # may need to add others
    my $tables_sql = 'show tables like "%_' . $ext . '"';
    my $sth        = $old_dbi->prepare($tables_sql);
    $sth->execute();
    while ( my $table_name = $sth->fetchrow_array() ) {

      my %columns;
      my $try_sth = $new_dbi->prepare("describe $table_name");
      if ( $try_sth->execute ) {
        while ( my $col = $try_sth->fetch ) {
          $columns{ $$col[0] } = 1;
          # print join("\t",@$arse)."\n";
        }

        $try_sth = $old_dbi->prepare("describe $table_name");
        $try_sth->execute;
        while ( my $col = $try_sth->fetch ) {
          # If running the empty column test (time consuming test)
          if ( defined($empty_column) ) {
            if ( $table_name =~ "strain_gtype_poly" ) {
              my $empty_colums = $old_dbi->prepare(
                 "select $$col[0] from $table_name where $$col[0]!='' limit 1"
              );
              $empty_colums->execute();
              my $res = $empty_colums->fetchrow_array();
              # If the column is empty in the old mart
              if ( $res eq "" ) {
                # Check if it was empty in the new mart. This is fine.
                if ( exists( $old_empty_columns{$table_name}{ $$col[0] } ) ) {
                  1;
                }
                # Check if the columns is not gone, no need to report it
                elsif ( !defined( $columns{ $$col[0] } ) ) {
                  1;
                }
                # Column now contain data in the new mart.
                else {
                  print EMPTY_COLUMN "** For table $table_name column " . $$col[0] . " now contain data\n";
                }
              }
              # Column is not empty in the old mart but is in the new mart.
              else {
                if ( exists( $old_empty_columns{$table_name}{ $$col[0] } ) ) {
                  print EMPTY_COLUMN "** For table $table_name column "
                    . $$col[0]
                    . " is now empty\n";
                }
              }
            } else {
              my $empty_colums = $old_dbi->prepare(
                "select $$col[0] from $table_name where $$col[0] is not null limit 1" );
              $empty_colums->execute();
              my $res = $empty_colums->fetchrow_array();
              # IF the column is empty in the old mart
              if ( $res eq "" ) {
                # Check if it was empty in the new mart. This is fine.
                if ( exists( $old_empty_columns{$table_name}{ $$col[0] } ) ) {
                  1;
                }
                # Check if the columns is not gone, no need to report it
                elsif ( !defined( $columns{ $$col[0] } ) ) {
                  1;
                }
                # Column now contain data in the new mart.
                else {
                  print EMPTY_COLUMN "** For table $table_name column "
                    . $$col[0]
                    . " now contain data\n";
                }
              }
              # Column is not empty in the old mart but is in the new mart.
              else {
                if ( exists( $old_empty_columns{$table_name}{ $$col[0] } ) ) {
                  print EMPTY_COLUMN "** For table $table_name column " . $$col[0] . " is now empty\n"; }
              }
            } ## end else [ if ( $table_name =~ "strain_gtype_poly")]
          } ## end if ( defined($empty_column...))

          if ( !defined( $columns{ $$col[0] } ) ) {
            print COLUMN "** For table $table_name OLD column " . $$col[0] . " has gone\n"; }
        } ## end while ( my $col = $try_sth...)
      } else {
        print TABLE "***** GONE Table $table_name *****\n";
      }
    } ## end while ( my $table_name = ...)
  } ## end foreach my $ext ( "_main", ...)

####################################
  # End first check
####################################

foreach my $rel ( "new", "old" ) {
  my $user;
  my $pass;
  my $host;
  my $port;
  my $count_ref;
  my $tables_ref;
  my $species_ref;
  my $release;
  my $dbi;

  if ( $rel eq "new" ) {
    $release     = $newrel;
    $count_ref   = \%new_count;
    $tables_ref  = \%new_tables;
    $species_ref = \%new_species;
    $user        = $newuser || "ensro";
    $pass        = $newpass || undef;
    $host        = $newhost || "ens-staging2";
    $port        = $newport || 3306;

    $dbi = dbi( $host, $port, $user, $new_dbname, $pass);
    print "Looking up data for " . $new_dbname . "\n";

  } else {
    $release     = $oldrel;
    $count_ref   = \%old_count;
    $tables_ref  = \%old_tables;
    $species_ref = \%old_species;
    $user        = $olduser || "ensro";
    $pass        = $oldpass || undef;
    $host        = $oldhost || "mart1";
    $port        = $oldport || 3306;

    $dbi = dbi( $host, $port, $user, $old_dbname, $pass );
    print "Looking up data for " . $old_dbname . "\n";
  }

  foreach my $ext ( "_main", "_dm" ) {    # may need to add others
    my $tables_sql = 'show tables like "%_' . $ext . '"';
    my $sth        = $dbi->prepare($tables_sql);
    $sth->execute();
    while ( my $table_name = $sth->fetchrow_array() ) {
      #hsapiens_gene_ensembl__protein_feature_pfscan__dm
      #hsapiens_gene_ensembl__gene__main

      if ( $table_name =~ /^meta/ ) {
        next;
      }
      my ( $long_species, $feat, $type ) = split /__/, $table_name;

      $species_ref->{$long_species} = 1;
      $tables_ref->{$feat}          = 1;
      my $table_count_sth =
        $dbi->prepare("SELECT count(1) as count FROM $table_name");
      $table_count_sth->execute();
      my $how_many;
      $table_count_sth->bind_columns( \$how_many );
      $table_count_sth->fetch;

      if ( defined($how_many) ) {
        $count_ref->{$long_species}{$feat} = $how_many;
      } else {
        print "how_many = undef for $long_species, $feat??\n";
      }
      # print "species = $species, feat = $feat, type = $type, count = $how_many\n";
    } ## end while ( my $table_name = ...)
  } ## end foreach my $ext ( "_main", ...)
} ## end foreach my $rel ( "new", "old")

  #
  # Look for new or deleted species
  #
  my %all_species;
  foreach my $old_sp ( sort keys %old_species ) {
    if ( !defined( $new_species{$old_sp} ) ) {
      print OUT "Cannot find $old_sp in new database\n";

      #
      # Rhoda wants all tables listed anyway
      #      delete $old_species{$old_sp};  # no point analysing this any more
    } else {
      $all_species{$old_sp} = 1;
    }
  }
  foreach my $new_sp ( sort keys %new_species ) {
    if ( !defined( $old_species{$new_sp} ) ) {
      print "############## Cannot find $new_sp in old database. Is this new?\n";
      # delete $new_species{$new_sp};  # no point analysing this any more
    } else {
      $all_species{$new_sp} = 1;
    }
  }

  #
  # Compare the tables sizes (number of rows)
  #
  foreach my $sp ( sort keys %all_species ) {
    foreach my $tab ( sort keys %old_tables ) {
      if ( !defined( $old_count{$sp}{$tab} ) ) {
        next;
      }
      if ( $old_count{$sp}{$tab} > 0 ) {
        $new_count{$sp}{$tab} |= 0;
        my $diff = abs( $old_count{$sp}{$tab} - $new_count{$sp}{$tab} )/
          $old_count{$sp}{$tab};
        if ( $diff > $percent and $new_count{$sp}{$tab} > $old_count{$sp}{$tab}  ) {    #10% change
          print OUT $sp . "<->" . $tab . "\t" . $old_count{$sp}{$tab} . "\t" . $new_count{$sp}{$tab} . "\t" . ceil( $diff*100 ) . "\n";
        }
        elsif($diff > $percent and $new_count{$sp}{$tab} < $old_count{$sp}{$tab} )
        {
          print OUT $sp . "<->" . $tab . "\t" . $old_count{$sp}{$tab} . "\t" . $new_count{$sp}{$tab} . "\t" . ( "-" . ceil($diff*100) ) . "\n";
        }
        next;
      }
      if ( defined( $new_count{$sp}{$tab} ) && $new_count{$sp}{$tab} > 0 ) {
        print OUT $sp . "<->" . $tab . "*\t" . $old_count{$sp}{$tab} . "\t" . $new_count{$sp}{$tab} . "\n";
      }
    } ## end foreach my $tab ( sort keys...)
  } ## end foreach my $sp ( sort keys ...)

  # Sort the files to create more readable outputs
  system("sort -t '_' -k2 $table_file -o $table_file ");
  close(TABLE);

  system("sort -t '_' -k2 $column_file -o $column_file");
  close(COLUMN);

  system("sort -k4,4nr $diff_file -o $diff_file");
  close(OUT);

  if (defined($empty_column_file)) {
    system("sort -t '_' -k2 $empty_column_file -o $empty_column_file");
    close(EMPTY_COLUMN);
  }


} # foreach mart

sub dbi {
  my ( $host, $port, $user, $dbname, $pass ) = @_;
  my $dbi2 = undef;

  if ( !defined $dbi2 || !$dbi2->ping() ) {
    my $connect_string =
      sprintf( "dbi:mysql:host=%s;port=%s;database=%s",
               $host, $port, $dbname );

    $dbi2 = DBI->connect( $connect_string, $user, $pass,
      # { 'RaiseError' => 1 } )
      )
      or warn( "Can't connect to database: " . $DBI::errstr )
      and return undef;
    $dbi2->{'mysql_auto_reconnect'} = 1;    # Reconnect on timeout
  }

  return $dbi2;
}
