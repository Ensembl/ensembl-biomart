#!/usr/bin/env/perl
use strict;
use warnings;

use Getopt::Long qw(:config no_ignore_case);
use DBI;

# Patch a mart by altering the linkVersion attribute in the template XML.

my ($host, $port, $user, $pass, $dbname, $release, $old_release, $tmp_dir);

GetOptions(
  "host=s", \$host,
  "P|port=i", \$port,
  "user=s", \$user,
  "p|pass=s", \$pass,
  "dbname=s", \$dbname,
  "release=s", \$release,
  "old_release:s", \$old_release,
  "tmp_dir:s", \$tmp_dir,
);
$old_release = $release - 1 unless $old_release;
$tmp_dir = "." unless $tmp_dir;

my $db_string = "DBI:mysql:$dbname:$host:$port";
my $dbh = DBI->connect($db_string, $user, $pass);

my $sel_sth = $dbh->prepare('SELECT dataset_id_key, xml FROM meta_conf__xml__dm;');
$sel_sth->execute();
while ( my @row = $sel_sth->fetchrow_array() ) {
  my $dataset_id_key = $row[0];
  my $xml_file = "$tmp_dir/$dataset_id_key.xml";
  my $xml = $row[1];

  $xml =~ s/(linkVersion="\w+)$old_release"/$1$release"/gm;

  open FILE, ">$xml_file";
  print FILE $xml;
  close FILE;

  print `gzip -f $xml_file`;
  my $compressed_xml = file_to_bytes("$xml_file.gz");

  my $message_digest = `md5sum $xml_file.gz`;
  $message_digest =~ s/\s+.*//;

  my $upd_sth = $dbh->prepare('UPDATE meta_conf__xml__dm SET xml = ?, compressed_xml = ?, message_digest = ? WHERE dataset_id_key = ?');
  $upd_sth->execute($xml, $compressed_xml, $message_digest, $dataset_id_key);

  unlink "$xml_file.gz";
}

$sel_sth = $dbh->prepare('SELECT template, compressed_xml FROM meta_template__xml__dm INNER JOIN meta_template__template__main USING (template);');
$sel_sth->execute();
while ( my @row = $sel_sth->fetchrow_array() ) {
  my $template = $row[0];
  my $xml_file = "$tmp_dir/$template.xml";

  open FILE, ">$xml_file.gz";
  syswrite FILE, $row[1];
  close FILE;

  print `gunzip $xml_file.gz`;
  my $xml = file_to_bytes($xml_file);
  $xml =~ s/(link_version=\w+)$old_release/$1$release/gm;
  open FILE, ">$xml_file";
  print FILE $xml;
  close FILE;

  print `gzip -f $xml_file`;
  my $compressed_xml = file_to_bytes("$xml_file.gz");

  my $upd_sth = $dbh->prepare('UPDATE meta_template__xml__dm SET compressed_xml = ? WHERE template = ?');
  $upd_sth->execute($compressed_xml, $template);

  unlink "$xml_file.gz";
}

sub file_to_bytes {
  my $file_name = $_[0];
  open my $file, "<", $file_name or die "Could not open $file_name";
  my $bytes = do { local $/; <$file> } or die "Could not read $file_name into memory";
  $bytes;
}
