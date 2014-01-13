#!/bin/env perl
# Copyright [2009-2014] EMBL-European Bioinformatics Institute
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
# Script for splitting datasets from a multi-species mart 

use warnings;
use strict;
use DBI;
use Carp;
use Log::Log4perl qw(:easy);
use List::MoreUtils qw(any);
use Data::Dumper;
use DbiUtils;
use Getopt::Long;
use XML::DOM;
Log::Log4perl->easy_init($DEBUG);

my $logger = get_logger();

sub search_nodes {
    my ($doc, $tag) = @_;
    my $found_nodes = [];
    # print all HREF attributes of all CODEBASE elements
    my $nodes = $doc->getElementsByTagName ($tag);
    my $n = $nodes->getLength;
    for (my $i = 0; $i < $n; $i++) {
	my $node = $nodes->item ($i);	
	my $tc = $node->getAttributeNode("tableConstraint");
	my $field = $node->getAttributeNode("field");
	if($field && $tc) {
	    push @{$found_nodes}, {table=>lc($tc->getValue), field=>lc($field->getValue), tag=>$tag}; 
	}
    }
    return $found_nodes;
}

# db params
my $db_host = 'mysql-eg-prod-1.ebi.ac.uk';
my $db_port = '4238';
my $db_user = 'ensro';
my $db_pwd;
my $mart_db;
my $template = './templates/eg_template_template.xml';

sub usage {
    print "Usage: $0 [-host|-h <host>] [-port|-P <port>] [-u|-user user <user>] [-p|-pass <pwd>] [-mart <src>] [-template <template file>]\n";
    print "-h|-host <host> Default is $db_host\n";
    print "-P|-port <port> Default is $db_port\n";
    print "-u|-user <host> Default is $db_user\n";
    print "-p|-pass <password> (No default)\n";
    print "-mart <target mart> (No default)\n";
    print "-template <template file> \n";
    exit 1;
};

my $options_okay = GetOptions (
    "host|h=s"=>\$db_host,
    "port|P=s"=>\$db_port,
    "user|u=s"=>\$db_user,
    "pass|p=s"=>\$db_pwd,
    "mart=s"=>\$mart_db,
    "template=s"=>\$template,
    "help"=>sub {usage()}
    );


if(!$options_okay || !$template || !$mart_db) {
    usage();
}


my $parser = new XML::DOM::Parser;
my $doc = $parser->parsefile ($template);

# Avoid memory leaks - cleanup circular references for garbage collection
# create hash of filters and attributes:
## Option with tableConstraint="main" and field="ox_whatever_bool"

my $re = qr/(ox|efg)_.*_bool/;
my %f_nodes = map { my $t = $_->{field}; $t =~ s/_bool//; lc $t => 1} grep {$_->{table} eq "main" && $_->{field} =~ /$re/ } @{search_nodes($doc,"Option")};

## Option with tableConstraint="ox_whatever"
$re = qr/(ox|efg)_.*/;
my %o_nodes =  map { $_->{table} =~ s/__dm//;$_->{table} => 1 } grep {$_->{table} =~ /$re/ } @{search_nodes($doc,"Option")};
## Attribute with tableConstraint="ox_whatever"
my %a_nodes = map { $_->{table} =~ s/__dm//; $_->{table} => 1 } grep {$_->{table} =~ /$re/ } @{search_nodes($doc,"AttributeDescription")};
$doc->dispose;

# now check the tables against these lists
my $mart_string = "DBI:mysql:host=$db_host:port=$db_port;database=$mart_db";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

$mart_handle->do("use $mart_db");
# get tables
my %tabs = map{$_ =~ s/.*gene__(ox_.*)__dm/$1/; lc($_)=>1} query_to_strings($mart_handle,"show tables like '%\\_\\_ox\\_%\\_\\_dm'");
my %tabs2 = map{$_ =~ s/.*gene__(efg_.*)__dm/$1/; lc($_)=>1} query_to_strings($mart_handle,"show tables like '%\\_\\_efg\\_%\\_\\_dm'");
# get keys
my %keys = query_to_hash($mart_handle,"select TABLE_NAME,COLUMN_NAME from information_schema.columns where TABLE_SCHEMA='$mart_db' AND TABLE_NAME LIKE '%\\_\\_dm' and column_name like '%\\_key'");
for my $main (qw(gene transcript translation)) {
    %keys = (%keys, query_to_hash($mart_handle,"select TABLE_NAME,COLUMN_NAME from information_schema.columns where TABLE_SCHEMA='$mart_db' AND TABLE_NAME LIKE '%\\_$main\\_\\_main' and column_name like '$main\\_%\\_key'"));
}
my %ds_keys;
while (my ($table,$key) = each(%keys)) {
    my $t = $table;
    $t =~ s/.*gene__(.*)__(dm|main)/$1/;
    push @{$ds_keys{$t}{$key}}, $table;
}

my $missing = {};
for my $tab (\%tabs,\%tabs2) {
    for my $table (keys %$tab) {
	if(!$f_nodes{$table}) {
	    push @{$missing->{filter}}, $table;
	}
	if(!$o_nodes{$table}) {
	    push @{$missing->{option}}, $table;
	}
	if(!$a_nodes{$table}) {
	    push @{$missing->{attribute}}, $table;
	}
    }
}

my $key;
my $field;
if(defined $missing->{filter}) {
    print "Missing boolean filters:\n";
    for my $table (sort @{$missing->{filter}}) {
	my $name= $table;
	$key = get_key(\%ds_keys,$table);
	if($name =~ /efg_/) {
	    $name =~ s/efg_//;
#	    $key = "transcript_id_1064_key";
	} else {
	    $name =~ s/ox_//;
#	    $key="gene_id_1020_key";
	}
	my $opt = <<END;
          <Option displayName="with $name ID(s)" displayType="list" field="${table}_bool" internalName="with_$name" isSelectable="true" key="$key" legal_qualifiers="only,excluded" qualifier="only" style="radio" tableConstraint="main" type="boolean">
            <Option displayName="Only" internalName="only" isSelectable="true" value="only"/>
            <Option displayName="Excluded" internalName="excluded" isSelectable="true" value="excluded"/>
          </Option>
END
	print $opt;
    }
}
if(defined $missing->{option}) {
    print "Missing list filters:\n";
    for my $table (sort @{$missing->{option}}) {
	my $name= $table;
	$key = get_key(\%ds_keys,$table);
	if($name =~ /efg_/) {
	    $name =~ s/efg_//;
	    $field="display_label_11056";
	} else {
	    $name =~ s/ox_//;
#	    $key="gene_id_1020_key";
	    $field="dbprimary_acc_1074";
	}
	my $opt = <<END;
	<Option checkForNulls="true" displayName="$name ID(s)" displayType="text" field="$field" internalName="$name" isSelectable="true" key="gene_id_1020_key" legal_qualifiers="=,in" multipleValues="1" qualifier="=" tableConstraint="${table}__dm" type="list"/>
END
	print $opt;
    }
}
if(defined $missing->{attribute}) {
    print "Missing attributes:\n";
    
    for my $table (sort @{$missing->{attribute}}) {
	my $name= $table;
	$key = get_key(\%ds_keys,$table);
	if($name =~ /efg_/) {
	    $name =~ s/efg_//;
#	    $key = "transcript_id_1064_key";
	    $field="display_label_11056";
	} else {
	    $name =~ s/ox_//;
#	    $key="gene_id_1020_key";
	    $field="dbprimary_acc_1074";
	}
	my $opt = <<END;
        <AttributeDescription checkForNulls="true" displayName="$name ID" field="$field" internalName="$name" key="$key" maxLength="40" tableConstraint="${table}__dm"/>
END
print $opt;
    }
}

sub get_key {
    my ($keys,$table) = @_; 
	my $key;
    my $keyHash = $keys->{$table};
    if(!defined $keyHash) {
	warn "Could not find key for table $table\n";
	$key = "gene_id_1020_key";
    } else {
    my @keySet = keys(%$keyHash); 
    # take the key with the largest number of keys
    @keySet = sort {scalar(@{$keyHash->{$a}}) <=> scalar(@{$keyHash->{$b}})} @keySet;
    $key = $keySet[0];
    if(scalar(@keySet)>1) {
	warn "More than one key found for $table - using $key";
    }   
    }
    return $key;
}
