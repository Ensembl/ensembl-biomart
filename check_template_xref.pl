#!/bin/env perl
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
my $db_host = 'mysql-cluster-eg-prod-1.ebi.ac.uk';
my $db_port = '4238';
my $db_user = 'ensrw';
my $db_pwd = 'writ3rp1';
my $mart_db;
my $template;

sub usage {
    print "Usage: $0 [-h <host>] [-P <port>] [-u user <user>] [-p <pwd>] [-src_mart <src>] [-target_mart <targ>]\n";
    print "-h <host> Default is $db_host\n";
    print "-P <port> Default is $db_port\n";
    print "-u <host> Default is $db_user\n";
    print "-p <password> Default is top secret unless you know cat\n";
    print "-mart <target mart> Default is $mart_db\n";
    print "-template <template file> \n";
    exit 1;
};

my $options_okay = GetOptions (
    "h=s"=>\$db_host,
    "P=s"=>\$db_port,
    "u=s"=>\$db_user,
    "p=s"=>\$db_pwd,
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

my $re = qr/ox_.*_bool/;
my %f_nodes = map { my $t = $_->{field}; $t =~ s/_bool//; lc $t => 1} grep {$_->{table} eq "main" && $_->{field} =~ /$re/ } @{search_nodes($doc,"Option")};

## Option with tableConstraint="ox_whatever"
$re = qr/ox_.*/;
my %o_nodes =  map { $_->{table} =~ s/__dm//;$_->{table} => 1 } grep {$_->{table} =~ /$re/ } @{search_nodes($doc,"Option")};
## Attribute with tableConstraint="ox_whatever"
my %a_nodes = map { $_->{table} =~ s/__dm//; $_->{table} => 1 } grep {$_->{table} =~ /$re/ } @{search_nodes($doc,"AttributeDescription")};
$doc->dispose;

# now check the tables against these lists
my $mart_string = "DBI:mysql:$mart_db:$db_host:$db_port";
my $mart_handle = DBI->connect($mart_string, $db_user, $db_pwd,
			       { RaiseError => 1 }
    ) or croak "Could not connect to $mart_string";

$mart_handle->do("use $mart_db");
# get tables
my %tabs = map{$_ =~ s/.*gene__(ox_.*)__dm/$1/; lc($_)=>1} query_to_strings($mart_handle,"show tables like '%__ox_%__dm'");
my $missing = {};
for my $table (keys %tabs) {
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

if(defined $missing->{filter}) {
    print "Missing boolean filters:\n";
    for my $table (sort @{$missing->{filter}}) {
	my $name= $table;
	$name =~ s/ox_//;
	my $opt = <<END;
          <Option displayName="with $name ID(s)" displayType="list" field="${table}_bool" internalName="with_$name" isSelectable="true" key="gene_id_1020_key" legal_qualifiers="only,excluded" qualifier="only" style="radio" tableConstraint="main" type="boolean">
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
	$name =~ s/ox_//;
	my $opt = <<END;
	<Option checkForNulls="true" displayName="$name ID(s)" description="Filter to include genes with $name IDs" displayType="text" field="dbprimary_acc_1074" internalName="$name" isSelectable="true" key="gene_id_1020_key" legal_qualifiers="=,in" multipleValues="1" qualifier="=" tableConstraint="${table}__dm" type="list"/>
END
	print $opt;
    }
}
if(defined $missing->{attribute}) {
    print "Missing attributes:\n";
    
    for my $table (sort @{$missing->{attribute}}) {
	my $name= $table;
	$name =~ s/ox_//;
	my $opt = <<END;
        <AttributeDescription checkForNulls="true" displayName="$name ID" field="dbprimary_acc_1074" internalName="$name" key="gene_id_1020_key" maxLength="40" tableConstraint="${table}__dm"/>
END
print $opt;
    }
}
