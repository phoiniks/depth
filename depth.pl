#!/usr/bin/perl -w
use strict;

use DBD::CSV;

# agrell@havi.de, 2014

#########################################################################
# Dieses Skript überprüft die Schachtelungstiefe von in einer CSV-Datei #
# verzeichneten Pfaden. Dies ist beispielsweise wichtig bei mehrfach    #
# eingebetteten E-Mail-Attachments.                                     #
# Es erwartet eine CSV-Datei mit den folgenden Feldern:                 #
# folderid, compid, src_path und dst_path. Das Skript ermöglicht über   #
# die Erzeugung einer weiteren CSV-Datei die Auswahl verschieden tief   #
# geschachtelter Dokumente/Dokumenthierarchien, was im Anschluss bei    #
# einem Kopiervorgang zwecks Kontrolle nützlich ist.                    #
#########################################################################

if ( -e "depth.csv" ) {
    unlink "depth.csv";
}

open CSV, ">>", "depth.csv";

my $dbh = DBI->connect("DBI:CSV:attrs", '', '', {f_ext => ".csv/r", RaiseError => 1, csv_eol => "\n", csv_sep_char => "|", }) or die "Cannot connect: $DBI::errstr";

my $table = $ARGV[0];

$dbh->{csv_tables}->{$table}->{skip_rows} = 0;
$dbh->{csv_tables}->{$table}->{col_names} =
    [qw(folderid compid src_path dst_path)];

my $sth = $dbh->prepare("select folderid, compid, src_path, dst_path from $table");

$sth->execute;

my %hash;
my $h = \%hash;

while (my @row = $sth->fetchrow_array) {
    if (exists $h->{$row[0]}) {
	$h->{$row[0]}++;
	}
    else {
	$h->{$row[0]} = 1;
    }
}

my %occurrences;
my $o = \%occurrences;

foreach ( my @keys = keys %$h ) {
    $o->{$h->{$_}} = $_;
}

my %levels;
my $l = \%levels;

foreach ( sort my @keys = keys %$o ) {
    print "$_ components in folderid $o->{$_}:\n\n";

    $sth = $dbh->prepare("select * from $table where folderid=$o->{$_}");
    $sth->execute;

    while (my @row = $sth->fetchrow_array) {
	print $row[1], "\n";
	my $depth = $row[1] =~ tr/\.//;
	printf "Depth: %d\n", $depth;
	$l->{$depth} = $o->{$_}; 
	printf "%s\n", join "|", @row;
	printf CSV "%s\n", join "|", @row;
    }

    print "\n\n";
}

print "Examples for different depth levels in this collection (level => folderid):\n\n";
while ( my ( $k, $v ) = each %$l ) {
    print "$k => $v\n";
}
