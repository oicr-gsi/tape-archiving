#!/usr/bin/perl

# A script for updating the seqware database with the MD5sum and file size of the given seqware files.
# The only argument this script takes in is the location of the File Provancence Report.

# Uses SGECaller.pl to calculate MD5sum and file sizes for given files in FPR. 

# Sample Usage:
# ./seqWareMD5Updater.pl <FPR_PATH>
# Example:
# ./seqWareMD5Updater.pl /files/fpr-2015-05-15.txt

# Last Modified: June 10, 2015 by Andrew Duncan

use strict;
use warnings;

use Cwd 'abs_path';
use File::Path qw(make_path remove_tree);

# Redirect error stream to stderr.log
open(STDERR, ">>stderr.log") or die "Failed to open error log file";
`date > stderr.log`;

# Set up output dir (timestamp)
my $OutputDir = `pwd`;
my $MD5OutputDir = `pwd`;
chomp ($OutputDir);
chomp ($MD5OutputDir);
$OutputDir .= "/MD5_Dir";
$MD5OutputDir .= "/output";

# Create Output Dir (Temp)
print "Creating output dir at $OutputDir\n";
if (-e $OutputDir) { remove_tree ($OutputDir) }
make_path ($OutputDir) or die "$@";

use DBI;

# Connect to database
my $path = `pwd`;
chomp($path);
$ENV{PGSYSCONFDIR} = $path;

my $dbh = DBI->connect("dbi:Pg:service=test", undef, undef, { AutoCommit => 1}) or die "Can't connect to the database: $DBI::errstr\n";

my $Length = @ARGV; # Number of inputs

# Check that the correct number of arguments have been provided
if ( $Length != 1 ) {
	die "You have entered $Length argument(s). 1 argument is required to run.\n" ;
}

# Get the absolute path of File Provenance Report
my $FPR = shift @ARGV;
$FPR = abs_path($FPR);

# Check if FPR exists
if ( ! -e $FPR ){
	print "File Provenance Report does not exist at $FPR";
	exit;
}

# Create necessary files from FPR
print "Grabbing columns from File Provenance Report...\n";
print "Creating files:\n";
print "$OutputDir/FileSWIDPath.txt\n";
print "$OutputDir/FilePath.txt\n";

my $LineCount = 0;

# Create a file with two columns,  File SWID and File Path
open my $FPR_FH, '<', $FPR or die "Can't open file '$FPR'\n";
open my $FILE_SWID_PATH_ONE_FH, '>', "$OutputDir/FileSWIDPath.txt" or die "Can't open file '$OutputDir/FileSWIDPath.txt'\n";
while (my $line = <$FPR_FH>) {
	if ($LineCount > 0) {
		my @fields = split ("\t", $line);
		print $FILE_SWID_PATH_ONE_FH "$fields[46]\t$fields[44]\n";
	}
	$LineCount += 1;
}

close ($FILE_SWID_PATH_ONE_FH);
close ($FPR_FH);

# Create a file which contains one column of all the given file paths
open my $FILE_SWID_PATH_TWO_FH, '<', "$OutputDir/FileSWIDPath.txt" or die "Can't open file '$OutputDir/FileSWIDPath.txt'\n";
open my $FILE_PATH_FH, '>', "$OutputDir/FilePath.txt" or die "Can't open file '$OutputDir/FilePath.txt'\n";
while (my $line = <$FILE_SWID_PATH_TWO_FH>) {
        my @fields = split ("\t", $line);
       	print $FILE_PATH_FH "$fields[0]\n";
}

close ($FILE_PATH_FH);
close ($FILE_SWID_PATH_TWO_FH);

print "Files created.\n";

# Call SGEcaller Script to compute MD5 and file sizes for given files
print "Calculating MD5sums and file sizes for files in the File Provenance Report...\n";
`./SGECaller.pl "$OutputDir"/FilePath.txt`;

my $MD5File = $MD5OutputDir . "/Files.Index";
my $copyMD5File = $OutputDir . "/Files.Index";

my $RawIndex = $MD5OutputDir . "/rawindex.fil";

if (-z "$RawIndex") {
        print "No files have been added or modified since last run!\n";
        exit;
}

# Ensure that Files.Input exists before using it 
sleep(1) while not -e $MD5File;

# Once above is run, should have a file in CWD/output called Files.Index
`cp "$MD5File" "$copyMD5File"`; # Copy MD5 File so that I can alter it without changing the original

# Open MD5 File created by SGECaller.pl
my $FileSWIDPath = $OutputDir."/FileSWIDPath.txt";
open my $FILE_SWID_PATH_FH, '<', $FileSWIDPath or die "Can't open file '$FileSWIDPath'\n";
open my $MD5_FILE_FH, '<', $copyMD5File or die "Can't open file '$copyMD5File'\n";

# Iterate through MD5/FileSize file (Files.Index)
print "Updating SeqWare database...\n";

while ( my $LineA = <$MD5_FILE_FH> ) { # MD5_PATH_SIZE
	chomp( $LineA );
	my ( $MD5, $PathA, $Size ) = split (/\t/,$LineA);
	seek $FILE_SWID_PATH_FH, 0, 0; # This may not be required (Removing might improve efficiency)
	if ( index( $MD5, "--" ) != -1) {
		`echo $LineA >> stderr.log`;
		next;
	}
	while ( my $LineB = <$FILE_SWID_PATH_FH> ) { # SWID_PATH
		chomp( $LineB );
		my ( $SWID, $PathB ) = split (/\t/,$LineB);
		if ( $PathA eq $PathB ) {
			# Update SQL table with MD5 and file size
			my $Count = $dbh->selectrow_array('SELECT count(*) FROM file WHERE FILE_PATH = ?', undef, $PathA); 
			if ($Count >= 1) {
				$dbh->do('UPDATE file SET size = ?, md5sum = ? WHERE sw_accession = ?', undef, $Size, $MD5, $SWID);
			} else {
				print STDERR "Could not find file $PathA in database.\n";
			}
			last;
		}
	}
}

# Disconnect from database
$dbh->disconnect;

# Close files
close ( $FILE_SWID_PATH_FH );
close ( $MD5_FILE_FH );

print "Updating complete.\n";

# Cleaning up files
`rm -r $OutputDir`;

print "Script completed in ";
print time - $^T;
print " seconds\n";
