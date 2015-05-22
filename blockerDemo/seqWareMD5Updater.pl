#!/usr/bin/perl

# A script for updating the seqware database with the MD5sum and file size of the given seqware files.
# The only argument this script takes in is the location of the File Provancence Report.

# Uses SGECaller.pl to calculate MD5sum and file sizes for given files in FPR. 

# Sample Usage:
# ./seqWareMD5Updater.pl <FPR_PATH>
# Example:
# ./seqWareMD5Updater.pl /files/fpr-2015-05-15.txt

# Last Modified: May 22, 2015 by Andrew Duncan

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


# Set up database information
my $dbname = "seqware_meta_db_1_1_0_150429";
my $hostname = "hsqwstage-www2.hpc";
my $dsn = "dbi:Pg:dbname=$dbname;host=$hostname";
my $user = "hsqwstage2_rw";
my $password = "lxf4VkHQ";

# Connect to database
my $dbh = DBI->connect($dsn, $user, $password, { AutoCommit => 1 }) or die "Can't connect to the database: $DBI::errstr\n";

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

# Grab columns (file_path and file_SWID) from FPR, remove the first line (header)
print "Grabbing columns from File Provenance Report...\n";
print "Creating files:\n";
print "$OutputDir/FileSWIDPath.txt\n";
print "$OutputDir/FilePath.txt\n";

`cut -f 47,45 "$FPR" > "$OutputDir"/FileSWIDPath.txt`; # Grab FileSWID and FilePath columns
`tail -n +2 "$OutputDir"/FileSWIDPath.txt > "$OutputDir"/tempFileSWIDPath.txt`; # Remove header
`cp "$OutputDir"/tempFileSWIDPath.txt "$OutputDir"/FileSWIDPath.txt`;
`rm "$OutputDir"/tempFileSWIDPath.txt`;

# Create a file with just paths (input for SGECaller.pl)
`cut -f 2 "$OutputDir"/FileSWIDPath.txt > "$OutputDir"/FilePath.txt`;

print "Files created.\n";

# Call SGEcaller Script to compute MD5 and file sizes for given files
print "Calculating MD5sums and file sizes for files in the File Provenance Report...\n";
`./SGECaller.pl "$OutputDir"/FilePath.txt`;

my $MD5File = $MD5OutputDir . "/Files.Index";
my $copyMD5File = $OutputDir . "/Files.Index";

# Ensure that Files.Input exists before using it 
while (  ) {
	if ( -e $MD5File ) {
		last;
	}
	sleep(2); # Wait 2 seconds
} 

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
	while ( my $LineB = <$FILE_SWID_PATH_FH> ) { # SWID_PATH
		chomp( $LineB );
		my ( $SWID, $PathB ) = split (/\t/,$LineB);
		if ( $PathA eq $PathB ) {
			# Update SQL table with MD5 and file size
			my $Count = $dbh->selectrow_array('SELECT count(*) FROM file WHERE FILE_PATH = ?', undef, $PathA); 
			if ($Count == 1) {
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
