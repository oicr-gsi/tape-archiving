#!/usr/bin/perl

# A script for updating the seqware database with the MD5sum and file size of the given nonseqware files.

# This should be run per project, it should not be run for multiple.  You must pass this script the name of the project
# and the project directory

# Please note that file paths listed in the File.Index input file must be absolute paths, since they are unique

# ./nonSeqWareMD5Updater.pl <Project_Name> <Project_Path>
# For example:
# Project is lungcancer
# ./nonSeqWareMD5Updater.pl lungcancer ./lungcancer

# Last Modified: June 3, 2015 by Andrew Duncan

use strict;
use warnings;

use Cwd 'abs_path';
use DBI;
use File::stat;
use Time::localtime;

my $Length = @ARGV; # Number of inputs

# Redirect error stream to stderr.log
open(STDERR, ">>stderr.log") or die "Failed to open error log file";
`date > stderr.log`;

# Check that the correct number of arguments have been provided
if ( $Length != 2 ) {
	die "You have entered $Length argument(s). 2 arguments are required to run.\n" ;
}

# Set up output dir
my $MD5OutputDir = `pwd`;
chomp ($MD5OutputDir);
$MD5OutputDir .= "/output";

# Get location of Project directory
my $InputFile = $ARGV[1];
chomp( $InputFile );
$InputFile = abs_path( $InputFile );

# Connect to database
my $path = `pwd`;
chomp($path);
$ENV{PGSYSCONFDIR} = $path;

my $dbh = DBI->connect("dbi:Pg:service=test", undef, undef, { AutoCommit => 1}) or die "Can't connect to the database: $DBI::errstr\n";

# Get project name
my $Project = $ARGV[0];

# Update files that no longer exist
my $sql = 'SELECT file_path FROM reporting.file WHERE project = ?';
my $sth = $dbh->prepare($sql);

$sth->execute($Project);
while (my @row = $sth->fetchrow_array) {
	if (! -e $row[0]) {
		$dbh->do('UPDATE reporting.file SET file_size = 0 WHERE file_path = ?', undef, $row[0]);
	}
}

# Call SGECaller
print "Calculating MD5sums and file sizes for files in the given directory...\n";
`./SGECaller.pl "$InputFile" nonseqware`;

my $MD5File = $MD5OutputDir . "/Files.Index";
my $RawIndex = $MD5OutputDir . "/rawindex.fil";

if ( -z "$RawIndex") {
        print "No files have been added or modified since last run!\n";
        exit;
}


# Ensure that Files.Input exists before using it
sleep 2 while not -e "$MD5File";

# Open File.Input
open my $MD5_FILE_FH, '<', $MD5File or die "Can't open file '$MD5File'\n";

# Iterate through MD5/FileSize file (File.Index)
print "Updating SeqWare database...\n";
while ( <$MD5_FILE_FH> ) {
	chomp();
	my ($MD5, $Path, $Size) = split (/\t/,$_);

	# Check if current file path is in the database or not
	my $Count = 0;
	$Count = $dbh->selectrow_array('SELECT count(*) FROM reporting.file WHERE FILE_PATH = ?', undef, $Path);	

	my $Last_Seen = `date "+%F %T"`;
	
	# Check if an MD5sum was calculated
	if ( index( $MD5, "--" ) == -1) {		
		# Check if file is already in table or not
		if ( $Count == 0 ) { # File does not already exist in the table, run insert command
			$dbh->do('INSERT INTO reporting.file (file_path, file_size, md5sum, project, last_seen) VALUES (?,?,?,?,?)',
       			 undef, $Path, $Size, $MD5, $Project, $Last_Seen);
	
		} else { # File exists in the table, run update command
			$dbh->do('UPDATE reporting.file SET FILE_SIZE = ?, MD5SUM = ?, LAST_SEEN = ? WHERE FILE_PATH = ?', undef, $Size, $MD5, $Last_Seen, $Path);
		}
	} else {
		`echo $_ >> stderr.log`;
	}
}

# Close file
close ( $MD5_FILE_FH );

# Disconnect from database
$dbh->disconnect;

print "Updating complete.\n";
print "Script completed in ";
print time - $^T;
print " seconds\n";
