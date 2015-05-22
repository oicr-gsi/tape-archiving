#!/usr/bin/perl

# A script for updating the seqware database with the MD5sum and file size of the given nonseqware files.

# This should be run per project, it should not be run for multiple.  You must pass this script the name of the project
# and the input file
# The input file will be of the same format as the Files.Index file produced from SGECaller.pl (MD5<tab>AbsoluteFilePath<tab>FileSize)

# Please note that file paths listed in the File.Index input file must be absolute paths, since they are unique

# ./nonSeqWareMD5Updater.pl <Project_Name> <File.Index_Path>
# For example:
# Project is lungcancer
# ./nonSeqWareMD5Updater.pl lungcancer ./lungcancer/output/File.Index

# Last Modified: May 22, 2015 by Andrew Duncan

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

# Get location of File.Index
my $InputFile = $ARGV[1];
chomp( $InputFile );
$InputFile = abs_path( $InputFile );

# Set up database information 
# ***Make sure this is correct***
my $dbname = "seqware_meta_db_1_1_0_150429";
my $hostname = "hsqwstage-www2.hpc";
my $dsn = "dbi:Pg:dbname=$dbname;host=$hostname";
my $user = "hsqwstage2_rw";
my $password = "lxf4VkHQ";

# Connect to database
my $dbh = DBI->connect($dsn, $user, $password, { AutoCommit => 1 }) or die "Can't connect to the database: $DBI::errstr\n";

# Get project name
my $Project = $ARGV[0];

# Open File.Input
open my $MD5_FILE_FH, '<', $InputFile or die "Can't open file '$InputFile'\n";

# Iterate through MD5/FileSize file (File.Index)
print "Inserting/updating SeqWare database...\n";
while ( <$MD5_FILE_FH> ) {
	chomp();
	my ($MD5, $Path, $Size) = split (/\t/,$_);

	# Check if current file path is in the database or not
	my $Count = 0;
	$Count = $dbh->selectrow_array('SELECT count(*) FROM reporting.file WHERE FILE_PATH = ?', undef, $Path);	

	# Last time MD5 script is run is last time file was seen
	my $Last_Seen = "";
	$Last_Seen = (stat($MD5_FILE_FH)->mtime);
	$Last_Seen = `date -d @"$Last_Seen" "+%F %T"`;
	
	# If file still exists, then this is now the last seen time
	if ( -e $Path ) {
		$Last_Seen = `date "+%F %T"`;
	}
	
	# Check if file is already in table or not
	if ( $Count == 0 ) { # File does not already exist in the table, run insert command
		$dbh->do('INSERT INTO reporting.file (file_path, file_size, md5sum, project, last_seen) VALUES (?,?,?,?,?)',
       		 undef, $Path, $Size, $MD5, $Project, $Last_Seen);
	
	} else { # File exists in the table, run update command
		$dbh->do('UPDATE reporting.file SET FILE_SIZE = ?, MD5SUM = ?, LAST_SEEN = ? WHERE FILE_PATH = ?', undef, $Size, $MD5, $Last_Seen, $Path);
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
