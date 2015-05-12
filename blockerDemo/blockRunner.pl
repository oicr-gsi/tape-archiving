#!/usr/bin/perl
#$ -cwd
#$ -o LOGDIR_TAG
#$ -e LOGDIR_TAG
#$ -N JOBNAME_TAG
#$ -S /usr/bin/perl

use strict;

=head1 blockRunner.pl

When presented with a list of files padded with 32 characters of "-" it attempts to calculate the MD5Sums of these and 
fills in the MD5's in place, in order.

It is designed to be called by 'SGECaller.pl' / via SGE's qsub - hence the need for certain '_TAG's to be substituted.  

The script is relatively light-weight at 100 lines of 'real code'

=head2 Important points - why / how this script works

=head4 The preparation of the file with placeholders

It is assumed that the file is pre-prepared in a very specific format:

Using code like this:

	my ($PathName) = abs_path ($_);	#The only thing on the line is the path (full or otherwise we don't care at this point)	
	#Build the new output line:
	my $Line=	"-"x32 .
				"\t".
				$PathName.
				"\n";
	print $INDEXFILE_FH $Line; 	#Bubble over the new details, nicely formatted

The initial result is this:

 ----------------------------------------\t/foo/bar/1
 ----------------------------------------\t/foo/bar/2
 ----------------------------------------\t/foo/bar/3

The code fills in the MD5 or one of the error codes listed below; the multiple "-" characters are assumed/used to detect which files
are left to process.

=head4 The use of standard error codes:

 Not-Found		
 Not-Readable
 Not-MD5d

(note that all these match the Regex: /\\w-{1}\\w/ used by the collector.pl script to detect errors)

=head4 printLineToFile ($FH, $RawLine, $NewInfo)

The MD5 is written in place as we monkey around with the filepointer; hence all data written to the filepoint is done through 
this function that handles this for you.

=cut
 
#This will be substituted in prior to calling:
my $IndexFile 			= 	"INDEXFILE_TAG";	#Where we get out instructions from as to which file to run.


#For testing:
my $WAITTIME =2;
$|=1;
#Also remember the jobname & logdir tags above.


#Open the index file for bi-directional reading:
open my $INDEXFILE_FH, "+<", $IndexFile or die "Cannot open output file '$IndexFile' (main open command)\n";

#Disable Buffering:
select((select($INDEXFILE_FH), $|=1)[0]);

my $C_Line=0;             #Track the line we are processing
my $N_Files_wcresult= `wc -l $IndexFile`;
#print "D: '$N_Files_wcresult'\n";

my $N_Files_Total = $N_Files_wcresult =~ m/^(.+?) /;
#print "D: of $N_Files_Total files to process in total\n";

while (<$INDEXFILE_FH>)  #run through instructions file
        {
        $C_Line ++;
#Ok, we want this line:
        #Disable buffering from now on (might not be needed if you are using 'sys' style commands such as 'syswrite')
		
		#my $LineLength = length ($_."\n");
		
		my (undef, $Path, $Size)	=	split (/\t/,$_);
		chomp ($Path);	#remove the newline from the path
		
		# Andrew Code Addition
		# print out path and file size to std out (redirected to a file)
		# $Size should represent the current lines related file size
		print "'$Path' is of size '$Size'\n";		 


		#Reset to the start of the line:			
#		print "D: File Pointer set to: ", tell ($INDEXFILE_FH),"\n";
		my $BackupLength= length ($_);
#		print "D: Backup Length = '$BackupLength'\n";
		seek($INDEXFILE_FH, -$BackupLength,  1) or die "Could not reset filepointer!\n";
#		print "D: Path to file: '$Path'\n";
		my $MD5Result = `md5sum $Path`;
#		`touch $IndexFile`;
						#Try to get a lock:
		# Compute the MD5 - may take a time:
		
#		print "D: '$_'\n";
		# Does the file exist?
		unless (-e $Path)
			{
			printLineToFile ($INDEXFILE_FH,$_,"Not-Found");			next;	#Process next line (if present)		
			}
		unless (-r $Path)
			{
			printLineToFile ($INDEXFILE_FH,$_,"Not-Readable");			next;	#Process next line (if present)		
			}
		
		my ($MD5Value)	=	$MD5Result =~ m/^(.{32})/;
#		print "D: MD5 value calculated: '$MD5Value'\n";
		sleep ($WAITTIME); #Pause so we can see what is going on	
		if (defined $MD5Value && $MD5Value eq "")
			{
			printLineToFile ($INDEXFILE_FH,$_,"Not-MD5d");				next;	#Process next line (if present)
			}
			else
			{
		#Get and then test the lock one last time...(in case a 'cleaner' has come through the file and or we have stalled)
		printLineToFile ($INDEXFILE_FH,$_,"$MD5Value");
#		print "D: Writing to file, file # $C_Line done\n";
			}
        }
close $INDEXFILE_FH;     #Be neat, though possibly the next step will clean it up anyway
print "#: All files Done!\n";

sub printLineToFile
{
=head2 printLineToFile ($FH, $RawLine, $NewInfo)

This handles the movement of the filepointer around based on the new info to be written and 
 the length of the line being processed.
 
The routine assumes the filepointer has been reset to the start of the line already.

Any attempt to write more than 32 characters to the file is blocked (it would remove the tab character and overwrite the filepath)

=cut

my ($FH, $RawLine, $NewInfo)	=	@_;

unless (defined $FH && defined $RawLine && defined $NewInfo)	{	return 0;	}
#Tag is too long?
if (length ($NewInfo) >32)	{		return 0;		}

print $FH $NewInfo;
my $ForwardJump = length ($RawLine) - length ($NewInfo);
#print "D: printLineToFile: Forward jump is: $ForwardJump\n";
seek($INDEXFILE_FH, +$ForwardJump,  1) or die "Could not reset filepointer!\n";
	
}
