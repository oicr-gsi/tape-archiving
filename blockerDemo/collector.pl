#!/usr/bin/perl
#$ -cwd 
#$ -o ./output/logs/myoutput.log
#$ -e ./output/logs/myerror.log
#$ -S /usr/bin/perl

=head1 collector.pl

This scans down the 'qstat' output and compares it with the contents of the 'job list' file (JobRecord.tab) to see if the jobs are all finished.
It is designed to be called by 'SGECaller.pl'

It assumes that all the jobs it will be watching have the name format: 

 $JobName = "IndMD5R\_$StartTime\_$C_Block"

 "qstat -q spbcrypto  | grep IndMD5R"

The Index files have the filenames in the format: Indx_208.tab.tmp


=head2 Ultimately the Files.Index is created 

=cut

use strict;
use Cwd;

#The next lines are needed until we can get SGE / qsub to cwd for me to a specific directory: 
my $WORKINGDIR = "WORKDIR_TAG";
# or an alternative would be:
# my $WORKINGDIR = shift @ARGV;
# $WORKINGDIR = "output"; #To demonstrate on the CLI directly
unless ($WORKINGDIR =~ m/_TAG$/)	#i.e. did somebody substitute the tag in for the new / correct working directory?  (The TAG will either be replaced completely or no part of it at all)
 	{ 	chdir ($WORKINGDIR)or die "Cannot change working directory to: '$WORKINGDIR'\n";	}
 #else 	{ 		warn "D: Not changing working directory\n"; 	}

#Both these times are in seconds:
my $WAITTIME = 5;	#Time to pause between polls

my $LIFESPAN = 10 * 60 * 60 * 24;		#If we go over this time (added to the start time) then we 'self terminate'


#For testing; NB: it is highly likely that any more than a few hundred small files won't finish before  
  # $LIFESPAN    	= 60;	#4 iterations basically 
my $TIMESTAMP = `date`;	#Because Unix's 'date' output is more human readable than Perl's!
chomp ($TIMESTAMP);		#Remove the newline character to make printing easier.
#
my ($JobName, $StartTime);
$|=1;


my $JobTabFile = "JobRecord.tab";
#print "D: Working directory is: '",`pwd`,"\n";
unless (-e $JobTabFile) {	die "Couldn't find the job record tab '$JobTabFile'\n";}

=head2 Read in the table file to get the Start time

...so we can exit gracefully if we have overstayed our welcome.

#JobName=\t$GenericJobName\n";	#The job name; so we can filter by it...if needed
print $JobRecordFile "$C_Block\t$JobID\tS\n"; 		#Print the result to the job ID file

=cut  

open my $JOBTAB_fh, "<", $JobTabFile or die "Couldn't open the job record tab '$JobTabFile' (but could see it)\n";

while (<$JOBTAB_fh>)	#This is probably unnecessary as the jobname is on the first line:
	{
	if (/^#JobName=\tIndMD5R_(.+?)$/)	#Match the start time, then use the side effect of the match to load the Start Time
		{	$StartTime = $1; $JobName = "IndMD5R\_$StartTime"; last;}
	}
close ($JOBTAB_fh);
#print "D: Start Time: '$StartTime' & '$JobName'\n";

=head2 Start main loop

#Start the main 'while loop' - the majority of the program

=cut

do 	#Should (continue) to execute?   We run once then test this condition to decide on 2nd or more:  while (($StartTime + $LIFESPAN) > time) 
	{
		
=head3 Run qstat to capture the job IDs to pick through  

We filter out the worst of the junk by piping through grep & cut
At this point we should have a simple list of Job IDs already / still running.

=cut 
	unless (-e $JobTabFile)
		{		sleep ($WAITTIME); next;		}
#	print "D: Running qstat\n";
	my $QSTATCommand = "/opt/ogs2011.11/bin/linux-x64/qstat 2>&1 | grep IndMD5 | cut -f1 -d\" \""; # removed -q spbcrypto
	my @RelevantJobs = `$QSTATCommand`;
	#Check qstat ran...sometimes the Isilon prevents this (according to IT Helpdesk)
	if (grep (/error:/, @RelevantJobs))		#In which case we wait - and come back.
		{
		sleep ($WAITTIME);
		next;
		}
	 

=head3 Compare the two lists: scan through the data table on the filesystem 

Remember, the lines are written out using code such as this:

 push @Output, join ("\t", $Block, $JobID, "R", $DoneCount,$TotalCount)
 print $JobRecord_FH, join ($C_Block, $JobID, "R", 0, $BlockCounts{$C_Block}), "\n";

Hence the order of the fields:

 Current Block; SGE ID; Number files done in block; number of files in block total 

=cut 

	my @Output;		#This will be the lines in the new file with the updated information.

	#This diagnostic /trakcing information we can print out immediately at the top of the file
	push @Output, "#JobName=\t$JobName\n";
	push @Output, "#LastPoll=\t$TIMESTAMP\n";		#Not used by any program; nice to have for human diagnostic purposes though
	 
	my $AllDoneFlag = 1;	#Start optimistic; we can change based on the evidence we find...
	my $NoErrorsDetected =1; 
	
	
	open my $JOBTAB_fh, "<", $JobTabFile or die "Couldn't open the job record tab '$JobTabFile' (but could see it)\n";
	while (<$JOBTAB_fh>)
		{
		if (/^#/)		{ next;	}	#Skip the header line(s); any we care about will get recreated with new information
		if (/^\S*$/)	{ next;	}	#Skip all blank lines as well
		unless (/^\d+\t/)	{	next;}	#Skip the status line if present; we will recreate it
		chomp ();	#Remove the new lines from the incoming data
		# print "D: Line in table has '",scalar (split /\t/,$_),"' fields in it\n";
		#Maybe, ultimately:
#		unless (scalar (split /\t/,$_) == 4)			{	die "Error in field count:\n";			}
		
		#Get the information already present:
		my ($Block, $JobID, $Status, $DoneCount, $TotalCount)	=	split (/\t/,$_);

		#Set some defaults in case we didn't get anything back from that split:
		$DoneCount ||=0; $TotalCount ||=0; $Status ||="?";
		
		#Actually we don't really, care about the status so much in the file - unless everthing is acutually done
		# print "D: $Block, $JobID, $Status\n";
		


		#Build the filename:
		my $IndexFile = "Indx_$Block.tab.tmp";
		if (-e $IndexFile)	#If the index file does exist then...there is something strange going on - but beyond us to fix it.
			{
			my $DoneCommand= "cut -f1 $IndexFile | grep -v \"\\-\\-\" | wc -l";
#			print "D: Done Command: '$DoneCommand'\n"; 
			$DoneCount = `$DoneCommand`;	#Anything with an MD5 won't have a two "--" next to each other		
			#Strip off the new lines:			
			chomp ($DoneCount); chomp ($TotalCount);
			
=head3 Did the job finish already?  Could we find the index file in fact?

 R = Running (qstat reports the job still)
 F = Finished (qstat doesn't report the job)
 ? = We can't find the temporary file do counts on...maybe this is a temporary FS glitch and it will repears.  

=cut
				if (grep (/^$JobID$/,@RelevantJobs))	#Does this exist in the array
					{		push @Output, join ("\t", $Block, $JobID, "R", $DoneCount,$TotalCount)."\n";		$AllDoneFlag =0;	}	#Job is 'Running'	
				else
					{		push @Output, join ("\t", $Block, $JobID, "F", $DoneCount,$TotalCount)."\n";		}						#Job is 'Finished'
			}
			else { 
				# The 'index file' (.tab file) didn't exist...so we fill in whatever we have/had and we let the default of '0' be written out for the 'Done Count' / status = "?"   
						push @Output, join ("\t", $Block, $JobID, "?", 0	,$TotalCount)."\n";				$AllDoneFlag =0;		#Job might be finished... 
			}
		
		}
	close ($JOBTAB_fh);	
	
=head2 Write out the new version of the $JobTabFile

The Jobname and last used count first.

=cut
	
	#Temporary until we are happy we are building this properly:

#	die "HIT BLOCK\n";
	#
	unlink ($JobTabFile);
 	open my $JOBTABOP_fh, ">", $JobTabFile or die "Couldn't open the job record tab '$JobTabFile' (but could see it)\n";
 	print $JOBTABOP_fh @Output;
# 	print "D: \n",@Output;	#Enable if you want the same to STDOUT
 	close ($JOBTABOP_fh);
	
#JobName=\tIndMD5R_(.+?)

=head2 All Done?  No Failures?

There are 4 states: $AllDoneFlag = 0 or 1; $NoErrorsDetected = 1 or 0
(the values in [] are the states of these two variables)

 RUNNING 				[0,1]		= still running: no errors detected	
 RUNNING_WITH_ERRORS 	[0,0]		= still running: errors detected
 FINISHED_ALL_CORRECT 	[1,1]		= finished; all correct (no errors)
 FINISHED_WITH_ERRORS 	[1,0]		= finished; but with errors

If the 'All done tag' state is detected then the main 'while' loop is terminated.

Typical error detection command is: cut -f1 *.tab.tmp | grep -E  '\w-{1}\w' | wc -l

But we also consider not being able to get the status of a block run "?" as an error:
 
 
 
=cut


=head3 Error detection: We count two cases as an 'error':

 1) The MD5 calculation errored for some reason and was recorded
 2) We can't find the .tmp.tab index file - in which we can't tell if it is finished 
 

=cut	
	my $ErrorCallCMD = "cut -f1 *.tab.tmp | grep -E  '\\w-{1}\\w' | wc -l";
#		print "D: Error Call Cmd #1: 'ErrorCallCMD': '$ErrorCallCMD'\n";
	my ($ErrorCallResult) = `$ErrorCallCMD`;
	my ($CountofErrors) = $ErrorCallResult =~ m/^(\d+?)/;   
	#print "D: Count of MD5'ing Errors: '$CountofErrors'\n";
		
 	unless (defined $CountofErrors && $CountofErrors == "0")
 		{	$NoErrorsDetected	=0;	}
 	
 	#2nd Case: (we only run if errors weren't detected in case #1) Could we get all 
	if ($NoErrorsDetected ==0)
		{	#Still no errors?  Let's search for more:
		if (grep /\?/,@Output)
			{			$NoErrorsDetected =0;			}			
			
			
		} 	
		#Mark the current status in the bottom of the file:
		open $JOBTABOP_fh, ">>", $JobTabFile or die "Couldn't open the job record tab '$JobTabFile'\n";
 		my $EPOCTIME = time;	#Makes calculations easier on completion:
# The best case:  All done & no errors: so we might as well terminate:
 	if ($AllDoneFlag ==1 && $NoErrorsDetected ==1) 			{print $JOBTABOP_fh "FINISHED_ALL_CORRECT :\t$TIMESTAMP\t$EPOCTIME\n";		} 
#We are done, but errors detected:
 	if ($AllDoneFlag ==1 && $NoErrorsDetected ==0) 			{print $JOBTABOP_fh "FINISHED_WITH_ERRORS :\t$TIMESTAMP\t$EPOCTIME\n"; 	}
# The two running states:
 	if ($AllDoneFlag ==0 && $NoErrorsDetected ==1) 			{print $JOBTABOP_fh "RUNNING :\t$TIMESTAMP\t$EPOCTIME\n"; 					}
 	if ($AllDoneFlag ==0 && $NoErrorsDetected ==0) 			{print $JOBTABOP_fh "RUNNING_WITH_ERRORS :\t$TIMESTAMP\t$EPOCTIME\n"; 		}
# 		print "D: Status calls are: '$AllDoneFlag' & '$NoErrorsDetected' (All done & NoErrors)\n";
# 	print join ("\n",@Output);
 	close ($JOBTABOP_fh);	
	if ($AllDoneFlag ==1)
		{
		# Create a record of the time run
		my $dateVal = "`date +%s`";
                my $dateTime = "`$dateVal` > ../tmstmp.txt";
                my $tmstmpFile = `$dateTime`;
		
		#Clean up: create the main index file; delete the intermediates:
		my $CollectIndex_CMD = "cat *.tab.tmp > Files.Index 2>&1";
		my $CollectIndexResult ="";
		$CollectIndexResult = `$CollectIndex_CMD`;
#		print "D: Index collection command was: '$CollectIndex_CMD'\nD: The result was: '$CollectIndexResult\n";
		my $CleanUpCMD = "rm *.tab.tmp; rm Block*.pl; rm collector.pl";	#Yes, really - we delete ourselves!
		print "#: Cleanup CMD: '$CleanUpCMD'\n";
		my $CleanupRes = `$CleanUpCMD`;
		last;
		}
	#Temporary:
#	exit;
	sleep ($WAITTIME);
	
	} while (($StartTime + $LIFESPAN) > time);	#Run loop once, then terminate.
	
print "#Program terminating at:", time,"\n";


