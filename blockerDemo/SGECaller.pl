#!/usr/bin/perl
=head1 SGECaller.pl <OPT: alternative path to survey>

Demonstrates the calling of another script 'blockRunner.pl' into which it fills in various values.

If no path or file is specified on the command line the "find" is done on the directory "./testDir" which should contain
a few (10 currently) files for demonstration purposes; otherwise the path or file is used.

You can pass either a directory of files you want to compute MD5sums for or a file which contains a list of files you want to compute MD5sums for.

The variable names are deliberately similar to the those in 'indexer.pl' as it assumed that the two code 
bases will be merged ultimately.

The first section also partitions a list of files into 'blocks', creates scripts targetting them and 
then launches the SGE jobs to start the scripts processing.  The actual processing done is calling the 'md5sum' program, 
but this is more a proof-of-concept here.
 
It demonstrates chunking into block and parallel write to various indexes it computes the MD5 Sums directly into 
the index file overwriting the existing placeholders on success.

=head2 Example of use:

 ./SGECaller.pl /.mounts/labs/prod/backups/production/projects/wholeExomeBetaAgilent

(This is about 3500 files, and 66GB so a nice split and two files in each 'block'; it takes around 500s / 8 minutes to complete)

Note that this program requires two other scripts: 'collector.pl' and 'blockRunner.pl' to operate.
Also - to really run - it needs SGE / qsub access.  It handles the lack of this politely by building all the files 
but not attempting to invoke the compute jobs.

=head2 Outcomes of MD5 Compution 

Note that only the first column is overwritten:

The are 3 (three) general states:

 1) The MD5 sum of 32 chars is written out and all the "-" overwritten.
 2) If the file isn't found then the tag: "Not-Found" is dropped into the MD5 location
 3) If the file is found but can't be read (check / adjust the access permissions) then the tag: "Not-Readable" is used
 4) If the file is found, initially tests as readable but the MD5 can't be computed for some reason (even screwier access permissions?) the tag: "Not-MD5d" is written. 
  
Note that the error tags have 1 (one) "-" character in them; untested / uncomputed files have 32 "-" and the 
successful MD5s have none (0) "-".  This is by design so that progress can be monitored by a simple:

 wc -l output/*.tab 					#Number total to process
 watch -n 5 'cut -f1 output/*.tab | grep "\-\-" | wc -l'			#Number of files to go	(~1s)
 watch -n 5 'cut -f1 output/*.tab | grep -v "\-\-" | wc -l'			#Number of files done (or erroring!) (~1s)
 watch -n 5 'cut -f1 *.tab  | grep -e "[0-9a-f]\{32\}"  | wc -l'	#Formally, explictly, the number of files with correct MD5 Sums (V.V. Slow: ~jobs
 10 minutes)
 
Whereas: 
  cat output/*.tab | wc -l
 832458 

Both these are relatively fast operations: <1s on 800 000 entries across 200 files.
 

=head2 Note on Speed / Number of Files Trade-off & Block Size

This version of the program is optimised for a lower number of files, essentially there are two per block:

 1) The script doing the running - a version of 'blockRunner.pl' with all its parameters filled in
 2) The index file with 'a sensible number'  - see discussion below on block size - of files to process on that node.

There also a 'raw_index.fil' (the output of the unix 'find' command) created and 2 log files per job 
(these might be turned off though in general usage).
 
 
BlockSize is choosen such that 'blocks' will cycle across the various nodes ($NODES_TO_USE): ~5 blocks 
per node on average over the life of the task.  This - it is asserted - is nicer for others wanting to use the nodes 
and also helps if they crash.

To give an idea: ~800 000 files across 100 nodes leads to 1666 per node.
The acutal calculation being:

	$BlockSize = int ($N_Files_Total / $NODES_TO_USE /5); 

=head2 Default parameters

 my $MAX_JOB_LIMIT 	= 20000;		#This is actually 75 000, but we force 
 my $NODES_TO_USE	= 100;			#SPB Crypto has ~110 at time of writing 	
 my $DEFBLOCK_SIZE = 4;		#Number of jobs per grid job.

 my $BlockSize = $DEFBLOCK_SIZE;	#Likely code will change this value

=cut

=head2 Block Size

The '$BlockSize' is the maximum number of jobs allocated to one list to process, the assumption being that the 
list will be processed on one node / 'aka slot' of the SGE.

For testing setting this very low (say 4) will force a split across many lists even of a small number of files are used.

Ultimately though it is indended to be set to something approximating to the number of files to process divided 
by the number of slots available on the SGE Queue being used.  

 Still, though to give some resiliency it would be better to set this such that each slot gets - on average - 5 or so lists
processed through it.  Why 5?  It is a nice number based on no direct evidence but limits the uncorrected failures of SGE
to something sensible.

This is a balance between splitting (more scripts, files and inodes used) and clumping 
(less scripts, but each takes longer to run).

If the number of files to process is high (Specifically: N. of Files > $NODES_TO_USE) then the algorithm
chooses a block size of int ($N. of files / $NODES_TO_USE / 5) otherwise it just uses $NODES_TO_USE  

The jobnames are of the format:

 my $JobName = "IndMD5R_$C_Block\_".time;

i.e.:

 IndMD5R_1_1426024605

=head2 Examples of JobRecord.tab

=head3 Initially, after launch:

The collector script is currently queued at this point so isn't even running yet:

 #JobName=       IndMD5R_1426708314
 #LastPoll=      Wed Mar 18 15:52:08 EDT 2015
 1       7669408 F       0       2
 2       7669409 F       0       2
 ....
 612     7670021 R       0       2
 613     7670022 R       0       2
 614     7670023 R       0       2
 RUNNING

In the above table the columns are tab separated:
 1) Block number 
 2) SGE ID
 3) Status: R/F/?>
 4) Number of file attempted (maybe successful, may have errors due to file access/presence) in block
 5) Total number of files in block

And the Status line is also tab separated:

 1) The Status, see below
 2) The Time (date, from unix)
 3) The Time in EPOC Seconds

From 'collector.pl':

 There are 4 states: $AllDoneFlag = 0 or 1; $NoErrorsDetected = 1 or 0
 (the values in [] are the states of these two variables)

 RUNNING 				[0,1]		= still running: no errors detected	
 RUNNING_WITH_ERRORS 	[0,0]		= still running: errors detected
 FINISHED_ALL_CORRECT 	[1,1]		= finished; all correct (no errors)
 FINISHED_WITH_ERRORS 	[1,0]		= finished; but with errors

If the 'All done tag' state is detected then the main 'while' loop is terminated.

Typical error detection command is: cut -f1 *.tab.tmp | grep -E  '\w-{1}\w' | wc -l

But we also consider not being able to get the status of a block run "?" as an error:

=head3 When the 'collector.pl' script is surveying the file:
 R=Running; F=Finished; ?=Don't know/can't survey

 #JobName=       IndMD5R_1426708314
 #LastPoll=      Wed Mar 18 15:52:08 EDT 2015
 1       7669408 F       2       2
 2       7669409 F       2       2
 ....
 612     7670021 R       0       2
 613     7670022 R       0       2
 614     7670023 R       0       2
 RUNNING :       Wed Mar 18 15:52:08 EDT 2015    1426708355

=head3 Ultimately, when finished:

 #JobName=       IndMD5R_1426708314
 #LastPoll=      Wed Mar 18 15:52:08 EDT 2015
 1       7669408 F       2       2
 2       7669409 F       2       2
 612     7670021 F       2       2
 613     7670022 F       2       2
 614     7670023 F       2       2
 FINISHED_ALL_CORRECT :  Wed Mar 18 15:52:08 EDT 2015    1426708807



=head2 Typical qstat output for a nearly finished job:

 job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
 -----------------------------------------------------------------------------------------------------------------
 7662898 0.50000 QRLOGIN    mmoorhouse   r     03/18/2015 11:23:00 default@cn5-45.hpc.oicr.on.ca      1
 7662899 0.50000 QRLOGIN    mmoorhouse   r     03/18/2015 11:23:06 default@cn5-61.hpc.oicr.on.ca      1
 7669407 0.50000 collector. mmoorhouse   r     03/18/2015 15:52:08 spbcrypto@cn3-111.hpc.oicr.on.     1
 7669769 0.50000 IndMD5R_14 mmoorhouse   r     03/18/2015 15:54:01 spbcrypto@cn3-110.hpc.oicr.on.     1
 7669770 0.50000 IndMD5R_14 mmoorhouse   r     03/18/2015 15:54:01 spbcrypto@cn3-106.hpc.oicr.on.     1
 7669783 0.50000 IndMD5R_14 mmoorhouse   r     03/18/2015 15:54:13 spbcrypto@cn3-109.hpc.oicr.on.     1
 7669796 0.50000 IndMD5R_14 mmoorhouse   r     03/18/2015 15:54:14 spbcrypto@cn3-113.hpc.oicr.on.     1
 
 (i.e. 4 jobs/blocks left to complete)
 

=head2 Note on job lanch order and the operation of collector.pl

Assuming there is an SGE launch option on this machine 
(this is tested for by calling qstat; if not files are written but nothing is launched) the 'collector' script (collector.pl) is 
launched first, then the real processing jobs
The collector script is really one big do-while loop that terminates when it detects all the jobs have finished
or it has run for too long (see discussion below).

=head3 Termination of 'collector.pl'

The collector.pl script has a main loop that - in pseudo code - that will self terminate when it detects all the outputs are ready 
or an appropriate amount of time has elapsed.

 do {

 ...update JobRecord.tab... (or wait until it exists)

 ...check if all files have been processed...
 } while (($StartTime + $LIFESPAN) > time)
  
where:

 my $WAITTIME = 5;	#Time to pause between polls
 my $LIFESPAN = 10 * 60 * 60 * 24;		#If we go over this time (added to the start time) then we 'self terminate'

=cut


use strict;

use File::Basename;							#Manipulate paths 1
use File::Path qw(make_path remove_tree);	#Manipulate paths 2
use File::Spec;								#Manipulate paths 3
use Cwd 'abs_path';						#Recurse paths back to their 'source'
use File::Temp qw/ tempfile tempdir /;
use DBI;
use Time::Local;

#The constants / programmable servicable parts:

my $OutputDir = `pwd`;
chomp($OutputDir);
$OutputDir.="/output";
# "$HOME/Data/tapeArchiving/blockRunner/output";

my $MAX_JOB_LIMIT 	= 20000;		#This is actually 75 000, but we force 
my $NODES_TO_USE	= 100;			#SPB Crypto has ~110 at time of writing 	
my $DEFBLOCK_SIZE = 4;		#Number of jobs per grid job.

my $BlockSize = $DEFBLOCK_SIZE;	#Likely code will change this value

my $InputPath = shift @ARGV;	#A way to direct the input program elsewhere: 

unless (defined $InputPath) {	$InputPath  = "./testDir";	}	

unless (-e $InputPath)	{	die "Cannot find input location: '$InputPath'\n";	}

# Set up database information
my $dbname = "seqware_meta_db_1_1_0_150429";
my $hostname = "hsqwstage-www2.hpc";
my $dsn = "dbi:Pg:dbname=$dbname;host=$hostname";
my $user = "hsqwstage2_rw";
my $password = "";

# Connect to database
my $dbh = DBI->connect($dsn, $user, $password, { AutoCommit => 1 }) or die "Can't connect to the database: $DBI::errstr\n";

=head2 Get the runner script off disk where it is easier to edit:

=cut
print "#: Reading 'Runner' file\n";
my $RunnerFile = "blockRunner.pl";
unless (-e $RunnerFile)		{	print "Cannot find the base file I'll modify and invoke instances of as SGE array jobs '$RunnerFile'\n";	}
#Slurp (sort of...) this into a variable:
my $RunnerScript_base;
open my $RUN_fh, '<', $RunnerFile or die "Can't open file '$RunnerFile'\n";
while (<$RUN_fh>)	{	$RunnerScript_base=$RunnerScript_base.$_;}
print "#: Read: ",length $RunnerScript_base," characters from '$RunnerFile'\n";
close ($RUN_fh);

=head2 Same for the monitor / collector script: 

=cut 

print "#: Reading 'Collector/Monitor' file\n";
my $MonitorFile = "collector.pl";
unless (-e $MonitorFile )		{	print "Cannot find the base file I'll modify and launch the monitor / collector script from: '$MonitorFile'\n";	}
#Slurp (sort of...) this into a variable:

my $CollectorScript_base;
open my $COLLECTOR_fh, '<', $MonitorFile or die "Can't open file '$MonitorFile'\n";
while (<$COLLECTOR_fh>)	{	$CollectorScript_base=$CollectorScript_base.$_;}
print "#: Read: ",length $CollectorScript_base," characters from '$MonitorFile'\n";
close ($COLLECTOR_fh);


=head2 (re)create the output & log directory

=cut

#Abs path everything possible:
$InputPath = File::Spec ->rel2abs ($InputPath);
 
if (-e $OutputDir)	{	remove_tree ($OutputDir);	}

#Make the output directory:
print "# Building Output Directory: '$OutputDir'\n";
make_path ($OutputDir) or die "$@";

#Make the log directory:
my $LogDir = "$OutputDir/logs";

if (-e  $LogDir)	{	remove_tree ($LogDir);	}
make_path ($LogDir);

unless (-e  $LogDir)	{	die "Cannot create log directory: '$LogDir'\n";	}

print "D: log directory: '$LogDir'\n";

=head2 Pull a list of files - and detect the failures:

Now get a list of files to use and write these to a list: (deliberately not a .tab so we can use bash to iterate across *.tab)

- and write the errors into a separate output file: yes, we should use the IPC3 module - but this is too advanced 
for our cluster...also we want a record of the errors to keep in a predictable location.

Currently the program goes on regardless - and simply notes the errors in 
'$SearchErrorFile' / '$OutputDir/searchErrors.txt'

=cut 

my $SearchErrorFile = File::Spec ->rel2abs ("$OutputDir/searchErrors.txt");

#Open the file and close it quickly again if this suceeds:
open my $SEARCH_fh, ">", $SearchErrorFile or die "Cannot open the file '$SearchErrorFile' where would store the errors\n";
close $SEARCH_fh;
my $RawIndexFile = "$OutputDir/rawindex.fil";
print "#: Raw index file is: '$RawIndexFile'\n";
print "#: Any errors from find will be written to: '$SearchErrorFile'\n";

# Check if given argument is a directory or a file, and act accordingly.
if (-d $InputPath){
	`find -L $InputPath -type f -size +1c -print 2> $SearchErrorFile > $RawIndexFile`;
} elsif (-f $InputPath) {
	`cat $InputPath > $RawIndexFile`;
}

#Check that really happened ;-)
unless (-e $RawIndexFile )		{die "Cannot find '$RawIndexFile' - assuming find failed\n";}
unless (-e $SearchErrorFile)	{die "Cannot find '$SearchErrorFile' - very strange because I created it before the 'find'\n";}

my ($SizeofErrorFile) = -s $SearchErrorFile;  
$SizeofErrorFile ||=0;	#Set a default of zero - we hope it is this anyway!

unless ($SizeofErrorFile !=0)
	{print "#: Errors were detected during the search; these are recorded in: '$SearchErrorFile'\n";}
else
	{print "#: Clean traverse during the search; nothing in: '$SearchErrorFile'\n";}


=head2 Deduce the block size

Essentially:
 
 $N. of files / $NODES_TO_USE / 5

=cut

# Remove files from rawIndex.file that have not been modified since 
my $RawIndexFileTemp = "$OutputDir/rawindex.fil.temp";

open my $RAW_INDEX_FILE_FH, "<", $RawIndexFile or die "Cannot open raw index file '$RawIndexFile'\n";
open my $RAW_INDEX_FILE_FH_TEMP, ">", $RawIndexFileTemp or die "Cannot write raw index file temp '$RawIndexFileTemp'\n";

my $RunTimeDate = `date +"%F %T"`;

while (<$RAW_INDEX_FILE_FH>) {
	chomp();
	my ($FilePath) = $_;
	my $LastModifiedTime = `stat -c %Z $FilePath`;
	
	my $Count = $dbh->selectrow_array('SELECT count(*) FROM md5_size_last_run WHERE FILE_PATH = ?', undef, $FilePath);
	
	if ($Count > 0) { # If file exists in DB
		my $sql = 'SELECT last_run FROM md5_size_last_run WHERE FILE_PATH = ?';
		my $sth = $dbh->prepare($sql);
		$sth->execute($FilePath);
		while (my @row = $sth->fetchrow_array) {
			my $LastRunEpoch = `date -d "$row[0]" '+%s'`;
			if ($LastRunEpoch < $LastModifiedTime) {
				print $RAW_INDEX_FILE_FH_TEMP "$_\n";
				$dbh->do('UPDATE md5_size_last_run SET last_run = ? WHERE file_path = ?', undef, $RunTimeDate, $FilePath);
			}
		}
	} else { # If file does not exist in DB
		print $RAW_INDEX_FILE_FH_TEMP "$_\n";
		$dbh->do('INSERT INTO md5_size_last_run (file_path, last_run) VALUES (?,?)', undef, $FilePath, $RunTimeDate);
	}
}

close ($RAW_INDEX_FILE_FH_TEMP);
close ($RAW_INDEX_FILE_FH);

`cat $RawIndexFileTemp > $RawIndexFile`;
`rm $RawIndexFileTemp`;

# Disconnect from database
$dbh->disconnect;

#Just ask bash & wc:
my $N_Files_wcresult= `wc -l $RawIndexFile`;
if ($N_Files_wcresult == 0){ # Will just exit the script if no files are to be worked on (due to empty dir or no newly modified files)
	print "Script will halt due to one of the following:\n";
	print "1) The directory provided has no files in it\n";
	print "2) All of the given files have not been modified since last run of this script.  No need to run again.\n";
	exit;
}
my ($N_Files_Total) = $N_Files_wcresult =~ m/^(.+?) /;
print "#: There are '$N_Files_Total' files to process\n";
if ($N_Files_Total > $NODES_TO_USE)
	{	#Do block size calculations:
	$BlockSize = int ($N_Files_Total / $NODES_TO_USE /5); 
	if ($BlockSize < 1)
		{	$BlockSize = 1;}
	print "#: Selcting new block size as: '$BlockSize', not default of: $DEFBLOCK_SIZE\n";	
	}

print "#: Using block size of: '$BlockSize'\n";
print "#: Hence ~",int ($N_Files_Total/$BlockSize), " files in each block (spread across $NODES_TO_USE slots)\n";

#die "HIT BLOCK\n";

#The base name of the fully converted indexes (substitute the XXX placeholder for a index number)
my $IndexFileBase = "$OutputDir/Indx_XXX.tab.tmp";

=head2 (Re)open the file and add in the MD5 placeholders:

Mimic this:
  
	my $Line=	"-"x32 .
				"\t".
				$PathName.
				"\n";

(Also convert to abs paths on the way past)

=cut 

#Open the raw index for opening and 'bubbling into blocks':
open my $RAW_INDEXFILE_FH, "<", $RawIndexFile or die "Cannot open raw index file '$RawIndexFile'\n";

#Control variables:
my $C_IndexFile = $IndexFileBase;
my @AllIndexFiles;
my %BlockCounts;

#A simple counter:
my $Counter=0;

#Prime the first block:
my $Tally = 0;
my $C_Block =1;

#Priming step:
$C_IndexFile  =~ s/XXX/$C_Block/;
push @AllIndexFiles, $C_IndexFile;

#This includes opening the file:
open my $INDEXFILE_FH, ">", $C_IndexFile or die "Cannot open main index file for output '$C_IndexFile'\n";

while (<$RAW_INDEXFILE_FH>)
	{
	$Counter++;		# Increment once per file - irrespective of Blocks (and all such complexity)
	$Tally ++;		# Increment once per block
	#If you need the current status summarised, enable this next line:
	chomp ();	#Strip off the newlines
	#print "D: $Counter\t$C_Block\t$Tally\t$_\n";	
	
	 
	my ($PathName) = $_; #abs_path ($_);	#The only thing on the line is the path (full or otherwise we don't care at this point)	
	#Build the new output line:
	my $Line=	"-"x32 .
				"\t".
				$PathName.
                                "\t".
                                (-s $PathName).
				"\n";
	print $INDEXFILE_FH $Line; 	#Bubble over the new details, nicely formatted

	if ($Tally >= $BlockSize)	#Start new block?  Close and reopen output file
		{
		#Note the number of subjobs in this block: We will need this when writing out JobRecord.tab 
		$BlockCounts{$C_Block} = $Tally;
		$Tally=0;	#Reset Counter
		
		#print "D: Block switch: '$C_Block'; new index file: '$C_IndexFile'\n"; 
		#Close File:
		close ($INDEXFILE_FH);	#Release the file; reopening it under a slightly name...
		
		#Reopen file with new name:
		$C_Block ++;  #Increment first:
#		print "D: Block change to $C_Block\n";
		
		$C_IndexFile = $IndexFileBase;
		$C_IndexFile  =~ s/XXX/$C_Block/;
		
		open $INDEXFILE_FH, ">", $C_IndexFile or die "Cannot open main index file for output '$C_IndexFile'\n";
		#Increment / reset counters, store the number of files in the block:
		}
	$BlockCounts{$C_Block} = $Tally;	#Note this here
	}


#Close up our file handles: 
close ($INDEXFILE_FH);
close ($RAW_INDEXFILE_FH);		#Officially we can delete this at this point; for diag we keep it around...

my $NBlocks = $C_Block;
print "#: I needed to create $NBlocks blocks for the $Counter files I found as Block size set to '$BlockSize'\n";

$|=1; #Deactivate buffering on STDOUT:
 
=head2 Write out the script files:

=cut

my $TrailTag = "RF"; #Not used actually:

my $StartTime =time;


my $IndexerSGEScript_basename = "$OutputDir/Block_XXX.pl";
my $CollectorSGEScipt_basename = "$OutputDir/collector.pl";

foreach my $C_Block (1..$NBlocks)
	{
	# Take a copies so we can substitute into them:
	my $ThisBlockScript = $RunnerScript_base;	
	my $ThisBlockScriptFileName = $IndexerSGEScript_basename;
	$ThisBlockScriptFileName =~ s/XXX/$C_Block/;
	
	#Note this, needed for launch:
	push @AllIndexFiles, $C_IndexFile;
	
	#Also the name of the index file:
	my $C_IndexFile = $IndexFileBase;
	$C_IndexFile  =~ s/XXX/$C_Block/; 

	#print "D: N files in block: $NFilesInBlock\n";
	my $NSubJobs= $BlockCounts{$C_Block};
	print "# For block '$C_Block'; Jobs= '$NSubJobs'; script is: '$ThisBlockScriptFileName'\n";
	#print "D: for Block: '$C_Block' = '$NSubJobs'\n";

=head3 

 Need to fill in values on the way past...though not that many actually...

 #$ -o LOGDIR_TAG
 #$ -e LOGDIR_TAG
 #$ -N JOBNAME_TAG
 #$ -S /usr/bin/perl
 
 my $IndexFile 			= 	"INDEXFILE_TAG";	#Where we get out instructions from as to which file to run.
 my $BlockNumber			=	"BLOCK_TAG";

=cut 
	#This is block specific so we need to define it at each iteration:
	
	my $JobName = "IndMD5R\_$StartTime\_$C_Block";
	
	$ThisBlockScript =~ s/LOGDIR_TAG/$LogDir/g;		#The index file: actually all we need
	$ThisBlockScript =~ s/JOBNAME_TAG/$JobName/g;
	$ThisBlockScript =~ s/INDEXFILE_TAG/$C_IndexFile/g;		#The index file: actually all we need 

	#Write the script out:
	open my $ScriptFile_FH, ">", $ThisBlockScriptFileName or die "Cannot open Script file '$ThisBlockScriptFileName'\n";
	print $ScriptFile_FH  $ThisBlockScript;
	close $ScriptFile_FH;
	}




#Check we have SGE (qstat, but hence assumed launch) available:
 
my $SGE_Present =0;

if (`qstat -l spbcrypto 2>&1` =~ m/error:/) 
	{	print "FAILED: qstat (no access to SGE queues?)\n";	}
	else
	{	print "# PASSED: qstat (I have access to SGE queue)\n"; $SGE_Present =1;}

#We 'push' the commands to this array; then launch them - after the collector / monitor script: 
my @SGELaunchCMD_s;

foreach my $C_Block (1..$NBlocks)
	{
	my $ThisScript = $RunnerScript_base;						#Yes, we are re-deducing these a
	my $ThisBlockScriptFileName = $IndexerSGEScript_basename;
	$ThisBlockScriptFileName =~ s/XXX/$C_Block/;
	print "Launching job: '$C_Block' = $ThisBlockScriptFileName\n";	
	
	my $SGECommand = "qsub -q spbcyrpto $ThisBlockScriptFileName";
	#print "D: $SGECommand\n";
	push @SGELaunchCMD_s, $SGECommand;
	}
=head2 Launch Monitor / Collector Job 

=cut
 
my $CollectorScriptFileName = "$OutputDir/collector.pl";
	
my $CollectorScript = $CollectorScript_base;	# Take a copy so we subsitute into it.
#Substitute in the 'working directory' that we ask the script to cd to because we can't find how to get SGE to do this:
$CollectorScript =~ s/WORKDIR_TAG/$OutputDir/;	#Actually, we don't need to tweak much...	

#Write the script out:
open my $CollectorOP_FH, ">", $CollectorScriptFileName or die "Cannot open Script file '$CollectorScriptFileName '\n";
print $CollectorOP_FH  $CollectorScript;
close $CollectorOP_FH;
my $CollectorLaunchResult = "";
print "#: Wrote out collector script: '$CollectorScriptFileName'\n";
if ($SGE_Present ==1)
	{	$CollectorLaunchResult = `qsub -q spbcrypto $CollectorScriptFileName`; } # Launch the QSub Command
else
	{	$CollectorLaunchResult = "No SGE Detected, hence won't / can't launch the collector script\n";	}
chomp ($CollectorLaunchResult); 
print "Collector script launch result was: '$CollectorLaunchResult'\n";

=head2 Launch Jobs (+ note this in 'Job Record.tab')

And record the Job IDs to a file ($JobRecordFile) so we can survey them more easily...

Initially we are parsing lines such as this for the job ID:

 "Your job 7402663 ("dummy.bash") has been submitted"

=head3 Lines created in JobRecord.tab:

=cut


=head2 Launch all the main compute jobs stored in @SGELaunchCMD_s:

=cut
#Open the JobRecord.tab file:
	my $JobRecordFile = "$OutputDir/JobRecord.tab";
	#open my $JobRecord_FH, ">", "$JobRecordFile" or die "Cannot open '$JobRecordFile'\n";
	my $GenericJobName = "IndMD5R\_$StartTime"; #This is the base of the job names we will be monitoring; though the collector uses formal IDs
	#print $JobRecord_FH "#JobName=\t$GenericJobName\n";	#The job name; so we can filter by it...if needed
	my @JobTabArray;
	push @JobTabArray, "#JobName=\t$GenericJobName\n";
	if ($SGE_Present ==1)
		{
		#Open the 'Job Record file' to store the job IDs we launch

	#Iterate through all the SGE Jobs:
		my $C_Block=1;
		foreach my $C_Job (@SGELaunchCMD_s)
			{
			my $SGEResult = `$C_Job`;
			$SGEResult =~ s/[\n\s]+$//g;	$SGEResult =~ s/[\r\n]/\n#:  /g;
			print "SGE Result was: $SGEResult\n";
	
	#Now store this to the list of job IDs:
			my ($JobID) = $SGEResult =~ m/Your job (\d+?) \(/;
			$JobID ||= 0;	#Set a default of zero if we couldn't parse it for whatever reason:	
	
			#print $JobRecord_FH join ("\t", $C_Block, $JobID, "R", 0, $BlockCounts{$C_Block}), "\n";
			push @JobTabArray, join ("\t", $C_Block, $JobID, "R", 0, $BlockCounts{$C_Block}), "\n";
			$C_Block ++;	#Increment the block counter
			}
		push @JobTabArray, "RUNNING\n";
		#print $JobRecord_FH "RUNNING\n";	#put in this initial marker
		
		}
#	print "D: Terminating after the first launch until demostration complete\n";	last;
	 
	else
		{	print "No SGE Detected, hence can't launch.\n";	
#			print $JobRecord_FH "FINISHED_WITH_ERRORS\n";	#put in this initial marker; as we aren't launching jobs we can't change this.
		}
	
	open my $JobRecord_FH, ">", "$JobRecordFile" or die "Cannot open '$JobRecordFile'\n";
	print $JobRecord_FH @JobTabArray;
	close $JobRecord_FH;

