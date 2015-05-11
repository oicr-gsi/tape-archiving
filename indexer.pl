#!/usr/bin/perl
=head1 indexer.pl <Directory>

A script that replaces a similarly named bash one (reproduced below) - but with a little 'hardening' 
that when called issues MD5 jobs to the sbpcrypto queue to MD5 every file in the directory / path supplied.
Each file is MD5'd invidually if they are real files (see below about symlinks) and the result written to a created directory (./index) 
in the output.  
The idea is these MD5 files can be concatenated together to form a proper index; the symlinks are split off into two other files 
depending on whether they are either valid or invalid symlinks (Symlinks.active or Symlinks.broken).
The directories are also noted in a file, should this be useful to others - say another program wants to replicate the directory 
structure and GPG the files as is expected.

The program is designed to operate at the 10TB+ & 1 000 000+ file range so special workarounds for the limits on the number of 
jobs (for example) of 75 000 is implemented.

The spb_crypto cluster queue is used by the shell scripts created.  At the time of writing this has 114 'slots' - i.e. this 
number of jobs can run simultaneously and has a limit of 75 000 jobs that the queue can handle. 

=head3 SGE Jobs

There are two pairs of MD5 jobs launched each pair being for 'real' files and the active symlinks:

 1) An MD5 SGE Array job that calculates the MD5 sums. 
 2) A 'collector' job that waits for the first job and concatenates all the MD5 results together and deleted the individual output files.
 
(To state: 4 jobs total, assuming there are real files and active symlinks found; the 'active symlinks' job runs if such symlinks are found.)

=head2 Usage

=head3 Simple usage:
 
 ./indexer.pl <Path to Survery> <Output Path> 

=head4 Module loading

To run this on the (aged) systems of the cluster try loading these modules to get access to the (Perl) modules:

 module load perl/5.20.1
 module load spb-perl-pipe/dev

=head3 Under test  

A typical command line when testing would be:

 ./indexer.pl --clobber --noSGEOK -d=1 ~/tickets/tapeArchiveSPB_2983/testDir/links/

(overright output OK (--clobber); don't fail on no SGE (--noSGEOK) and give extra STDOUT printout for every file (-d=1)

=head4 Parameters

Formally there are these parameters:

	"clobber|c" => \$ClobberOutput_F,		= Overwright output Ok; default: not, exit on output dir exists
	"diag|d|D=i" => \$ExFrequency,			= Diagnostic printout frequency; default: never, no diag output
	"noSGEOK|nogrid" => \$SkipSGECheck		= Skip the SGE Check & job launch - allows running on a non cluster machine; default: not, i.e. test & launch jobs

=head2 Split of 'objects' across files:

If no output directory is supplied then the output is created in a directory of the input directory supplied 
suffixed with the tag $OUTDIRTAG ("_op") - assuming that such a directory doesn't already exist
(in which the script terminates unless --clobber is supplied as a parameter).  

 i.e. : /.mounts/labs/prod/backups/production/projects/BCPR
 
would become:

 ./BCPR_op

and the sub-directory structure would also be created:
 
 ./BCPR_op/MD5_working
 ./BCPR_op/index
 ./BCPR_op/scripts

=head3 In ./index there are many lists:

These are a useful separation:

 Symlinks.active
 Symlinks.broken
 Files
 Directories
 NullFiles

and also a .json file to store some of the parameters:

 MD5_result.json
 
=head4 Symlinks - Active & Broken

Symlinks are split from the main list into two other files:

 Symlinks.active
 Symlinks.broken

If active the symlink is followed back to an absolute path (the relevative path is also printed) to 
create the fullest link possible.
 
=head4 Null / Empty files
 
Also the file that are zero byte files are routed to NullFiles as trying to encrypt them or MD5 them is just silly 
(the MD5 of null is: d41d8cd98f00b204e9800998ecf8427e)

=head4 'Real' files (& Directories)
For help with building output directory trees and MD5 / GPG'ing files we note the directories & real files in:

=head4 JSON Output

Currently this is a very simple document designed to point to the other files above rather than output all 
the information contained in them.

=head2 Typical output for a range of test files:

For the testDir akak testDir_op inputs:

=head3 The MD5 Result: Files.md5

 testDir_op/index/Files.md5.RF 
 5c650eda6453051a1a4621454bfc4c1e  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/test_dir.struct.tar         23500800
 2ef63ba2438d71a0ee499d86d1684772  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/B/7.file    2097152
 1d24423151aa24605556257fa701b762  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C/11.file   2560000
 265a251ace2e58f0b9cbf6cc50bf9df6  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C/10.file   2097152
 89fc417d136b29b945d6ead5e94eafc0  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C/12.file   3170304

(There are likely to be two of these: one for real files, one for active links)

=head3 The script running:

 # Target Directory Supplied: 'testDir'
 # (Setting output directory to a default of): 'testDir_op'
 # Target (read from) directory set to: '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir'
 # Output (write to ) directory set to: '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op'
 # PASSED: qstat (I have access to SGE queue)
 # PASSED: md5sum (I have access to the MD5 program on the CLI)
 # Pre-Flight checks complete: GO Decision!
 # Hence: Broken links File      = '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Symlinks.broken'
 # Hence: Active links File      = '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Symlinks.active'
 # JSON Report File                      = '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/MD5_result.json'
 # Building Output Directory: '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op'
 # Building home for MD5 scripts: '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts'
 # Building home for MD5 results: '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working'
 # Building home for Indices: '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index'
 # Marking out territory with a timestamp file:
 #       Wed Feb  4 14:44:14 EST 2015 -> /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/TimeStamp
 # Reading list from: /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir (aka. /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir)
 # Printing out 1 in every 0 objects (files, symlinks)
 # Found 23 file-objects in 0 s
 # Now starting trawl for broken symlinks:
 # Type counts:
 # Original survey:      23
 # ---------
 # Directories:          7
 # Normal files:         10
 # Empty files:          0
 # Working links:        3
 # Broken links:         3
 # ---------
 #   Deviance:           0
 # 1) SGE Script is: (RF) '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts/MD5_RealFiles.pl'
 # Will be run with the job name: 'MD5_Tape_RF_1423080229'
 # Qsub / Bash script is: 1480 bytes in size on disk
 # Launch General MD5sum QSub request:
 # SGE QSub launch result was:'Your job-array 6498984.1-10:1 ("MD5_Tape_RF_1423080229") has been submitted'
 #
 #
 # 2) Prepare MD5sum Collector QSub job for real files: (RF)
 #: MD5 Collector Command: 
 # : 'qsub -q spbcrypto -hold_jid MD5_Tape_RF_1423080229 -N MD5_Collector_RF_1423080229 -b y -S /bin/bash -o /dev/null -e /dev/null "cat /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.RF >> /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Files.md5.RF; rm /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.RF
 "'
 # Launch MD5sum Collector QSub job: (RF)
 # Collector command launch returned: 
 #: 'Your job 6498985 ("MD5_Collector_RF_1423080229") has been submitted'
 # Active Symlinks Detected: (3 of) - so running MD5s on these
 # 3)  SGE Script for active links is: (ALS) '/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts/MD5_ActiveSymlinks.pl'
 # Will be run with the job name: 'MD5_Tape_ASL_1423080229'
 # Qsub / Bash script is: 1439 bytes in size on disk
 # General MD5sum Qsub request for active symlinked files (ALS):
 # SGE QSub launch result was:'Your job-array 6498986.1-3:1 ("MD5_Tape_ASL_1423080229") has been submitted'
 # 4) MD5sum Collector QSub job for acitve symlinked files (ALS):
 #: MD5 Collector Command: 
 # : 'qsub -q spbcrypto -hold_jid MD5_Tape_ASL_1423080229 -N MD5_AL_Collector_1423080229 -S /bin/bash -b y -o /dev/null -e /dev/null "cat /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.ASL >> /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Files.md5.ASL; rm /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.ASL
 "'
 # Collector command launch returned: 
 #: 'Your job 6498987 ("MD5_AL_Collector_1423080229") has been submitted'



???? TO ENTER NEARER COMPLETION ????
 
=head3 Directories

 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/broken_links
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/links
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/A
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/B
 
=head3 Files
(first 10 shown):

 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/test_dir.struct.tar
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C/11.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C/10.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/C/12.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/A/4.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/A/6.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/A/5.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/B/8.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/B/9.file

=head3 Nulls (real files, but 0 bytes)

None: but imagine something like this:

 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/nulls/A/Null_6.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/Null_5.file

=head3 Symlinks.active

 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/links/f.2 ->  ../../target_links.2.file ->    /u/mmoorhouse/tickets/tapeArchiveSPB_2983/target_links.2.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/links/f.3 ->  ../../target_links.3.file ->    /u/mmoorhouse/tickets/tapeArchiveSPB_2983/target_links.3.file
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/links/f.1 ->  ../../target_links.1.file ->    /u/mmoorhouse/tickets/tapeArchiveSPB_2983/target_links.1.file

=head3 Symlinks.broken

 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/broken_links/B3 -> /foo/bar
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/broken_links/B2 -> /foo/bar
 /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/broken_links/B1 -> /foo/bar

=head3 JSON File

This is under heavy, active development.  Also it looks awful to human eyes.

 cat  testDir_op/index/MD5_result.json 
{"ALS Job":{"Collector Command":"qsub -q spbcrypto -hold_jid MD5_Tape_ASL_1423079054 -N MD5_AL_Collector_1423079054 -S /bin/bash -b y -o /dev/null -e /dev/null \"cat /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.ASL >> /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Files.md5.ASL; rm /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.ASL\n\"","Job Name":"MD5_Tape_ASL_1423079054"},"Paths":{"MD5 Script Files":{"RF Script":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts/MD5_RealFiles.pl","ALS Script":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts/MD5_ActiveSymlinks.pl"},"MD5 Working":{"Dir":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working"},"Index":{"Dir":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index"},"Scripts":{"Dir":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts"},"Output":{"As Passed":"","ABS":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op"},"Input":{"As Passed":"testDir","ABS":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir"},"Index Files":{"Active Links":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Symlinks.active","Null":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/NullFiles","Broken Links":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Symlinks.broken","Files":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Files"},"JSON File":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/MD5_result.json"},"RF Job":{"Collector Command":"qsub -q spbcrypto -hold_jid MD5_Tape_RF_1423079054 -N MD5_Collector_RF_1423079054 -b y -S /bin/bash -o /dev/null -e /dev/null \"cat /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.RF >> /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Files.md5.RF; rm /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/*.md5.RF\n\"","Job Name":"MD5_Tape_RF_1423079054"},"Counts":{"Objects":"23","Active Links":"3","Null":"0","Files":"10","Directories":"7"},"Files":{"TimeStamp":{"Path":"/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/TimeStamp"}}}

=head2 The orginal bash script.

 #!/bin/bash

 #To delete jobs, use this:
 # qstat | grep MD5 | cut -f1 -d" " | xargs qdel
 echo Designed to compute MD5s give a list of files with parrallelisation across the spbcrypto queue
 rm split.list.*
 rm *.md5s
 N=20
 DIR=$1
 echo "Will compute MD5s in this directory:" $1
 #DIR="/.mounts/labs/prod/backups/production/projects/BCPR/"
 mkdir $DIR\/md5s
 #DIR="/u/mmoorhouse/tickets/tapeTests_2723"
 LISTFILE="BCPR.paths.orginals.lst"
 echo "Geting file list: "
 find ${DIR} -type f > ${LISTFILE}
 T_LINES=$(wc -l <${LISTFILE})
 echo "Found this many files total: " ${T_LINES}
 ((LINES_PER_PART = (${T_LINES} + ${N} - 1) / ${N}))
 echo "Will divide by (" ${N} ") and hence each list will have: " $LINES_PER_PART " entries in it"
 split -d --lines ${LINES_PER_PART} ${LISTFILE} split.list.
 LISTOFLISTS=`ls -1 split.list.*`
 echo "Starting Launch"
 for i in ${LISTOFLISTS}
 do
    echo "Processing this list:" $i
 qsub -cwd -b y -N MD5_$i -q production -e $DIR/md5s/$i.error.log xargs "md5sum < $i > $DIR/md5s/$i.md5s"
 #echo  qsub -cwd -b y -N MD5_$i -q production -e /u/mmoorhouse/tickets/tapeArchiveSPB_2983/md5s/$i.error.log 'xargs md5sum < $i > $i.md5s'
 done
 
=head3 Example of the Array Job in SGE doing the same as above

The actual code executed (see subsequent section below) by SGE is written with 'TAGS' that get substituted for real, useful values.
The code immediately below was a transition step:
  
 #!/bin/bash
 # : A very simple program
 #$ -t 1-8

 #The Shell script proper:
 WD=/u/mmoorhouse/tickets/tapeArchiveSPB_2983/arrayGridTests
 LISTOFFILES=$WD/testfiles.lst
 cd $WD
 FILE=$(awk "NR==$SGE_TASK_ID" $LISTOFFILES)
 
 md5sum $FILE > $WD/output/$SGE_TASK_ID.md5

=head3 Example of broken symlink:

  /.mounts/labs/prod/backups/production/projects/wouters_miRNA/raw/solid0139_20090522_Run1_Wouters_AC/finchResults/W5_smRNA/results.01/cycleplots/thumb/solid0139_20090522_Run1_Wouters_AC_F3_P1_02_V1_scaled_satay.png 
  -> 
  /.mounts/sata/bas003/archive/a139/results/solid0139_20090522_Run1_Wouters_AC/W5_smRNA/results.01/cycleplots/thumb/solid0139_20090522_Run1_Wouters_AC_F3_P1_02_V1_scaled_satay.png
 
 
=head2 Modules and 'Constant' declarations
  
=cut 

use strict;
use 5.10.0;	# 


unless(eval { "require JSON;  JSON->import();  1;"})                    {       die "Cannot load the POSIX Module: try 'use module JSON'\n";}
unless(eval { require File::Temp;  File::Temp->import();  1;})  {       die "Cannot load the File::Temp Module\n";}

use JSON;	#Helps with reporting & communicating various quantities
#We use a lot of File:: modules here ;-)
use File::Basename;							#Manipulate paths 1
use File::Path qw(make_path remove_tree);	#Manipulate paths 2
use File::Spec;								#Manipulate paths 3
use Cwd 'abs_path';						#Recurse paths back to their 'source'
use Cwd;								#Get the Current Working Directory
use POSIX qw(ceil floor);						#For the ceiling functionality 
use Getopt::Long;			#To process the switches / options:

=head2 Set some defaults & pseudo-constants

=cut 

my $ClobberOutput_F =0;
my $OUTDIRTAG = "_op";
my $SkipSGECheck = 0;
my $ExFrequency = 0;	#the frequency of diagnostic printing; if enabled (set to 0 to disable)
my @KEYS = ("BC3E454B", "E16641B3", "1C1742CB");
my %JSON_Struct;		#Ultimately we dump this as a JSON output, upon success of the script 

#Both of these are assumptions:
my $MAX_JOB_LIMT 	= 	75000;	#Maximum number of jobs available to us 
my $MAXNODES	=	20;	#Maximum number of jobs we will ask each node to do as part of a job

=head3 Create the skeleton SGE Script in this next section

Perl is used to allow the processing of multiple sets of data - and keep the results separate.
Hence the raw code to be executed by SGE is written with 'TAGS' in it that get substituted for real, useful values 
prior to it getting written out to disk and passed to 'QSub'.

Check the real code for the current version, but expect it to be similar to this below in general terms.
This version uses 'SGE Array Jobs' (against Brent's advice!).

 For a general description of array jobs see: https://www.google.ca/webhp?ie=UTF-8#q=sge%20array%20jobs

In this a series of "TAGS" 
The hashes in the first few lines mean something to SGE apparently...

=head4 The template of what is actually used:

 #!/usr/bin/perl
 # : Compute the MD5 sums for files passed (if there are any)
 #$ -t 1-NFILES_TAG
 #$ -cwd
 #$ -o /dev/null
 #$ -e /dev/null
 #$ -S /usr/bin/perl

 #$ -N MD5_Tape_TRAIL_TAG_TIME_TAG

 my $WANTEDLINE=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub
 open INPUTFILE , "FILELIST_TAG";

 my $Count=0;
 while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        unless ($Count == $WANTEDLINE)  {       next;   }       #Skip until the line we want:
 #Below here only processed for wanted lines: 
        my ($InputFile) =  split (/[\t\n]/,$_);
		chomp ($InputFile);
		my $OutputFile = "MD5OUT_TAG/$WANTEDLINE".".md5.TRAIL_TAG";
	
 #Issue the command (magically _TAG will have been subsitituted in by the main Perl script by the time this runs
		`md5sum $InputFile > $OutputFile`;
		exit;           #Exit here is an optimisation as we don't care about the other lines in the file: another instance will process them
        }

=head4 As would be created on disk (i.e. tags subsituted for real values)

 #!/usr/bin/perl
 Compute the MD5 sums for Active Symlinks (if there are any)
 #$ -t 1-10
 #$ -cwd
 #$ -o /dev/null
 #$ -e /dev/null
 #$ -S /usr/bin/perl

 #$ -N MD5_Tape_RF_1422647355

 my $WANTEDLINE=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub
 open INPUTFILE , "/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/index/Files";

 my $Count=0;
 while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        unless ($Count == $WANTEDLINE)  {       next;   }       #Skip until the line we want:
 #Below here only processed for wanted lines: 
        my ($file) =  split (/[\t\n]/,$_);
    chomp ($file);
    my $Output = "/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/MD5_working/$WANTEDLINE".".md5.RF";

 #Issue the command (magically _TAG will have been subsitituted in by the main Perl script by the time this runs
            `md5sum $file > $Output`;
            exit;           #Exit here is an optimisation as we don't care about the other lines in the file: another instance will process them
        }
=cut

=head3 Real Code (MD5 Script)


=cut

my $SGEScriptBase = <<'MD5_SCRIPT';
#!/usr/bin/perl
# : Compute the MD5 sums for either files or active symlinks (if there are any)
#$ -t 1-NBLOCKS_TAG
#$ -cwd
#$ -o /dev/null
#$ -e /dev/null
#$ -S /usr/bin/perl

#$ -N JOBNAME_TAG
#These get substituted in prior to use:
my $InstructionsFile	= 	"FILELIST_TAG";
my $ItemType 			=	"TRAIL_TAG";
my $MD5ResultsDir		=	"MD5OUT_TAG";
my $BlockSize			=	"BLOCKSIZE_TAG";

#This is supplied by SGE / QSub:
my $Block=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub

#Hence, calculate the actual line numbers between which we will process:
my $WantedLine_Start = ($Block-1) * $BlockSize;
my $WantedLine_End = ($Block) * $BlockSize;

open INPUTFILE , "$InstructionsFile" or die "Cannot open file '$InstructionsFile'\n";
my $Count=0;
my @LinesToProcess;
while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        if ($Count <= $WantedLine_Start) 	{       next;   }       #Skip until the lines we want:
        if ($Count > $WantedLine_End )			{		last;	}		#We've got all the lines we want; terminate search
#Below here only processed for wanted lines: 
		chomp ();
        my ($InputFile) =  split (/[\t\n]/,$_);		#There is only the filename in this case
        #print "D: Processing Line: $Count, '$InputFile'\n";	#Ok, so imagine this is an MD5 call!
		push @LinesToProcess, [$InputFile,$Count];	#These are in order...but store the 'Count' to save having to calculate it later.
        }
close INPUTFILE;	#Release - just saves confusion if other instances are needing this

#So, iterate through the array, not the file:

foreach my $C_File (@LinesToProcess)
	{
	my ($FileName, $Index) = ($$C_File[0], $$C_File[1]);	#I.e. first (0th) is the filename, second is it's index	
#Issue MD5 request (& do reality checks):
                $FileName=~s/ /\\ /g; #Escape spaces in file names
                $FileName=~s/([{|(})%@])/\\$1/g; # Escape special characters
		my $MD5Result= `md5sum $FileName`;
                $FileName=~s/\\//g;   #Un-escape
		unless ($MD5Result =~ m/^[a-f0-9]{32} /)			{next;}		#I.e. create a 'hole' in the output file
		chomp($MD5Result);
#Get the size (& do reality checks):
		my $Size = -s $FileName;
		unless (defined $Size && $Size >=0)					{next;}		#If we've just MD5'd it we should be able to get a 
		my $OutputFile = "$MD5ResultsDir/$Block".".md5.$ItemType";
#Write these out:
		open OUTPUT, ">>$OutputFile" or die "Cannot open output file: '$OutputFile'\n";
		print OUTPUT "$MD5Result \t$Size\n";
		close OUTPUT;
        }
MD5_SCRIPT


=head3 Real Code (Collector Script)

=cut 

my $MD5CollectorScriptBase = <<'COLLECTOR_SCRIPT';
#!/usr/bin/perl
use strict;
my $TrailTag 		= "TRAIL_TAG";
my $WDir		= "WD_TAG";
my $NFilesExpected 	= "NFILES_TAG";
my $OutputIndexFile	= "INDEXFILE_TAG";

#We derive this:
my $CompleteFile = "$OutputIndexFile.completed"; 

my $CollectorCommand = 
"cat $WDir\/MD5_working/*.md5.$TrailTag >> $OutputIndexFile";
#Actually, we don't care about the errors: unless the Index file is created and it has the right number of files in it
`$CollectorCommand`;	  

unless (-e $OutputIndexFile)
	{	die "Cannot file the composite index file I just created: '$OutputIndexFile'\n";	}


my $CheckerCommand =
"wc -l $OutputIndexFile 2>/dev/null";
my ($Checker_Result)= `$CheckerCommand`;

my ($NFilesInIndex)= $Checker_Result=~ m/^(\d+?) /;

my $CleanerCommand = 
"rm $WDir\/MD5_working/*.md5.$TrailTag 2>/dev/null";
#Nothing really we can do if this fails - maybe print an error back to STDERR in the future?
#`$CleanerCommand`;

unless (defined $NFilesInIndex && $NFilesInIndex ne "" && $NFilesInIndex == $NFilesExpected)
	{	die "File miss-match count; this is bad!\n";	}
#If we got to here then:
	`date > $CompleteFile`;
	
COLLECTOR_SCRIPT




=head3 Get the options passed (if any), otherwise set their defaults
(see the real code for current defaults)

Minimum required diskspace (in TB)
 my $MINDISKSPACE = 0.1 * 1024 * 1024 * 1024 ;
	(i.e. 100GB - we are computing MD5s, not duplicating the data)

=cut

GetOptions (
	"clobber|c" => \$ClobberOutput_F,
	"diag|d|D=i" => \$ExFrequency,
	"noSGEOK|nogrid" => \$SkipSGECheck
 )
	or usage ("Error in command line arguments\n");

#And the rest of parameters:
my ($TargetDIR, $OutputDir) = @ARGV;

#Is the target defined and is it either a directory or a link?
unless (defined $TargetDIR && (-d $TargetDIR or -l $TargetDIR))	{	usage ("Could not find input path '$TargetDIR'");	}
print "# Target Directory Supplied: '$TargetDIR'\n";

$JSON_Struct{"Paths"}{"Input"}{"As Passed"} = $TargetDIR;	#JSON Load

$TargetDIR =~ s/\/$//;

#Were we given an output directory?  If not build one from the input path and the CWD
unless (defined $OutputDir && $OutputDir ne "")	
	{
#Split the path and the Volumes apart; use Core modules to do it 
#	(doing it on the absolute path thanks to the CWD module helps)

#	my ($Volume, $Directories, $Name) =
#                       File::Spec->splitpath(abs_path($TargetDIR));
    #We don't used the Volume or $Directories; leave these for the perl complier to optimise them away
    #Path clean up (should be necessary, but won't do harm)
#	$Name =~ s/\/$//; $Name =~ s/^\///;
	#
	$JSON_Struct{"Paths"}{"Output"}{"As Passed"} = "";	#JSON Load
	$OutputDir=$TargetDIR.$OUTDIRTAG;
	print "# (Setting output directory to a default of): '$OutputDir'\n";	
	}

#Convert to absolute paths:
$TargetDIR 	= abs_path($TargetDIR);
$OutputDir	= abs_path($OutputDir);
print "# Target (read from) directory set to: '$TargetDIR'\n";
print "# Output (write to ) directory set to: '$OutputDir'\n";
$JSON_Struct{"Paths"}{"Output"}{"ABS"} = $OutputDir;	#JSON Load	
$JSON_Struct{"Paths"}{"Input"}{"ABS"} = $TargetDIR;	#JSON Load


=head2 Determine whether to delete output directory (or not or die)

As a safety check we require that we find a 'timestamp' file in the directory; just in case something goes wrong...

=cut

my $TIMESTAMPFILE= "$OutputDir/TimeStamp"; 

if ($ClobberOutput_F == 0 && -e $OutputDir)
	{	usage ("BLOCKED: Output directory exists; will not delete it (or use --clobber to override)");	}
	
if ($ClobberOutput_F ==1 && -e $OutputDir)
	{
	if (-e $TIMESTAMPFILE)	#Ok, we recognise this directory as one of ours!
		{
		print "# OK - deleting output directory (file '$TIMESTAMPFILE' exists - so it seems a directory created by a previous indexer.pl instance\n";
		remove_tree ($OutputDir) or die "Could not delete output directory '$OutputDir' when I tried:$@\n";
		}
		else
		{
#		print "Will not delete output directory '$OutputDir' --clobber supplied, but I didn't find the file '$TIMESTAMPFILE'\n";
		usage ("BLOCKED: Will not delete output directory '$OutputDir' as I couldn't find the '$TIMESTAMPFILE' (timestamp file)");
		}
	}	

$JSON_Struct{"Files"}{"TimeStamp"}{"Path"} = $TIMESTAMPFILE;	#JSON Load

=head2 Pre-Flight Checks

 These are fatal if we fail them...but things are promising if we get even this far...
 
=cut

#Test SGE / Que:

if ($SkipSGECheck ==0)
	{
	if (`qstat -l sbpcrypto 2>&1` =~ m/error:/)
		{	print "FAILED: qstat (no access to SGE queues?)\n"; usage ("BLOCKED: Need access to SGE Grid - run me on cluster/head node? (or use --noSGEOK to override)");	}
		else
		{	print "# PASSED: qstat (I have access to SGE queue)\n"; }
	}else
	{	print "Skipping SGE / Cluster check\n";	}
#Test MD5:	
if (`md5sum --version` =~ m/^md5sum \(GNU coreutils\)/)	
	{	print "# PASSED: md5sum (I have access to the MD5 program on the CLI)\n"; }
	else
	{	usage ("BLOCKED weird: couldn't get positive report from md5sum"); }

print "# Pre-Flight checks complete: GO Decision!\n";

=head2 Derive other paths & file names

Directories:

 scripts
 MD5 Results
 index

Files:

 Symlinks.active
 Symlinks.broken
 Files
 Directories
 NullFiles

=cut 

my $MD5ScriptsDir 	= "$OutputDir/scripts";
my $MD5ResultsDir 	= "$OutputDir/MD5_working";
my $IndexDir		= "$OutputDir/index";

$JSON_Struct{"Paths"}{"Scripts"}{"Dir"} 		= $MD5ScriptsDir;	#JSON Load
$JSON_Struct{"Paths"}{"MD5 Working"}{"Dir"} 	= $MD5ResultsDir;	#JSON Load
$JSON_Struct{"Paths"}{"Index"}{"Dir"}		 	= $IndexDir;	#JSON Load

$MD5ScriptsDir 	=~s /\/\//\//g;
$MD5ResultsDir 	=~s  /\/\//\//g;
$IndexDir		=~s  /\/\//\//g;


my $ActiveLinksFile 	= "$IndexDir/Symlinks.active";
my $BrokenLinksFile 	= "$IndexDir/Symlinks.broken";
my $RealFiles 			= "$IndexDir/Files";
my $DirectoriesFile 	= "$IndexDir/Directories";
my $NullFilesFile		= "$IndexDir/NullFiles";
my $JSONFile			= "$IndexDir/MD5_result.json";

$JSON_Struct{"Paths"}{"Index Files"}{"Active Links"} 		= $ActiveLinksFile;	#JSON Load
$JSON_Struct{"Paths"}{"Index Files"}{"Broken Links"} 		= $BrokenLinksFile;	#JSON Load
$JSON_Struct{"Paths"}{"Index Files"}{"Files"} 				= $RealFiles;	#JSON Load
$JSON_Struct{"Paths"}{"Index Files"}{"Null"} 				= $NullFilesFile;	#JSON Load
$JSON_Struct{"Paths"}{"JSON File"} 								= $JSONFile;	#JSON Load


print "# Hence: Broken links File 	= '$BrokenLinksFile'\n";
print "# Hence: Active links File 	= '$ActiveLinksFile'\n";
print "# JSON Report File			= '$JSONFile'\n";

=head2 Build Output Directories

The scripts, md5 results and index directories inside the base output directory

=cut 

print "# Building Output Directory: '$OutputDir'\n";
make_path ($OutputDir) or die "$@";
print "# Building home for MD5 scripts: '$MD5ScriptsDir'\n";
make_path ($MD5ScriptsDir) or die "$@";
print "# Building home for MD5 results: '$MD5ResultsDir'\n";
make_path ($MD5ResultsDir) or die "$@";
print "# Building home for Indices: '$IndexDir'\n";
make_path ($IndexDir) or die "$@";


#Get the timestamp:
my $TIMESTART = `date`;chomp ($TIMESTART);
#Write it out to disk:
print "# Marking out territory with a timestamp file:\n# \t$TIMESTART -> $TIMESTAMPFILE\n";
open TIMESTAMP , ">$TIMESTAMPFILE" or die "Cannot open timestamp file for writing: '$TIMESTAMPFILE'\n"; 
print TIMESTAMP $TIMESTART,"\n"; close TIMESTAMP;

=head2 Open the files to put lists in:

 Symlinks.active
 Symlinks.broken
 Files
 Directories
 NullFiles
 
=cut 

open ACTIVELNK, ">$ActiveLinksFile" or die "Cannot open '$ActiveLinksFile' (active links file)\n;";
open BROKENLNK, ">$BrokenLinksFile" or die "Cannot open '$BrokenLinksFile' (broken links file)\n;";
open FILES, ">$RealFiles" or die "Cannot open '$RealFiles' (normal files)\n;";
open DIRS, ">$DirectoriesFile" or die "Cannot open '$DirectoriesFile' (directories)\n;";
open NULLS, ">$NullFilesFile" or die "Cannot open '$NullFilesFile' (null / empty files)\n;";

=head2 Start the survey / list the files:


=head3 Diagnostic output

The output routine is deliberately split into two sections:
The first is the a diagnostic output section that allows experimental tests to be done without
affecting the main loop that writes out files etc. 

Currently the list of files (technically 'objects') gets loaded into memory via the command:

 `find $TargetDIRAbsPath`;

Perhaps this isn't the best solution and re-direction to a temporary file would be better 
followed by a line-by-line read and routining to the other 4 open files.

Still this works for now.  

=cut


my $count = 1;
my @Directories;	#For the deduction of the shortest common path: should we bother

my $TargetDIRAbsPath= File::Spec->rel2abs($TargetDIR);
print "# Reading list from: $TargetDIR (aka. $TargetDIRAbsPath)\n";
print "# Printing out 1 in every $ExFrequency objects (files, symlinks)\n";

my $StartTime= time;
#Actually run the 'find':

my @Objects=	`find $TargetDIRAbsPath`;

#Deduce various statistics 
my $EndTime = time - $StartTime;
my $NObjects = scalar (@Objects);

print "# Found $NObjects file-objects in $EndTime s\n";
print "# Now starting trawl for broken symlinks:\n";

#Arrays to store the lists:
my $EmptyFiles=0; my $BrokenLinks=0 ; my $ActiveLinks=0; my $NormalFiles=0; my $Directories=0;
my $Objects=0;

foreach my $C_Object (@Objects)
	{
	chomp ($C_Object);	#strip the newlines introducted by 'find'
	$Objects++;
	$count ++;	#Increment counter
	my $ObjSize= -s $C_Object;	#Get size of object
# - The diagnostic printout, if requested:
	if ($ExFrequency != 0 && $count != 0 && $count %$ExFrequency == 0)
		{	
		print "D: '$C_Object'= $ObjSize";
		sleep (1);
	if (-d $C_Object)
		{	print "\t a directory\n;"		}
	
	if (-l $C_Object && not( stat ($C_Object)))
		{	print "\t Link - a broken originall pointed at -> ",readlink($C_Object),"\n";	}
		
	if (-l $C_Object && stat ($C_Object))
		{	print "\t Link - and functional points to -> ",readlink($C_Object)," -> ",abs_path($C_Object),"\n";	}
		}
# - End diagnostic output

=head4 Test the 'Object' (is it a directory, file, link etc.)

We survey the directories - and as an optimisation we test the most frequent cases first:

=cut

#Start the survey:
	if (-d $C_Object)
		{	
		$Directories++;			
		print DIRS $C_Object,"\n";
		push @Directories, $C_Object;
		next;	}
		
	#This is normal, real file: note it and skip:
	if  (not (-l $C_Object) &&  $ObjSize >0)
		{	
                if ($C_Object=~/\W$/) { # We'll skip strange files marking them as empty
                  $EmptyFiles++;
                  print NULLS $C_Object,"\n";
                  print "#: Found strangely named file [$C_Object] that will not be backed up\n";
                  next;
                }
		$NormalFiles++;
               	print FILES $C_Object,"\n";		
		next;	}
		
	#An empty file: 
	if (not (-l $C_Object) && $ObjSize == 0)
		{	
		$EmptyFiles++; 		
		print NULLS $C_Object,"\n";
		next;	}
#For these tests we assume the 'object' is a symlink:
	my $StatResult=stat ($C_Object); 
	
	#A symlink to a broken file:
	if (-l $C_Object && not($StatResult))
		{
		$BrokenLinks ++;		
		print BROKENLNK $C_Object, " -> ", readlink($C_Object),"\n";	
		next;
		}
	
	#A symlink to a file we can still find:
	if (-l $C_Object && $StatResult)
		{	
		$ActiveLinks ++;	
		print ACTIVELNK abs_path($C_Object),"\t -> \t ", $C_Object," -> \t",readlink($C_Object),"\n";	
		next;	}

	die "Strange file / object: '$C_Object'\n";
	}
	
=head3 Printout Section - for humans:

=cut

print "# Type counts:\n";
print "# Original survey:   \t$Objects\n";
print "# ---------\n";
print "# Directories:       \t$Directories\n";
print "# Normal files:      \t$NormalFiles\n";
print "# Empty files:       \t$EmptyFiles\n";
print "# Working links:     \t$ActiveLinks\n";
print "# Broken links:      \t$BrokenLinks\n";
print "# ---------\n";
print "#   Deviance:         \t", $Objects 
		- $NormalFiles
		- $Directories 
		- $EmptyFiles 
		- $ActiveLinks 
		- $BrokenLinks,"\n"; 
		
$JSON_Struct{"Counts"}{"Objects"} 				= $Objects;	#JSON Load
$JSON_Struct{"Counts"}{"Files"} 				= $NormalFiles;	#JSON Load
$JSON_Struct{"Counts"}{"Active Links"} 			= $ActiveLinks;	#JSON Load
$JSON_Struct{"Counts"}{"Directories"} 			= $Directories;		#JSON Load
$JSON_Struct{"Counts"}{"Null"} 					= $EmptyFiles;		#JSON Load

=head3 Cleanup of open list files

=cut

#close all the open files (we will reopen FILES to read from)
close ACTIVELNK; close BROKENLNK; close NULLS; close DIRS; close FILES;



=head2 Next Section: Build MD5 / SGE Scripts:

=cut 

#From here on, absolute paths only (might be already: either because ABS path supplied or we have already ABS'd it):

$RealFiles 			= File::Spec->rel2abs($RealFiles);
$OutputDir			= File::Spec->rel2abs($OutputDir);
$MD5ResultsDir		= File::Spec->rel2abs($MD5ResultsDir);


=head3 Do 'block size' calculations

This is a work-around for the low (???!) number of SGE jobs we are allowed.

See the code in 'splitControl.pl' and 'splitJobDemo.pl' for a full demonstration.  
In summary though: above 75 000 files to MD5 we ask each job to process multiple files (c.f. one per job as would be normal).

 while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        if ($Count < $WantedLine_Start) 	{       next;   }       #Skip until the lines we want:
        if ($Count >= $WantedLine_End)			{		last;	}		#We've got all the lines we want; terminate search
 #Below here only processed for wanted lines: 
		push @LinesToProcess, $_;	
        }
        
In the code the +0.5 is a hack to ensure that the int() calculations always round up.  

=cut

my $TotalJobs = $NormalFiles + $ActiveLinks;
print "#: Files to process: '$NormalFiles+$ActiveLinks ( = $TotalJobs )'\n";
#Reality check: are we able to process what we have been asked?
if ($NormalFiles >= $MAX_JOB_LIMT * $MAXNODES)
	{	die	"Even with $MAXNODES jobs per node, this is still too many files ($NormalFiles)!  (Max job number: $MAX_JOB_LIMT)\n";	}

if ($ActiveLinks >= $MAX_JOB_LIMT * $MAXNODES)
	{	die	"Even with $MAXNODES jobs per node, this is still too many active links ($ActiveLinks)! Max job number: $MAX_JOB_LIMT)\n";	}


#A mini-algorithm to pick the best block size: use all the fancy 'multi-line' stuff only if we have to:
my $NBlocks_RF=1;		#For the real files

if ($NormalFiles > $MAX_JOB_LIMT)
	{	$NBlocks_RF = calculateBlockSize ($NormalFiles, $MAX_JOB_LIMT*$NormalFiles/$TotalJobs );	} #Also this call uses $MAX_JOB_LIMT & $MAXNODES for reality checks
	

my $NBlocks_ALS =1;		#For the active symlinks
if ($ActiveLinks > $MAX_JOB_LIMT)
	{	$NBlocks_ALS = calculateBlockSize ($ActiveLinks, $MAX_JOB_LIMT*$ActiveLinks/$TotalJobs);	} #Also this call uses $MAX_JOB_LIMT & $MAXNODES for reality checks


#print "D: ceiling (0.1)",ceil(0.1),"\n"; print "D: ceiling (0.5)",ceil(0.5),"\n"; print "D: ceiling (0.9)",ceil(0.9),"\n";

#Allocate the jobs in proportion to the number of nodes we have available:

print "#: Actual Block size needed Real Files: '$NBlocks_RF'\n";
print "#: Actual Block size needed Active Symlinks: '$NBlocks_ALS'\n";
#print "#: Hence I need $TotalJobs slots of $MAX_JOB_LIMT maximum I have been allowed\n";

#For each list:
#my $NBlocks_RF 	= ceil ($NormalFiles/$NBlocks_RF);
#my $NBlocks_ALS = ceil ($ActiveLinks/$BlockSize);

print "#: Or split out: for Real Files:   '$NBlocks_RF'  (number of blocks)\n";
print "#: Or split out: for Active Links: '$NBlocks_ALS' (number of blocks)\n";
print "#: I.e. Block sizes will be:\n";


my $BlockSize_RF 	= ceil ($NormalFiles/$NBlocks_RF);
my $BlockSize_ALS 	= ceil ($ActiveLinks/$NBlocks_ALS);


print "#: for Real Files:   '$BlockSize_RF'\n";
print "#: for Active Links: '$BlockSize_ALS'\n";

=head3 Create the basic MD5 running script (#1) (trail tag: RF)

This gets created and run even if no files are found (c.f. the active links)

 $TrailTag = "RF"; #Set the trail tag for 'real files'

=cut 

#Substitute the TAGS in the basic script for our use today for 'RF' (Real files)


	
my $SGEScript = $SGEScriptBase;					#Take a copy of the 'Base'


#Substitute in the various tags:
$SGEScript =~s /FILELIST_TAG/$RealFiles/;
$SGEScript =~s /WD_TAG/$OutputDir/;
$SGEScript =~s /MD5OUT_TAG/$MD5ResultsDir/;
$SGEScript =~s /BLOCKSIZE_TAG/$BlockSize_RF/;
$SGEScript =~s /NBLOCKS_TAG/$NBlocks_RF/;


#Now the parts that need deriving here:
my $TrailTag ="RF"; #Set the trail tag for 'real files'
$SGEScript =~s /TRAIL_TAG/$TrailTag/;	#RF = Real files
my $time = time;
$SGEScript =~s /TIME_TAG/$time/;
my $JobName = "IND_$TrailTag\_$time";
$SGEScript =~s /JOBNAME_TAG/$JobName/;

#Now, target this line: "#$ -N MD5_Tape_TRAIL_TAG_TIME_TAG" to get the Job Name so we can wait for it ultimately:
my $QSUBScript= "$MD5ScriptsDir/MD5_RealFiles.pl";

#If you want this into the STDOUT 'log', then enable this next line (otherwise look in the file referenced) 
#my $PrettyFormattedSGEScript = $SGEScript; $PrettyFormattedSGEScript =~ s/[\r\n]/\n#:  /g; print $PrettyFormattedSGEScript; 

print "# 1) SGE Script is: (RF) '$QSUBScript'\n";
print "# Will be run with the job name: '$JobName'\n";


#Record these details: sadly the split across the data structure is a little awkward: 

$JSON_Struct{"Paths"}{"MD5 Script Files"}{"RF Script"}	= $QSUBScript;	
$JSON_Struct{"RF Job"}{"Job Name"}						= $JobName;

#Write out file (even if we can't QSUB it ultimately: we might want to inspect it)
open QSUBSCRIPT, ">$QSUBScript" or die "Cannot create: '$QSUBScript'\n";
print QSUBSCRIPT $SGEScript;
close QSUBSCRIPT;
`chmod +x $QSUBScript`;
print "# Qsub / Bash script is: ",-s $QSUBScript," bytes in size on disk\n";

=head4 Do QSub - if we have grid access and launch the collector script (#2) (trail tag: RF)

=cut 
if ($SkipSGECheck ==0)
	{
		
=head3 First: Ask for the MD5s creating

We expect to get back a job saying this:

 Your job-array 5509703.1-10:1 ("MD5_Tape_1421184762") has been submitted

Which should match what we passed here:

 #$ -N MD5_Tape_TIME_TAG
=cut
	print "# Launch General MD5sum QSub request:\n";
	my $SGEResult= `qsub -q spbcrypto $QSUBScript`; # 2>&1`; # Prep the qsub command & launch it
	$SGEResult =~ s/[\n\s]+$//g;	$SGEResult =~ s/[\r\n]/\n#:  /g;
	
	print "# SGE QSub launch result was:'$SGEResult'\n";
	
=head3 Then collect up all the MD5s when jobs are finished:

...might be a while of course...

 This is an example of a success:
 'Your job 6931158 ("MD5_COL_RF_1424389513") has been submitted'
 
=cut
	
	print "#\n#\n";
	
	print "# 2) Prepare MD5sum Collector QSub job for real files: (RF)\n";
	#Take a copy and substitute in the path: this scritpt is so short & simple we don't even write it to a file:
	my $MD5CollectorScript = $MD5CollectorScriptBase;
	#Substitute these in:
	$MD5CollectorScript =~s /TRAIL_TAG/$TrailTag/g;	#RF = Real files
	$MD5CollectorScript =~s /WD_TAG/$OutputDir/g;
	$MD5CollectorScript =~s /NFILES_TAG/$NormalFiles/g;
	my $IndexFile = "$IndexDir\/Files.md5.$TrailTag";
	
	$MD5CollectorScript =~s /INDEXFILE_TAG/$IndexFile/g;
	
	my $IndexCollectorScriptRF 	= 	"$MD5ScriptsDir/CollectorRF.pl";
#	my $CollectorScriptRF 		=	"";
	open COLLECTORRF, ">$IndexCollectorScriptRF" or die "Cannot open '$IndexCollectorScriptRF'\n";
	print COLLECTORRF $MD5CollectorScript;
	close COLLECTORRF;
        `chmod +x $IndexCollectorScriptRF`;
	print "D: Written out '$IndexCollectorScriptRF'\n";

	#Build the QSub command:
	#Ultimately add back in: -o /dev/null -e /dev/null 
	my $MD5CollectorCommand= 
	"qsub -q spbcrypto -hold_jid $JobName -N MD5_COL_$TrailTag\_$time -b y -S /bin/bash \"$IndexCollectorScriptRF\"";
	
	print "#: MD5 Collector Command: \n# : '$MD5CollectorCommand'\n";
	#Launch the Qsub job:
	print "# Launch MD5sum Collector QSub job: (RF)\n";
	my $CollectorResult= `$MD5CollectorCommand`;	
	$CollectorResult =~ s/[\r\n]$//; 	$CollectorResult =~ s/[\r\n]/\n#:  /g;
	print "# Collector command launch returned: \n#: '$CollectorResult'\n";
	print "# If all that worked then the file: '$IndexFile.completed' should have been created\n";
	
#Store various items about the command:
	$JSON_Struct{"RF Job"}{"Collector Script"}				= $IndexCollectorScriptRF;
	$JSON_Struct{"RF Job"}{"Collector Command"}				= $MD5CollectorCommand;
	
	}
#	die "HIT BLOCK\n";

=head3 MD5'ing of Active Symlinks - if needed

If detected, a similar MD5'ing of the Symlinks is done; the code below for launching / collecting 
the MD5sums is very similar to that above for the 'normal files'.

(It is so similar that one option considered was to run both real files & active Symlinks together in the same SGE grid stack,
then separate them afterwards...but it is easier to do to result collection stage). 

But by this point the previous script is written to disk and away on the QSub Queue, 
so we can re-use lots of the parts and write out the new script to a different location ($ActiveLinksQSubScriptFile) and 
control using a different job name ($Symlink_AL_JobName)

 $TrailTag = "ASL"; #Set to Active Symbolic Links	

=cut 

if ($ActiveLinks >0)
	{
	print "# Active Symlinks Detected: ($ActiveLinks of) - so running MD5s on these\n";	
	#Ok, going to do some work: First, ABS path everything we can:
	$MD5ResultsDir					= File::Spec->rel2abs($MD5ResultsDir);
	my $ActiveLinksFile 			= File::Spec->rel2abs($ActiveLinksFile);
	my $ActiveLinksQSubScriptFile 	= "$MD5ScriptsDir/MD5_ActiveSymlinks.pl";
	
	my $SGEScript = $SGEScriptBase;					#Take a copy of the 'Base' script

	$SGEScript =~s /FILELIST_TAG/$ActiveLinksFile/;
	$SGEScript =~s /WD_TAG/$OutputDir/;
	$SGEScript =~s /MD5OUT_TAG/$MD5ResultsDir/;
	$SGEScript =~s /BLOCKSIZE_TAG/$BlockSize_ALS/;
	$SGEScript =~s /NBLOCKS_TAG/$NBlocks_ALS/;


#Now the parts that need deriving here:
	my $TrailTag ="ALS"; #Set the trail tag for 'Active Symlinks'
	$SGEScript =~s /TRAIL_TAG/$TrailTag/;	#RF = Real files
	my $time = time;
	$SGEScript =~s /TIME_TAG/$time/;
	my $JobName_ALS = "IND_$TrailTag\_$time";
	$SGEScript =~s /JOBNAME_TAG/$JobName_ALS/;
	
	#If you want this into the STDOUT 'log', then enable this next line (otherwise look in the file referenced) 
	#my $PrettyFormattedSGEScript = $SGEScript; $PrettyFormattedSGEScript =~ s/[\r\n]/\n#:  /g; print $PrettyFormattedSGEScript; 
	
	
	print "# 3)  SGE Script for active links is: (ALS) '$ActiveLinksQSubScriptFile'\n";
	print "# Will be run with the job name: '$JobName_ALS'\n";

	$JSON_Struct{"Paths"}{"MD5 Script Files"}{"ALS Script"}	= $ActiveLinksQSubScriptFile;	
	$JSON_Struct{"ALS Job"}{"Job Name"}			= $JobName_ALS;

	#Write this out to disk:

	open QSUBCOMMAND, ">$ActiveLinksQSubScriptFile" or die "Cannot create: '$ActiveLinksQSubScriptFile'\n";
	print QSUBCOMMAND $SGEScript;
	close QSUBCOMMAND;
        `chmod +x $ActiveLinksQSubScriptFile`;
	
	print "# Qsub / Bash script is: ",-s $ActiveLinksQSubScriptFile," bytes in size on disk\n";
	
	if ($SkipSGECheck ==0)
		{
		
=head3 First: Ask for the MD5s creating

We expect to get back a job saying this:

 Your job-array 5509703.1-10:1 ("MD5_Tape_AL1421184762") has been submitted

Which should match what we passed here:

 #$ -N MD5_Tape_ALTIME_TAG
 
=cut
		print "# General MD5sum Qsub request for active symlinked files (ALS):\n";
		my $SGEResult= `qsub -q spbcrypto $ActiveLinksQSubScriptFile`; # Prep the qsub command & launch it


		$SGEResult =~ s/[\n\s]+$//g;	$SGEResult =~ s/[\r\n]/\n#:  /g;
	
		print "# SGE QSub launch result was:'$SGEResult'\n";
		
		print "# 4) MD5sum Collector QSub job for acitve symlinked files (ALS):\n";

		my $MD5CollectorScriptALS = $MD5CollectorScriptBase;
                #Substitute these in:
		$MD5CollectorScriptALS =~s /TRAIL_TAG/$TrailTag/g;	#ALS= Active Symlinks
		$MD5CollectorScriptALS =~s /WD_TAG/$OutputDir/g;
		$MD5CollectorScriptALS =~s /NFILES_TAG/$ActiveLinks/g;
		
		my $IndexFileALS = "$IndexDir\/Files.md5.$TrailTag";
	
		$MD5CollectorScriptALS =~s /INDEXFILE_TAG/$IndexFileALS/g;
	
	my $IndexCollectorScriptALS 	= 	"$MD5ScriptsDir/CollectorALS.pl";
#	my $CollectorScriptRF 		=	"";
	open COLLECTORRF, ">$IndexCollectorScriptALS" or die "Cannot open '$IndexCollectorScriptALS'\n";
	print COLLECTORRF $MD5CollectorScriptALS;
	close COLLECTORRF;
        `chmod +x $IndexCollectorScriptALS`;
	print "D: Written out '$IndexCollectorScriptALS'\n";


	my $MD5CollectorCommandALS= 
	"qsub -q spbcrypto -hold_jid $JobName_ALS -N MD5_COL_$TrailTag\_$time -b y -S /bin/bash \"$IndexCollectorScriptALS\"";
	
	print "#: MD5 Collector Command: \n# : '$MD5CollectorCommandALS'\n";
	#Launch the Qsub job:
	print "# Launch MD5sum Collector QSub job: (RF)\n";
	my $CollectorResult= `$MD5CollectorCommandALS`;	
	$CollectorResult =~ s/[\r\n]$//; 	$CollectorResult =~ s/[\r\n]/\n#:  /g;
	print "# Collector command launch returned: \n#: '$CollectorResult'\n";
	print "# If all that worked then the file: '$IndexFileALS.completed' should have been created\n";

	
	#Take a copy and substitute in the path: this script is so short & simple we don't even write it to a file:
#		my $MD5CollectorScriptAL = $MD5CollectorScriptBase;
#		$MD5CollectorScriptAL =~s /WD_TAG/$OutputDir/g;
#		$MD5CollectorScriptAL =~s /TRAIL_TAG/$TrailTag/g;	#ASL = Active Symlinks
#		my $MD5CollectorCommandAL= 
#		"qsub -q spbcrypto -hold_jid $Symlink_AL_JobName -N MD5_AL_Collector_$time -S /bin/bash -b y -o /dev/null -e /dev/null \"$MD5CollectorScriptAL\"";
#	
#		print "#: MD5 Collector Command: \n# : '$MD5CollectorCommandAL'\n";
#		my $CollectorResult= `$MD5CollectorCommandAL`;	
#		$CollectorResult =~ s/[\r\n]$//; 	$CollectorResult =~ s/[\r\n]/\n#:  /g;
#		print "# Collector command launch returned: \n#: '$CollectorResult'\n";
#	
#		$JSON_Struct{"ALS Job"}{"Collector Command"}				= $MD5CollectorCommandAL
#		
	$JSON_Struct{"ALS Job"}{"Collector Script"}				= $IndexCollectorScriptALS;
	$JSON_Struct{"ALS Job"}{"Collector Command"}				= $MD5CollectorCommandALS;
		}
	}


unless 	($SkipSGECheck ==0) 
	{	print "# No grid option in effect - SGE job launching skipped\n";	}

=head2 JSON Report Output

If we get to this point then the program has run sufficiently well to produce output, so we write it out into the JSON Report.

=cut

my $JSON_Report_Text= JSON->new->utf8->encode(\%JSON_Struct);
open JSONOUT, ">$JSONFile"	or die "Cannot open '$JSONFile'\n";
say JSONOUT $JSON_Report_Text;
close JSONOUT;	

#
#
#
######
=head2 usage ("Message"): Sub Routine to print usage

Call it with a text string if you want this printed in addition to the defualt, basic usage

=cut 
sub usage {
my $Message = shift @_;
unless (defined $Message && $Message ne "")	{$Message =" ";}
die "Usage: ./indexer.pl <Path to Survery> <Output Path> : $Message\n";
}


#####
#

sub calculateBlockSize 
{
=head2 my $NBlocks_RF = calculateBlockSize ($NormalFiles, $MaxJobsAllowed);	#Also this call uses $MAX_JOB_LIMT & $MAXNODES

When called runs an iterative loop that determines the minumum block size that won't exhaust the number of grid jobs 

=cut

my ($NFiles, $JobsAllowed) = @_;
unless (defined $NFiles && $NFiles ne "" && defined $JobsAllowed && $JobsAllowed ne "") 	{return 0;}
print "D: calculateBlockSize(): $NFiles, $JobsAllowed\n";

my $BlockSize=1;
while ($BlockSize <= $MAXNODES && $TotalJobs > $JobsAllowed)
	{
	$BlockSize ++;				
	$TotalJobs = ceil ($NFiles / $BlockSize);
	print "D: For block size of  $BlockSize  (number of total files $JobsAllowed: I would use: $TotalJobs jobs / slots \t c.f $MAX_JOB_LIMT available)\n";
	}

 if ($BlockSize >= $MAXNODES)		#Can't imagine it would be greater, but still: check
	{	die "Max block size (number of tasks per node reached) - either I've committed an error or there are are really lot (too many!) jobs for the queue ($MAX_JOB_LIMT)\n";	}

print "D: calculateBlockSize(): '$BlockSize'\n";
return ($BlockSize);
}

=head2 Calculate block size:

 my $BlockSize=1;
 my $TotalJobs = ceil ($NormalFiles / $BlockSize) + ceil ($ActiveLinks / $BlockSize);	#Prime the step






=cut  

