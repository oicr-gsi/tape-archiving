#!/usr/bin/perl
=head1 checker.pl

When supplied with a directory this program checks that the directory contains the encrypted files listed in the 
indexes (of real files) in it.


=head2 Method of operation

Essentially this runs through the index file and decrpypts the corresponding files it finds; pipes the result through MD5Sum 
and compares the result of the unencrypted file to that recorded in the index.

=head3 Currently it does not (explictly) check for the presence of the other index files or that the 'encrypted' directory
is contaminated with non-encrypted files (i.e. you can add things in and it will not baulk).

=head2 Module loading:

To run this on the (aged) systems of the cluster try loading these modules to get access to the (Perl) moduels:

 module load perl/5.20.1
 module load spb-perl-pipe/dev

=cut


use strict;
use JSON;	#Helps with reporting various quantities

#We use a lot of File:: modules here ;-)
use File::Basename;							#Manipulate paths 1
use File::Path qw(make_path remove_tree);	#Manipulate paths 2
use File::Spec;								#Manipulate paths 3
use Cwd 'abs_path';						#Recurse paths back to their 'source'
use Cwd;								#Get the Current Working Directory 
use Getopt::Long;			#To process the switches / options:
use 5.10.0;	# 



=head3 Defaults and constants:

=cut
#If supplied with a directory we assume this file is meant if we aren't pointed at one specifcally:
my $GPGJSONDEFAULT				= "index/GPG_result.json";
#This is the location of the 'instrcutions' file for the GPG Job:
my $INSTRUCTIONSFILE 			= "checker_instruction.tab";

my @KEYS = "4019DEF8";		#We need to have the private / secret key to decrypt the data


#This we will try get from the JSON file typically, but the --GPGEnc parameter has precedence over it:
my $BaseDirForEncFiles	="";

#Are we safe to run without SGE access? (won't attempt decryption )  
my $SkipSGECheck =0;

=head2 The 'usage ()' function

=cut

########
sub usage {
my ($Message) = @_;
unless (defined $Message && $Message ne "")
	{$Message ="-";}

die "Usage checker.pl <Directory>\n$Message\n";
}
#########


=head3 These tags get substituted

But check the real code for this.

 my $JobName = "CHK_".time;
 $DecryptBaseScript =~ s/NFILES_TAG/$FilesProcessed/g;
 $DecryptBaseScript =~ s/JOBNAME_TAG/$JobName/g;
 $DecryptBaseScript =~ s/FILELIST_TAG/$FileListMD5s/g;
 $DecryptBaseScript =~ s/RESULTSOUTDIR_TAG/$DirToSurvey/g;		
 $DecryptBaseScript =~ s/MD5OUT_TAG/$GPGScriptDir/g;

=cut 


my $DecryptBaseScript = <<'MD5_SCRIPT';
#!/usr/bin/perl
# : Decrypt to MD5 sums for all files supplied
#$ -t 1-NFILES_TAG
#$ -cwd
#$ -o /dev/null
#$ -e /dev/null
#$ -S /usr/bin/perl

#$ -N JOBNAME_TAG

use strict;
my $JobID = "JOBNAME_TAG";	#Because we need this
my $WANTEDLINE = $ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub
my $ResultOutputDir = "MD5OUT_TAG";	#Where we will put the results
my $FileList = "FILELIST_TAG";
open INPUTFILE , "$FileList" or die "Cannot open list of files '$FileList' containing MD5s\n";

my $Count=0;
while (<INPUTFILE>)
        {
        $Count++;  #Increment the line counter
        unless ($Count == $WANTEDLINE)  {       next;   }       #Skip until the line we want:
#Below here only processed for wanted lines: 
        my ($MD5, $FilePath) =  split (/[\t\s]+/,$_);
        print "D: MD5 = '$MD5'; '$FilePath'\n";
#We try to emulate a command like this:        
# gpg -d /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/encrypted/real_fs/B/9.file.gpg 2> /dev/null | md5sum
        my $GPGCommand = "gpg --decrypt $FilePath | md5sum";
        
        print "D: '$GPGCommand'\n"; 
		my $GPGResult = `$GPGCommand`;
		print "D: '$GPGResult'\n";
		my $ErrorFile = "$ResultOutputDir/$JobID\_$WANTEDLINE\.encrypt-error";
	#Temporary: 
#		open OUTPUT, ">$ErrorFile" or die "Cannot open output file: '$ErrorFile'\n";
#		print OUTPUT "$FilePath = $GPGResult\n";
#		close OUTPUT;
		
		unless ($GPGResult =~ m/$MD5/)	#We bother parsing the output - provided the MD5 sum is there
			{	
#Note the error:
			
			open OUTPUT, ">$ErrorFile" or die "Cannot open output file: '$ErrorFile'\n";
			print OUTPUT "$FilePath = $GPGResult\n";
			close OUTPUT;
			exit;           #Exit here is an optimisation as we don't care about the other lines in the file: another instance will process them
        	}
        }
MD5_SCRIPT

=head3 The 'collector' script:

=cut
 
my $CollectorBaseScript = <<'COLLECTOR_SCRIPT';
#!/usr/bin/perl

#This program is a 'collector script' it surveys a particular directory and if it doesn't find any files 
#drops a 'OK_TO_ARCHIVE' tag into the directory (containing the EPOC time stamp)

use strict;

#Imagine this is like a standard @ARGV:
my ($GPGOutputDirectory, $Jobname) =	("OUTPUTDIR_TAG", "JOBNAME_TAG");
#How to set these outside to check them on the commandline:
 
#my ($GPGOutputDirectory, $Jobname) = ("/u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op/scripts/", "FLUFFY");

#Check we got our command line arguments:
unless (defined $GPGOutputDirectory)		{die "No output Directory supplied\n";}
unless (defined $Jobname)					{die "No Jobname of checker\n";}

#Remember we have already 'waited' via the SGE 'wait-for' job 
 
sleep (2);		#Wait a little for all the jobs to create their errored output ()

#open the directory: there shouldn't be many files in it
opendir(my $dh, $GPGOutputDirectory) || die "can't opendir $GPGOutputDirectory: $!";
while (my $file = readdir($dh)) 
	{
	#So we will also test "." & ".."
    if ($file	=~ m/$Jobname/)
    	{	print "An error file was detected '$file' - so no approving directory\n";	
    		die "An error file was detected '$file' - so no approving directory\n";
    	}
    }
closedir $dh;
    

	
#Ok, so a clean run - what we like!
#Mark the directory as approved:
my $AprovedFile  = "$GPGOutputDirectory/../APPROVED_TO_WRITE";
open APPROVALFILE, ">$AprovedFile" or die "Cannot open approval file: '$AprovedFile'\n";
print APPROVALFILE time;
close APPROVALFILE;

COLLECTOR_SCRIPT




=head2 Start Code Proper

=head3 Process CLI Options

=cut

=head3 Process (the rest of) the command line options

=cut
my $GPGJSON; #To store the results of the GPG encrpytion process 
my $MD5summedIndex; #Custom location of MD5 Summed list of files (SGEcaller.pl makes Files.index, indexer.pl makes Files.md5.RF
GetOptions (
	"EncDir|encryptedDir|basedir|outdir=s" => \$BaseDirForEncFiles,
	"noSGEOK|nogrid" => \$SkipSGECheck,
        "index=s"          => \$MD5summedIndex,
	"JSON|JSONOUT|JSONGPG=s"	=> \$GPGJSON,	#	= The JSON file containing details of the programming running
 )
	or usage ("Error in command line arguments\n");

# Build the file name containing the list of files to sruvey in a very specific way:

#TO DO: Allow multiple input files / directories...maybe if time allows:
my @ItemsPassed;

my $Time = time;

#Get the directory (we will need this no matter what else):
my $DirToSurvey = shift @ARGV;
$DirToSurvey =~ s/\/$//;	#strip trailing slash off:

unless (defined ($DirToSurvey) && -d $DirToSurvey)
	{	usage ("No Directory to survey supplied\n");	}
($MD5summedIndex && -e $MD5summedIndex) or $MD5summedIndex = "$DirToSurvey/index/Files.md5.RF";
#Convert to absolute path:
$DirToSurvey = abs_path($DirToSurvey);

#Now assumed (boldly!) that these files exist; if everything is correct they should
#and if not we are going to fail approval of this directory anyway 

#my 		$FileListMD5s 	= "$DirToSurvey/index/Files.md5.RF";		#This (should) exist
my             $FileListMD5s   =  $MD5summedIndex;            #This (should) exist
my 		$GPGJSONFile   = "$DirToSurvey/index/GPG_result.json";		#As should this


#Check these exist:

unless (-e $FileListMD5s && -r $FileListMD5s)	{usage ("Cannot open main list of files: '$FileListMD5s'\n");}
print "#: Found the list of files passed: '$FileListMD5s'\n";

unless (-e $GPGJSONFile && -r $GPGJSONFile)	{usage ("Cannot open JSON GPG: '$GPGJSONFile'\n");}
print "#: Found the list of files passed: '$GPGJSONFile'\n";


=head3 Do path manipulations: now not needed?

As a reminder:

 my $ASSUMEDLISTLOCATION = "index/Files.md5.RF";

=cut

#if ($FileListMD5s =~ m/$ASSUMEDLISTLOCATION$/ && $BaseDirForEncFiles eq "")	#Does the file passed conform to what we expect (and we weren't told explictly)? 
#	{
#	print "#: Attempting to deduce the path to the GPG files - as I wasn't told explictly\n";
#	print "#: Detected output as cannonical - ends as we expect it: '$ASSUMEDLISTLOCATION' - likely we can guess the location of the GPG Files\n";
#	$BaseDirForEncFiles = $FileListMD5s;
#	$BaseDirForEncFiles =~ s/$ASSUMEDLISTLOCATION//;
#	print "D: My current guess: '$BaseDirForEncFiles'\n";
#	if (-e $BaseDirForEncFiles && (-d $BaseDirForEncFiles or -l $BaseDirForEncFiles))
#		{	print "# Deductions complete got a directory at: '$BaseDirForEncFiles' is base of GPG files I will assume\n";		}
#		else
#		{	print usage ("Cannot find a directory or symlink at '$BaseDirForEncFiles', so aborting");	}
#	}
#	elsif ($BaseDirForEncFiles ne "")
#		{
#		print "#: Testing '$BaseDirForEncFiles' supplied by the --GPGBase parameter\n";
#		if (-e $BaseDirForEncFiles && (-d $BaseDirForEncFiles or -l $BaseDirForEncFiles))
#			{	print "# Ok, path appears valid got a directory at: '$BaseDirForEncFiles' is base of GPG files I will assume\n";		}	
#		else
#			{	usage ("Can't find the base directory supplied by --GPGBase: '$BaseDirForEncFiles'");	}
#		}
#else
#	{
#	usage ("Can't deduce the directory containing the encrypted files; try supplying --GPGDir");
#	}

=head2 Try to find - and load - JSON from the encryption

This must exist by this point in the process:

=cut 
my $JSONText;
my $JSON_Struct_ref;

if (-e $GPGJSONFile && -r $GPGJSONFile)
	{
	open JSONFILE, $GPGJSONFile or die "Cannot open JSON File '$GPGJSONFile' but it exists and it is readable - weird\n";
	while (<JSONFILE>)		{$JSONText .= $_ ;}	close JSONFILE;	#Read in the file (isn't large)
	$JSON_Struct_ref = decode_json ($JSONText) or die "Some problem parsing the text from the JSON file: '$GPGJSONFile'";
	print "#: Got JSON Data from: '$GPGJSONFile'\n";
	}

#If you are interested in what we loaded, enable the next line (it will be as rough as Data::Dumper output is...)



=head3 Now process the precedence of the --GPGEnc parameter

=cut

if ($BaseDirForEncFiles eq "" && exists $$JSON_Struct_ref{"Paths"}{"Encrypted Files"})
	{	$BaseDirForEncFiles = $$JSON_Struct_ref{"Paths"}{"Encrypted Files"};	}
#print "D: '",$$JSON_Struct_ref{"Paths"}{"Encrypted Files"},"'\n";
#use Data::Dumper; print Dumper $JSON_Struct_ref; die "HIT BLOCK\n";

#Get the original base directory:

#my ($JobName) = $SGEScript =~ m/-N (MD5_Tape_.*?)\n/;

my $OriginalDirBase ="";

if (exists $$JSON_Struct_ref{"Paths"}{"Shortest Common Path"})
	{#Strangly this doesn't have to exist in the 'real on disk' sense; it just has to be present in the JSON structure.
	$OriginalDirBase = $$JSON_Struct_ref{"Paths"}{"Shortest Common Path"};	
	}
else
	{	die "Can't get the path of the original files from the JSON file\n";	}	#Not having this is so worrying as to be a show stopper (technically we could re-deduce it)


=head3 Set the output paths for various files

The GPG decrypted script, its instruction file, the collector script, the output JSON file.

=cut


#This is a convenience and eases variable interpolation: 
my $GPGScriptDir 			= $$JSON_Struct_ref{"Paths"}{"Script Dir"};
unless (-e $GPGScriptDir)
	{	print "D: GPG script dir: '$GPGScriptDir' does not exist\n";	}
my $DecryptScript 				= "$DirToSurvey/scripts/decryptTest.pl";	
my $CollectorBaseScriptFile 	= "$GPGScriptDir/checker_collector.pl";
my $DecryptInstructionsTabFile 	= "$GPGScriptDir/$INSTRUCTIONSFILE";

print "Decrypter script: 			'$DecryptScript'\n";
print "Collector base script file: 	'$CollectorBaseScriptFile'\n";
print "Instructions file:			'$DecryptInstructionsTabFile'\n";

#Ultimately this is the location of our JSON file
my 		$OutputJSONFile		= "$DirToSurvey/index/Checker_result.json";

#die "HIT BLOCK\n";

#Just helpful to do this:
$DirToSurvey =~ s/\/$//;

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


my $GPGKeyString="";
#Test Keys: 
print "#: Testing gpg (in general terms):\n";
unless (`gpg --version`                         =~ m/^gpg \(GnuPG\) /)  {       die "Couldn't find GPG\n";      }
foreach my $C_Key (@KEYS)
        {
        print "#: Testing gpg keyring (for '$C_Key'):\n";
        unless (`gpg --list-secret-keys`       =~ m/$C_Key/ or `gpg --list-secret-keys`        =~ m/$C_Key/)                   
                {       die "Couldn't find the Key '$C_Key' in the keyring\n";  }
        $GPGKeyString=$GPGKeyString. " -r $C_Key";              
        }

print "#: For GPG I will use these keys (to decrypt) - all found that I was asked to check: '$GPGKeyString'\n";

print "#: Pre-Flight Checks complete: we are go\n# GO Decision!\n";

=head2 Processing the list of files proper:

=cut

my $FilesProcessed = 0;

my $FilesOK =0;
my %MD5s;	#We might not use this ultimately; for now though...store in memory.


my %Directories;

print "D: BaseDir for encrypted files:'$BaseDirForEncFiles'\n";
#die "HIT BLOCK\n";
open INSTRUCTTAB, ">$DecryptInstructionsTabFile" or die "Cannot create instruction file for GPG Decrypt: '$DecryptInstructionsTabFile'\n";
open MDFILES, $FileListMD5s;
while (<MDFILES>)
	{
	$FilesProcessed++;
	my ($MD5, $File) = split (/\s+/,$_);
#		my (undef, $Dir) = fileparse ($_);
##Note the directory, if we haven't seen it before:
# 
#	if (defined ($Dir) && $Dir ne "" 	# Check the Dir is parsable - if not, something slipped through our other tests. 
#	&& not exists ($Directories{$_}))	# If it new to us then it is interesting 	
#		{	
#		my (undef, $Path) = (File::Spec->splitpath ($_));	#Get the proper directory part
#		#Note the path and the number of the directories:
#		$Directories {$Dir}=scalar (File::Spec->splitdir ($Path));	
#	$Directories {$Dir}=scalar (File::Spec->splitdir ($File));	
	chomp ($File);
	
	print "D: File: $FilesProcessed\t$File = $MD5\n";
	my $GPGFile = $File;
	#$GPGFile =~ s/$OriginalDirBase//;
	#$GPGFile = $BaseDirForEncFiles.$GPGFile;
	
	$GPGFile =~ s/$OriginalDirBase//;
	$GPGFile = $BaseDirForEncFiles.$GPGFile.".gpg";
	print "D: GPG File: '$GPGFile'\n";
	unless (-e $GPGFile)
		{
			warn "$GPGFile ($FilesProcessed) does not exist\n";
		}
	print INSTRUCTTAB "$MD5\t$GPGFile\n";
	print "D: $FilesProcessed will actually test: '$GPGFile' MD5 (original): $MD5\n";
	}
close MDFILES;
close INSTRUCTTAB;

#die "HIT BLOCK\n";
=head3 Prepare the SGE Script

=cut

 
=head3 Substitute into the SGE Jobs

Replace all these '_TAGs' in the code

=cut
my $JobName = "CHK_".$Time;
$DecryptBaseScript =~ s/NFILES_TAG/$FilesProcessed/g;
$DecryptBaseScript =~ s/JOBNAME_TAG/$JobName/g;
$DecryptBaseScript =~ s/FILELIST_TAG/$DecryptInstructionsTabFile/g;
$DecryptBaseScript =~ s/RESULTSOUTDIR_TAG/$DirToSurvey/g;		
$DecryptBaseScript =~ s/MD5OUT_TAG/$GPGScriptDir/g;

print "D: Decrypt script:\n $DecryptBaseScript\n'\n written to: '$DecryptScript'\n";

open SGESCRIPT, ">$DecryptScript" or die "Cannot open SGE Script '$DecryptScript'\n";
print SGESCRIPT $DecryptBaseScript;
close SGESCRIPT;

`chmod a+x $DecryptScript`;
my $GPGLaunchResult = `qsub -q spbcrypto $DecryptScript`;
print "#: GPG (decrypt) Launch result was: '$GPGLaunchResult'\n# Waiting 2s before qstat\n";
#

sleep (2);
print "#\n# Result of qstat:\n# \n";
my $QStatResult = `qstat -q spbcrypto`;
$QStatResult=~ s/[\n\s]+$//g;
$QStatResult  =~ s/[\r\n]/\n#:  /g;
print "#:   $QStatResult#\n#\n";

=head3 Now launch 'The collector'

...might be a while of course...


 qsub -q spbcrypto -hold_jid $JobName -N MD5_Collector_$time -b y -o /dev/null -e /dev/null 
  \"$CatCalcdMD5sScript\"";

 my $CatCalcdMD5sScript = <<'END_2NDSCRIPT';
 cat WD/md5s/*.md5 >> WD/index/Files.md5 
 rm WD/md5s/*.md5

=cut
	
	print "#\n#\n";
	
	print "# 2) Prepare the Collector QSub job to check for errors reported\n";
	#my ($GPGOutputDirectory, $Jobname) =	("OUTPUTDIR_TAG", "JOBNAME_TAG")
#Do the subsitutions on the collectory script:
	$CollectorBaseScript =~ s/OUTPUTDIR_TAG/$GPGScriptDir/g;
	$CollectorBaseScript =~ s/JOBNAME_TAG/$JobName/g; 
	open COLLECTOR, ">$CollectorBaseScriptFile";	print COLLECTOR $CollectorBaseScript; close COLLECTOR;
	
	#Take a copy and substitute in the path: this scritpt is so short & simple we don't even write it to a file:
	my $MD5CollectorCommand= 	
	"qsub -q spbcrypto -hold_jid $JobName -N CHK_Collector\_$Time -b y -S /bin/bash -o /dev/null -e /dev/null \"$CollectorBaseScriptFile\"";
	
	print "#: MD5 Collector Command: \n# : '$MD5CollectorCommand'\n";
	#Launch the Qsub job:
	print "# Launch MD5sum Collector QSub job: (RF)\n";
	my $CollectorResult= `$MD5CollectorCommand`;	
	$CollectorResult =~ s/[\r\n]$//; 	$CollectorResult =~ s/[\r\n]/\n#:  /g;
	print "# Collector command launch returned: \n#: '$CollectorResult'\n";


#my $MD5Result =~ m/^[a-f0-9]{32} /;
#die "HIT BLOCK\n";

#
#
#
###########
=head2 common_prefix (\@Array): find the shortest 'common path' 

Pass it a reference of arrays and it returns the shortest commong 'root path' 

The implementation is taken from: 

 http://rosettacode.org/wiki/Find_common_directory_path#Perl

=cut

sub common_prefix {
    my $sep = shift;
    my $paths = join "\0", map { $_.$sep } @_;
    $paths =~ /^ ( [^\0]* ) $sep [^\0]* (?: \0 \1 $sep [^\0]* )* $/sx;
    return $1;
}
