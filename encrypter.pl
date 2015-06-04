#!/usr/bin/perl
=head1 encrypter.pl <list of files> <output location>


This script is designed to operate on 'clustEncrypt.pl' (and shares a common code history) output to 
create a GPG'd version of the file at the output location in a parallel directory structure.

When pointed at a 'Files' file created by clustEncrypt.pl - or other process that creates a 
similar list of real files - it spawns a GPG job on the SGE queue.


The program is designed to operate at the 10TB+ & 50 000+ file range so lists are written out to files rather than being held
in memory (for example).

The spb_crypto cluster queue is used by the shell scripts created. 

=head2 Usage

=head3 Simple usage:
 
 ./encrypter.pl <Path to Survery> <Output Path> 

=head3 Under test  

A typical command line when testing would be:

 ./encrypter.pl  --clobber --noSGEOK -d=1 ~/tickets/tapeArchiveSPB_2983/testDir
 
 Overwrite output OK (--clobber); don't fail on no SGE (--noSGEOK) and give extra STDOUT printout for every file (-d=1)
 
=head3 Parameter Details

Formally there are these parameters:

	"clobber|c" => \$ClobberOutput_F,		= Overwrite output Ok; default: not, skip when the script & instruction files / directories already exists
	"space|min_diskspace|diskspace=f",      = Minumim extra disk space required, assumes a doubling so: 0.1 = 110% of original files (can be negative interestingly; set to 0 to disable); default 0.1
	"diag|d|D=i" => \$ExFrequency,			= Diagnostic printout frequency; default: never, no diag output
	"noSGEOK|nogrid" => \$SkipSGECheck		= Skip the SGE Check & job launch - allows running on a non cluster machine; default: not, i.e. test & launch jobs
	"NoStripPath|nsp" =>                    = Do *Not* deduce the shortest common path from the input files supplied - and then remove this from the outputed GPG files 
	"JSON|JSONOUT|JSONGPG=s"	=> \,		= The JSON file containing details of the programming running
	"help|h"								= Print out the command line options


=head4 Deactivating path manipulation: --NoStripPath 

This is helpful in controlling / pruning the overly deep directory trees that can result 'leaf-wards' from the output directory; it might 
be become computational expensive for longer (100 000+ file lists), hence the ability to deactivate the deduction of the 'most common path'.
The default is to deduce this most common path; hence the "No" in "NoStripPath".

Also if no output path is supplied then the program assumes you mean 'all output to go in the same directory as the list 
of input files' - likely the output from computeMD5s.pl i.e. '../index/Files'.  
This switch supresses the feature that strips off the './index/' - note that only the trailing '/index/' directory componement is effected.
(The '/Files' - being a real file is also removed).

Some examples:
 Original path:											Output Path Used:
 /../archives/myproject/data/index/Files 			=> 	/myproject/data/
 /../archives/myproject/Superindex/index/Files		=>	/../archives/myproject/Superindex
  
=head4 Disk Space parameter: --space=0 --space=0.2

The default setting of 0.1 assumes that the encrypted output files will be the same size as the input ones +10% (acutally, they won't increase in size - GPG's compression ensures this  
but somebody might use the extra disk space during the run).
Set this to 0 to disable the check.  As it uses df (not du) and a sum of the filesizes in the index file it computationally fast.   

=head4 Not active parameters

Not currently activate, but the code is present:
"NoAutoIndexRemove|nai|nair"	=>		= Do *Not* remove the final directory 'index' when setting the default path

  
=head2 Keys used:

The important line is:

 my @KEYS = ("BC3E454B", "E16641B3", "1C1742CB");	#OICR Tape (pub only), OICR General (pub only), SPB 2014 ()Private Key)

This polls the accessible GPG for the public key version of these keys - as listed below: 

 $ gpg --list-keys

 pub   2048R/1C1742CB 2014-08-21
 uid                  SeqProdBio2014 (August 2014 Version) <seqprodbio@oicr.on.ca>
 sub   2048R/4019DEF8 2014-08-21

 pub   4096R/E16641B3 2013-09-25
 uid                  Ontario Institute for Cancer Research <systems@oicr.on.ca>
 sub   4096R/113D3D70 2013-09-25

 pub   2048R/BC3E454B 2013-09-25
 uid                  Ontario Institute for Cancer Research (Tape Backup) <systems@oicr.on.ca>
 sub   2048R/1FA4AEA7 2013-09-25

=head2 Directory / Path Mananagement

The directory structure is built recursively if it doesn't already exists in advance of any GPG commands issued (if it exists it is left alone).

$

There is no check that the files found (if any) are complete GPG files: this program launches the jobs to create files.
  
=head2 Typical output for a range of test files:

=head2 Real Code Below Here

=head2 Modules and 'Constant' declarations
  
=cut 

use strict;

#We use a lot of File:: modules here ;-)
use File::Basename;							#Manipulate paths 1
use File::Path qw(make_path remove_tree);	#Manipulate paths 2
use File::Spec;								#Manipulate paths 3

use Cwd 'abs_path';						#Recurse paths back to their 'source'
use Cwd;								#Get the Current Working Directory 
use Getopt::Long;                       #To process the switches / options:
use POSIX qw(ceil);			#To enable ceiling calculations
use 5.10.0;	# 
use JSON;	#Helps with reporting various quantities
use Data::Dumper;

=head2 Set some defaults & pseudo-constants

=cut 

my $ClobberOutput_F =0;
my $MINDISKSPACEFRACTION = 0.1;		#In bytes remember
my @KEYS = ("BC3E454B", "E16641B3", "1C1742CB");	#OICR Tape (pub only), OICR General (pub only), SPB 2014 ()Private Key) 
my $SkipSGECheck = 0;
my $ExFrequency = 0;	#the frequency of diagnostic printing; if enabled (set to 0 to disable) 
my $NoStripPath = 0;
my $MAX_JOB_LIMT = 75000;
my $MAXNODES = 100;

#my $NoAutoIndexRemove=0;
my $BasePath = "";
#my $OUTDIRTAG = "_out";

=head3 Create the skeleton SGE Scripts in this next section

Check the real code for the current version, but expect it to be similar to this below in general terms.
This version uses 'SGE Array Jobs' (against Brent's advice!).
 For a general description see: https://www.google.ca/webhp?ie=UTF-8#q=sge%20array%20jobs

The hashes in the first few lines mean something to SGE apparently...

 !!! REMEMBER DON'T EDIT THIS AND EXPECT CHANGES IN EXECUITON !!! - this is documentation,
 The active code in immediately below this in a 'HERE' document!

 #!/usr/bin/perl
 #$ -t 1-NFILES_TAG
 #$ -cwd
 #$ -S /usr/bin/perl
 #$ -e /dev/null
 #$ -o /dev/null
 #$ -N GPG_Tape_GPG_TIME_TAG

 my $WANTEDLINE=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub

 open INPUTFILE , "FILELIST_TAG";

 my $Count=0;
 while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        unless ($Count == $WANTEDLINE)  {       next;   }       #Skip until the line we want:
 #Below here only processed for wanted lines:
        chomp ();               #Strip new lines off 
        my ($Input, $Output) =  split (/\t/,$_);
 #       print "D: $Input ---> $Output\n";
 #Issue the command (magically KEYS_!TAG will have been subsitituted in by the main Perl script by the time this runs
        `gpg --trust-model always KEYS_TAG -o "$Output" -e "$Input"`;
        exit;           #Exit here is an optimisation: leave other lines for other instances 
        }
 !!! REAL Code follows - alter that, not this above !!!
 
=cut 

my $GPGBaseScript = <<'END_SCRIPT';
#!/usr/bin/perl

#$ -t 1-NBLOCK_TAG
#$ -cwd
#$ -S /usr/bin/perl
#$ -e /dev/null
#$ -o /dev/null
#$ -N GPG_Tape_GPG_TIME_TAG

my $Block=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub

open INPUTFILE , "FILELIST_TAG" or die "Couldn't read from FILELIST_TAG";

my $BlockSize = "BLOCKSIZE_TAG";

#Hence, calculate the actual line numbers between which we will process:
my $WantedLine_Start = ($Block-1) * $BlockSize;
my $WantedLine_End = ($Block) * $BlockSize;

my $Count=0;
my @LinesToProcess;
while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        if ($Count <= $WantedLine_Start)        {       next;   }       #Skip until the lines we want:
        if ($Count > $WantedLine_End )                  {               last;   }
#Below here only processed for wanted lines:
        chomp;               #Strip new lines off 
        s/ /\\ /g; #Escape spaces in file names
        s/([)(}{|%@\$])/\\$1/g; # Escape special characters
        my ($Input, $Output) =  split (/\t/,$_);
#       print "D: $Input ---> $Output\n";
        push @LinesToProcess, [$Input,$Output];
        }
close INPUTFILE;

foreach my $I_File (@LinesToProcess)
        {
        my ($In, $Out) = ($$I_File[0], $$I_File[1]);    #I.e. first (0th) is the filename, second is it's encrypted version
#Issue MD5 request (& do reality checks):
                # This should be done not too many times in one job, given 100 nodes we may have up to 
                `gpg --trust-model always --no-random-seed-file KEYS_TAG -o "$Out" -e "$In"`;
        }        

END_SCRIPT

=head3 Collector Script 
 The purpose of this collector script is to wait until all encryptor jobs
 finish and then run find on 'encrypted' directory and put the list of 
 encrypted files into a file for further use
=cut

my $GPGCollectorScriptBase = <<'COLLECTOR_SCRIPT';
#!/usr/bin/perl
#$ -cwd
#$ -S /bin/bash
#$ -e /dev/null
#$ -o /dev/null
#$ -b y

use strict;
my $WDir                = "WD_TAG";
my $OutputGpgFile       = "GPGTALLY_TAG";

 #Escape spaces and special characters
 $WDir=~s/ /\\ /g; #Escape spaces in file names
 $WDir=~s/([}{%@])/\\$1/g; # Escape special characters
 $OutputGpgFile=~s/ /\\ /g; #Escape spaces in file names
 $OutputGpgFile=~s/([}{%@])/\\$1/g; # Escape special characters

 # Really, really simple
 my $CheckerCommand = "find $WDir -type f > $OutputGpgFile 2>/dev/null";
 `$CheckerCommand`;
        
COLLECTOR_SCRIPT


#Mimic this:
#gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $TMPOUT/$FName.gpg -e $InputFile"

=head2 Start code proper

=head3 Get the options passed (if any), otherwise set their defaults
(see the real code for current defaults)

Can I clobber / remove output directories:
 my $ClobberOutput_F =0;

Minimum required diskspace (in percent greater than the initial size: 0.1 = 110% space required)
 my $MINDISKSPACE = 0.1

=cut

#If supplied a directory as an 'list of input files' , add this on and try to find a file: 
my $DEFAULTINDEXFILESUBLOCATION = "/index/Files.md5.RF";
my $DEFAULTGPGLOGLOCATION       = "/index/Files.encrypted";
my $DEFAULTOUTPUTLOCATION	= "/encrypted";
my $GPGJSONDEFAULT		= "/index/GPG_result.json";

#This will get populated by the deductions we make below:
my $GPGJSONFile = "";

GetOptions (
	"clobber|c" => \$ClobberOutput_F,
	"space|min_diskspace|diskspace=f" => \$MINDISKSPACEFRACTION,
	"diag|d|D=i" => \$ExFrequency,
	"noSGEOK|nogrid" => \$SkipSGECheck,
	"NostripPath|nsp" => \$NoStripPath,
	"JSON|JSONOUT|JSONGPG=s"	=> \$GPGJSONFile,	
#	"NoAutoIndexRemove|nai|nair"	=>		\$NoAutoIndexRemove,
	"help|h"		=> \&options
 )
	or usage ("Error in command line arguments: run with --help to list\n");
#Reference to a subroutine to print the options:
sub options {
print <<'OPTIONS';
	"clobber|c" =>                          = Overwright output Ok; default: not, skip when the script & instruction files / directories already exists
	"space|min_diskspace|diskspace=f",      = Minumim extra disk space required, assumes a doubling so: 0.1 = 110% of original files (can be negative interestingly; set to 0 to disable); default 0.1
	"diag|d|D=i" =>                         = Diagnostic printout frequency; default: never, no diag output
	"noSGEOK|nogrid" =>                     = Skip the SGE Check & job launch - allows running on a non cluster machine; default: not, i.e. test & launch jobs
	"NoStripPath|nsp" =>                    = Do *Not* deduce the shortest common path from the input files supplied - and then remove this from the outputed GPG files
	"JSON|JSONOUT|JSONGPG=s"	=> \,		= The JSON file containing details of the programming running  
	"help|h"                                = Print out the command line options
	
OPTIONS
usage ("Options as above");	#Now terminate with a polite error.
}

#my $JSON_MD5_Struct_ref;		#JSON output of the MD5 indexer gets loaded here
my %JSON_GPG_Struct;		#The JSON output of this program gets built here, then ouput upon success

#print "D: clobber flag: $ClobberOutput_F \n";die;
#And the rest of parameters: 

#Ultimately we want these 3 variables populated:
my $InputList = "";
my $OutputDir = "";				#Where the command scripts, indexes, JSON files can be found / will go
my $OutputEncryptedDir = "";	#The location of the actual files
 
#What we get is passed on the command line: 
my ($InputListAsSupplied, $OutputDirAsSupplied) = @ARGV;

=head4 Input List

Maybe we can populate the output directory as well....

=cut 

unless (defined $InputListAsSupplied)
	{usage ("No list of files supplied");}

print "# Target supplied as input list or directory: '$InputListAsSupplied'\n";

#But - were we supplied a directory?
#If so, make some bold assumptions that this is the location we wanted as the 'root' of any files created out path

if (-d $InputListAsSupplied)	
	{
	print "#: We were supplied a directory instead of an input list; fine I will add on: '$DEFAULTINDEXFILESUBLOCATION' and look there for a file\n";
	$InputListAsSupplied =~ s/\/+$//;	#Strip trailing slashes	
	$InputList = $InputListAsSupplied.$DEFAULTINDEXFILESUBLOCATION;
	$OutputDir = $InputListAsSupplied;		#A little strange - but possible and convenient
	}
elsif (-f $InputListAsSupplied or -l $InputListAsSupplied)	
	{
	$InputList = $InputListAsSupplied;	#Ok, so this is a real file: that they meant to point us at.  
	}
$InputList = File::Spec->rel2abs($InputList);
#All this done: Could I find an input file?
unless (-e $InputList && (-l $InputList or -f $InputList))	
	{	usage ("Could not find input file containing files: '$InputList'");	}
else
	{	print "# Found this file to take my input from: '$InputList'\n";	}

=head4 Output Directory

If an output directory was supplied, this has precedence, 
otherwise assume the previous code section has done its thing... ...and then we test the file.

To clarify there are three possible states here :

 Input is not supplied				;		*					=	Can't continue
 Input supplied is invalid (F or D)	;		*					=	Can't continue
 

 Input supplied is File				;	No output directory		=	Can't continue: not enough information: need the Output Dir
 																	explictly
 
 Input supplied is Directory		;	No output directory		=	Fine...I can (probably) the guess the list file 
 																	and use the input directory as output
 Input supplied is Directory		;	Output directory 		=	Fine...(probably) the guess the list file

=cut
#Do we have enough information to deduce sensible outputs?
if (not (-d $InputListAsSupplied) && $OutputDirAsSupplied eq "")
	{	usage ("Not enough information to continue: please supply an output directory in addition to an input file '$InputListAsSupplied'\n");	}

#Do we have specific instructions to take precedence over anything else?
if (defined $OutputDirAsSupplied && $OutputDirAsSupplied ne "")
	{	
	
		$OutputDir = abs_path(File::Spec->rel2abs($OutputDirAsSupplied));
		$OutputEncryptedDir = $OutputDir.$DEFAULTOUTPUTLOCATION;
	}
 if (-d $InputListAsSupplied && $OutputDirAsSupplied eq "")
#If we weren't given an output directory and haven't be able to deduce one: use the current input directory:	
	{
	print "#: Setting default output location(s) as I've tested this for being a directory:\n";
    my $ABSpath_ListPath = File::Spec->rel2abs($InputListAsSupplied);	
    
	#Now clean it up, add on the location for the encrypted files
    $ABSpath_ListPath  =~ s/\/$//;	#Remove the trailing slash
    $OutputDir = $ABSpath_ListPath;	
    $OutputEncryptedDir = $ABSpath_ListPath."/". $DEFAULTOUTPUTLOCATION;
    #Cleanup the paths by removing double slashes (and assume that the )
    $OutputEncryptedDir =~ s/\/{2,}/\//g;    $OutputDir =~ s/\/{2,}/\//g; 	
	}

#Fix the JSON Output File - if none was supplied:
 #If we weren't give a JSON MD5 input file, then try to guess its location
 unless (defined $GPGJSONFile && $GPGJSONFile ne "")
	{
	$GPGJSONFile = $OutputDir."/".$GPGJSONDEFAULT;
	print "# No JSON file specified (--JSONGPG), so setting to the default '$GPGJSONFile'\n";
	}


print "# Take input from (list of files to encrypt): '$InputList'\n";
print "# Will write scripts etc. to       : '$OutputDir'\n";
print "# Will write encrypted files to    : '$OutputEncryptedDir'\n";
print "# JSON containing GPG results will be: '$GPGJSONFile'\n";

$JSON_GPG_Struct{"Paths"}{"Input Directory As Supplied"} 	= $InputListAsSupplied;	#JSON Load
$JSON_GPG_Struct{"Paths"}{"Output Directory"} 				= $OutputDir;			#JSON Load
$JSON_GPG_Struct{"Paths"}{"Encrypted Files"} 				= $OutputEncryptedDir;	#JSON Load

if (defined $OutputDirAsSupplied)
	{$JSON_GPG_Struct{"Paths"}{"Output Directory As Supplied"} 	= $OutputDirAsSupplied;}	#JSON Load
	

#Enable this next line if you want to just check the input / output path maniuplations: 
#die "HIT BLOCK\n";

=head3 Try to load the JSON file: 


$JSON_Struct{"Paths"}{"Output"}{"ABS"} = $OutputDir;	#JSON Load	
$JSON_Struct{"Paths"}{"Input"}{"ABS"} = $TargetDIR;	#JSON Load


Currently this isn't in use...because it doesn't contain anything we'd rely on and is more for audit purposes. 
 my $MD5JSONDEFAULT				= "/index/MD5_result.json";
 my $MD5JSONFile;

 #If we weren't give a JSON MD5 input file, then try to guess its location
 unless (defined $MD5JSONFile && $$MD5JSONFile ne "" )
	{
	$MD5JSONFile = $OutputDir."/".$MD5JSONDEFAULT;
	print "# No JSON file specified (--JSONMD5), so trying the default '$MD5JSONFile'\n";
	}

 print "# JSON containing MD5 results 	  : '$MD5JSONFile'\n";

 #If we weren't give a JSON MD5 input file, then try to guess its location
 unless (defined $MD5JSONFile && $$MD5JSONFile ne "" )
	{
	$MD5JSONFile = $OutputDir."/".$MD5JSONDEFAULT;
	print "# No JSON file specified (--JSONMD5), so trying the default '$MD5JSONFile'\n";
	}

 my $JSONText="";

 if (-e $MD5JSONFile and -r $MD5JSONFile)
	{#The file exists, so load it:	
	open JSONFILE, $MD5JSONFile;
	if (tell (JSONFILE) != -1)
		{	
		while (<JSONFILE>)	{$JSONText .= $_ ;}		close JSONFILE;	#Read in the file (isn't large)
		}	
	$JSON_MD5_Struct_ref = decode_json ($JSONText) or die "Some problem parsing the text from the JSON file: '$MD5JSONFile'";
	print "#: Got JSON Data from: '$JSON_MD5_Struct_ref' (this will help, but isn't essential\n";

	#Enable this if you want to know:
 #	use Data::Dumper; print Dumper $JSON_MD5_Struct_ref;
	}

=cut 

	
=head2 Determine whether to delete output directory (or not or die)

As a safety check we require that we find a 'timestamp' file in the directory; just in case something goes wrong...

=cut

my $TIMESTAMPFILE= "$OutputDir/TimeStamp"; 

if ($ClobberOutput_F == 0 && -e $OutputEncryptedDir)
	{	usage ("BLOCKED: Output directory exists; will not delete it (or use --clobber to override)");	}
	
if ($ClobberOutput_F ==1 && -e $OutputEncryptedDir)
	{
	if (-e $TIMESTAMPFILE)	#Ok, we recognise this directory as one of ours!
		{
		print "# OK - deleting output directory (file '$TIMESTAMPFILE' exists - so it seems a directory created by a previous instance\n";
		remove_tree ($OutputEncryptedDir) or die "Could not delete output directory '$OutputEncryptedDir' when I tried:$@\n";
		}
		else
		{
#		print "Will not delete output directory '$OutputDir' --clobber supplied, but I didn't find the file '$TIMESTAMPFILE'\n";
		usage ("BLOCKED: Will not delete output directory '$OutputDir' as I couldn't find the '$TIMESTAMPFILE' (timestamp file)");
		}
	}	

$JSON_GPG_Struct{"Files"}{"TimeStamp"}{"Path"} 				= $TIMESTAMPFILE;		#JSON Load
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

#Test Keys:
my $GPGKeyString="";
 
print "Testing gpg (in general terms):\n";
unless (`gpg --version`                         =~ m/^gpg \(GnuPG\) /)  {       die "Couldn't find GPG\n";      }
foreach my $C_Key (@KEYS)
        {
        print "Testing gpg keyring (for '$C_Key'):\n";
        unless (`gpg --list-keys`       =~ m/$C_Key/ or `gpg --list-secret-keys`        =~ m/$C_Key/)                   
                {       die "Couldn't find the Key '$C_Key' in the keyring\n";  }
        $GPGKeyString=$GPGKeyString. " -r $C_Key";              
        }

print "For GPG I will use these keys: '$GPGKeyString'\n";
$JSON_GPG_Struct{"GPG Keys"} 	= $GPGKeyString;	#JSON Load

print "# Pre-Flight Checks complete: GO Decision!\n";
#die "HIT BLOCK\n";

=head2 Build output base directory, set paths and set control files

The scripts, gpg results directory inside the base output directory $OutputDir
The actual file tree of GPG'd files goes in the $OutputEncryptedDir (i.e. a sub directory of $OutputDir)

=cut 

#The general output directory:

unless (-e $OutputDir)
	{
	print "# Building Output Directory: '$OutputDir'\n";
	make_path ($OutputDir) or die "Tried to build Output Directory: '$OutputDir'\n";
	}
	else
	{	print "# Output Directory already exists\n";	}

# The encrypted scripts directory (likely this exists from the MD5 sum calculation)
unless (-e $OutputEncryptedDir)
	{
	print "# Building Output Directory: '$OutputEncryptedDir'\n";
	make_path ($OutputEncryptedDir) or die "Tried to build Output Directory: '$OutputEncryptedDir'\n";
	}
	else
	{	print "# Output Directory already exists\n";	}


my $GPGScriptDir= "$OutputDir/scripts";				#Where we put the GPG files, script and 'instruction file'
unless (-e $GPGScriptDir && -d $GPGScriptDir)
	{	make_path ($GPGScriptDir)	or die "Can't create my working directory for GPG: '$GPGScriptDir'\n";		}

my $GPGInstructionFile 		= "$GPGScriptDir/encrypter_instruction.tab";	#The 'instruction' file for the GPG Array Job
my $GPGScriptFile			= "$GPGScriptDir/gpg.pl";		#The bash file containing the GPG Array Job



#The files we couldn't find to GPG (i.e. were missing):
my $MissingFiles 		= "$OutputDir/MissingForGPG.lst";
print "# If I find files to be missing when we try to GPG them, they will be noted in '$MissingFiles'\n";

#This will fail (implicity) if the directory $GPGScriptDir doesn't exist either (but it should by now):
if (-e $GPGInstructionFile && $ClobberOutput_F ==0 )
	{	die "Output Directory already contains the GPG Instruction file '$GPGInstructionFile' and --clobber was not used; will not overwrite\n";	}

#Load a group of things into the JSON structure:
$JSON_GPG_Struct{"Paths"}{"Script Dir"} 		= $GPGScriptDir;	#JSON Load
$JSON_GPG_Struct{"Files"}{"Missing Files"} 		= $MissingFiles;		#JSON Load
$JSON_GPG_Struct{"Files"}{"GPG Instructions"} 		= $GPGInstructionFile;		#JSON Load
$JSON_GPG_Struct{"Files"}{"GPG Script"} 		= $GPGScriptFile;		#JSON Load

=head2 Do timestamp

=cut

#Get the timestamp:
my $TIMESTART = `date`;chomp ($TIMESTART);
#Write it out to disk:
print "# Marking out territory with a timestamp file:\n# \t$TIMESTART -> $TIMESTAMPFILE\n";
open TIMESTAMP , ">$TIMESTAMPFILE" or die "Cannot open timestamp file for writing: '$TIMESTAMPFILE'\n"; 
print TIMESTAMP $TIMESTART,"\n"; close TIMESTAMP;
#die "HIT BLOCK\n";

=head2 Open the files to put lists in - and build the paths if they don't (already exist):

=cut


#Open the list of files: to survey their paths and note down the real files that (still) exist

open INPUTLIST, "$InputList"	or die "Cannot open '$InputList'\n";
#Also those that are missing (there shouldn't be many of these...but note them here)
open MISSINGFILES, ">$MissingFiles" or die "Cannot open '$MissingFiles' (missing input files)\n;";

=head2 Start the survey / check the file list supplied:

We run through the list of files supplied.  All we care about is that the file exists - and it is 'real' file we can GPG.

We assume the MD5'ing / filtering for nulls has already been done.

The 'shortest common path' is decribed at:

 http://rosettacode.org/wiki/Find_common_directory_path#Perl

print common_prefix('/', @paths), "\n";

=cut

my $FileCount = 0;	#The total files we will survey (less headers, blank lines etc - but there shouldn't be any)
my %MissingFilesPaths = ();	#The failures; the differnce to $FileCount is the successes
my $NMissingFiles = 0;
my @FilesToEncrypt;
my %Directories; 	#We keep this as a hash so we can 'unique' it easily very quickly using 'keys'
my @DirectoriesToCreate;	#The processed version of the paths we will pass to make_path ();
my $TotalSizesofInputFiles = 0;		#So we can run a disk check; prehaps give an idea of the time to process 

my $InputListABSPath= File::Spec->rel2abs($InputList);

print "# Reading list from: $InputList (aka. $InputListABSPath)\n";
print "# Printing out 1 in every $ExFrequency objects (files, symlinks; 0= none)\n";

my $StartTime= time;
while (<INPUTLIST>)		# $_ will be set to the file names as they are presented to us
	{
	$FileCount++;
	chomp ();								# Remove new lines; just mess up file processing 
	if (/^\s*$/ or /^#/)	
		{	next;	}	# Skip blank lines
	#
	chomp ($_);
	#Match lines such as this:
#(MD5						)    spaces (path) 															\t(size)	
#5c650eda6453051a1a4621454bfc4c1e  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/test_dir.struct.tar         23500800
#2ef63ba2438d71a0ee499d86d1684772  /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir/real_fs/B/7.file    2097152
	#split up the incoming line:
	my ($MD5, $FilePath, $ReportedFS)	= $_ =~ m/^([a-z0-9]{32})[\s,\t]+(.*?)[\s,\t]+(\d+?)$/;
#	print "D: Split is: '$MD5', '$FilePath', '$ReportedFS'\n";
	#Run a parse/reality check on the filesize: if we got the filesize, then likely the other two items parsed correctly too:
	unless (defined $ReportedFS && $ReportedFS ne "")
		{	warn "Unable to parse this line of the input File (should be: MD5 Path filesize)\n"; next;}
	#(re)-check the filesize on disk: should be the same:
	my ($FileSizeObservedNow) = -s $FilePath;
#	print "D: file is '$FileSize'\n";
	unless ($ReportedFS == $FileSizeObservedNow)		#Does the file exist, is it non-blank?	
		{		
		$NMissingFiles  ++;				#A simple count
		$MissingFilesPaths{$FilePath} = $FileCount;		#An actual record and store the line number in the file.
		print MISSINGFILES $_, " \t at line $FileCount\n";	#Note the misisng ones permanently
		warn "$FileCount : Difference in filesizes: $FileSizeObservedNow was $ReportedFS for file: '$FilePath'\n";
		next;	#Skip to next file
		}
	#So, as the index file says...we will process this file 
	#Add on the filesize count:
	$TotalSizesofInputFiles += -s ($FilePath);
#Ok - real file:
	#Note the location:
	push @FilesToEncrypt, $FilePath;
	#Store the directory:
	my (undef, $Dir) = fileparse ($FilePath);
#Note the directory, if we haven't seen it before:
	if (defined ($Dir) && $Dir ne "" 	# Check the Dir is parsable - if not, something slipped through our other tests. 
	&& not exists ($Directories{$Dir}))	# If it new to us then it is interesting 	
		{	
#		my (undef, $Path) = (File::Spec->splitpath ($_));	#Get the proper directory part
		#Note the path and the number of the directories:
		$Directories {$Dir}=scalar (File::Spec->splitdir ($Dir));	
		}
	}
close INPUTLIST;

#Deduce various statistics:

my $EndTime = time - $StartTime;
my $NFilesFound = $FileCount-$MissingFiles;

print "# Found $NFilesFound files in $EndTime s\n";
print "# Total size of all files found: $TotalSizesofInputFiles (bytes)\n";
if ($NMissingFiles !=0)
	{	print "# Found $NMissingFiles missing files; recorded these in '$MissingFiles' - and the line numbers in the original file\n";	}
else
	{	print "# Found 0 missing files\n";	}

$JSON_GPG_Struct{"Counts"}{"Missing Files"} 		= $NMissingFiles; #JSON Load
$JSON_GPG_Struct{"Counts"}{"Files to Encrypt"}		= scalar (@FilesToEncrypt); #JSON Load
$JSON_GPG_Struct{"Counts"}{"Directories"}			= keys %Directories;
$JSON_GPG_Struct{"Size of Files"}					= $TotalSizesofInputFiles; #JSON Load
 
=head3 Now we can do our disk space check:

For this we call the 'df' tool and ask for the value in bytes, 

=cut 
if ($MINDISKSPACEFRACTION != 0)
	{
	my @DiskSpaceRes = `df $OutputDir`;
	my $DiskSpaceRes = $DiskSpaceRes[-1];
	my ($DiskSpaceAvailable) = $DiskSpaceRes =~ m/(\d+?) +\d+?\%/;
	print "D: Diskspace: '$DiskSpaceAvailable'\n";
#
	unless (defined $DiskSpaceAvailable && $DiskSpaceAvailable =~/^\d+$/)
		{	warn "Can't get disk space for volume of '$OutputDir: strange, but not fatal\n";	}
	
	my $DiskSpaceNeeded = int ($TotalSizesofInputFiles * $MINDISKSPACEFRACTION + $TotalSizesofInputFiles);		#i.e. assume the size will double and add on a bit ($MINDISKSPACEFRACTION)

	if ($DiskSpaceAvailable < $DiskSpaceNeeded)
		{	usage ("Out of disk error: will not run... ($DiskSpaceAvailable actual; cf required $DiskSpaceNeeded {bytes}");	}
	else
		{print "#: Storage space check passed: $DiskSpaceNeeded (needed), $DiskSpaceAvailable (at output location); safety factor of '$MINDISKSPACEFRACTION' specified\n"}
#
	}
else
	{	print "#: Disk space check deactivated by '--space=0' parameter\n";	}
#print "# PASSED: disk space available ($DiskSpace KB)\n"; 


 
=head3 Work through the directories, adjusting the paths as needed

=cut 


print "# A reminder: output path set to: '$OutputDir'\n";

#Should we bother deducing the common base path of the files we will process?
my $CommonPath;
unless ($NoStripPath!=0)	#Run if we are going to use the result; semi computationally intensive
	{
	$CommonPath=common_prefix ('/',keys %Directories);
	print "# --NostripPath is *not* in operation, hence: shortest common path = '$CommonPath'\n";
	}
$JSON_GPG_Struct{"Paths"}{"Shortest Common Path"} 				= $CommonPath;			#JSON Load
my $NDirs= keys %Directories;

$JSON_GPG_Struct{"Counts"}{"Directories to Create"}			=	$NDirs;			#JSON Load

print "# A maximum of $NDirs directories to process (adjust path + create)\n";
#print "#: Directories in order of depth:\n";
foreach my $C_Dir (sort  { $Directories{$b} <=> $Directories{$a} } keys %Directories)
	{
#	print "D: Initially: '$C_Dir'\n";
	my $Proposed_Out_Dir = $C_Dir; #Ultimately, where we will ask GPG to write files to	
		unless ($NoStripPath!=0)
		{	
			$Proposed_Out_Dir =~ s/$CommonPath//;	#Strip off the 'common directory'
#			print "D: After path substitution: '$Proposed_Out_Dir'\n";	
		}
	
	my $C_Out_Dir = $OutputEncryptedDir.$Proposed_Out_Dir;
	#print "D: Final directory needs creating: '$C_Out_Dir' , initially: '$C_Dir' or as ABS Path", File::Spec ->rel2abs($C_Out_Dir),"'\n";
	unless (-e $C_Out_Dir)
		{
#		print "D: Directory needs creating: '$C_Out_Dir'; as ABS path: ", File::Spec->rel2abs($C_Out_Dir)," - so noting\n";
		push @DirectoriesToCreate, $C_Out_Dir;
		}
	delete $Directories{$C_Dir};		#We can do this as keys %Directories already has returned its list; but recover the memory		
	}

=head3 Actually create the directories

=cut

my $RemainingDirectories = keys %Directories;		#Count them, for test / reporting purposes
unless ($RemainingDirectories ==0)	{die "Remaining directories after checking through them and deleting keys: STOPPING\n";	}
my $NDirsToCreate = scalar @DirectoriesToCreate;
print "#: Remaining in the list of Directories to survey: $RemainingDirectories (should be 0); I found $NDirsToCreate new directories need creating using make_path\n";

#die "HIT BLOCK\n";
foreach my $C_DirToCreate (@DirectoriesToCreate)
	{	
	#print "D: Directory to create: '$C_DirToCreate'\n";
	if (-e $C_DirToCreate)		{	next;	}	#It really shouldn't already exist given the screening above, but skip it if so			
	make_path ($C_DirToCreate) or die "Cannot create directory : '$C_DirToCreate'\n";	}


=head2 Prepare the Instruction List

Here we assume that all the files are valid, and still present etc. 

=cut

#Now actually process the list of files
$OutputDir =~ s/\/$//;
open INSTRUCTIONLIST, ">$GPGInstructionFile" or die "Could not open the 'instruction file' through which GPG gets passed paths\n";
foreach my $C_File (@FilesToEncrypt)
	{	
	
	my($filename, $Dir, $suffix) = fileparse ($C_File);
	
		$Dir =~ s/\/$//;
#		print "D: Processing: '$C_File' in '$Dir'\n";
		my $Proposed_Out_Dir = $Dir; #Ultimately, where we will ask GPG to write files to	
		unless ($NoStripPath!=0)
		{	
			$Proposed_Out_Dir =~ s/$CommonPath//;	#Strip off the 'common directory'
#			print "D: After path substitution: '$Proposed_Out_Dir'\n";	
		}
	
	my $C_Out_Dir = $OutputEncryptedDir.$Proposed_Out_Dir;	#Build the path:
	
	$C_Out_Dir = File::Spec->rel2abs($C_Out_Dir);
	my $C_OutputFile = $C_Out_Dir."/$filename\.gpg";
	#print "D: Would create output file as:'$C_OutputFile'\n";
	my $OutputLine = "$C_File\t$C_OutputFile\n";
	#Enable this next line to print the data back to STDOUT:
	#print "D: $OutputLine"; 
	print INSTRUCTIONLIST "$OutputLine";
	}
close INSTRUCTIONLIST;

=head2 Prepare the GPG Command script

gpg  --trust-model always -e -r "BC3E454B" -r "E16641B3" -r "1C1742CB"

=cut

#Convert to Abs paths prior to substituting:

$GPGInstructionFile 	= File::Spec->rel2abs($GPGInstructionFile);
$GPGScriptDir	= File::Spec->rel2abs($GPGScriptDir);

#Reality check: are we able to process what we have been asked?
if ($NFilesFound >= $MAX_JOB_LIMT * $MAXNODES)
        {       die     "Even with $MAX_JOB_LIMT jobs per node, this is still too many files ($NFilesFound)!  (Max job number: $MAX_JOB_LIMT)\n";    }

my $NBlocks = $NFilesFound < $MAXNODES ? 1 : $MAXNODES; 
my $BlockSize        = ceil ($NFilesFound/$NBlocks);
print "#: GPG Array script will use $NBlocks of size $BlockSize\n";       

my $GPGCommand = $GPGBaseScript;
#Substitute in the values needed (i.e. 'Fill in the form'
$GPGCommand =~ s/NBLOCK_TAG/$NBlocks/;   
$GPGCommand =~ s/KEYS_TAG/$GPGKeyString/;
$GPGCommand =~ s/GPG_TIME_TAG/$StartTime/;
$GPGCommand =~ s/FILELIST_TAG/$GPGInstructionFile/;
$GPGCommand =~ s/WD_TAG/$GPGScriptDir/;
$GPGCommand =~s /BLOCKSIZE_TAG/$BlockSize/;
open GPGFILE ,">$GPGScriptFile"	or die "Cannot open GPG / SGE Bash file: '$GPGScriptFile'\n";

my ($JobName) = $GPGCommand =~ m/-N (GPG_Tape_.*?)\n/;
print "#: GPG Array script:\nD: written to '$GPGScriptFile'\n";
print "#: Jobname is '$JobName'\n";

$JSON_GPG_Struct{"GPG Job"}{"Job Name"}		= $JobName;

#print "'$GPGCommand'\n";
print GPGFILE $GPGCommand;
close GPGFILE;
`chmod a+x $GPGScriptFile`;	#Make it executable

#die "HIT BLOCK\n";

=head2 Launch GPG jobs:

 Stunning anti-climatic really... 

=cut 
my $GPGLaunchResult = `qsub -q spbcrypto $GPGScriptFile`;
print "#: GPG Launch result was: '$GPGLaunchResult'\n# Waiting 2s before qstat\n";


sleep (2);
print "#\n# Result of qstat:\n# \n";
my $QStatResult = `qstat -q spbcrypto`;
$QStatResult=~ s/[\n\s]+$//g;
$QStatResult  =~ s/[\r\n]/\n#:  /g;
print "#:   $QStatResult#\n#\n";

#$JSON_GPG_Struct{"GPG Job"}{"Launch Result"}		= $GPGLaunchResult;
#
# Launch Collector script
#
######

my $GPGCollectorScript = $GPGCollectorScriptBase;
my $GPGLogFile         = $OutputDir.$DEFAULTGPGLOGLOCATION;
my $time = time;

#Substitute these in:
$GPGCollectorScript =~s/WD_TAG/$OutputEncryptedDir/g;
$GPGCollectorScript =~s/GPGTALLY_TAG/$GPGLogFile/g;

my $GPGCollectorScriptFile = "$GPGScriptDir/CollectorGPG.pl";
open COLLECTORGPG, ">$GPGCollectorScriptFile" or die "Cannot open '$GPGCollectorScriptFile'\n";
print COLLECTORGPG $GPGCollectorScript;
close COLLECTORGPG;
`chmod +x $GPGCollectorScriptFile`;
print "D: Written out '$GPGCollectorScriptFile'\n";

#die "HIT BLOCK!\n";

#Build the QSub command:
#Ultimately add back in: -o /dev/null -e /dev/null 
my $GPGCollectorCommand = "qsub -q spbcrypto -hold_jid $JobName -N GPG_COL_$time $GPGCollectorScriptFile"; 
print "D: Command is $GPGCollectorCommand\n";
print "#:   Launching Collector Job#\n#\n";
`$GPGCollectorCommand`;


=head2 JSON Report Output

If we get to this point then the program has run sufficiently well to produce output, so we write it out into the JSON Report.

=cut

my $JSON_Report_Text= JSON->new->utf8->encode(\%JSON_GPG_Struct);
open JSONOUT, ">$GPGJSONFile"	or die "Cannot open '$GPGJSONFile'\n";
say JSONOUT $JSON_Report_Text;
close JSONOUT;	


# Mini-Sub Routine to print usage:
sub usage {
my $Message = shift @_;
unless (defined $Message && $Message ne "")	{$Message =" ";}
die "Usage: ./encrypter.pl  <List of Files> <Output Path> : $Message\n";
}
#
=head2 common_prefix (\@Array): find the shortest 'common path' 

Pass it a reference of arrays and it returns the shortest common 'root path' 

The implementation is taken from: 

 http://rosettacode.org/wiki/Find_common_directory_path#Perl

=cut

sub common_prefix {
    my $sep = shift;
    my $paths = join "\0", map { $_.$sep } @_;
    $paths =~ /^ ( [^\0]* ) $sep [^\0]* (?: \0 \1 $sep [^\0]* )* $/sx;
    return $1;
}
###End - Mini-Sub



