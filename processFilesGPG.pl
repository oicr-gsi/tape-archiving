#!/usr/bin/perl
=head1 processFilesGPG.pl <list of files> <output location>


This script is designed to operate on 'clustEncrypt.pl' (and shares a common code history) output to 
create a GPG'd version of the file at the output location in a parallel directory structure.

When pointed at a 'Files' file created by clustEncrypt.pl - or other process that creates a 
similar list of real files - it spawns a GPG job on the SGE queue.


The program is designed to operate at the 10TB+ & 50 000+ file range so lists are written out to files rather than being held
in memory (for example).

The spb_crypto cluster queue is used by the shell scripts created. 

=head2 Usage

=head3 Simple usage:
 
 ./processFilesGPG.pl <Path to Survery> <Output Path> 

=head3 Under test  

A typical command line when testing would be:

 ./processFilesGPG.pl  --clobber --noSGEOK -d=1 ~/tickets/tapeArchiveSPB_2983/testDir/links/

 Overright output OK (--clobber); don't fail on no SGE (--noSGEOK) and give extra STDOUT printout for every file (-d=1)
 
=head4 Parameter Details

Formally there are these parameters:

	"clobber|c" => \$ClobberOutput_F,		= Overwright output Ok; default: not, skip when the script & instruction files / directories already exists
	"space|min_diskspace|diskspace=i",		= Minumim disk space to run (in TB); default: 100GB
	"diag|d|D=i" => \$ExFrequency,			= Diagnostic printout frequency; default: never, no diag output
	"noSGEOK|nogrid" => \$SkipSGECheck		= Skip the SGE Check & job launch - allows running on a non cluster machine; default: not, i.e. test & launch jobs
	"NoStripPath|nsp" =>                    = Do *Not* deduce the shortest common path from the input files supplied - and then remove this from the outputed GPG files
	"NoAutoIndexRemove|nai|nair"	=>		= Do *Not* remove the final directory 'index' when setting the default path 
	"help|h"								= Print out the command line options


NB: "NoStripPath" is helpful in controlling / pruning the overly deep directory trees that can result 'leaf-wards' from the output directory; it might 
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

If the files are already found to exist at the output location they are deleted if --clobber was used. 

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
use Getopt::Long;			#To process the switches / options:

=head2 Set some defaults & pseudo-constants

=cut 

my $ClobberOutput_F =0;
my $MINDISKSPACE = 10 * 1024 * 1024 * 1024 ;
my @KEYS = ("BC3E454B", "E16641B3", "1C1742CB");	#OICR Tape (pub only), OICR General (pub only), SPB 2014 ()Private Key) 
my $SkipSGECheck = 0;
my $ExFrequency = 0;	#the frequency of diagnostic printing; if enabled (set to 0 to disable) 
my $NoStripPath = 0;
my $NoAutoIndexRemove=0;
my $BasePath = "";
#my $OUTDIRTAG = "_out";

=head3 Create the skeleton SGE Script in this next section

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
        exit;           #Exit here is an optimisation as we don't care about the other lines in the file: another instance will process them
        }
 !!! REAL Code follows - alter that, not this !!!
 
=cut 

my $GPGBaseScript = <<'END_SCRIPT';
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
        exit;           #Exit here is an optimisation as we don't care about the other lines in the file: another instance will process them
        }
END_SCRIPT

#Mimic this:
#gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $TMPOUT/$FName.gpg -e $InputFile"

=head2 Start code proper

=head3 Get the options passed (if any), otherwise set their defaults
(see the real code for current defaults)

Can I clobber / remove output directories:
 my $ClobberOutput_F =0;

Minimum required diskspace (in TB)
 my $MINDISKSPACE = 0.1 * 1024 * 1024 * 1024 ;
	(i.e. 100GB - we are computing MD5s, not duplicating the data)

=cut

GetOptions (
	"clobber|c" => \$ClobberOutput_F,
	"space|min_diskspace|diskspace=i" => \$MINDISKSPACE,
	"diag|d|D=i" => \$ExFrequency,
	"noSGEOK|nogrid" => \$SkipSGECheck,
	"baseOutputPath|OutputDir|bp|outDir" => \$BasePath,		#Not needed (yet)
	"NostripPath|nsp" => \$NoStripPath,
	"NoAutoIndexRemove|nai|nair"	=>		\$NoAutoIndexRemove,
	"help|h"		=> \&options
 )
	or usage ("Error in command line arguments: run with --help to list\n");
#Reference to a subroutine to print the options:
sub options {
print <<'OPTIONS';
	"clobber|c" =>                          = Overwright output Ok; default: not, skip when the script & instruction files / directories already exists
	"space|min_diskspace|diskspace=i",      = Minumim disk space to run \(in TB\); default: 100GB
	"diag|d|D=i" =>                         = Diagnostic printout frequency; default: never, no diag output
	"noSGEOK|nogrid" =>                     = Skip the SGE Check & job launch - allows running on a non cluster machine; default: not, i.e. test & launch jobs
	"NoStripPath|nsp" =>                    = Do *Not* deduce the shortest common path from the input files supplied - and then remove this from the outputed GPG files
	"NoAutoIndexRemove|nai|nair"	=>		= Do *Not* remove the final directory 'index' when setting the default path 
	"help|h"                                = Print out the command line options
	
OPTIONS
usage ("Options as above");	#Now terminate with a polite error.
}

#print "D: clobber flag: $ClobberOutput_F \n";die;
#And the rest of parameters: 
my ($InputList, $OutputDir) = @ARGV;

#Is the list of files defined?
unless (defined $InputList && (-e $InputList && (-l $InputList or -f $InputList)))	
	{	usage ("Could not find input file containing files: '$InputList'");	}
print "# Target Directory Supplied: '$InputList'\n";

#Were we given an output directory?  If not build one from the input path and the CWD
unless (defined $OutputDir && $OutputDir ne "")	
	{
	my $WD= File::Spec->rel2abs(getcwd());
	#print "D: WD = '$WD'\n";
#Split the path and the Volumes apart; use Core modules to do it 
#	(doing it on the absolute path thanks to the CWD module helps)

	my (undef, $Directories) =
                       File::Spec->splitpath(abs_path($InputList));
    #We don't used the Volume ; leave these for the perl complier to optimise them away
    $OutputDir=$Directories;
    $OutputDir =~ s/\/$//;	#Remove the trailing slash

if ($NoAutoIndexRemove==0)	#Remove any trailing 'index' 
	{
    $OutputDir=~ s/index$//;
	$OutputDir =~ s/\/$//;	#Remove the trailing slash
	}
	#
	print "# (Setting output directory to a default of): '$OutputDir'\n";	
	}
print "# File List (read from) set to: '$InputList'\n";
print "# Output (write to) directory set to: '$OutputDir'\n";	

#Enable this last line if you want to just check the input / output path maniuplations: 
#die "HIT BLOCK\n";


=head2 Determine whether to delete output directory (or not or die)

As a safety check we require that we find a 'timestamp' file in the directory; just in case something goes wrong...

=cut

my $TIMESTAMPFILE= "$OutputDir/TimeStamp"; 

#if ($ClobberOutput_F == 0 && -e $OutputDir)
#	{	usage ("BLOCKED: Output directory exists; will not delete it (or use --clobber to override)");	}
#	
#if ($ClobberOutput_F ==1 && -e $OutputDir)
#	{
#	if (-e $TIMESTAMPFILE)	#Ok, we recognise this directory as one of ours!
#		{
#		print "# OK - deleting output directory (file '$TIMESTAMPFILE' exists - so it seems a directory created by a previous computeMD5s.pl instance\n";
#		remove_tree ($OutputDir) or die "Could not delete output directory '$OutputDir' when I tried:$@\n";
#		}
#		else
#		{
##		print "Will not delete output directory '$OutputDir' --clobber supplied, but I didn't find the file '$TIMESTAMPFILE'\n";
#		usage ("BLOCKED: Will not delete output directory '$OutputDir' as I couldn't find the '$TIMESTAMPFILE' (timestamp file)");
#		}
#	}	


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

#Test disk space:

#my @DiskSpaceRes = `df -k $OutputDir`;
#my $DiskSpaceRes = $DiskSpaceRes[-1];
#my ($DiskSpace) = $DiskSpaceRes =~ m/(\d+?) +\d+?\%/;
##print "D: Diskspace: '$DiskSpace'\n";
#
#unless (defined $DiskSpace && $DiskSpace =~/^\d+$/)
#	{	warn "Can't get disk space for volume of '$OutputDir: strange, but not fatal\n";	}
#if ($DiskSpace < $MINDISKSPACE)
#	{	usage ("Out of disk error: will not run... ($DiskSpace actual; cf required $MINDISKSPACE {kb}");	}
#
#print "# PASSED: disk space available ($DiskSpace KB)\n"; 
print "# Pre-Flight Checks complete: we are go\n# GO Decision!\n";
#die "HIT BLOCK\n";

=head2 Build output base directory, set paths and set control files

The scripts, gpg results directory inside the base output directory $OutputDir

=cut 

#The general output directory:


unless (-e $OutputDir)
	{
	print "# Building Output Directory: '$OutputDir'\n";
	make_path ($OutputDir) or die "Tried to build Output Directory: '$OutputDir'\n";
	}
	else
	{	print "# Output Directory already exists\n";	}

my $GPGWD= "$OutputDir/scripts_gpg";				#Where we put the GPG files, script and 'instruction file'
unless (-e $GPGWD && -d $GPGWD)
	{	make_path ($GPGWD)	or die "Can't create my working directory for GPG: '$GPGWD'\n";		}

my $GPGInstructionFile 	= "$GPGWD/gpg_data.tab";	#The 'instruction' file for the GPG Array Job
my $GPGBashFile			= "$GPGWD/gpg.pl";		#The bash file containing the GPG Array Job

#The files we couldn't find to GPG (i.e. were missing):
my $MissingFiles 		= "$OutputDir/MissingForGPG.lst";
print "# Hence: Files found to be missing when we try to GPG them will be noted in '$MissingFiles'\n";


#This will fail (implicity) if the directory $GPGWD doesn't exist either (but it should by now):
if (-e $GPGInstructionFile && $ClobberOutput_F ==0 )
	{	die "Output Directory already contains the GPG Instruction file '$GPGInstructionFile' and --clobber was not used; will not overwrite\n";	}

=head3 Build the output directories: 


=cut 
#Get the timestamp:
my $TIMESTART = `date`;chomp ($TIMESTART);
#Write it out to disk:
print "# Marking out territory with a timestamp file:\n# \t$TIMESTART -> $TIMESTAMPFILE\n";
open TIMESTAMP , ">$TIMESTAMPFILE" or die "Cannot open timestamp file for writing: '$TIMESTAMPFILE'\n"; 
print TIMESTAMP $TIMESTART,"\n"; close TIMESTAMP;

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
		
	unless (-f $_ && -s $_ >=1)		#Does the file exist, is it non-blank?	
		{		
		$NMissingFiles  ++;				#A simple count
		$MissingFilesPaths{$_} = $FileCount;		#An actual record and store the line number in the file.
		print MISSINGFILES $_, " \t at line $FileCount\n";	#Note the misisng ones permanently
		next;
		}
#Ok - real file:
	#Note the location:
	push @FilesToEncrypt, $_;
	#Store the directory:
	my (undef, $Dir) = fileparse ($_);
#Note the directory, if we haven't seen it before:
 
	if (defined ($Dir) && $Dir ne "" 	# Check the Dir is parsable - if not, something slipped through our other tests. 
	&& not exists ($Directories{$_}))	# If it new to us then it is interesting 	
		{	
		my (undef, $Path) = (File::Spec->splitpath ($_));	#Get the proper directory part
		#Note the path and the number of the directories:
		$Directories {$Dir}=scalar (File::Spec->splitdir ($Path));	
		}
	}
close INPUTLIST;

#Deduce various statistics:

my $EndTime = time - $StartTime;
my $NFilesFound = $FileCount-$MissingFiles;

print "# Found $NFilesFound files in $EndTime s\n";
if ($NMissingFiles !=0)
	{	print "# Found $NMissingFiles missing files; recorded these in '$MissingFiles' - and the line numbers in the original file\n";	}
else
	{	print "# Found 0 missing files\n";	}


=head2 Work through the directories, adjusting the paths as needed

=cut 

print "# A reminder: output path set to: '$OutputDir'\n";
my $CommonPath="";	#if we need it (likely unless --nsp is in effect)

#Should we bother deducing the common base path of the files we will process?
 
unless ($NoStripPath!=0)	#Run if we are going to use the result; semi computationally intensive
	{
	$CommonPath=common_prefix ('/',keys %Directories);
	print "# --NostripPath is *not* in operation, hence: shortest common path = '$CommonPath'\n";
	}
	
my $NDirs= keys %Directories;

print "# A maximum of $NDirs directories to process (adjust path + create)\n";
print "#: Directories in order of depth:\n";
foreach my $C_Dir (sort  { $Directories{$b} <=> $Directories{$a} } keys %Directories)
	{
#	print "D: Initially: '$C_Dir'\n";
	my $Proposed_Out_Dir = $C_Dir; #Ultimately, where we will ask GPG to write files to	
		unless ($NoStripPath!=0)
		{	
			$Proposed_Out_Dir =~ s/$CommonPath//;	#Strip off the 'common directory'
#			print "D: After path substitution: '$Proposed_Out_Dir'\n";	
		}
	
	my $C_Out_Dir = $OutputDir.$Proposed_Out_Dir;
	#print "D: Final directory needs creating: '$C_Out_Dir' , initially: '$C_Dir' or as ABS Path", File::Spec ->rel2abs($C_Out_Dir),"'\n";
	unless (-e $C_Out_Dir)
		{
		print "D: Directory needs creating: '$C_Out_Dir'; as ABS path: ", File::Spec->rel2abs($C_Out_Dir)," - so noting\n";
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
	
	my $C_Out_Dir = $OutputDir.$Proposed_Out_Dir;	#Build the path:
	
	$C_Out_Dir = File::Spec->rel2abs($C_Out_Dir);
	my $C_OutputFile = $C_Out_Dir."/$filename\.gpg";
	#print "D: Would create output file as:'$C_OutputFile'\n";
	my $OutputLine = "$C_File\t$C_OutputFile\n";
	print "D: $OutputLine"; 
	print INSTRUCTIONLIST "$OutputLine";
	}
close INSTRUCTIONLIST;

=head2 Prepare the GPG Command script

gpg  --trust-model always -e -r "BC3E454B" -r "E16641B3" -r "1C1742CB"

Script will be similar to this:


???Insert when it approaches completion???


=cut

#Convert to Abs paths prior to substituting:

$GPGInstructionFile 	= File::Spec->rel2abs($GPGInstructionFile);
$GPGWD	= File::Spec->rel2abs($GPGWD);


my $GPGCommand = $GPGBaseScript;
#Substitute in the values needed (i.e. 'Fill in the form'
$GPGCommand =~ s/NFILES_TAG/$NFilesFound/;
$GPGCommand =~ s/KEYS_TAG/$GPGKeyString/;
$GPGCommand =~ s/GPG_TIME_TAG/$StartTime/;
$GPGCommand =~ s/FILELIST_TAG/$GPGInstructionFile/;
$GPGCommand =~ s/WD_TAG/$GPGWD/;
open GPGFILE ,">$GPGBashFile"	or die "Cannot open GPG / SGE Bash file: '$GPGBashFile'\n";

print "D: In GPG Array script:\nD: written to '$GPGBashFile'\n";
print "'$GPGCommand'\n";
print GPGFILE $GPGCommand;
close GPGFILE;
`chmod a+x $GPGBashFile`;	#Make it executable

#die "HIT BLOCK\n";

=head2 Launch GPG jobs:

=cut 
my $GPGLaunchResult = `qsub -q spbcrypto $GPGBashFile`;
print "#: GPG Launch result was: '$GPGLaunchResult'\n# Waiting 2s before qstat\n";


sleep (2);
print "#\n# Result of qstat:\n# \n";
my $QStatResult = `qstat -q spbcrypto`;
$QStatResult=~ s/[\n\s]+$//g;
$QStatResult  =~ s/[\r\n]/\n#:  /g;
print "#:   $QStatResult#\n#\n";
#
#
#
######

# Mini-Sub Routine to print usage:
sub usage {
my $Message = shift @_;
unless (defined $Message && $Message ne "")	{$Message =" ";}
die "Usage: ./processFilesGPG.pl  <List of Files> <Output Path> : $Message\n";
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


