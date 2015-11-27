#!/usr/bin/perl
=head1 decrypter.pl - modified checker.pl script

When supplied with a directory this program checks that the directory contains the encrypted files listed in the 
indexes (of real files) in it. After that, it decrypts to a specified output dir with optional chcking against md5sums


=head2 Method of operation

Essentially this runs through the index file and decrpypts the corresponding files it finds; pipes the result through MD5Sum 
and compares the result of the unencrypted file to that recorded in the index. Saves into output dir

=head3 Currently it does not (explictly) check for the presence of the other index files or that the 'encrypted' directory
is contaminated with non-encrypted files (i.e. you can add things in and it will not baulk).

=head2 Module loading:

To run this on the (aged) systems of the cluster try loading these modules to get access to the (Perl) moduels:

 module load perl/5.20.1
 module load spb-perl-pipe/dev

=cut


use strict;
use JSON;	#Helps with reporting various quantities
use constant DEBUG=>0;

#We use a lot of File:: modules here ;-)
use File::Basename;							#Manipulate paths 1
use File::Path qw(make_path remove_tree);	#Manipulate paths 2
use File::Spec;								#Manipulate paths 3
use Cwd 'abs_path';						#Recurse paths back to their 'source'
use Cwd;								#Get the Current Working Directory 
use Getopt::Long;			#To process the switches / options:
use POSIX qw(ceil);
use 5.10.0;	# 



=head3 Defaults and constants:

=cut
#If supplied with a directory we assume this file is meant if we aren't pointed at one specifcally:
#This is the location of the 'instrcutions' file for the GPG Job:
my $INSTRUCTIONSFILE 			        = "decrypter_instruction.tab";
my $ENCRYPTINSTRUCTIONS                         = "checker_instruction.tab";
my $MAX_JOB_LIMT = 75000;
my $MAXNODES     = 100;
my @KEYS = "4019DEF8";		#We need to have the private / secret key to decrypt the data


#This we will try get from the JSON file typically, but the --GPGEnc parameter has precedence over it:
my $BaseDirForEncFiles	="";
my $BaseDirForDecFiles  ="";

#Are we safe to run without SGE access? (won't attempt decryption )  
my $SkipSGECheck =0;

=head2 The 'usage ()' function

=cut

########
sub usage {
my ($Message) = @_;
unless (defined $Message && $Message ne "")
	{$Message ="-";}

die "Usage decrypter.pl <Directory> <OUTdir>\n$Message\n";
}
#########


=head3 These tags get substituted

But check the real code for this.

 my $JobName = "DCRYP_".time;
 $DecryptBaseScript =~ s/NFILES_TAG/$FilesProcessed/g;
 $DecryptBaseScript =~ s/JOBNAME_TAG/$JobName/g;
 $DecryptBaseScript =~ s/FILELIST_TAG/$FileListMD5s/g;
 $DecryptBaseScript =~ s/RESULTSOUTDIR_TAG/$DirToSurvey/g;		
 $DecryptBaseScript =~ s/MD5OUT_TAG/$GPGScriptDir/g;

=cut 


my $DecryptBaseScript = <<'DCRYPT_SCRIPT';
#!/usr/bin/perl
# : Decrypt all files supplied
#$ -t 1-NBLOCK_TAG
#$ -cwd
#$ -o /dev/null
#$ -e /dev/null
#$ -S /usr/bin/perl

#$ -N JOBNAME_TAG

use strict;
my $JobID     = "JOBNAME_TAG";	   #Because we need this
my $Block=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub
my $BlockSize = "BLOCKSIZE_TAG";

my $FileList   = "FILELIST_TAG";
open INPUTFILE , "$FileList" or die "Cannot open list of files '$FileList' containing MD5s\n";

#Hence, calculate the actual line numbers between which we will process:
my $WantedLine_Start = ($Block-1) * $BlockSize;
my $WantedLine_End = ($Block) * $BlockSize;
my @LinesToProcess;

my $Count=0;
while (<INPUTFILE>) {
        $Count++;  #Increment the line counter
        if ($Count <= $WantedLine_Start)        {       next;   }       #Skip until the lines we want:
        if ($Count > $WantedLine_End )                  {               last;   }

        #Below here only processed for wanted lines: 
        my ($FilePath, $OutPath)  = m/^(.+?)[\t\s]+(.+?)$/;
        print "D: '$FilePath' to '$OutPath'\n";
        push @LinesToProcess, [$FilePath,$OutPath];
}
close INPUTFILE;

foreach my $FileData (@LinesToProcess) {
        my ($FilePath, $OutPath) = ($$FileData[0], $$FileData[1] );

        $FilePath =~ s/ /\\ /g; # Escape spaces
        $FilePath =~ s/([;<>)(}{|%@?\$])/\\$1/g; # Escape special characters
	$OutPath =~ s/ /\\ /g; # Escape spaces
        $OutPath =~ s/([;<>)(}{|%@?\$])/\\$1/g;  # Escape special characters
        
        my $GPGCommand = "gpg --no-random-seed-file --decrypt $FilePath > $OutPath";
        $FilePath =~ s/\\//g;   # Un-escape
        $OutPath  =~ s/\\//g;   # Un-escape
        print "D: '$GPGCommand'\n"; 
	my $GPGResult = `$GPGCommand`;
	print "D: '$GPGResult'";
		
        }
DCRYPT_SCRIPT


=head3 Process (the rest of) the command line options

=cut
my $GPGJSON; #To store the results of the GPG encrpytion process 
my $MD5summedIndex; #Custom location of MD5 Summed list of files (SGEcaller.pl makes Files.index, indexer.pl makes Files.md5.RF
my $outDir;
my $CHECKMD5;

GetOptions (
	"EncDir|encryptedDir|basedir|outdir=s" => \$BaseDirForEncFiles,
	"noSGEOK|nogrid" => \$SkipSGECheck,
	"JSON|JSONOUT|JSONGPG=s"	=> \$GPGJSON,	#	= The JSON file containing details of the programming running
 )
	or usage ("Error in command line arguments\n");

# Build the file name containing the list of files to survey in a very specific way:

#TO DO: Allow multiple input files / directories...maybe if time allows:
my @ItemsPassed;

my $Time = time;

#Get the directory (we will need this no matter what else):
my $DirToSurvey = shift @ARGV;
my $DirToWrite  = shift @ARGV;
$DirToSurvey =~ s/\/$//;	#strip trailing slash off:
$DirToWrite =~ s/\/$//;        #strip trailing slash off:

unless (defined ($DirToSurvey) && -d $DirToSurvey) {	usage ("No Directory to survey supplied\n");	}
unless (-d $DirToSurvey."/encrypted") { usage ("Directory does not have encrypted folder\n");    }
         
#Convert to absolute path:
$DirToSurvey = abs_path($DirToSurvey);
$DirToWrite  = abs_path($DirToWrite);

#Now assumed (boldly!) that these files exist; if everything is correct they should
#and if not we are going to fail approval of this directory anyway 

#Check these exist:

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


#This is a convenience and eases variable interpolation: 
my $DecryptScript 		= "$DirToSurvey/scripts/decryptScript.pl";	
my $DecryptInstructionsTabFile 	= "$DirToSurvey/scripts/$INSTRUCTIONSFILE";

print "Decrypter script: 			'$DecryptScript'\n";
print "Instructions file:			'$DecryptInstructionsTabFile'\n";

#Ultimately this is the location of our JSON file
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

print "D: BaseDir for encrypted files:'$BaseDirForEncFiles'\n" if DEBUG;
#die "HIT BLOCK\n";
open INSTRUCTTAB, ">$DecryptInstructionsTabFile" or die "Cannot create instruction file for GPG Decrypt: '$DecryptInstructionsTabFile'\n";
open ENCFILES, $DirToSurvey."/scripts/".$ENCRYPTINSTRUCTIONS or die "Couldn't find checker instructions, was this processed with checker.pl ?";

while (<ENCFILES>)
	{
	$FilesProcessed++;
        my ($Input, $File) = m/^([a-z0-9]{32})[\s\t]+(.+?gpg)$/;;
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

        # We need to check if it is now in the original directory, if not - adjust the path using $DirToSurvey
        $File =~s!.*encrypted!$DirToSurvey/encrypted!;
        if (! -e $File || $File!~/.gpg$/ || $File!~/encrypted/) {  #skip
          print "D: File: $File\tDoes not exist or does not have gpg extension, skipping\n" if DEBUG;
          next;
        }
       
        my $OutFile = $DirToWrite."/decrypted/".$1 if ($File =~m!encrypted/(.+)$!);
        $OutFile =~s/.gpg$//;
        my $outdir = dirname($OutFile);
        if (!-d $outdir) { make_path($outdir); }
        

	print "D: File: $FilesProcessed\t$File > $OutFile\n" if DEBUG;
	print INSTRUCTTAB "$File\t$OutFile\n";
	}
close MDFILES;
close INSTRUCTTAB;

#die "HIT BLOCK\n";
=head3 Prepare the SGE Script

=cut

 
=head3 Substitute into the SGE Jobs

Replace all these '_TAGs' in the code

=cut

#Reality check: are we able to process what we have been asked?
if ($FilesProcessed >= $MAX_JOB_LIMT * $MAXNODES)
        {       die     "Even with $MAX_JOB_LIMT jobs per node, this is still too many files ($FilesProcessed)!  (Max job number: $MAX_JOB_LIMT)\n";    }

my $NBlocks = $FilesProcessed < $MAXNODES ? 1 : $MAXNODES; 
my $BlockSize        = ceil ($FilesProcessed/$NBlocks);
print "#: GPG Array script will use $NBlocks of size $BlockSize\n";

my $JobName = "DCR_".$Time;
$DecryptBaseScript =~ s/NBLOCK_TAG/$NBlocks/g;
$DecryptBaseScript =~ s/JOBNAME_TAG/$JobName/g;
$DecryptBaseScript =~ s/FILELIST_TAG/$DecryptInstructionsTabFile/g;
$DecryptBaseScript =~ s/BLOCKSIZE_TAG/$BlockSize/;

print "D: Decrypt script:\n $DecryptBaseScript\n'\n written to: '$DecryptScript'\n" if DEBUG;

open SGESCRIPT, ">$DecryptScript" or die "Cannot open SGE Script '$DecryptScript'\n";
print SGESCRIPT $DecryptBaseScript;
close SGESCRIPT;

`chmod a+x $DecryptScript`;

if (! -d $DirToWrite."/decrypted/") {
 print "D: Decrypt dir does not exist, will make it\n";
 my $dirToMake = $DirToWrite."/decrypted/";
 `mkdir -p $dirToMake`;
} else {
  print STDERR "Directory $DirToWrite/decrypted/ exists, proceed anyway? (y/n}\n";
  my $answer = <STDIN>;
  if ($answer !~/^y/i) { die "Aborting...";}
}

#die "HIT BLOCK\n";

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
