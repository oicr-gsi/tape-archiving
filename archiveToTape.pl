#!/usr/bin/perl

=head1 archiveToTape.pl

When supplied with a directory this program checks that it conforms to the necessary standards - and if it does - initiates the tape output script Brian Ott wrote.
(well, it would - but not during testing).

The program necessarily makes a whole set of assumptions and anything not matching these is rejected.

=head2 Checks done:

 a) Ensure no un-encrypted files are present in the output directory (ensures security).
 b) Check that some (or all) of the files decrypt to the original (i.e. we have at least the SPB key in place)
 c) Verify all files are present that should be: the original files + indexes.
 d) Filter out any temporary files present (should be none).

 See: https://jira.oicr.on.ca/browse/SEQPRODBIO-2983

=cut


use strict;
use 5.10.0;	# 
use JSON;	#Helps with reporting various quantities

#We use a lot of File:: modules here ;-)
use File::Basename;							#Manipulate paths 1
use File::Path qw(make_path remove_tree);	#Manipulate paths 2
use File::Spec;								#Manipulate paths 3
use Cwd 'abs_path';						#Recurse paths back to their 'source'
use Cwd;								#Get the Current Working Directory 
use Getopt::Long;			#To process the switches / options:


=head3 Defaults and constants:

=cut
#If supplied with a directory we assume this file is meant if we aren't pointed at one specifcally:
my $ASSUMEDLISTLOCATION = "index/Files.md5.RF"; 

#This we will try to guess, but if we can't the --GPGDir parameter will set this:

my $BaseDirForGPGFiles	="";

#Are we safe to run without SGE access? (won't attempt decryption )  
my $SkipSGECheck =0;

=head2 The 'usage ()' function

=cut



########
sub usage {
my ($Message) = @_;
unless (defined $Message && $Message ne "")
	{$Message ="-";}

die "Usage archiveToTape.pl <Directory>\n$Message\n";
}
#########

my $DecryptBaseScript = <<'MD5_SCRIPT';
#!/usr/bin/perl
# : Decrypt to MD5 sums for all files supplied
#$ -t 1-NFILES_TAG
#$ -cwd
#$ -S /usr/bin/perl

#$ -N MD5_Decrypt_TIME_TAG

my $WANTEDLINE=$ENV{"SGE_TASK_ID"};     #This is passed as an environment variable by SGE / Qsub
my $BaseDir = BASEDIR_TAG;
my $FileList = FILELIST_TAG;
open INPUTFILE , "FILELIST_TAG" or die "Cannot open list of files '$FileList' containing MD5s\n";

my $Count=0;
while (<INPUTFILE>)
        {
        $Count++;               #Increment the line counter
        unless ($Count == $WANTEDLINE)  {       next;   }       #Skip until the line we want:
#Below here only processed for wanted lines: 
        my ($MD5, $FilePath) =  split (/[\t\n]/,$_);
        
        my $EncryptedFilePath = $FilePath;
        #Magically translate one path into the other...
        ...somehow
		my $GPGCommand = "gpg --decrypt $FilePath | md5sum";
		my $GPGResult = `$GPGCommand`;
		unless ($GPGResult =~ m/$MD5/)
			{	
#Note the error:
			my $ErrorFile = "$BaseDir/scripts/$WANTEDLINE.encrypt-error";
			open OUTPUT, ">$ErrorFile" or die "Cannot open output file: '$ErrorFile'\n";
			print OUTPUT "$FilePath = $GPGResult\n";
			close OUTPUT;
			exit;           #Exit here is an optimisation as we don't care about the other lines in the file: another instance will process them
        	}
MD5_SCRIPT



=head2 Process CLI Options

=cut

=head3 Process (the rest of) the command line options

=cut
 
GetOptions (
	"GPGDir|encryptedDir|basedir|outdir=s" => \$BaseDirForGPGFiles,
	"noSGEOK|nogrid" => \$SkipSGECheck
 )
	or usage ("Error in command line arguments\n");

# Build the file name containing the list of files to sruvey in a very specific way:


#Get the directory (we will need this no matter what!):
my $DirToSurvey = shift @ARGV;
$DirToSurvey =~ s/\/$//;	#strip trailing slash off:

unless (defined ($DirToSurvey) && -d $DirToSurvey)
	{	usage ("No Directory to survey supplied\n");	}

#Convert to absolute path:

$DirToSurvey = abs_path($DirToSurvey);


my 		$FileListMD5s = "$DirToSurvey/index/Files.md5.RF";		#This (should) exist
my 		$DecryptScript = "$DirToSurvey/scripts/decryptTest.pl";	#This we will create

unless (-e $FileListMD5s && -r $FileListMD5s)	{usage ("Cannot open main list of files: '$FileListMD5s'\n");}
print "#: Found the list of files passed: '$FileListMD5s'\n";

print "D: and  : Decrypt script: '$DecryptScript'\n";

=head3 Do path manipulations - see if we guess where we are running

 my $ASSUMEDLISTLOCATION = "index/Files.md5.RF";

=cut

if ($FileListMD5s =~ m/$ASSUMEDLISTLOCATION$/ && $BaseDirForGPGFiles eq "")	#Does the file passed conform to what we expect (and we weren't told explictly)? 
	{
	print "#: Attempting to deduce the path to the GPG files - as I wasn't told explictly\n";
	print "#: Detected output as cannonical - ends as we expect it: '$ASSUMEDLISTLOCATION' - likely we can guess the location of the GPG Files\n";
	my $BaseDirForGPGFiles = $FileListMD5s;
	$BaseDirForGPGFiles =~ s/$ASSUMEDLISTLOCATION//;
	print "D: My current guess: '$BaseDirForGPGFiles'\n";
	if (-e $BaseDirForGPGFiles && (-d $BaseDirForGPGFiles or -l $BaseDirForGPGFiles))
		{	print "# Deductions complete got a directory at: '$BaseDirForGPGFiles' is base of GPG files I will assume\n";		}
		else
		{	print usage ("Cannot find a directory or symlink at '$BaseDirForGPGFiles', so aborting");	}
	}
	elsif ($BaseDirForGPGFiles ne "")
		{
		print "#: Testing '$BaseDirForGPGFiles' supplied by the --GPGBase parameter\n";
		if (-e $BaseDirForGPGFiles && (-d $BaseDirForGPGFiles or -l $BaseDirForGPGFiles))
			{	print "# Ok, path appears valid got a directory at: '$BaseDirForGPGFiles' is base of GPG files I will assume\n";		}	
		else
			{	usage ("Can't find the base directory supplied by --GPGBase: '$BaseDirForGPGFiles'");	}
		}
else
	{
	usage ("Can't deduce the directory containing the encrypted files; try supplying --GPGDir");
	}



=head2 First we survey the files we should have.

Here we do a very quick : is the file present - in its .gpg version and try  

=cut

$DecryptBaseScript =~ s/FILELIST_TAG/$FileListMD5s/;
$DecryptBaseScript =~ s/BASEDIR_TAG/$DirToSurvey/;
$DecryptBaseScript =~ s/MD5OUT_TAG/$DirToSurvey\/scripts/;

my $BaseDirtoFindGPGFiles = $DirToSurvey;
$DirToSurvey =~ 
die "HIT BLOCK\n";


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

print "# Pre-Flight Checks complete: we are go\n# GO Decision!\n";

my $FilesProcessed = 0;

my $FilesOK;
open MDFILES, $FileListMD5s;
while (<MDFILES>)
	{
	$FilesProcessed++;
	my ($MD5, $File) = split (/\s+/,$_);
	chomp ($File);
	print "D: File: $FilesProcessed\t$File\n";
	}
close MDFILES;
my $MD5Result =~ m/^[a-f0-9]{32} /;

#
#
#
###########
sub common_prefix {
    my $sep = shift;
    my $paths = join "\0", map { $_.$sep } @_;
    $paths =~ /^ ( [^\0]* ) $sep [^\0]* (?: \0 \1 $sep [^\0]* )* $/sx;
    return $1;
}