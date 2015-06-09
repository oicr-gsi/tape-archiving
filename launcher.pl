#!/usr/bin/perl

=head1 launcher.pl

 When pointed at a directory checks for a series of files including the 'APPROVED_TO_WRITE' one that is 
 the hallmark of 'checker.pl' program.

 launcher.pl [Dir to Send] --SubclientName MyBackup [--clobber and other options]

 Essentially it tries to produce commands such as this:

 qsub -q backups -N SomeCreativeName -S /bin/bash -b y "qscript_ndmp archive JellyBeansCureCancer /.mounts/labs/prod/backups/production/JellyBeans/experiment101"

 The 'Sub Client Name' i.e. the 'Tape Name' is based on name of the directory supplied (can be overridden by the --SubclientName parameter)
 Any results returned are written into the GENERAL_INDEX_DIR (can be overridden by the "--index" parameter)


=head2 NB: Currently this doesn't launch the 'output to tape' command while we are building it. 
  
=cut

=head2 Loading modules

 Technically these aren't required for this program due to any specific new feature, but they are for 
 the others in the package and so to keep things consistent.

 module load perl/5.20.1
 module load spb-perl-pipe/dev


=cut 

use File::Basename;			  # Manipulate paths 1
use File::Path qw(make_path remove_tree); # Manipulate paths 2
use File::Spec;				  # Manipulate paths 3
use Cwd 'abs_path';			  # Recurse paths back to their 'source'
use Getopt::Long;			  # To process the switches / options:
use FindBin qw($Bin);

use strict;

=head2 Set up Some Defaults and variables 

We will fill these in using Get::Options and the deductions we make 

=cut

my $SubclientName	= "";		# we will fill this in
my $MasterIndexLocation = "/.mounts/labs/PDE/data/tapeBackup/qscript_ndmp_logs";
my $SubclientLocationIndexPath	=	"";
my $REPORTURLLOCATION   = "http://ma3.hpc.oicr.on.ca/backups/";
my $Clobber	        = 0;		# Can we overwrite output?
my $SkipSGECheck        = 0;	        # Allow no SGE - we can't launch anything
my $TStamp_Human        = timestamp(); 
my $timestamp           = time;	        # Two versions of the timestamp 
my $ifsPath             = "";
#my $GENERAL_INDEX_DEFAULT = "/tickets/tapeArchiveSPB_2983/mainIndex";

GetOptions (
	"index|Index|index=s" 		=> \$MasterIndexLocation,
	"SubclientName=s" 		=> \$SubclientName,
	"clobber|c"			=> \$Clobber,
	"noSGEOK|nogrid"		=> \$SkipSGECheck,
 )
	or usage ("Error in command line arguments\n");

###
sub usage {
my ($Message) = @_;
unless (defined $Message && $Message ne "")
	{$Message ="-";}

die "Usage launcher.pl <Directory>\n$Message\n";
}
###
=head2 Pre-Flight Checks

=cut 

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



my $PathToArchive = shift @ARGV;

#A very simple check:
unless (defined $PathToArchive)
	{usage ("Need a directory to archive!\n");}

#Make sure we have a valid Path
if ($PathToArchive =~m!/.mount!) {
    $ifsPath = $PathToArchive;
    $ifsPath =~s!/.mounts!/ifs!;
} else {
    die "Path to Archive needs to start with /.mount, otherwise this CLI won't work!";
}

#We work in ABS paths:
$PathToArchive = File::Spec->rel2abs($PathToArchive);

#Can't process stuff that isn't there:
unless (-e $PathToArchive)
	{usage ("Directory supplied does not exist: '$PathToArchive'\n");}

#...or that hasn't been approved:	
my $OKtoWriteFile = "$PathToArchive/APPROVED_TO_WRITE";
unless (-e $OKtoWriteFile)
	{	usage ("Could not find the 'ok to write' file: '$OKtoWriteFile'\n");	}

#A priming step; run once...
unless (-e $MasterIndexLocation)
	{	
	print "#: Creating new master index - this should be a one time thing\n";
	make_path ($MasterIndexLocation) or die "Couldn't create the main index directory - that didn't exist\n";
	open TIMESTAMP,  "> $MasterIndexLocation/createTimestamp" or die "Couldn't create master index timestamp: '$MasterIndexLocation/createTimestamp'\n";
	print TIMESTAMP "$TStamp_Human \t $timestamp\n"; close TIMESTAMP;
	}
	
#Now this not existing is fatal:
#unless (-e $MasterIndexLocation)
#	{	usage ("Could not find the main index location: '$MasterIndexLocation'\n");		}

#We'll fill this in:
my $JobIndexLocation = "";

=head2 Deduce the SubClient Name

=cut 
#print "D: '$SubclientName' = $directories'\n";

#If we were told to look in one place, but call the job / output something else:                  
unless (defined $SubclientName && $SubclientName ne "")
	{
#	print "D: '$PathToArchive'\n";
	#This is not the best regex, but it works
	($SubclientName) = $PathToArchive =~ m/\/([^\/.]+)$/;
	print "#: Setting default subclient name: '$SubclientName'\n";	
	}
else
	{	print "#: Subclient name was set using --SubclientName parameter to '$SubclientName'\n";	}

=head2 Deduce the location of the output index:

=cut

$SubclientLocationIndexPath = $MasterIndexLocation."/".$SubclientName;
print "#: Index path is: $SubclientLocationIndexPath\n";
 
 
if (-e $SubclientLocationIndexPath) {
   if ( $Clobber == 0) { 
    #According to Brian this is a show-stopper: subclient names must be unique.
    die "Subclient directory already exists - jobs must be unique names; use --clobber to override\n";
    } else {
     remove_tree($SubclientLocationIndexPath);
    }
} 

#Ok, create the path:
make_path ($SubclientLocationIndexPath) or die "Cannot create: '$SubclientLocationIndexPath' (place for the index ultimately)\n";

=head2 Start building the main job command:

Basically we are wanting to emulate something like this:

 qsub -q backups -N GI_LNC_1423688529 -S /bin/bash 
 -o /u/mmoorhouse/tickets/tapeArchiveSPB_2983/mainIndex/testDir_op/STDOUT.txt -e /u/mmoorhouse/tickets/tapeArchiveSPB_2983/mainIndex/testDir_op/STDERR.txt 
 -b y "qscript_ndmp archive testDir_op /u/mmoorhouse/tickets/tapeArchiveSPB_2983/testDir_op

Also we substitute anything not alpha-numeric in the subclient name into underscores.


Quite how we are going to get back this information ultimately...but for now: 
assume from the STDOUT from the SGE job....and for testing the file '~mmoorhouse/tapeArchiveSPB_2983/example_Info.txt'
 
=cut

#Sanitise the subclient name - but keep an original copy: 
my $OriginalSubclientName = $SubclientName;
$SubclientName =~ s/[^A-Za-z0-9]/_/g; # Replace all non-alphanumericals with "_"

#Escape spaces in the path:
$PathToArchive =~ s/ /\\ /g;

#Pick a jobname based on the timestamp:

my $JobName ="GI_LNC_$timestamp";
#Also we need to construct the STDOUT+ STDERR files 


my $STDOUTFile = "$JobName.o"; 
my $STDERRFile = "$JobName.e";
my $LOGFILE    = "$SubclientLocationIndexPath/$JobName.log";

# Touch files, it seems to be needed
`touch $STDOUTFile $STDERRFile`;

my $QSubMainCommand = "qsub -cwd -q backups -N $JobName -S /bin/bash ".
	              "-o $STDOUTFile -e $STDERRFile".
	              " -b y \"qscript_ndmp archive $SubclientName $ifsPath\""; # >>$STDOUTFile\""; 

print "#: Launcher Qsub Command = '$QSubMainCommand'\n";

# Launch, actually

my $log_messages = `$QSubMainCommand`;
if ($log_messages) {
   open(LOG,">$LOGFILE") or die "Cannot write to logfile";
   print LOG $log_messages;
   print LOG "STDOUT:$Bin/$STDOUTFile\n";
   print LOG "STDERR:$Bin/$STDERRFile\n";
   close LOG;
}

=head2 Now build the 'waiter' command: 

 Currently NOT implemented, backup_reporter script will scan the log directory and parse data on regular basis 

=head3 This will parse a report such as this:

 Job ID: 310153

 Subclient: wouters_miRNA_1

 Date: 2014-12-17

 Directory: /.mounts/labs/prod/backups/production/projects/wouters_miRNA_encrypted

 Media: 006767
 006986



 ##############################################



 Directory Listing
 ...etc...
 
 The truly important parts are the 'Media' section and the 'Subclient' name.
 These have their information extracted using RegEx pattern matching.
 
 =cut 

This will hang around and wait for the other job $JobName to finish - and whatever Brian's 
script returns as output ultimately.

It does useful things such as associate the Jobname and Media IDs together and patch
the actual index into the master index location.

It also bulk copies the index files across to the new location and links to the 'master file'

=cut

#my $OrginalIndexLocation= "$PathToArchive/index/";
#my $FilesASL = "$PathToArchive/index/Files.md5.ASL";

#my $IndexLocationRF = "$SubclientLocationIndexPath/index";
#A very simple file that people can 'join' against:
#my $MediaUsedFile = "$SubclientLocationIndexPath/Media";

#open MEDIAFILE, ">$MediaUsedFile"	or die "Cannot open the media file in the index location: '$MediaUsedFile'\n";
#print MEDIAFILE 
#close MEDIAFILE;
#Add in: ???? curl ??? Maybe (egaTx) & sge (computMD5)tests: ????

##
#
#
###############
sub timestamp {
=head3 timestamp () = timestamp

When called converts Perl's incomprehensible timestamp into something more human readable
 
20150211 15:02:21 

=cut 
#Taken verbatim from: 
#http://stackoverflow.com/questions/12644322/how-to-write-the-current-timestamp-in-a-file-perl
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
    my $nice_timestamp = sprintf ( "%04d/%02d/%02d %02d:%02d:%02d",
                                   $year+1900,$mon+1,$mday,$hour,$min,$sec);
    return $nice_timestamp;

}
