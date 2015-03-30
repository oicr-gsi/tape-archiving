#!/usr/bin/perl

=head1 clustEncrypt.pl <From> <To> <OPT: SmallList.lst>

When called it will use create qsub jobs to do the encryption on the cluster.

It creates commands such as this:

 qsub -cwd -b y -N s$DonorID\_$rg -q production -e /u/mmoorhouse/tickets/tapeTests_2723/LargeEncrypt.log -o $BaseOutputPath.log "bash $BaseOutputPath.cmd" $

(actually this is done in two parts: the GPG command then the QSub)
 
In part this is good demonstration of the use of the cluster to do parallel computation / jobs.
Note that this program does not delete existing output files.  


=head2 Example of use:

time ./clustEncrypt.pl /.mounts/labs/prod/backups/production/projects/wouters_miRNA /.mounts/labs/prod/backups/production/projects/wouters_miRNA_encrypted > wouters_encrypt.qsub.log

=head3 SGE Job monitoring & killing:
List queue contents:

 qstat -q spbcrypto
 
 qdel <job IDs>
 
 To monitor progress of the output:
 du -shL --exclude=wouters_miRNA_encrypted/qsub_bash_scripts*  wouters_miRNA_encrypted
 
 To monitor the QSub:
 watch 'qstat -q spbcrypto -s p | wc -l
 
=head3 Queue limiting: being nice to SGE qsub

There is a 0.1s pause introduced between each QSub job, also there parameter '$QueueLimit' limits the number the program 
will launch at any one time; set this to 0 if you want everything.

 $QueueLimit = 5000

=head2 Size split limits

This program only attempts to QSub encrypt on large ($SIZESPLIT in bytes) files and instead pushs the rest to a 'smallFiles.lst' for alterative processing
- the intention being on a single core machine - to save flooding the SGE with lots of silly little jobs that have high overhead.


 mmoorhouse@platinum:~/tickets/tapeTests_2723$ time ./clustEncrypt.pl wouters_miRNA wouters_miRNA_encrypted  | tail -n 50

Testing gpg (in general terms):
Testing gpg keyring (for 'SeqProdBio2014' [private] ):
Testing gpg keyring (for 'SeqProdBio2014' [public] ):
Preflight checks complete: starting our run
Files smaller than: '10485760' bytes (~10 MB) will not be processed and passed to 'smallFiles_1765263.lst' instead.
D: Get Directories command as expanded: 'find -L `ls -d -1 $PWD/wouters_miRNA` -type d'
Found: 2170 directories
Found: 80624 files in total
Summary: 80624 (nominal) files to consider in total; when surveyed: 3677 will be QSubed as they exceed 10485760 bytes and so 72484 files will be pushed to smallFiles_1765263.lst as they were too small to QSub
Also: found '4463' links (of which '4451' were broken links); I will exclude all links and note their from<>to linkage in 'Links_Found_During_Archive.tab'

real    0m42.719s
user    0m0.624s
sys     0m10.456s

=head2 Hertitage 

The prorgam ows much of its core operation - SGE parts excepted - to the encryptTapeDirDemo.pl that is essentially the single processor version of this. 

=cut
=head3 We Use a Limited set of modules: 

All core, currently.

=cut

use strict;
#use Cwd;
use File::Path qw(make_path);
use File::Spec;


my $SIZESPLIT;
#Set to ~10MB (normal use)
$SIZESPLIT = 10 * 1024 * 1024;	#Size in bytes 

#Queue Limit: (set to 0 for everything)
my $QueueLimit=20000;

#Set small for testing if needed:
$SIZESPLIT = 1;

my $BASHBASEDIRDEFAULT = "qsub_bash_scripts";

my $timeStamp = substr (time,-7);
my $SMALLFILEDEF = "smallFiles_$timeStamp.lst";
my $LINKFILEDEF = "symlinks_$timeStamp.lst";

my ($From, $To) = (shift @ARGV, shift @ARGV);

#Were we passed input and output (from / to) paths?:

unless (defined ($From) && defined ($To))
	{	die "Usage: clustEncrypt.pl <From> <To> <OPT: SmallList.lst>\n";	}

unless (-e ($From))
	{	die "Cannot find source ('from') path: '$From'\nUsage: clustEncrypt.pl <From> <To> <OPT: SmallList.lst>\n";	}

$From =~ s/\/$//; $To =~ s/\/$//;
print "Processing files from: '$From'\nOutputting files to: '$To'\n";

=head3 Within this the output directory try to build the bash script directory

=cut

#(The Output path we might have to build ourselves, not a show stopper)

my $BASHBASEDIR = "$To/$BASHBASEDIRDEFAULT";
$BASHBASEDIR =~ s/ /\\ /g;	#Escape the spaces here...if there are any.  Perl won't care, Bash might.

unless (-e $BASHBASEDIR)	#Likely it doesn't exist actually
	{	make_path($BASHBASEDIR);	}
unless (-e $BASHBASEDIR)	#Now the path should exists, show-stopping if it doesn't.
	{
		die "Could not create the bash script directory '$BASHBASEDIR'\n";
	}

=head3 Prepare the small files & link file names & the small file link size

And do a few tests: 

=cut
my $LinkFile = $LINKFILEDEF;	#Just softwire this to the timestamp currently;
   
#Did somebody try to change the size split?:

my $SmallFilesList = shift @ARGV;
unless (defined $SmallFilesList && $SmallFilesList ne "")	{$SmallFilesList = $SMALLFILEDEF;}


=head3 Run GPG Key tests:

=cut

my @KEYS = ("BC3E454B", "E16641B3", "1C1742CB");
my $GPGKeyString="";
 
print "Testing gpg (in general terms):\n";
unless (`gpg --version` 			=~ m/^gpg \(GnuPG\) /)	{	die "Couldn't find GPG\n";	}
foreach my $C_Key (@KEYS)
	{
	print "Testing gpg keyring (for '$C_Key'):\n";
	unless (`gpg --list-keys`	=~ m/$C_Key/ or `gpg --list-secret-keys`	=~ m/$C_Key/)			
		{	die "Couldn't find the Key '$C_Key' in the keyring\n";	}
	$GPGKeyString=$GPGKeyString. " -r $C_Key";		
	}


print "For GPG I will use these keys: '$GPGKeyString'\n";

print "Preflight checks complete: starting our run\n";
print "Files smaller than: '$SIZESPLIT' bytes (~",sprintf ("%i", $SIZESPLIT/1024/1024)," MB) will not be processed and passed to '$SmallFilesList' instead.\n";



#die "HIT BLOCK: PreFlight Complete\n";

=head3 Get the Directory Structure to duplicate:

Using unix find is easiest.

=cut

#Just easier to let unix find do what it does best:
my $DirsCommand = "find -L $From -type d";
print "D: Get Directories command as expanded: '$DirsCommand'\n";
my @DirsOrginal = `$DirsCommand`;

#foreach my $C_Dir (@DirsOrginal)	{	chomp ($C_Dir);print "D: Org Dir: '$C_Dir'\n";	}
print "Found: ",$#DirsOrginal+1," directories\n";

=head3 Iterate through this structure to find real files:

This is the complex / time consuming part:
We actually have three cases for each file
 
=head4 1) The file is large and needs to be encrypted using a QSub job

=head4 2) The file is small and can be encrypted here on platinum 

=head4 3) The file is a link and needs to be removed and noted in a table

Note the files into an index array on the way past.
Also check the file finding command for a size limitation, this is > ~100MB for example ($SIZESPLIT)

=cut

$|=0;

#Just easier to let unix find do what it does best (again):
my @Files = `find -L $From -type f -o -type l`;
my $N_OrgFiles = $#Files+1; 
print "Found: $N_OrgFiles files in total\n";

#Case 1 - we need to build the QSub commands later in the program:
my @FilesToQSub;	
#Case 2 is pushed straight to a file, hence the open command:
open (SMALLFILES, ">$SmallFilesList")	or die "Cannot open the file to store the small files in '$SmallFilesList'\n";
#Case 3 is pushed straight to a file, hence the open command:
open (LINKFILE,">$LinkFile")	or die "Cannot open the file '$LinkFile' to store the links I may find\n";

#Some counters...
my $N_TooSmallToSub = 0;
my $N_BrokenLinks=0;
my $N_Symlinks =0;



foreach my $C_File (@Files)
	{
	if ($C_File =~ m/^\s*$/) {	next;	}	#Shouldn't really be any blank lines, still: skip if so.
	chomp ($C_File);						#Remove new lines

#The 0 case: file has disappeared (very weird!)
#unless (-e $C_File && not (-l $C_File))	
#	{	die "This file: '$C_File' has moved since I surveyed it!  File system deemed unfinished / unstable - terminating\n" ; }
	
=head4 Case 3 is the easiest: Link

(because we don't deal with it here really: just note it).

=cut 
	
	if (-l $C_File)		
		{
		my $EndPoint = readlink ($C_File);
		print LINKFILE "$C_File \t $EndPoint\n";
		$N_Symlinks++; 	#Count & Move on...
		if (-l $C_File && !-e $C_File)			{	$N_BrokenLinks ++;	}	#Simply because we are curious as to the link status
		next;
		}

#We actually don't care about the link status currently:

	#Is the file a broken link?
#	
#			next;
#		}
=head4 Case 2: File is small

=cut 
	my $FileSize= -s $C_File;
	
	#print "$C_File = size $FileSize in bytes\n";
	if ($FileSize < $SIZESPLIT)			#Is the file too small to need a QSub?
		{	$N_TooSmallToSub ++;				#Let somebody else deal with it if so...just count it here.
			print SMALLFILES "$C_File\n";		
			next;
		}
	
#	print "\t$C_File is large: '$FileSize'\n";
	push @FilesToQSub, $C_File;
	}

print "Summary: ", $#Files+1, " (nominal) files to consider in total; when surveyed: ", $#FilesToQSub+1, " will be QSubed as they exceed $SIZESPLIT bytes ",
		"and so $N_TooSmallToSub files will be pushed to $SmallFilesList as they were too small to QSub\n";
print "Also: found '$N_Symlinks' links (of which '$N_BrokenLinks' were broken links); I will exclude all links and note their from<>to linkage in 'Links_Found_During_Archive.tab'\n";

@Files = undef;

close SMALLFILES;
close LINKFILE;

=head3 Start creating the directories

Sort on length so that 'leaf' directories come to the top and get created first:

=cut 
#die "HIT BLOCK\n";
#Remember that in this next line the From --->>> To substitution hasn't been done yet: 
my @NewDirStructure = sort { length $b <=> length $a } @DirsOrginal; 

my $DirCount =0;
foreach my $C_Dir (@NewDirStructure)
	{
	#Create the new path:
	my $NewDir = $C_Dir;
	chomp ($NewDir);
	$NewDir =~s/$From/$To/;
	unless (-e $NewDir)	
		{	
		make_path ($NewDir);
		$DirCount ++;			
			#print "Creating path: '$NewDir'\n";		
		}
#	print "D: Directory: $C_Dir\n";		
	}
print "Created: '$DirCount' directories\n";
#Files & Directories Collected
my $EncrpytErrorCount =0;

=head3 Build QSub & GPG commands to run the encryption

Ultimately these are compiled (in the 'brought together' sense) and run using backticks from the '$QSubCommand'.


We do - essentially 2 operations:

 1) Encrypt the file using GPG.
 2) MD5 print both the old & new files and capture the output.

To do this we build most of the commands & paths in advance into various variables such as:
 $GPGCommand 
 $LogFile

that are combined together into: 
 $QSubCommand
 
Which is then written out to a bash script.

=cut
#To store the acutal SGE / QSub commands: (Well, maybe...ultimately)
 
my @QSUBCommands;

print "Starting preparation of QSub jobs:\n";
print "D: There are: ",$#FilesToQSub+1," files in my queue for passing to QSub\n";
#die "HIT BLOCK\n";
my $QSubCounter=0;

=head3 Construct the file paths to things that don't change (log, MD5 lists):

(The spelling 'mistake' "Encryptd" is deliberate to align the two names for this and original)

For this we use Absolute paths helpfully supplied by the function File::Spec module:

 unless (File::Spec->file_name_is_absolute($To))
	{	$ToABS= File::Spec->rel2abs($To);	}

We create log, MD5 Original & MD5 of Encrypted.

=cut

#my $Location
my $ToABS;
unless (File::Spec->file_name_is_absolute($To))
	{	$ToABS= File::Spec->rel2abs($To);	}
if ($ToABS eq "")	{	$ToABS = $To;}	#Total emergency hack!
print "D: Will use the Absolute path the Output ('To') directory: '$ToABS'\n";
my $LogFile 				= "$ToABS/$timeStamp\_log.lst";
print "D: Log file will be: '$LogFile'\n";
#die "HIT BLOCK\n";
#A couple of counters
my $FoundBashFiles =0;
my $FoundOutputFiles =0;
my $N_ReallyQSubbed = 0;

=head2 Iterate through the files collected; create the qsub commands and bash scripts they relate to 

The bash scripts clean themselves up on completion, fortunately...still we create an awful lot of md5s

=cut

foreach my $FileIndx (0..$#FilesToQSub)
	{
		print "#$FileIndx\n";
		
	#Two 'convenience variables': 
	my $C_File = $FilesToQSub[$FileIndx];
	#Escape all spaces in the path (cascades down to the output file as well) if they are present:
	$C_File =~ s/ /\\ /g;
	my $NewFile = $C_File;
	print "Processing file # $FileIndx \t$NewFile'\n";
	
	#my $LogFile 				= "$ToABS/$timeStamp\_log.lst";

	chomp ($NewFile);
	$NewFile =~s/$From/$To/;	#Substitute the left most occurance in the path.
	$NewFile=$NewFile.".gpg";	#We add 'gpg' to all encrypted files
	my $BashFileName = "$BASHBASEDIR/$FileIndx.bash";	#This will be the name of the bash script we create for this job:
	my $GPGFileName_Org = "$BASHBASEDIR/$FileIndx.org.md5";
	my $GPGFileName_New = "$BASHBASEDIR/$FileIndx.new.md5";
	
#Might be useful in the future: allows the output file to be deleted to avoid GPG stalling. 
#	if (-e $NewFile)	{	die "Cannot delete old output file for some reason: '$NewFile'\n";	}

=head4 Do our two parallelisation checks: does either the output file (GPG) or the bash script already exist?  


=cut 
#Check 1: New (GPG'd) file?
#	print "D: New File: '$NewFile'\n";
	if (-e $NewFile)			
		{	
#			print "D: OP file found\n"; 
			$FoundOutputFiles++; next;		} #File already created by another process
#	print "D: Bash File: '$BashFileName'\n";
#Check 2: Bash File?
	if (-e $BashFileName)		
		{	
			print "D: Bash script found\n"; 
		$FoundBashFiles++; next;		} #If this bash file already exists - but the GPG doesn't (yet), then we presume it is under processing.
#	print "D: Creating QSUB Job\n";
#Mimic this:
#gpg --trust-model always -r EGA_Public_key -r SeqProdBio -o $TMPOUT/$FName.gpg -e $InputFile";
	my $GPGCommand 		= "gpg --trust-model always $GPGKeyString -o $NewFile -e $C_File";
	my $MD5Command		= "md5sum $C_File 2> /dev/null >> $GPGFileName_Org; md5sum $NewFile >> $GPGFileName_New 2> /dev/null";
	my $BashContents =
"
#!/bin/bash
$GPGCommand
$MD5Command
rm $BashFileName
";
	
	open BASHSCRIPT, ">$BashFileName" or die "Cannot open '$BashFileName'\n";
	print BASHSCRIPT $BashContents;
	close BASHSCRIPT;
	
	#qsub -cwd -b y -N s$DonorID\_$rg -q production -e /u/mmoorhouse/tickets/tapeTests_2723/LargeEncrypt.log -o $BaseOutputPath.log "bash $BaseOutputPath.cmd" $
	my $QSubCommand = "qsub -cwd -b y -N TapCrypt_$FileIndx -q spbcrypto -o /dev/null -e $LogFile \"bash $BashFileName\"";
	#Technically you could do this and bypass the queue & MD5Sums to just tun the GPG command: `$GPGCommand`;
	#If you want to know what the commands are, enable these two lines:
	#print "D: gpg = '$GPGCommand'\n";
	print "D: Created bash file: '$BashFileName'\n";
	print "D: QSub = '$QSubCommand'\n";
	#This actually launches the jobs (if it is enabled [best enable / disable both lines together]):
	my $QsubResult = `$QSubCommand`; 
	print "D: QSub Result is: '$QsubResult'\n";
	$N_ReallyQSubbed ++;
=head4 Job limiting

Number and frequency.

=cut

#If you want to limit the number of jobs queued, enable this next line:
	if ($QueueLimit !=0 and $N_ReallyQSubbed >= $QueueLimit)		
		{		print "Internal limiting of '$QueueLimit' queued jobs reached, skipping the rest\n"; last;		}
	#Enable this next line if you need to slow the issuing of qSUB commands: (sleep operates on seconds)
	print "***Pausing for 0.1s****\n"; 	sleep (0.1);
	print "----\n";
	}
	
print "All Done! 
Summary files were found in '$From': $N_OrgFiles
were too small to QSub: $N_TooSmallToSub
files were on the QSub list: ",$#FilesToQSub+1,"
Send to Queue: $N_ReallyQSubbed 
Skipped due to Output GPG already existing: $FoundOutputFiles
Skipped due to bash script already existing: $FoundBashFiles
Output directory containing GPG files: $To 
Bash script directory: $BASHBASEDIR
Files too small to QSub in file: $N_TooSmallToSub";
