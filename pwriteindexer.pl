#!/usr/bin/perl
=head1 pwriteindexer.pl

When run this program demonstrates the filling-in the MD5sums of the index file.

It is designed to simulate the 'locking' (actually 'claiming' might be a better term) of files for processing -
in this case simply MD5'ing them.  

This list of files is those in the current directory; in this version the entire contents of "demoIndexwrite.pl" 
have been pasted into this program prior to checking into git. 
     
Future versions are likely to demonstrate blocking, SGE Array Jobs etc. but this just demonstrates the 
writing of a 'secret' to the index file in place of the blank MD5sum and then it being filled in by the real MD5sum.

=head2 Locking Methodology

The secret (e.g. "1850_1425671642_39570") is a combination of the process ID, the time in seconds since EPOC and 
an integer random number 0 - 100 000 and it is understood this should not be duplicated by other well meaning clients.

An initial check is done on the file that the output is blank (i.e. repeated: "-" characters).

This is written to the file.

After a pause (human length) the file re-checked - twice.
If the 'secret' is still present the lock declared as sound and the MD5 sum calculated.

The index file created is:

 my $Indexfile = "Dyna.index";

=cut 

=head2 Nothing Parallel, Nothing Grid Related Here 

Note that there is no pretense that this program - in this version - writes in parallel: that is version 2.0.

=head2 Index File Format

=head3 Clean:

 --------------------------------        ./log
 --------------------------------        ./pwriteindexer.pl
 --------------------------------        ./Dyna.index
 --------------------------------        ./pwritedemo.pl
 --------------------------------        ./out.14
 --------------------------------        ./alpwriteindexer.pl

=head4 Partially filled in:

Note the 'Secret' "1850_1425671642_39570" 

 1850_1425671642_39570-----------        ./demopIndexwrite.pl
 --------------------------------        ./log
 50b1fca392aff5ece8525cb3f56ab419        ./pwriteindexer.pl
 504a2320a1a7a72c7e12da5efa6b363d        ./Dyna.index
 d924abac94ab1add26d4f58607037c05        ./pwritedemo.pl
 --------------------------------        ./out.14
 --------------------------------        ./alpwriteindexer.pl

=head3 Complete: 

 d41d8cd98f00b204e9800998ecf8427e        ./log
 50b1fca392aff5ece8525cb3f56ab419        ./pwriteindexer.pl
 504a2320a1a7a72c7e12da5efa6b363d        ./Dyna.index
 d924abac94ab1add26d4f58607037c05        ./pwritedemo.pl
 f03d2ef8f68328191c1b7a045808d23b        ./out.14
 d3414f3c539103855e658754719dda0c        ./alpwriteindexer.pl

=cut
#For testing we hardwire the index file
 
use strict;

#The name of the index file:
my $Indexfile = "Dyna.index";

#The time to wait between lock tests: (remember to make this twice your 'watch -n' refresh cycle)
my $WAITTIME=3;

#Pause between iterations:
my $ITERATIONPAUSE=5;

=head2 Create the index file:

This is basically the contents of "demoIndexwrite.pl" almost verbatim (a re-declaration of $Indexfile was removed)

=cut

my @FileList = `find .  -not -path '*/\.*' -type f `;

my $NFiles = scalar @FileList;

print "Found $NFiles files to enter into our index:\n";

open INDEXFILE, ">$Indexfile" or die "Cannot open '$Indexfile' \n";

print "Index will be: '$Indexfile'\n";
my $StartLocation =0;
foreach my $C_Path (0..$#FileList)
	{
	my $PathName = $FileList[$C_Path];
	chomp ($PathName); 
#	my $PathLength= length ($PathName);
	print "D: '$PathName'\n";
	my $Line=	"-"x32 .
				"\t".
				$PathName.
				"\n";
	$StartLocation=$StartLocation+length ($Line);
#	print "D:$C_Path: '$Line'\n";
	print INDEXFILE $Line;
	}
close INDEXFILE;


unless (-e $Indexfile)	{	die "Could not create the index file to survey: '$Indexfile'\n";	}

my $NLines = `wc -l $Indexfile`;
unless ($NLines >= 1)	{	die	"Low number of lines returned from '$NLines'\n";	}

#Main iterative loop:
$|=1;
my @Array = (1..$NLines);

print "Shuffling Target Points:\n"; fisher_yates_shuffle(\@Array);
my $Time= time;
my $Secret = $$."_".time."_".int(rand(100000));


foreach my $C_WantedLine (@Array)
	{

#	sysopen my $INDEXFILE_FH, "$Indexfile", 'O_RDWR|O_CREAT' or die "Cannot open output file '$Indexfile' (main open command)\n";
	open my $INDEXFILE_FH, "+<", $Indexfile or die "Cannot open output file '$Indexfile' (main open command)\n";
	print "D: Driving to line: '$C_WantedLine'\n";
	my $C_Line=1;
	while (<$INDEXFILE_FH>)
		{
#		print  "D: For line: $C_Line\t seek output: ", tell ($INDEXFILE_FH),"\n"; 
		unless ($C_Line == $C_WantedLine)	{	$C_Line ++;	next;	}

		#Yes, we process this line:
		chomp ();		
		
		#Disable buffering from now on (might not be needed if you are using 'sys' style commands such as 'syswrite')
		select((select($INDEXFILE_FH), $|=1)[0]);
		
		#Reset the file pointer to the start of the line: 
		my $LineLength = length ($_."\n");
		
		seek($INDEXFILE_FH, -$LineLength,  1) or die "Could not reset filepointer!\n";
		print "D: File pointer set to: ",tell ($INDEXFILE_FH),"\n";
		#Try to get a lock:
		
		#If we can't then report a locking problem and terminate:
		
		unless (getLock ($Secret, $INDEXFILE_FH, $Indexfile))			
			{	
				#syswrite $INDEXFILE_FH , "$Secret\_LP";				last;
				print $INDEXFILE_FH "$Secret\_LP";				die "Locking problem detected at: '$C_Line', stopping\n";
			}
		my (undef, $Path)	=	split (/\t/,$_);

		#Compute the MD5 - may take a time:
		my $MD5Result = `md5sum $Path`;
		my ($MD5Value)	=	$MD5Result =~ m/^(.{32})/;
		print "D: MD5 value calculated: '$MD5Value'\n";
		
		if (defined $MD5Value && $MD5Value eq "")
			{
			print $INDEXFILE_FH "$Secret\_NOMD5";				last;
			#syswrite $INDEXFILE_FH, "$Secret\_NOMD5";				last;
			}
		#Get and then test the lock one last time...(in case a 'cleaner' has come through the file and or we have stalled)
		
		
		my $SecretLength= length ($Secret);
		my $SecretAfterMD5Run = "";
		read ($INDEXFILE_FH, $SecretAfterMD5Run, $SecretLength);
		print "D: Secret after MD5 write: '$SecretAfterMD5Run'\n";
#		sysseek($INDEXFILE_FH, -$SecretLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...
		seek($INDEXFILE_FH, -$SecretLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...
		unless ($SecretAfterMD5Run eq $Secret)	{				last;	}	#Whatever we did, it was too late
		print "D: After MD5 calc, my secret is still present: so writing out MD5\n";
		
		#As everything worked, just do this:
		print $INDEXFILE_FH "$MD5Value";
																last;
		}
	close $INDEXFILE_FH;
	print "D: Success on this cycle.  Iterating to next.\n";
	print "D: Sleeping $ITERATIONPAUSE s\n";sleep ($ITERATIONPAUSE);
	
	#If you just want to process the first file, enable this next line:
#	last;
	}
#
#
####
sub fisher_yates_shuffle {
    my $array = shift;
    my $i;
    for ($i = @$array; --$i; ) {
        my $j = int rand ($i+1);
        next if $i == $j;
        @$array[$i,$j] = @$array[$j,$i];
    }
}
	
sub getLock 
{
=head1 getLock ($Secret, $INDEXFILE, $Filename); returns 1 on success

When called writes out this instance's 'secret' ($Secret) to the location in the file ($INDEXFILE), 
waits $WAITTIME reading it back it.
If it is the same; then great - 1 is returned and we are assumed to have a lock.

If not then 0 is returned.

The filepointer is assumed to have being reset to start of the line prior to calling.
It will be put back if at all possible (i.e. not if the file no longer exists etc.)

Example code from egaTxFer.pl:

open LOCKFILE, ">$LockFile" or return 1;        #Return if we can't open the file (rather than die...we don't make such decisions)
print LOCKFILE $Tag;    #Print out our secret tag
close (LOCKFILE);

sleep ($WAITTIME);

#First Check:
my $TagFromFile="";
print "Test lock #1/2\n";
open LOCKFILE, $LockFile or return 0;	#If we can't open it: fine it is because     
$TagFromFile= <LOCKFILE>;
close LOCKFILE;
unless ($TagFromFile =~ /^$Tag$/)       {return 1;}

Then again for the second check.


=cut
my ($Sec, $FH, $Filename)	=	@_;
#Screen the worst of the null / empty values passed:
unless (defined $Sec && $Sec ne ""
		&& defined $$FH )	{	return 0;	}
		
my $SecLength = length ($Sec);	#The length of secret / the amount we have to back up in the file...
my $ReturnedTag ="";	#What we get back

#Test the location - has anybody else tried to write a tag here?

#sysread ($FH, $ReturnedTag, $SecLength);
read ($FH, $ReturnedTag, $SecLength);

#sysseek($FH, -$SecLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...
seek($FH, -$SecLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...

unless ($ReturnedTag =~ m/^-{$SecLength}$/)
	{
	print "getLock (): Improper returned tag: '$ReturnedTag' detected - failing lock\n";
	return 0;
	}
	
print "D: getLock(): Writing out secret '$Sec'\n";

print $FH $Sec;
#syswrite $FH, $Sec;
#Try to encourage updates arcoss the network:
#The sync is really harsh:
#`sync`;
#So let's try a (lighter) touch:

`touch $Filename`;

#sysseek($FH, -$SecLength,  1);
seek($FH, -$SecLength,  1);

#Test 1:
#sysread ($FH, $ReturnedTag, $SecLength);
read ($FH, $ReturnedTag, $SecLength);

#sysseek($FH, -$SecLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...
seek($FH, -$SecLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...

print "D: getLock (): test 1 of 2 done\n";
unless ($ReturnedTag eq $Sec)	{	return 0;	}	#Test passed?
print "D: getLock (): test 1 of 2 done - and passed\n";
#print "D: getLock(): Test 1= Ok ('$ReturnedTag')\n";


#Sleep in between tests:
print "D: getLock (): sleeping ($WAITTIME s)...\n";
sleep ($WAITTIME);				#Give others a chance to grab a lock.

#Test 2:

#sysread ($FH, $ReturnedTag,$SecLength);
#sysseek($FH, -$SecLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...

read ($FH, $ReturnedTag,$SecLength);
seek($FH, -$SecLength,  1) or die "Could not reset filepointer!\n";		#Back us up in the file the length of the secret...

print "D: getLock (): test 2 of 2 done\n";
unless ($ReturnedTag eq $Sec)	{	return 0;	}	#Test passed?
print "D: getLock (): test 2 of 2 done - and passed\n";
print "D: getLock (): I have lock!\n";
return 1;		#1 = success for our return value
}
