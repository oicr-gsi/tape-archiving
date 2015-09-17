# EOL Tape Archiving Scripts

End of Life for a project means that all it's useful data needs to be transferred to tape and then all original files get deleted. This scenario is primarily for long-term storage.

These scripts are meant to prepare a project for archiving, encrypt it using GPG, verify the output, and then send it to CommVault to write to tape.

## Dependencies

* SGE Cluster, access to qrsh, qsub
* Perl 5.20.1
* JSON Perl module
* GPG keys - encrypts using three public keys; checker requires one of the matching private keys for verification
* Launcher only: requires access to backups queue, which hosts Brian Ott's CommVault scripts (OICR only)

To satisfy these requirements at OICR, you can qrsh to a cluster node and load modules perl/5.20.1 and spb-perl-pipe/dev.

## Indexer

This script works as the starting point of the whole process and its task is to prepare the list of files (it ignores anything else as normal files on the file system, so symbolic links and non-existing files will not be processed). This script also calculates MD5 sums which are used to ensure that encrypted data that are sent to tape correspond to the original files and can be reliably recovered.

Basic usage: ./indexer.pl /path/to/data /path/to/work/dir

## Encrypter

Script for gpg encryption. It takes over when indexer.pl finishes MD5-summing inputs and produces MD5*.complete file or files, which indicates succesful completion of this step.

Basic usage: ./encrypter.pl /path/to/work/dir/

## Checker

Checker script decrypts the files to STDOUT and pipes to md5sum command for verification of the files produced by encrypter.pl. As a reference, it uses MD5 sums calculated by indexer.pl script

Basic usage: ./checker.pl /path/to/work/dir/

## Launcher

This is a wrapper script for interaction with Brian Ott's interface to Commvault software which controls OICR tape-writing robotic devices. How this works is that the backup.hpc is just there to host the qscript which itself just takes your input turns it into CommVault language then submit it to CommVault which then translates it into a backup job. The actual mount is on the "media agent" which is the CommVault server that handles doing the actual backup and talking to the backend Isilon storage. 

Basic usage: ./launcher.pl /path/to/work/dir/

## For OICR/GSI Use

See the EOL Tape Archiving SOP on the GSI Wiki
