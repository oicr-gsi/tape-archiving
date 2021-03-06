# EOL Tape Archiving Scripts

End of Life for a project means that all its useful data needs to be transferred to tape and then all original files deleted. This scenario is primarily for long-term storage.

These scripts are meant to prepare a project for archiving, encrypt it using GPG, verify the output, and then send it to CommVault to write to tape.

### Dependencies

* SGE Cluster, access to qrsh, qsub
* Perl 5.20.1
* JSON Perl module
* GPG keys - encrypts using three public keys; checker requires one of the matching private keys for verification
* Launcher only: access to **backups** queue, which can access Brian Ott's CommVault scripts (OICR only)

## Software Pipeline

### Indexer

This script works as the starting point of the whole process and its task is to prepare the list of files. It creates a list and calculates MD5 sums for all files to be encrypted. These are used to ensure that encrypted data that is sent to tape corresponds to the original data and can be reliably recovered. It also catalogs all null files, and symlinks, active and broken, which are not encrypted or backed up.

Basic usage: ./indexer.pl /path/to/data /path/to/work/dir

### Encrypter

Script for GPG encryption. It reads to file list created by indexer.pl and encrypts all files using GPG. Three keys are used for encryption.

Basic usage: ./encrypter.pl /path/to/work/dir/

### Checker

Checker script decrypts the files to STDOUT and pipes to md5sum command for verification of the files produced by encrypter.pl. As a reference, it uses MD5 sums calculated by indexer.pl script.

Basic usage: ./checker.pl /path/to/work/dir/

### Launcher

This is a wrapper script for interaction with Brian Ott's interface to Commvault software which controls OICR tape-writing robotic devices. How this works is that the backup.hpc is just there to host the qscript which itself just takes your input, turns it into Commvault language, then submits it to Commvault, which translates it into a backup job. The actual mount is on the "media agent," which is the Commvault server that does the actual backup and talks to the back-end Isilon storage.

Basic usage: ./launcher.pl /path/to/work/dir/

## Usage at OICR/GSI

### Prerequisites

* Import OICR, SPB, and Tape public keys into GPG
* Import SPB private key into GPG
* Commvault account (contact Help Desk/Brian Ott)

### Procedure

1. Choose a project directory to archive
2. Add the project to the [backup tracker](http://www-pde.hpc.oicr.on.ca/html/backup.tracker/eol/) by including it in the file tracking file. The entry should contain the project name, file count, and total size in GB, separated by tabs

        find <dir> -type f | wc -l
        du -sh <dir>

3. qrsh to a cluster node and load modules to satisfy dependencies

        qrsh
        module load perl/5.20.1
        module load spb-perl-pipe/dev

4. Launch **indexer.pl**, passing the source and destination paths. Source path should be the existing project directory, and destination should usually be in **\<backups-dir\>/backup_\<project-name\>**. If using a different location, create a symlink to it here so it can be seen in the backup tracker. Progress can then be monitored via the backup tracker or qstat. Output and error logs will be written to your current working directory. Check for errors after the job finishes. A common error source is missing permissions

5. After indexer.pl finishes, launch **encrypter.pl** script, giving it destination path from the indexer step. Progress can be monitored via the backup tracker or qstat

6. After encrypter.pl finishes, launch **checker.pl** script, giving it the same path. Progress can again be monitored via the backup tracker or qstat. In case of success, a file APPROVED\_TO\_WRITE is produced. At this point, it is possible to send data to tape

7. After checker.pl finishes, launch **launcher.pl** script, giving it the same path again. Launcher takes care of interaction with Commvault to send the data to tape. The cluster job will finish quickly and output will be written to your current working directory. Check for errors. Progress of the backup can be monitored in the [Commvault GUI](http://cserve.ad.oicr.on.ca/console/).

8. Once the backup is complete, create a receipt file in **\<backups-dir\>/receipts/**. The receipt filename should be the same as the project name, and it should only contain the tape IDs, separated by commas. Find the tape ID\(s\) in CommVault:
    * cserve > Client Computers > backup.isilon.stg.oicr.on.ca > NAS > seqprodbio
    * right-click the backup, choose Browse and Restore
    * click List Media > OK
    * tape IDs are listed in "Barcode\\Mount Path" column

### Data Removal Procedure

1. Choose a project from the [Backup Tracker](http://www-pde.hpc.oicr.on.ca/html/backup.tracker/eol/) that has a tape ID and has files 'present'

2. Compare the size of the **encrypted** directory to the size in [Commvault](http://cserve.ad.oicr.on.ca/console/). The size in CommVault will usually be usually be smaller due to compression. To find the size in Commvault:
    * client computer > backup.isilon.stg.oicr.on.ca > NAS > seqprodbio
    * right-click the backup, choose Browse and Restore
    * click View Content

3. Check the following files in the project backup directory:
    * MissingForGPG.lst - should be empty
    * APPROVED\_TO\_WRITE - should exist
    * Check that MD5 files have data in them and no "--" stretches
    
            eval "head -2 MD5_working/1.md5.*; echo '~~~'; cat MissingForGPG.lst ; echo '~~~'; cat APPROVED_TO_WRITE; echo '~~~';wc -l MD5_working/1.md5.* ;echo '~~~'; find MD5_working/ -type f | xargs grep '--'; " > backup_check
        
    * Look at the NullFiles to see what's in there
    
            grep -vE "[oe][0-9]+$|done$|[l|L]og[0-9]*$" index/NullFiles | less
        
4. Remove the encrypted directory (using full file paths and triple-checking it's the right one), leaving everything else in the directory

5. Move the backup_<PROJECT> directory (sans encrypted dir) to a snapshotted location

        rsync -r <backups-dir>/backup_<project-name> <snapshotted-dir>/
        rm -rf <backups-dir>/backup_<project-name>
        ln -s <snapshotted-dir>/backup_<project-name> <backups-dir>/
        
6. Test to see if you can write in the final directory (and therefore delete)

        find . -type f | while read i ; do if [ ! -w "$i" ] ; then echo "No write: $i"; fi; done
        
7. Remove the directory containing the original files (using full file paths and triple-checking it's the right one), do not follow symlinks

8. Leave your mark in the backups directory

        date > <snapshotted-dir>/backup_<project-name>/DATA_REMOVED

