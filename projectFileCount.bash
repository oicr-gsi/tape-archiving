#!/bin/bash
#Assumes you have projects symlinked to somewhere sensible such as:
# /.mounts/labs/prod/backups/production/projects 
#To get a size count (similar to this):
#du -k --max-depth=1 projects
for directory in `ls -1 projects/`
do
	if [ -d projects/$directory ]
		then
		echo -n "$directory  "
		echo `find -L projects/$directory -type f 2> /dev/null | wc -l` 
		fi
done
