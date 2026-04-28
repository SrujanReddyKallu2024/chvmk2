#!/bin/bash

source /etc/profile.d/hadoop.sh
source conf.sh

# Function to log messages
log_message(){
    local log_file=$1
    local message=$2

    # If the log file directory doesnt exist, create it and echo this message if a new directory is created
    if [ ! -d "$(dirname "$log_file")" ]; then
        mkdir -p "$(dirname "$log_file")"
        echo "Created log directory: $(dirname "$log_file")"
    fi

    # If the log file doesn't exist, create it and echo this message if a new log file is created
    if [ ! -f "$log_file" ]; then
        touch "$log_file"
        echo "Created log file: $log_file"
    fi

    printf "\n$(date +%Y-%m-%d-%H-%M-%S) -  $message" | tee -a "$log_file"
}


# arguments 1. data_from_sts folder, 2. listener code home directory
download_file_tofrom_sts() {
    echo "start download"
    # make sure folder exists locally
    mkdir -p $1
    # make sure path in filespec.ls is updated
    $2/split.py
    echo "end download"
}


#arguments - 1. filename/filepattern, 2. file extension, 3. input folder in hdfs, 4. output folder in local path
transfer_file_to_local() {
    echo "searching files in hdfs"
    # create local dir
    mkdir -p $4    
    REQ_FILES=$(hdfs dfs -ls $3 | grep $1 | awk '{print $8}' | while read f; do basename $f; done)
    for f in $REQ_FILES
    do
        if hdfs dfs -test -e $3/$f; then
            if [ -e "$4/$f" ]; then
                echo "$f file already exists in $4, so didn't copy"
            else
                if [[ $2 == "csv" || $2 == "tsv" ]]; then
                    echo "start transfer of $2 file to local"  
                    hdfs dfs -getmerge -skip-empty-file $3/$f $4/$f
                    echo "copied $f file from $3 to $4"   
                else
                    echo "start transfer file to local"  
                    hdfs dfs -copyToLocal $3/$f $4
                    echo "copied $f file from $3 to $4"    
                fi
            fi
        else
            echo "$3/$f doesn't exist"
        fi
    done
}


#arguments - 1. filename/filepattern,  2. input folder in local, 3. output folder in hdfs, 4. local backup
transfer_file_to_hdfs() {        
    INPUT_FILES="$2/*$1*"
    hdfs dfs -mkdir -p $3
    echo "searching for $1 files in $INPUT_FILES"
    for f in $INPUT_FILES
    do
        [ -e "$f" ] || continue # skip if file is invalid
        echo "start transfer file to hdfs and backup to local"
        file_name=$(basename $f)
        if hdfs dfs -test -e $3/$file_name; then            
            echo "$file_name already exists in $3, will not copy"
        else            
            echo "copying file to hdfs" 
            hdfs dfs -copyFromLocal $f $3/ && echo copied $f
        fi
    done    
    echo "End transfer of $1 to hdfs path $3"    
}


#arguments - 1. filename/filepattern, 2. input folder
create_gzipped_file() {
    INPUT_FILES="$2/*$1*"
    for f in $INPUT_FILES  
    do  
        [ -e "$f" ] || continue # skip if file is invalid        
        gzip $f
        echo "gzipped $f"
        if [ -e $f.gz ]
        then
            echo "$f.gz exists in $2"
        else
            echo "$f.gz doesn't exist in $2"
            echo "Please check log for further detail." | mailx -s "gzipped output file not found in $2" $MAILLIST        
        fi        
    done
}


#arguments - 1. filename, 2. input folder in local, 3. output folder in local, 4. pubmatic ID
create_pubmatic_file() {
    echo "start pubmatic process"       
    mkdir -p $3
    INPUT_FILES="$2/*$1*"    	
    for f in $INPUT_FILES
    do
        if [[ "$f" == *"$1"* ]]
        then
            timestamp=$(date +%s%3N)
            echo $timestamp
            echo "started md5sum"
            md5=$(md5sum $f | awk '{ print $1 }')
            echo $md5            
            actual_name=$(basename $f)
            echo "started gzipping"            
            pigz -k -p5 $2/$actual_name
            echo "gzipped file"
            filecounts=$(cat $2/$actual_name | wc -l)
            filename="$4_${timestamp}_${md5}_${filecounts}.tsv.gz"            
            mv $2/$actual_name.gz $3/$filename
            echo "pubmatic file ($actual_name) ready for STS"
            sleep 10
            rm -rf $2/$actual_name && echo "removed uncompressed file"
            if [ -e $3/$filename ]
            then
            	echo "$filename exists in $3"
            else
            	echo "$filename not found in local path"
            fi
        else
            echo "pubmatic file doesn't exist in $2"
        fi
    done
}


#arguments - 1. filename/filepattern, 2. input folder in local, 
ungzip_file(){    
    INPUT_FILES="$2/*$1*.gz"
    echo "searching for $1 files in $INPUT_FILES"
    for f in $INPUT_FILES
    do
        [ -f "$f" ] || continue # skip if file is invalid
        gunzip $f && echo "unzipped gzipped file"               
    done
}


#Note: Doesn't move if file doesn't exist at source or it already exists at destination
#arguments: 1. locals or hdfs, 2. filename or pattern, 3. source folder (local or hdfs), 4. destination folder (local or hdfs)
move_file(){
    if [[ $1 == "locals" ]]; then
        mkdir -p $4
        echo "searching for files matching pattern"
        INPUT_FILES=$3/*$2*
        echo "moving local file"
        IFS=$'\n'
        for i in $INPUT_FILES
        do
            file_name=$(basename $i)
            if test -f "$i"; then
                if test -f "$4/$file_name"; then
                    echo "$file_name already exists in $4, cannot move"
                else
                    mv "$i" "$4"
                    echo "moved $i into $4"
                fi
            else
                echo "$i doesn't exist"
            fi
        done
        unset IFS
    else
        hdfs dfs -mkdir -p $4
        echo "moving HDFS file"
        INPUT_FILES=$(hdfs dfs -ls $3 | grep -E $(echo $2 | sed 's/\*/\.\*/g') | awk '{print $8}')
        for i in $INPUT_FILES
        do
            file_name=$(basename $i)
            if hdfs dfs -test -e $i; then
                if hdfs dfs -test -e $4/$file_name; then
                    echo "$file_name already exists in $4, will not move"
                else
                    hdfs dfs -mv $i $4
                    echo "moved $i into $4"
                fi
            else
                echo "$i doesn't exist"
            fi
        done
    fi
}


#arguments: 1. folder where the files to removed exist, 2. required age of file, if file's age is older than this then it will be removed
find_and_remove_files(){
    FILES_TO_DELETE=$(find $1 -mindepth 1 -mtime +$2)
    if [ -z "$FILES_TO_DELETE" ]; then
        echo "no files found in $1 that are older than $2 days"
    else
        for f in $FILES_TO_DELETE; do
            rm -rf $f
            echo "deleted $f"
        done
    fi
}

# Check if the resources are currently being used by another process
# process_using_resources(){ # TODO this needs to output a list of the lock files, otherwise it'll simply overwrite the variable 
#     # Default to 0 if no lock files are found
#     resources_locked=0
    
#     # Check for various lock files
#     if [ -f "$CS_LOCK_FILE" ]; then
#         resources_locked="Consumer Sync"
#     elif [ -f "$MKP_LOCKFILE" ]; then
#         resources_locked="Custom Segment"
#     elif [ -f "$IDG_LOCK_FILE" ]; then
#         resources_locked="ID Graph"
#     elif [ -f "$PUBM_LOCK_FILE" ]; then
#         resources_locked="Pubmatic"
#     fi

#     # Echo the result (string or 0)
#     echo "$resources_locked"
# }

# Function to create directories in HDFS if they don't already exist
create_hdfs_dirs() {
    # Example usage: pass the array of directories to the function
    # > DIRS_TO_MAKE=(/path/dir1 /path/dir2 /path/dir3)
    # > create_hdfs_dirs "${DIRS_TO_MAKE[@]}"
    
    local dirs=("$@")  # Accept directories as arguments
    for dir in "${dirs[@]}"; do
        hdfs dfs -test -d "$dir" 2>/dev/null  # Redirect stderr to /dev/null to silence warnings
        if [ $? -ne 0 ]; then
            hdfs dfs -mkdir -p "$dir" 2>/dev/null  # Silence mkdir stderr as well
            printf "success - %s created\n" "$dir"
        else
            printf "ERROR - %s already exists\n" "$dir"
        fi
    done
}

run_bash_script(){
    local bash_script_path=$1
    local log_file=$2

    # Extract the script name from the path
    local bash_script_name=$(basename $bash_script_path)

    # Check if a script exists and run it
    if [ -f $bash_script_path ]; then
        printf "%`tput cols`s" | tr ' ' '#' | tee -a $log_file # add a row of '#' to separate the logs
        log_message $log_file "Running script: ${bash_script_name}\n"
        bash $bash_script_path

        # Check if the script ran successfully
        if [ $? -eq 0 ]; then
            log_message $log_file "\tScript ran successfully\n"
        else
            log_message $log_file "\tError running script\n"
        fi
        
    else
        log_message $log_file "Script not found: ${bash_script_path}\n"
    fi
}

list_hdfs_dirs() { 
    # Function to list subdirectory paths in an HDFS directory, ordered by modification time
    # The latest modified directory will appear first in the output
    # With latest_only=true, returns only the most recently modified directory
    #
    # Usage:
    # > latest_dir=$(list_hdfs_dirs "/path/to/hdfs/dir" "pattern" true)
    # > all_dirs=$(list_hdfs_dirs "/path/to/hdfs/dir" "pattern")

    local dir_path="$1"   # The HDFS directory path
    local pattern="$2"    # Optional pattern to filter the subdirectories
    local latest_only="$3"  # Optional flag to return only the latest directory

    # Check if the directory exists
    if ! hdfs dfs -test -d "$dir_path" 2>/dev/null; then
        >&2 echo "ERROR: Directory $dir_path does not exist on HDFS."
        return 1
    fi

    # Get the full ls output
    local ls_output
    ls_output=$(hdfs dfs -ls "$dir_path" 2>/dev/null)

    # If the directory is empty, return empty
    if [ "$(echo "$ls_output" | wc -l)" -le 1 ]; then
        return 0
    fi
    
    # Fetch the list of subdirectory paths, sorted by newest first
    local input_dir_paths
    input_dir_paths=$(echo "$ls_output" \
        | grep '^d' \
        | sort -k6,7r \
        | awk '{print $8}')

    # If a pattern is provided, filter the directories
    if [ -n "$pattern" ]; then
        input_dir_paths=$(echo "$input_dir_paths" | while IFS= read -r sub_dir_path; do
            sub_dir_name=$(basename "$sub_dir_path")
            if [[ "$sub_dir_name" =~ $pattern ]]; then
                echo "$sub_dir_path"
            fi
        done)
    fi

    # If no directories remain after filtering, return empty
    if [ -z "$input_dir_paths" ]; then
        return 0
    fi

    # If latest_only is true, return just the first line
    if [[ "$latest_only" == "true" ]]; then
        echo "$input_dir_paths" | head -n 1
    else
        # Output all matched and sorted directory paths
        echo "$input_dir_paths"
    fi
}


check_filepath_exists(){
    # Function to check if a file exists either on HDFS or local file system.
    # If the file does not exist, it logs the error and sends an alert email.
    # EXAMPLE USAGE:
    # check_filepath_exists /user/unity/consumersync/stats/eu/monthly/eu_agg_2024-09.csv "EU Monthly Stats" "$CS_LOG_FILE"
    
    # Arguments
    local filepath="$1"        # Full path to the file to be checked (can be Local or HDFS)
    local process_name="$2"     # Process name to be included in the logs and alert (e.g. "EU Monthly Stats")
    local log_file="$3"         # Path to the log file where results will be logged (e.g. "$CS_LOG_FILE")

    # Extract filename and directory path from the given file path
    local filename=$(basename $filepath)  # Extracts the file name from the full file path
    local dirpath=$(dirname $filepath)    # Extracts the directory path from the full file path

    # Determine if the file is located on HDFS or the local file system
    # If the filepath starts with "/user/unity", it's considered to be on HDFS, otherwise it's local.
    if [[ $filepath == /user/unity* ]]; then
        local local_or_hdfs="hdfs"  # File is on HDFS
        local find_file_command="hdfs dfs -test -e $filepath 2>/dev/null"  # Command to check for file existence on HDFS
    else
        local local_or_hdfs="local" # File is on the local filesystem
        local find_file_command="test -e $filepath"  # Command to check for file existence locally
    fi

    # Log the action of checking the file's existence on either HDFS or local system
    log_message $log_file "Checking ${SERVER^} $local_or_hdfs for $filepath"    

    # Prepare details for the alert email if the file is missing
    alert_body="Hi,\n$process_name did not produce the output '$filename'.  Please see below for details.\n\n\tServer:\t\t${SERVER^}\n\tFilepath:\t$filepath\n\tLog File:\t$log_file"
    alert_subject="$process_name file not found ($filename)"   # Email subject line
    alert_from="emsactivate@experian.com"  # Sender email address for the alert
    alert_to="$MAILLIST"  # Recipient email address for the alert

    # Check if the file exists using the previously determined command (HDFS or local)
    if eval $find_file_command; then
        # If the file exists, log success and include the directory path
        log_message "$log_file" "\tsuccess - $filename exists in $local_or_hdfs: $dirpath\n"
    else
        # If the file does not exist, log an error, send an alert email, and log the alert action
        log_message "$log_file" "\tERROR - $filename does NOT exist in $local_or_hdfs: $dirpath"
        printf "$alert_body" | mailx -r "$alert_from" -s "$alert_subject" "$alert_to"  # Send email alert
        
        # Check if the email was sent successfully
        if [ $? -eq 0 ]; then
            log_message "$CS_LOG_FILE" "\t\tAlert Email sent to $MAILLIST"
        else
            log_message "$CS_LOG_FILE" "\t\tError sending email to $MAILLIST"
        fi
    fi
}

lookup_client_name_from_cid(){
    # Example usage: 
    # client_name=$(lookup_client_name_from_cid "cid00003" "$CS_LOG_FILE")
    # # Check if the function succeeded (error code 0)
    # if [ $? -eq 0 ]; then
    #     echo "$cid Client name: $client_name"
    # else
    #     echo "$cid Client name: NONE FOUND!!"
    # fi
    
    local cid="$1"
    local log_file="$2"

    # Validate that the cid argument is not empty
    if [ -z "$cid" ]; then
        log_message "$log_file" "Error: CID argument is missing."
        return 1
    fi

    # Identify the most recent lookup file in the resources directory based on the naming pattern
    local lookup_filepath=$(ls -t $CS_RESOURCES_DIR/cid_name_cmat_lookup_????-??-??.csv | head -n 1) # e.g. /home/unity/match-consumer-sync/resources/cid_name_cmat_lookup_2021-09-01.csv

    # Check if lookup file was found
    if [ ! -f "$lookup_filepath" ]; then
        log_message "$log_file" "Error: Lookup file not found."
        return 1
    fi

    # Find the row in the lookup file with the matching cid
    local matching_row=$(grep -m 1 "$cid" "$lookup_filepath")

    # If no matching cid is found, exit with an error message
    if [ -z "$matching_row" ]; then
        log_message "$log_file" "Error: CID $cid not found in lookup file."
        return 1
    fi

    # Extract the client name from the matching row (2nd column in CSV)
    local raw_client_name=$(echo "$matching_row" | cut -d',' -f2)

    # Transform the client name to lowercase and replace spaces with underscores
    local clean_client_name=$(echo "$raw_client_name" | tr '[:upper:]' '[:lower:]' | tr ' ' '_')

    # Output the cleaned client name
    echo "$clean_client_name"
}


# Function to check if a file with a given name pattern in a given folder is older than 15 minutes
# Returns 0 if the file is older than 15 minutes, 1 otherwise
# Arguments: 1. Input folder , 2. File pattern
is_file_ingestible() {
    local folder=$1
    local pattern=$2

    # Get the latest file matching the pattern
    local latest_file=$(find "$folder" -maxdepth 1 -name "*${pattern}*" -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2-)

    # Check if no files found or if the file has a .inprogress extension
    if [ -z "$latest_file" ]; then
        echo "No files found matching the pattern."
        return 1
    elif [[ "$latest_file" == *.inprogress ]]; then
        echo "The latest file has a .inprogress extension."
        return 1
    else
        echo "Latest file found: $latest_file"
    fi

    # Get the current time and the file's modification time
    local current_time=$(date +%s)
    local file_mod_time=$(stat -c %Y "$latest_file")

    # Calculate the time difference in minutes
    local time_diff=$(( (current_time - file_mod_time) / 60 ))

    # Check if the time difference is greater than 15 minutes
    if [ $time_diff -gt 15 ]; then
        return 0
    else
        return 1
    fi
}

get_csv_column_value() {
    local csv_filepath="$1"
    local column_name="$2"
    
    if [[ ! -f "$csv_filepath" ]]; then
        echo "Error: CSV file not found at '$csv_filepath'" >&2
        return 1
    fi
    
    # Using Python for robust CSV parsing
    python3 -c "
import pandas as pd
import sys
try:
    df = pd.read_csv('$csv_filepath')
    if '$column_name' in df.columns:
        print(df['$column_name'].iloc[0])
    else:
        sys.exit('Column \"$column_name\" not found')
except Exception as e:
    sys.exit(str(e))
"
    return $?
}

get_filename_date() {
    # Extracts the date part of a filename (after the last "_")
    # Example usage:
    # filename_date=$(get_filename_date "/user/unity/id_graph/lookup/experian/match_id_lookup_experian_202506140915.parquet")

    local filepath="$1"                                                 # "/user/unity/id_graph/lookup/experian/match_id_lookup_experian_202506140915.parquet"
    local filename=$(basename "$filepath")                              # "match_id_lookup_experian_202506140915.parquet"
    local filestem="${filename%.*}"                                     # "match_id_lookup_experian_202506140915"
    local date_part=$(echo "$filestem" | sed -E 's/.*_([^_]+)$/\1/')    # "202506140915"

    if [[ -z "$date_part" ]]; then
        echo "Error: Could not extract date part from '$filepath'" >&2
        return 1
    fi

    echo "$date_part"
}

latest_filepath_in_local_dirpath() {
    # Description:
    #   Given a directory and an optional filename pattern, this function returns the
    #   latest modified file that matches the pattern. If no pattern is provided,
    #   it defaults to '*' (all files). It supports wildcards in the pattern.

    # Example usage:
    # latest_log_filepath=$(latest_filepath_in_local_dirpath "/blah/logs" "*.log")
    # echo "Latest log file: $latest_log_filepath"

    # Check for at least one argument (the directory)
    if [ "$#" -lt 1 ]; then
        echo "Usage: latest_filepath_in_local_dirpath <directory> [pattern]" >&2
        return 1
    fi

    local dirpath="$1"
    local pattern="${2:-*}"

    # Verify that the directory exists.
    if [ ! -d "$dirpath" ]; then
        echo "Error: Directory '$dirpath' does not exist." >&2
        return 1
    fi

    # Use find to list only regular files that match the pattern.
    # The "-maxdepth 1" ensures that we only search in the provided directory,
    # and not recursively.
    local filepaths
    filepaths=$(find "$dirpath" -maxdepth 1 -type f -name "$pattern" 2>/dev/null)

    # Check if any filepaths matched the pattern.
    if [ -z "$filepaths" ]; then
        echo "Error: No filepaths matching pattern '$pattern' found in '$dirpath'." >&2
        return 1
    fi

    # Find the latest filepath by modification time using stat.
    local latest_filepath=""
    local latest_time=0
    for filepath in $filepaths; do
        # Get modification time in seconds since the epoch.
        local mtime
        mtime=$(stat -c %Y "$filepath")
        if [ "$mtime" -gt "$latest_time" ]; then
        latest_time="$mtime"
        latest_filepath="$filepath"
        fi
    done

    # Check if a latest filepath was found.
    if [ -z "$latest_filepath" ]; then
        echo "Error: Could not determine the latest filepath." >&2
        return 1
    fi

    echo "$latest_filepath"
}


#Check file exists, Arguments: 1. HDFS input location, 2. Filename, 3. Process Name
check_file_exists(){
    if hdfs dfs -test -e $1/$2
    then
        echo "$1/$2 exists"
    else
        echo "$1/$2 doesn't exist"
        echo "$2 not found in $1, please check logs" | mailx -r "idg_process_monitor@experian.com" -s "Latest ID-Graph ($3) - not found" $MAILLIST
        return 1
    fi
}