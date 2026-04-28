#!/bin/bash

source conf.sh
source process_functions.sh

LOGS_PATH="${LOGS_DIR}/ingest_do_${RUNDATE}.log"
INGEST_LOCK_FILE="/tmp/ingest_do_process_flow.lock"


#Arguments: 1. HDFS input location, 2. Filename, 3. Process Name
check_hdfs_file_exists(){
    local hdfs_dir="$1"
    local filename="$2"

    if hdfs dfs -test -e $hdfs_dir/$filename
    then
        echo "$hdfs_dir/$filename exists"
        return 0
    else
        echo "$hdfs_dir/$filename doesn't exist"        
        return 1
    fi
}

# Function to check if a file already exists in local
check_local_file_exists() {
    local local_dir="$1"
    local filename="$2"

    if [ -f "$local_dir/$filename" ]; then
        echo "File $local_dir/$filename already exists in local."
        return 0
    else
        echo "File $local_dir/$filename does not exist in local."
        return 1
    fi
}


# Function to get the latest file from a source directory and move it to a destination directory if it doesn't exist in HDFS
# Arguments: 1. Source directory, 2. Prefix of the file, 3. Suffix of the file, 4. Destination directory, 5. HDFS directory, 6. (Optional) Move instead of copying
# Returns: 0 if successful, or if no files found or if file already exists in HDFS or local path, 1 if failed to copy or move
get_latest_file() {
    local src="$1"
    local prefix="$2"
    local suffix="$3"
    local dest="$4"
    local hdfs_dir="$5"  # if "no_hdfs" provided, it will not check HDFS
    local move_only="${6:-false}"  # Default to false if not provided
    
    # Get the latest file matching the pattern
    latest_file=$(ls "$src" | grep "^${prefix}.*${suffix}" | sort | tail -n 1)

    if [ -z "$latest_file" ]; then
        echo "No files found matching pattern '^$prefix*$suffix$' in folder '$src'."
        return 0
    fi
        
    filepath="$src/$latest_file"
    echo "Full path of file: $filepath"

    if [ "$hdfs_dir" != "no_hdfs" ]; then        
        # exit if the file already exists in HDFS
        if check_hdfs_file_exists "$hdfs_dir" "$latest_file"; then
            echo "File $latest_file already exists in HDFS, skipping..."
            return 0
        fi
    fi

    # exit if the file already exists in local
    if check_local_file_exists "$dest" "$latest_file"; then
        echo "File $latest_file already exists in local, skipping..."
        return 0
    fi

    # if move_only is equal to string "move", move the file instead of copying
    if [ "$move_only" == "move" ]; then
        echo "Moving file $filepath to $dest"
        move_file locals "$latest_file" "$src" "$dest" && check_local_file_exists "$dest" "$latest_file"        
    else
        echo "Copying file $filepath to $dest"
        cp "$filepath" "$dest"
    fi

    # return 1 if the above copying or moving command failed
    if [ $? -ne 0 ]; then
        if [ "$move_only" == "move" ]; then
            echo "Failed to move file $filepath to $dest"
        else
            echo "Failed to copy file $filepath to $dest"
        fi
        return 1
    fi
}


## Ingest latest files
ingest_files(){    
    echo "Starting ingestion process..."
    local failure_count=0
    local failed_files=()

    increment_failure_count() {
        failure_count=$((failure_count + 1))
        failed_files+=("$1")        
    }
    ###################################

    # Digital Taxonomy
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "A01_" "_emsf_master.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "A01"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "A08_" "_absm_master.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "A08"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "A16_" "_cbaf_utility.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "A16"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "A40_" "_address_output.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "A40"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "A44_" "_cbaf_address.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "A44"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "A67_" "_cbaf_address.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "A67"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F15_" "_ChannelView_ProspectView.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "F15_DT"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F35_" "_P60_consview.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "F35"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F45_" "_cbaf_utility_1.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "F45_DT"
    get_latest_file "$AD_HOC_DIR" "R45_PostcodeDirectory_" ".csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" "move" || increment_failure_count "R45" ## name could change once Ross confirms
    get_latest_file "$AD_HOC_DIR" "Digital_Taxonomy_Master_Layout_" ".xlsx" "$DIGITAL_TAX_BASEDIR" "no_hdfs" || increment_failure_count "Master_Layout"
    get_latest_file "$AD_HOC_DIR" "YouGov" ".csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "Yougov" ##TODO: Analytics haven't confirmed
    get_latest_file "$ANALYTICS_INDIR" "ADHOC_SUPPRESSION" ".csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "Adhoc_Suppression" ##TODO: Analytics haven't confirmed
    get_latest_file "$ANALYTICS_INDIR" "Boots_Model_Segments_lookup" ".xlsx" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "Boots_Model_Segments_lookup" ##TODO: Analytics haven't confirmed
    get_latest_file "$ANALYTICS_INDIR" "Standard_Audiences" "_millitiles.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "SA_Millitiles" ##TODO: Analytics haven't confirmed
    get_latest_file "$ANALYTICS_INDIR" "Standard_Audiences" "_cutoffs.csv" "$DIGITAL_TAX_INDIR" "$DIGITAL_TAX_HDFS_INDIR" || increment_failure_count "SA_Cutoffs" ##TODO: Analytics haven't confirmed

    # Graphs
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F15_" "_ChannelView_LinkageView.csv" "$GRAPH_INDIR" "$IDG_HDFS_PATH/hems/*/input" || increment_failure_count "F15_IDG"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F16_" "_ChannelView_LinkageView.csv" "$GRAPH_INDIR" "$IDG_HDFS_PATH/mobile/*/input" || increment_failure_count "F16"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F35_" "_P60_consview.csv" "$GRAPH_INDIR" "$IDG_HDFS_PATH/cv/*/input" || increment_failure_count "F35_IDG"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F45_" "_cbaf_utility_1.csv" "$GRAPH_INDIR" "$IDG_HDFS_PATH/pc/*/input" || increment_failure_count "F45_IDG"
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F65_" "_cbaf_utility_1.csv" "$GRAPH_INDIR" "$IDG_HDFS_PATH/pc/*/input" || increment_failure_count "F65"

    # Audience Engine
    get_latest_file "$DATA_OFFICE_LOCAL_DIR" "F03_" "_PAMDirectory.csv" "$GRAPH_INDIR" "$IDG_HDFS_PATH/pc/*/input" || increment_failure_count "F03"

    ###################################
    # Return 1 if there were any failures
    if [ $failure_count -gt 0 ]; then
        echo "Ingestion process failed for the following files: ${failed_files[*]}, total failure count is $failure_count, please check logs at $LOGS_PATH" | mailx -r "process_monitor@experian.com" -s "Data Office File Ingestion - failed." $MAILLIST
        return 1
    fi

    echo "Ingestion process completed successfully."
    return 0

}


#Check if previous flow is running currently
can_do_process_run(){
    if [ -f $INGEST_LOCK_FILE ]
    then
        echo "Data Office ingestion process is already running"
        return 1
    else
        return 0
    fi
}


run_ingestion(){
    if can_do_process_run $INGEST_LOCK_FILE 
    then
        echo "######################################################"
        touch $INGEST_LOCK_FILE && echo "created ingestion lock file"        
        echo "Starting ingestion process for $RUNDATE"        
        ingest_files
        if [ $? -eq 1 ]; then
            echo "Alert: Ingestion of Data Office files failed with exit status 1, please check logs at ${LOGS_PATH}" | mailx -r "process_monitor@experian.com" -s "Data Office File Ingestion - failed." $MAILLIST
            return 1
        fi
        rm $INGEST_LOCK_FILE && echo "removed ingestion lock file"        
    else
        echo "Data Office ingestion process is already running on $RUNDATE"
    fi
}

run_ingestion  2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> $LOGS_PATH