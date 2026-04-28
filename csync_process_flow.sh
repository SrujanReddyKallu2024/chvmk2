#!/bin/bash

# SCRIPT SUMMARY
#   Run in cron to trigger the Consumer Sync process flow
#   This script will:
#       - Handle new Consumer Sync input files
#       - Trigger any Consumer Sync processes that are required
#       - Handle any scheduled reports

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# SOURCE SCRIPTS
source conf.sh
source process_functions.sh

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# SCRIPT VARIABLES
# =======================================

# Ensure all .sh and .py scripts in the CS_EVENT_SCRIPT_DIR directory are executable
for script in "${CS_EVENT_SCRIPT_DIR}/*.*" ; do
    if [ -f "$script" ]; then
        chmod +x "$script"
    fi
done

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# GENERAL SUPPORTING FUNCTIONS
# =======================================

function cs_process_can_run(){
    if [ -f $CS_LOCK_FILE ]
    then
        return 1  
    else
        return 0
    fi
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# NEW PARTNER DATA FILES
# =======================================

ingest_and_preprocess_partner_data_files(){
    printf "PARTNER DATA - checking for new files...\n"
    
    run_bash_script "${CS_EVENT_SCRIPT_DIR}/ingest_preproc_pulse_max.sh" $CS_LOG_FILE

    run_bash_script "${CS_EVENT_SCRIPT_DIR}/ingest_worldview.sh" $CS_LOG_FILE
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# NEW MARKETPLACE INPUT FILES
# =======================================

ingest_and_preprocess_mkt_input_files(){
    printf "MARKETPLACE INPUT - checking for new files...\n"

    run_bash_script "${CS_EVENT_SCRIPT_DIR}/ingest_onboarding.sh" $CS_LOG_FILE

    run_bash_script "${CS_EVENT_SCRIPT_DIR}/ingest_cid_name_lookup.sh" $CS_LOG_FILE
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# NEW CLIENT DATA FILES 
# =======================================

ingest_and_preprocess_client_data_files(){
    printf "CLIENT DATA - checking for new Local raw client data files...\n"

    run_bash_script "${CS_EVENT_SCRIPT_DIR}/ingest_client_raw_data.sh" $CS_LOG_FILE

    run_bash_script "${CS_EVENT_SCRIPT_DIR}/housekeep_errors_dir.sh" $CS_LOG_FILE

}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# RUN CONSUMER SYNC PROCESSES 
# =======================================

parse_processes_from_client_data_dir_name() {
    # Extracts the process name(s) from a directory name.
    # Expected dir_name format: {cidNNNNN}_{processes}_{yyyy-mm-dd-hh-mm}
    # Example: processes=$(parse_processes_from_client_data_dir_name "cid00000_eu_2024-01-01-12-01")
    #          echo processes -> "eu"
    # Example: processes=$(parse_processes_from_client_data_dir_name "cid00000_activate_insights_2024-01-01-12-01") 
    #          echo processes -> "activate insights"

    # Input: $1 (dir_name - the directory name string)
    local dir_name=$1

    # Check if input is provided
    if [ -z "$dir_name" ]; then
        log_message "$CS_LOG_FILE" "ERROR - parse_processes_from_client_data_dir_name function - No directory name provided for parsing.\n"
        return 1
    fi

    # Ensure the directory name contains at least two "_" characters for proper extraction
    if ! echo "$dir_name" | grep -q "_.*_"; then
        log_message "$CS_LOG_FILE" "ERROR - parse_processes_from_client_data_dir_name - Invalid directory name format. Must contain at least two '_' characters.  Received ${dir_name}\n"
        return 1
    fi

    # Remove the "cid" prefix, extract the process name, and remove the date suffix
    # 1. `cut -d'_' -f2-` removes the "cidNNNNN" part.
    # 2. `rev | cut -d'_' -f2- | rev` removes the date part by reversing the string, removing the date, and reversing it back.
    # 3. `sed 's/_/ /g'` replaces any remaining underscores in the process name with spaces.
    processes=$(echo "$dir_name" | cut -d'_' -f2- | rev | cut -d'_' -f2- | rev | sed 's/_/ /g')

    # Check if the process extraction was successful
    if [ -z "$processes" ]; then
        log_message "$CS_LOG_FILE" "ERROR - parse_processes_from_client_data_dir_name - Failed to extract processes from directory name (${dirname}).\n"
        return 1
    fi

    # Output the extracted processes
    echo "$processes"
    return 0
}

trigger_specified_csync_process(){
    local process=$1
    local dir_name=$2

    # Depending on what the $process is, run the appropriate bash script in the appropriate repo
    log_message "$CS_LOG_FILE" "\t\tTriggering process: ${process}\n"

    # Test the script exists and run it
    specific_repo=$(printf "$CS_REPO_PROCESS" "${process}") # "/home/unity/consumer-sync-%s" --> "/home/unity/consumer-sync-insights"
    script_to_run="${specific_repo}/bash_scripts/process_${process}.sh"
    if [ -f "$script_to_run" ]; then
        run_bash_script "$script_to_run" "$CS_LOG_FILE"
        exit_code=$?
        
        # If the script fails, attempt to move the file to an errors directory
        if [ $exit_code -ne 0 ]; then
            log_message "$CS_LOG_FILE" "ERROR - ConsumerSync Process failed (${process} process)\n"
            
            if hdfs dfs -mv "${CS_CLIENT_CLEAN_DIR}/$dir_name" "${CS_CLIENT_ERRORS_DIR}/${dir_name}"; then
                log_message "$CS_LOG_FILE" "INFO - Files (${dir_name}) successfully moved to errors directory\n"
            else
                log_message "$CS_LOG_FILE" "ERROR - Failed to move client files (${dir_name}) to errors directory\n"
            fi
        fi
    else
        log_message "$CS_LOG_FILE" "ERROR - ConsumerSync script not found at ${script_to_run}\n"
    fi
}

delete_or_move_client_data_files() {
    local dir_name=$1
    local clean_filepath="${CS_CLIENT_CLEAN_DIR}/${dir_name}"
    local cid=$(echo "$dir_name" | cut -d'_' -f1) # Extract the client ID from the directory name, part before the first "_" character
    local archive_filepath="${CS_CLIENT_ARCHIVE_DIR}/${cid}/"
    local raw_filepath="${CS_CLIENT_RAW_DIR}/${dir_name}"
    
    log_message "$CS_LOG_FILE" "\tHandling 'raw' and 'clean' client data directories on HDFS (${dir_name})\n"

    # Delete "clean" directory if it's an "_eu" directory, else move to archive
    if echo "$dir_name" | grep -q "_eu"; then
        log_message "$CS_LOG_FILE" "\t\tDeleting EU 'clean' directory: ${clean_filepath}\n"
        hdfs dfs -rm -r "$clean_filepath"
    else
        log_message "$CS_LOG_FILE" "\t\tMoving UK 'clean' directory to archive: ${dir_name}\n"
        hdfs dfs -mkdir -p "$archive_filepath"
        hdfs dfs -mv "$clean_filepath" "$archive_filepath"
    fi

    # Check if the clean filepath was handled correctly            
    if hdfs dfs -test -e "$clean_filepath"; then
        log_message "$CS_LOG_FILE" "ERROR - Failed to handle 'clean' client directory, it still exists at '${clean_filepath}'\n"
    fi

    # Delete the 'raw' directory
    hdfs dfs -rm -r "$raw_filepath"
    if hdfs dfs -test -e "$raw_filepath"; then
        log_message "$CS_LOG_FILE" "ERROR - Failed to delete 'raw' client directory, it still exists at '${raw_filepath}'\n"
    fi
}

run_csync_processes_on_client_data_files(){
    printf "CONSUMER SYNC - Checking if new HDFS clean client data files need to be processed...\n"

    # Get a list of all the directories in the CS_CLIENT_CLEAN_DIR
    mapfile -t INPUT_DIR_PATHS < <(list_hdfs_dirs "$CS_CLIENT_CLEAN_DIR")

    # Check if INPUT_DIR_PATHS has any elements
    if [ ${#INPUT_DIR_PATHS[@]} -eq 0 ]; then
        printf "No new client data directories in $CS_CLIENT_CLEAN_DIR\n"
        return 0
    fi

    log_message "$CS_LOG_FILE" "${#INPUT_DIR_PATHS[@]} new client data directories found in ${CS_CLIENT_CLEAN_DIR}\n"

    for dir_path in "${INPUT_DIR_PATHS[@]}"; do
        dir_name=$(basename "$dir_path")
        log_message "$CS_LOG_FILE" "\tProcessing new dir:\t${dir_name}\n"

        # Extract processes
        processes=$(parse_processes_from_client_data_dir_name "$dir_name")

        # If no processes were extracted, move the directory to errors
        if [ -z "$processes" ]; then
            log_message "$CS_LOG_FILE" "ERROR - No processes extracted from ${dir_name}. Moving to errors directory.\n"
            hdfs dfs -mv "${CS_CLIENT_CLEAN_DIR}/$dir_name" "${CS_CLIENT_ERRORS_DIR}/${dir_name}"
            continue
        fi

        log_message "$CS_LOG_FILE" "\t\tProcesses:\t${processes}\n"

        # Create an updated match_id lookup file for the client if required
        cid=$(echo "$dir_name" | cut -d'_' -f1) # Extract the client ID from the directory name, part before the first "_" character
        export CID_NEEDING_MATCH_ID_LOOKUP="$cid" # Set the CID needing Match ID Lookup as an environment variable for the script
        bash "${CS_EVENT_SCRIPT_DIR}/generate_hhk_lookup.sh" # this script requires a client nCID_NEEDING_MATCH_ID_LOOKUPame as an argument (cid value is used as the client name)
        # bash "${CS_EVENT_SCRIPT_DIR}/generate_hhk_lookup.sh" "$cid" # this script requires a client name as an argument (cid value is used as the client name)
        if [ $? -ne 0 ]; then
            log_message "$CS_LOG_FILE" "ERROR - Failed to check whether an updated Match ID Lookup file is required for client ${cid}. Moving to errors directory.\n"
            hdfs dfs -mv "${CS_CLIENT_CLEAN_DIR}/$dir_name" "${CS_CLIENT_ERRORS_DIR}/${dir_name}"
            continue
        fi

        process_failed=false

        for process in $processes; do
            trigger_specified_csync_process "$process" "$dir_name"
            exit_code=$?

            if [ $exit_code -ne 0 ]; then
                process_failed=true
                log_message "$CS_LOG_FILE" "ERROR - Process ${process} failed for ${dir_name}. Moving to errors directory.\n"
                hdfs dfs -mv "${CS_CLIENT_CLEAN_DIR}/$dir_name" "${CS_CLIENT_ERRORS_DIR}/${dir_name}"
                break  # Stop processing this dir_name, move to next one
            fi
        done

        # If all processes were successful, clean up the directory
        if [ "$process_failed" = false ]; then
            log_message "$CS_LOG_FILE" "\t\tSuccessfully processed directory: ${dir_name}\n"
            delete_or_move_client_data_files "$dir_name"
        fi
    done
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# SCHEDULED REPORTS
# =======================================

handle_scheduled_reports(){
    printf "SCHEDULED REPORTS - generating Consumer Sync reports (e.g. Monthly stats summary)...\n"

    # Define processes that require monthly reports
    local monthly_reports=("eu" "eutest" "test" "insights" "matchback") # add any new processes here which require monthly reports

    # Loop through each process and run the corresponding monthly report script
    for process in "${monthly_reports[@]}"; do
        # Generate the repo path using the cs_repo function
        local repo_path
        repo_path=$(printf "$CS_REPO_PROCESS" "${process}") # e.g. /home/unity/consumer-sync-insights
        
        # Construct the full path to the report script
        local script_path="${repo_path}/bash_scripts/report_${process}_monthly.sh" # e.g. /home/unity/consumer-sync-insights/bash_scripts/report_insights_monthly.sh

        run_bash_script "$script_path" "$CS_LOG_FILE"
    done

}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =======================================
# MAIN FUNCTION
# =======================================

main(){
    if cs_process_can_run; then
        printf "\nStarting ${0} script\n\n"

        # Create a lock file
        touch $CS_LOCK_FILE && log_message "$CS_LOG_FILE" "Created CSYNC lock file at ${CS_LOCK_FILE}\n"

        # Prepare new files for use by Consumer Sync processes
        ingest_and_preprocess_partner_data_files
        ingest_and_preprocess_mkt_input_files
        ingest_and_preprocess_client_data_files

        # Run the Consumer Sync processes
        # run_csync_processes_on_client_data_files

        # Routine reporting, e.g. Monthly reports
        handle_scheduled_reports

        # Remove Lock file
        rm $CS_LOCK_FILE && log_message "$CS_LOG_FILE" "Removed CSYNC lock file from ${CS_LOCK_FILE}\n"
    else
        printf "\nCannot start ${0} script as there is a Consumer Sync lock file present (${CS_LOCK_FILE})\n"
    fi
}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# =================================================
# RUN MAIN FUNCTION AND OPTIONALLY EMAIL ERRORS
# =================================================

# Run the main function
main