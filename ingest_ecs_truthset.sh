#!/bin/bash

source conf.sh
source process_functions.sh

trigger_ecs_ingestion() {
    if is_file_ingestible "$ECS_SOURCE_PATH" "ECS_Consumersync_Output"
    then
        echo "ECS file is ingestible, proceeding with ingestion..."        
        failed_files=()
        hdfs dfs -mkdir -p "$ECS_HDFS_PATH"
        all_files=$(ls "$ECS_SOURCE_PATH" | grep -E "ECS_Consumersync_Output.*\.csv")
        for file in $all_files
        do
            filename=$(basename "$file")
            # Check if the file is not already in HDFS if so don't copy
            if ! hdfs dfs -test -e "$ECS_HDFS_PATH/$RUNDATE/$filename"; then
                echo "Copying $file to HDFS..."
                hdfs dfs -mkdir -p "$ECS_HDFS_PATH/$RUNDATE/input"                
                hdfs dfs -copyFromLocal "$ECS_SOURCE_PATH/$filename" "$ECS_HDFS_PATH/$RUNDATE/input"
                if [ $? -ne 0 ]; then
                    echo "Failed to copy $file to $ECS_HDFS_PATH/$RUNDATE"
                    failed_files+=("$file")
                    continue
                else                    
                    echo "File $file copied to $ECS_HDFS_PATH/$RUNDATE successfully."                    
                fi                
            else
                echo "File $file already exists in HDFS, skipping..."
            fi
        done
        #
        if [ ${#failed_files[@]} -gt 0 ]; then            
            for failed in "${failed_files[@]}"; do
                echo "  - $failed"
            done
            echo "${#failed_files[@]} files failed to process: ${failed_files[@]}" |  mailx -r "process_monitor@experian.com" -s "ECS File Ingestion - Error" $MAILLIST
            return 1
        fi
        #
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $ECS_SOURCE_PATH -fp "ECS_Consumersync_Output"
    else
        echo "No new ECS file to ingest"
        return 2
    fi
}


# check if previous flow is running
function can_ecs_process_run(){
    if [ -f $ECS_LOCK_FILE ]
    then
        return 1
    else
        return 0
    fi
}


#check if previous flow has finished, if so then run other scripts
run_ecs_process_flow() {
    if can_ecs_process_run
    then
        printf '#%.0s' {1..100}; echo
        touch $ECS_LOCK_FILE && echo "created ecs lock file"
        #started process
        starttime=$(date +%s)
        echo "Started ECS ingestion process"
        trigger_ecs_ingestion
        status=$?
        if [ $status -eq 1 ]; then
            echo "ECS ingestion failed"
            return 1
        elif [ $status -eq 2 ]; then
            echo "No new ECS file"
            rm $ECS_LOCK_FILE && echo "removed ecs lock file"
            return 2
        fi
        spark-submit --master yarn --queue $QUEUE1 $SP_SCRIPT_DIR/create_ecs_base.py -m $RUNDATE
        move_file locals "ECS_Consumersync_Output" "$ECS_SOURCE_PATH" "$DATA_FROM_STS_BACKUP"
        #finished process
        endtime=$(date +%s)
        duration=$(($endtime - $starttime))
        echo "Finished ECS ingestion process, took $duration seconds to complete"
        rm $ECS_LOCK_FILE && echo "removed ecs lock file"        
    else
        echo "ECS process is already running, skipping this run."
    fi
}

# Main script execution
run_ecs_process_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/ecs_base-$RUNDATE.log"
