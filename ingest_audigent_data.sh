#!/bin/bash

source conf.sh
source process_functions.sh

YESTERDAY_DATE=$(date -d "$RUNDATE -1 day" +%Y-%m-%d)
AUD_LOCK_FILE="/tmp/aud_graph_process_flow.lock"

# Function to ingest visits data from local folder and move to HDFS location
# Arguments: 1. Input folder, 2. HDFS folder
ingest_aud_visits() {
    local input_folder="$1"  
    local hdfs_folder="$2"

    if is_file_ingestible "$input_folder" "${YESTERDAY_DATE}__part"
    then
        if test -e "$input_folder/visits_raw_${RUNDATE}.parquet" > /dev/null
        then
            echo "Visits file for $RUNDATE already exists: ${input_folder}/visits_raw_${RUNDATE}.parquet" | mailx -r "process_monitor@experian.com" -s "Audigent Visits File Ingestion - Error" $MAILLIST
            return 1
        else
            echo "No visits file for $RUNDATE exists"
            mkdir -p "${input_folder}/visits_raw_${RUNDATE}.parquet" && echo "Created directory for visits data: ${input_folder}/visits_raw_${RUNDATE}.parquet"
        fi
        move_file locals "${YESTERDAY_DATE}__part" $input_folder "${input_folder}/visits_raw_${RUNDATE}.parquet"
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $input_folder -fp visits_raw_${RUNDATE}.parquet -et $MAILLIST
        transfer_file_to_hdfs visits_raw_${RUNDATE}.parquet $input_folder $hdfs_folder
        mv "${input_folder}/visits_raw_${RUNDATE}.parquet" $DATA_FROM_STS_BACKUP && echo "moved from_id5__$RUNDATE.parquet to $DATA_FROM_STS_BACKUP"    
        echo "Visits data ingested successfully."
    else
        echo "No new visits data to ingest for ${RUNDATE}."
    fi

}


# Function to ingest id data from local folder and move to HDFS location
# Arguments: 1. Input folder, 2. HDFS folder
ingest_aud_ids() {
    local input_folder="$1"  
    local hdfs_folder="$2"

    for aud_id in "${AUD_IDS[@]}"; do
        if is_file_ingestible "$input_folder" "${aud_id}__${YESTERDAY_DATE}__part"
        then
            if test -e "$input_folder/${aud_id}_raw_${RUNDATE}.parquet" > /dev/null
            then
                echo "${aud_id} file for $RUNDATE already exists: ${input_folder}/${aud_id}_raw_${RUNDATE}.parquet" | mailx -r "process_monitor@experian.com" -s "Audigent ${aud_id} File Ingestion - Error" $MAILLIST
                return 1
            else
                echo "No ${aud_id} file for $RUNDATE exists"
                mkdir -p "${input_folder}/${aud_id}_raw_${RUNDATE}.parquet" && echo "Created directory for ${aud_id} data: ${input_folder}/${aud_id}_raw_${RUNDATE}.parquet"
            fi
            move_file locals "${aud_id}__${YESTERDAY_DATE}__part" $input_folder "${input_folder}/${aud_id}_raw_${RUNDATE}.parquet"
            spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $input_folder -fp ${aud_id}_raw_${RUNDATE}.parquet -et $MAILLIST
            transfer_file_to_hdfs ${aud_id}_raw_${RUNDATE}.parquet $input_folder $hdfs_folder
            mv "${input_folder}/${aud_id}_raw_${RUNDATE}.parquet" $DATA_FROM_STS_BACKUP && echo "moved ${aud_id}_raw_${RUNDATE}.parquet to $DATA_FROM_STS_BACKUP"
            echo "${aud_id} data ingested successfully."
        else
            echo "No new ${aud_id} data to ingest for ${RUNDATE}."
        fi
    done
}


preprocess_aud_data(){
    echo "Running Spark preprocessing script for Audigent data"
    spark-submit --master yarn --queue $QUEUE1 --archives "hdfs:///user/unity/environments/tcfenv_2.zip#venv" $IDG_SCRIPTDIR/idg_aud_p10_ingest.py -m $RUNDATE -et $MAILLIST
    if [ $? -ne 0 ]; then
        echo "Error in preprocessing Audigent data for $RUNDATE"
        return 1
    fi
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_aud_stats.py -m $RUNDATE
    echo "Audigent data preprocessing completed for $RUNDATE"
}

run_uplift_stats(){
    if [ $RUNDATE == $SAT_DATE ]
    then
        spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_aud_uplift_stats.py -m $RUNDATE
    else
        echo "Today is not $SAT_DATE"
    fi
}


#Check if previous flow is running currently
can_aud_process_run(){
    if [ -f $AUD_LOCK_FILE ]
    then
        echo "Audigent ingestion is already running"
        return 1
    else
        return 0
    fi
}

#Process starts here
run_aud_ingestion_flow() {
    if can_aud_process_run
    then
        touch $AUD_LOCK_FILE && echo "created aud lock file"
        echo "Starting Audigent data ingestion process for $RUNDATE"
        ingest_aud_visits $AUD_VISITS_DIR "${AUD_HDFS_DIR}/${RUNDATE}"        
        ingest_aud_ids $AUD_IDMATCH_DIR "${AUD_HDFS_DIR}/${RUNDATE}"
        echo "Audigent data ingestion process completed for $RUNDATE"
        sleep 20
        preprocess_aud_data
        sleep 20
        run_uplift_stats
        rm $AUD_LOCK_FILE && echo "removed aud lock file"
        echo "Audigent process completed successfully for $RUNDATE"        
    else
        echo "Audigent data ingestion process cannot run for $RUNDATE. Exiting."
    fi
}

run_aud_ingestion_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/aud_ingest-$RUNDATE.log"