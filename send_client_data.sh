#!/bin/bash

source /etc/profile.d/hadoop.sh
source conf.sh
source process_functions.sh

CD_LOCK_FILE="/tmp/client_data_send.lock"

# check if previous flow is running
function can_cd_process_run(){
    if [ -f $CD_LOCK_FILE ]
    then
        return 1
    else
        return 0
    fi
}


#check if previous flow has finished, if so then run other scripts
run_cd_process_flow() {
    if can_cd_process_run
    then
        printf '#%.0s' {1..100}; echo
        touch $CD_LOCK_FILE && echo "created cd lock file"

        #started process
        starttime=$(date +%s)
        echo "Started sending client data"

        python3 "${SP_SCRIPT_DIR}/send_client_data_ontime.py" -et $MAILLIST

        status=$?
        if [ $status -eq 1 ]; then
            echo "Failed to send client data"
            return 1
        fi
        #finished process
        endtime=$(date +%s)
        duration=$(($endtime - $starttime))
        echo "Finished sending client data, took $duration seconds to complete"
        rm $CD_LOCK_FILE && echo "removed cd lock file"
    else
        echo "Client data send process is already running, skipping this run."
    fi
}

# Main script execution
run_cd_process_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/cdsend-$RUNDATE.log"

