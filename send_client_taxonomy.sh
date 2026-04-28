#!/bin/bash

source conf.sh
source process_functions.sh

RUNDATE_DAY=$(date -d "$RUNDATE" +%d)
CT_LOCK_FILE="/tmp/client_tax_flow.lock"

MONTHLY_CIDS=("clientA" "clientB" "clientC")

create_client_taxonomy() {
    if [ "${RUNDATE_DAY}" == "01" ]
    then
        for cid in "${MONTHLY_CIDS[@]}"
        do
            python3 "${SP_SCRIPT_DIR}/create_client_tax.py" -cid $cid -et $MAILLIST
        done
    else
        echo "${RUNDATE} is not the first of the month"
    fi
}


# check if previous flow is running
function can_ct_process_run(){
    if [ -f $CT_LOCK_FILE ]
    then
        return 1
    else
        return 0
    fi
}


#check if previous flow has finished, if so then run other scripts
run_ct_process_flow() {
    if can_ct_process_run
    then
        printf '#%.0s' {1..100}; echo
        touch $CT_LOCK_FILE && echo "created ct lock file"

        #started process
        starttime=$(date +%s)
        echo "Started Client taxonomy process"
        create_client_taxonomy
        status=$?
        if [ $status -eq 1 ]; then
            echo "Client taxonomy generation failed"
            return 1
        fi
        #finished process
        endtime=$(date +%s)
        duration=$(($endtime - $starttime))
        echo "Finished Client Taxonomy process, took $duration seconds to complete"
        rm $CT_LOCK_FILE && echo "removed ct lock file"
    else
        echo "Client Taxonomy process is already running, skipping this run."
    fi
}

# Main script execution
run_ct_process_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/ctax-$RUNDATE.log"
