#!/bin/bash

source /etc/profile.d/hadoop.sh
source conf.sh

CURRENT_HOUR=$(date +%H)
IDS=( "ip" "fpid" "apnids" "ttdids" )
RT_LOCK_FILE="/tmp/rtapi_src_process_flow.lock"


run_graph_deduplication(){
    local failed_ids=()
    for id in "${IDS[@]}"
    do
        spark-submit --master yarn --queue $QUEUE1 $IDG_INSIGHTS_SCRIPTDIR/p10_dedupe_graphs.py -m $RUNDATE -it $id -et $MAILLIST        
        if [ $? -eq 0 ]; then
            echo "ran graph deduplication for $id"
        else
            echo "Failed to run graph deduplication for $id"
            failed_ids+=("$id")
        fi
    done

    # Report failures
    if [ ${#failed_ids[@]} -gt 0 ]; then
        echo "Failed IDs: ${failed_ids[*]}"
        return 1
    fi
}


run_tax_cv_formatting(){
    local fails=()
    spark-submit --master yarn --queue $QUEUE1 $IDG_INSIGHTS_SCRIPTDIR/p20_format_taxonomy.py -m $RUNDATE -et $MAILLIST
    if [ $? -eq 0 ]; then
        echo "ran taxonomy formatting"
    else
        echo "Failed to run taxonomy formatting"
        fails+=("taxonomy formatting")
    fi

    spark-submit --master yarn --queue $QUEUE1 $IDG_INSIGHTS_SCRIPTDIR/p30_format_cv.py -m $RUNDATE -et $MAILLIST
    if [ $? -eq 0 ]; then
        echo "ran CV formatting"
    else
        echo "Failed to run CV formatting"
        fails+=("CV formatting")
    fi

    # Report failures
    if [ ${#fails[@]} -gt 0 ]; then
        echo "Failed tasks: ${fails[*]}"
        return 1
    fi
}


run_segment_extraction(){
    local failed_ids=()
    for id in "${IDS[@]}"
    do
        spark-submit --master yarn --queue $QUEUE1 $IDG_INSIGHTS_SCRIPTDIR/p40_get_segs.py -m $RUNDATE -it $id -et $MAILLIST
        if [ $? -eq 0 ]; then
            echo "ran segment extraction for $id"
        else
            echo "Failed to run segment extraction for $id"
            failed_ids+=("$id")
        fi
    done

    # Report failures
    if [ ${#failed_ids[@]} -gt 0 ]; then
        echo "Failed IDs: ${failed_ids[*]}"
        return 1
    fi    
}


# Main script execution
trigger_src_data(){
    run_graph_deduplication
    status=$?
    if [ $status -eq 1 ]; then
        echo "Graph deduplication failed"
        return 1
    fi

    run_tax_cv_formatting
    status=$?
    if [ $status -eq 1 ]; then
        echo "Taxonomy/CV formatting failed"
        return 1
    fi

    run_segment_extraction
    status=$?
    if [ $status -eq 1 ]; then
        echo "Segment extraction failed"
        return 1
    fi
}


# check if previous flow is running
function can_rt_src_data_run(){
    if [ -f $RT_LOCK_FILE ]
    then
        return 1
    else
        return 0
    fi
}


#check if previous flow has finished, if so then run other scripts
run_rt_src_data_flow() {
    if can_rt_src_data_run
    then
        printf '#%.0s' {1..100}; echo
        touch $RT_LOCK_FILE && echo "created rt lock file"
        #started process
        starttime=$(date +%s)
        echo "Started RT Source Data creation process"

        trigger_src_data

        #finished process
        endtime=$(date +%s)
        duration=$(($endtime - $starttime))
        echo "Finished RT Source Data creation process, took $duration seconds to complete"
        rm $RT_LOCK_FILE && echo "Removed rt lock file"
    else
        echo "RT Source Data process is already running, skipping this run."
    fi
}

# Main script execution
run_rt_src_data_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/rt_src_data-$RUNDATE.log"