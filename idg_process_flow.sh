#!/bin/bash

source conf.sh
source process_functions.sh


#Check if previous flow or pubmatic process is running currently
can_idg_process_run(){
    if [ -f $IDG_LOCK_FILE ]
    then
        echo "An IDG process is already running"
        return 1
    elif [ -f $PUBM_LOCK_FILE ]
    then
        echo "Pubmatic process hasn't finished yet"
        return 1
    else
        return 0
    fi
}


#Check if flag file exists in given HDFS path and triggers function
check_trigger_flag(){
    if hdfs dfs -test -e $1
    then
        return 0
    else
        return 1
    fi
}


# Triggers graph creation on any day except Thursday, Friday, Saturday or Sunday and at 12PM, Argument: 1. Script to run
trigger_graph_on_certain_days() {    
    trigger_script=$1
    case $RUNDATE in
        $WEEK_START_DATE|$TUE_DATE|$WED_DATE)
            if [ $(date +%H) == "12" ]
            then                
                echo "Triggering graph creation for $RUNDATE"
                $trigger_script
            else
                echo "Time is not 12PM"
            fi
            ;;
        *)
            echo "Graph creation not triggered as current date is $RUNDATE"
            ;;
    esac
}


##HEMS
#HEMS ingestion
trigger_hems_ingestion(){
    hems_file_pattern="F15_*_ChannelView_LinkageView.csv"    
    if is_file_ingestible "$IDG_DATA_FROM_STS/internal/identifiers" "${hems_file_pattern}"
    then
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $IDG_DATA_FROM_STS/internal/identifiers -fp "${hems_file_pattern}"
        hdfs dfs -mkdir -p $IDG_HDFS_PATH/hems/$RUNDATE_MONTHYEAR
        transfer_file_to_hdfs "${hems_file_pattern}" $IDG_DATA_FROM_STS/internal/identifiers $IDG_HDFS_PATH/hems/$RUNDATE_MONTHYEAR/input
        move_file locals "${hems_file_pattern}" $IDG_DATA_FROM_STS/internal/identifiers $DATA_FROM_STS_BACKUP
        spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_hems_p10_ingest.py
        hdfs dfs -touchz $IDG_FLAG_PATH/idg_hems_"$RUNDATE"_inprogress && echo "created flag for hems idgraph trigger"
    else
        echo "New HEMS input file not found"
    fi
}

#HEMS graph creation
#Trigger hems graph if a new file has been ingested or as part of weekly process/new taxonomy for which an argument is passed
trigger_hems_graph(){    
    if check_trigger_flag $IDG_FLAG_PATH/idg_hems_"$RUNDATE"_inprogress || [ $# -gt 0 ]
    then
        echo "Running HEMS id graph creation for $RUNDATE"
        spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_hems_p20_create_graph.py
        sleep 20
        spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_hems_stats.py
        sleep 10
        check_file_exists $IDG_HDFS_PATH/graphs/hems hems_"$IDG_RUNDATE"*.parquet HEMS
        check_file_exists $IDG_HDFS_PATH/stats/hems/standard idg_hems_stats__"$IDG_RUNDATE"*.csv "HEMS Stats"
        hdfs dfs -rm -r "$IDG_FLAG_PATH/idg_hems_${RUNDATE}_inprogress" && echo "removed $IDG_FLAG_PATH/idg_hems_${RUNDATE}_inprogress flag"
        echo "Completed HEMS id graph creation for $RUNDATE"
    else
        echo "$IDG_FLAG_PATH/idg_hems_"$RUNDATE"_inprogress not found"
    fi
}



##MOBILE
#MOBILE ingestion
trigger_mobile_ingestion(){
    mobile_file_pattern="F16_*_ChannelView_LinkageView.csv"
    if is_file_ingestible "$IDG_DATA_FROM_STS/internal/identifiers" "${mobile_file_pattern}"
    then
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $IDG_DATA_FROM_STS/internal/identifiers -fp "${mobile_file_pattern}"
        hdfs dfs -mkdir -p $IDG_HDFS_PATH/mobile/$RUNDATE_MONTHYEAR
        transfer_file_to_hdfs "${mobile_file_pattern}" $IDG_DATA_FROM_STS/internal/identifiers $IDG_HDFS_PATH/mobile/$RUNDATE_MONTHYEAR/input
        move_file locals "${mobile_file_pattern}" $IDG_DATA_FROM_STS/internal/identifiers $DATA_FROM_STS_BACKUP
        spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_mobile_p10_ingest.py
        hdfs dfs -touchz $IDG_FLAG_PATH/idg_mobile_"$RUNDATE"_inprogress && echo "created flag for mobile idgraph trigger"
    else
        echo "New MOBILE input file not found"
    fi
}

#MOBILE graph creation
#Trigger mobile graph if a new file has been ingested or as part of weekly process/new taxonomy for which an argument is passed
trigger_mobile_graph(){
    if check_trigger_flag $IDG_FLAG_PATH/idg_mobile_"$RUNDATE"_inprogress || [ $# -gt 0 ]
    then
        echo "Running MOBILE id graph creation for $RUNDATE"
        spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_mobile_p20_create_graph.py
        sleep 20
        spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_mobile_stats.py
        sleep 10
        check_file_exists $IDG_HDFS_PATH/graphs/mobile mobile_"$IDG_RUNDATE"*.parquet MOBILE
        check_file_exists $IDG_HDFS_PATH/stats/mobile/standard idg_mobile_stats__"$IDG_RUNDATE"*.csv "MOBILE Stats"
        hdfs dfs -rm -r "$IDG_FLAG_PATH/idg_mobile_${RUNDATE}_inprogress" && echo "removed $IDG_FLAG_PATH/idg_mobile_${RUNDATE}_inprogress flag"
        echo "Completed MOBILE id graph creation for $RUNDATE"
    else
        echo "$IDG_FLAG_PATH/idg_mobile_"$RUNDATE"_inprogress not found"
    fi
}


##CV
#CV ingestion
trigger_cv_ingestion(){
    cv_file_pattern="F35_*_P60_consview.csv"
    if is_file_ingestible "$IDG_DATA_FROM_STS/internal/identifiers" "${cv_file_pattern}"
    then
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $IDG_DATA_FROM_STS/internal/identifiers -fp "${cv_file_pattern}"
        hdfs dfs -mkdir -p $IDG_HDFS_PATH/cv/$RUNDATE_MONTHYEAR
        transfer_file_to_hdfs "${cv_file_pattern}" $IDG_DATA_FROM_STS/internal/identifiers $IDG_HDFS_PATH/cv/$RUNDATE_MONTHYEAR/input
        move_file locals "${cv_file_pattern}" $IDG_DATA_FROM_STS/internal/identifiers $DATA_FROM_STS_BACKUP
        spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_cv_p10_ingest.py
        # hdfs dfs -touchz $IDG_FLAG_PATH/idg_cv_"$RUNDATE"_inprogress && echo "created flag for cv idgraph trigger"        
    else
        echo "New CV input file not found"
    fi
}

#CV graph creation
#Trigger cv graph if a new file has been ingested or as part of weekly process/new taxonomy for which an argument is passed
# trigger_cv_graph(){
#     if check_trigger_flag $IDG_FLAG_PATH/idg_cv_"$RUNDATE"_inprogress || [ $# -gt 0 ]
#     then
#         echo "Running CV id graph creation for $IDG_RUNDATE"
#         spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_cv_p20_create_graph.py
#         sleep 20
#         spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_cv_stats.py
#         sleep 10
#         check_file_exists $IDG_HDFS_PATH/graphs/cv cv_"$IDG_RUNDATE"*.parquet CV
#         check_file_exists $IDG_HDFS_PATH/stats/cv/standard idg_cv_stats__"$IDG_RUNDATE"*.csv "CV Stats"
#         hdfs dfs -rm -r $IDG_FLAG_PATH/idg_cv_"$RUNDATE"_inprogress && echo "removed $IDG_FLAG_PATH/idg_cv_"$RUNDATE"_inprogress flag"
#          echo "Completed CV id graph creation for $IDG_RUNDATE"
#     else
#         echo "$IDG_FLAG_PATH/idg_cv_"$RUNDATE"_inprogress not found"
#     fi
# }


##PC
#PC ingestion
#>>>TODO: ADD PC INGESTION SCRIPT WHEN AVAILABLE<<<
trigger_pc_ingestion(){    
    if is_file_ingestible "$IDG_DATA_FROM_STS/internal/identifiers" "cv_postcode_master.parquet" && is_file_ingestible "$IDG_DATA_FROM_STS/internal/identifiers" "cv_postcode_dominants.parquet"
    then
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $IDG_DATA_FROM_STS/internal/identifiers -fp cv_postcode
        hdfs dfs -mkdir -p $IDG_HDFS_PATH/pc/$RUNDATE_YEAR
        transfer_file_to_hdfs cv_postcode $IDG_DATA_FROM_STS/internal/identifiers $IDG_HDFS_PATH/pc/$RUNDATE_YEAR/input
        move_file locals cv_postcode $IDG_DATA_FROM_STS/internal/identifiers $DATA_FROM_STS_BACKUP
        hdfs dfs -touchz $IDG_FLAG_PATH/idg_pc_"$RUNDATE"_inprogress && echo "created flag for pc idgraph trigger"       
    else
        echo "New PC input file not found"
    fi
}

#PC graph creation
#Trigger pc graph if a new file has been ingested or as part of weekly process/new taxonomy for which an argument is passed
trigger_pc_graph(){
    if check_trigger_flag $IDG_FLAG_PATH/idg_pc_"$RUNDATE"_inprogress || [ $# -gt 0 ]
    then
        echo "Running PC id graph creation for $RUNDATE"
        spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_pc_p20_create_graph.py
        sleep 20
        spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_pc_stats.py
        sleep 10
        check_file_exists $IDG_HDFS_PATH/graphs/pc pc_"$IDG_RUNDATE"*.parquet PC
        check_file_exists $IDG_HDFS_PATH/stats/pc/standard idg_pc_stats__"$IDG_RUNDATE"*.csv "PC Stats"
        hdfs dfs -rm -r "$IDG_FLAG_PATH/idg_pc_${RUNDATE}_inprogress" && echo "removed $IDG_FLAG_PATH/idg_pc_${RUNDATE}_inprogress flag"
        echo "Completed PC id graph creation for $RUNDATE"
    else
        echo "$IDG_FLAG_PATH/idg_pc_"$RUNDATE"_inprogress not found"
    fi
}


##IP graph creation
trigger_ip_graph(){
    echo "Running IP id graph creation for $RUNDATE"
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_ip_p20_create_graph.py
    sleep 20    
    check_file_exists $IDG_HDFS_PATH/graphs/ip pre_ip_"$IDG_RUNDATE"*.parquet IP    
    echo "Completed IP id graph creation for $RUNDATE"
}

trigger_ip_confidence_score(){
    echo "Running IP confidence score calculation for $RUNDATE"
    spark-submit --master yarn --queue $QUEUE1 --archives "hdfs:///user/unity/environments/xgboost.zip#venv" $IDG_CONFIDENCE_SCRIPTDIR/ms010_model_score.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/graphs/ip ip_"$IDG_RUNDATE"*.parquet IP
    if [ $? -eq 0 ]; then        
        hdfs dfs -rm -r $IDG_HDFS_PATH/graphs/ip/pre_ip_"$IDG_RUNDATE"*.parquet && echo "removed pre-ip file"
    else
        echo "IP confidence score calculation failed, pre-ip file not removed"
        return 1
    fi
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_ip_stats.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/stats/ip/standard idg_ip_stats__"$IDG_RUNDATE"*.csv "IP Stats"
    echo "Completed IP confidence score calculation for $RUNDATE"
}


##UID graph creation
trigger_uid_graph(){
    echo "Running UID id graph creation for $RUNDATE"
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_uid_p20_create_graph.py
    sleep 20
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_uid_stats.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/graphs/uid uid_"$IDG_RUNDATE"*.parquet UID
    check_file_exists $IDG_HDFS_PATH/stats/uid/standard idg_uid_stats__"$IDG_RUNDATE"*.csv "UID Stats"
    echo "Completed UID id graph creation for $RUNDATE"
}


##MAIDS graph creation
trigger_maids_graph(){
    echo "Running MAIDS id graph creation for $RUNDATE"    
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_maids_p20_create_graph.py
    sleep 20
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_maids_stats.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/graphs/maids maids_"$IDG_RUNDATE"*.parquet MAIDS
    check_file_exists $IDG_HDFS_PATH/stats/maids/standard idg_maids_stats__"$IDG_RUNDATE"*.csv "MAIDS Stats"
    echo "Completed MAIDS id graph creation for $RUNDATE"
}


##FPID graph creation
trigger_fpid_graph(){
    echo "Running FPID id graph creation for $RUNDATE"
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_fpid_p20_create_graph.py
    sleep 20
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_fpid_stats.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/graphs/fpid fpid_"$IDG_RUNDATE"*.parquet FPID
    check_file_exists $IDG_HDFS_PATH/stats/fpid/standard idg_fpid_stats__"$IDG_RUNDATE"*.csv "FPID Stats"
    echo "Completed FPID id graph creation for $RUNDATE"
}


##TTDIDS graph creation
trigger_ttdids_graph(){
    echo "Running TTDIDS id graph creation for $RUNDATE"    
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_ttdids_p20_create_graph.py
    sleep 120
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_ttdids_p21_add_aud.py
    sleep 120
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_ttdids_stats.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/graphs/ttdids ttdids_"$IDG_RUNDATE"*.parquet TTDIDS
    check_file_exists $IDG_HDFS_PATH/stats/ttdids/standard idg_ttdids_stats__"$IDG_RUNDATE"*.csv "TTDIDS Stats"
    echo "Completed TTDIDS id graph creation for $RUNDATE"
}


##APNIDS graph creation
trigger_apnids_graph(){
    echo "Running APNIDS id graph creation for $RUNDATE"    
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_apnids_p20_create_graph.py
    sleep 120
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_apnids_p21_add_aud.py
    sleep 120
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_apnids_stats.py
    sleep 10
    check_file_exists $IDG_HDFS_PATH/graphs/apnids apnids_"$IDG_RUNDATE"*.parquet APNIDS
    check_file_exists $IDG_HDFS_PATH/stats/apnids/standard idg_apnids_stats__"$IDG_RUNDATE"*.csv "APNIDS Stats"
    echo "Completed APNIDS id graph creation for $RUNDATE"
}


##Lookup file creation
trigger_lookup_generation(){
    echo "Running Lookup file creation for $RUNDATE"    
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_lookup_p30_update.py
    check_file_exists id_graph/lookup/experian match_id_lookup_experian_"$IDG_RUNDATE"*.parquet IDG_LOOKUP
    echo "Completed Lookup creation for $RUNDATE"
}

##Client Lookup file creation
trigger_client_lookup_generation(){
    echo "Running Client Lookup file creation for $RUNDATE"    
    spark-submit --master yarn --queue $QUEUE1 $IDG_SCRIPTDIR/idg_lookup_p40_client_update.py --client_name $1
    check_file_exists "id_graph/lookup/$1" match_id_lookup_"$1"_"$IDG_RUNDATE"*.parquet IDG_CLIENT_LOOKUP
    check_file_exists "id_graph/lookup/$1" cbk_hh_lookup_"$1"_"$IDG_RUNDATE"*.parquet IDG_CLIENT_LOOKUP
    echo "Completed Client Lookup creation for $RUNDATE"
}


##Triggers

#Function to trigger all graph scripts
trigger_all_graphs() {
    trigger_lookup_generation
    # if above script didnt return 0 then exit out of this function
    if [ $? -ne 0 ]; then
        echo "Error in creating latest Match ID Lookup file (General Experian one)"
        exit 1
    fi
    trigger_ip_graph
    trigger_ip_confidence_score
    trigger_uid_graph
    trigger_maids_graph
    trigger_fpid_graph
    trigger_hems_graph 1
    trigger_mobile_graph 1
    trigger_pc_graph 1
    trigger_ttdids_graph
    trigger_apnids_graph
    # trigger_cv_graph 1
    trigger_client_lookup_generation "evorra"
    # trigger_client_lookup_generation "infosum"
    trigger_client_lookup_generation "lg"
    sleep 20
    spark-submit --master yarn --queue $QUEUE2 $IDG_STATSSCRIPTDIR/idg_crossover_stats.py
}


#Triggers all graph scripts if we receive new taxonomy file
trigger_when_new_taxonomy() {    
    if check_trigger_flag "$IDG_FLAG_PATH/idg_taxonomy_${RUNDATE}_inprogress" && [ $RUNDATE == $WED_DATE ] && [ $(date +%H) == "12" ]
    then
        echo "New Taxonomy file has been ingested, triggering updation"
        trigger_all_graphs        
        hdfs dfs -rm -r "$IDG_FLAG_PATH/idg_taxonomy_${RUNDATE}_inprogress" && echo "removed $IDG_FLAG_PATH/idg_taxonomy_${RUNDATE}_inprogress flag"
    else
        echo "New Taxonomy file hasn't been ingested yet"
    fi
}


# Triggers weekly graph updation on Saturday 6am
trigger_weekly_updation() {
    if [ $RUNDATE == $SAT_DATE ] && [ $(date +%H) == "06" ]
    then
        echo "Triggering weekly graph updation"
        trigger_all_graphs
    else
        echo "Weekly graph updation not triggered as current date and time is $(date +%Y-%m-%d' '%H:%M:%S)"
    fi
}


#Restart id-graph dashboard
trigger_dashboard_restart(){
    echo "restarting idgraph dashboard in nn01 server"
    ssh ukfhpapmtc01 -t "cd /home/unity/match-id-graph-dashboard && ./idgraph-dashboard-service.sh stop && echo 'stopped' && sleep 20 && cd /home/unity/match-id-graph-dashboard && ./idgraph-dashboard-service.sh start && echo 'started'"
    echo "idgraph dashboard started in nn01 server"
}


#Process starts here
run_idg_process_flow() {
    if can_idg_process_run
    then        
        printf '#%.0s' {1..100}; echo
        touch $IDG_LOCK_FILE && echo "created idg lock file"
        #started process
        starttime=$(date +%s)
        echo "Started Id-Graph Process"
        trigger_when_new_taxonomy
        trigger_weekly_updation
        trigger_hems_ingestion
        trigger_hems_graph  ## added to create graph on ingestion of data  
        trigger_mobile_ingestion
        trigger_mobile_graph ## added to create graph on ingestion of data
        trigger_cv_ingestion
        trigger_pc_ingestion
        trigger_graph_on_certain_days
        #finished process
        endtime=$(date +%s)
        duration=$(($endtime - $starttime))
        echo "Finished Id-Graph Process, took $duration seconds to complete"
        rm $IDG_LOCK_FILE && echo "removed idg lock file"
    else
        echo "A new process can't be run before previous process has finished"
    fi
}


run_idg_process_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/idg_datatransfer-$RUNDATE.log"
trigger_dashboard_restart