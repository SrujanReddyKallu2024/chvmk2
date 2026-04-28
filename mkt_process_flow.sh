#!/bin/bash

source conf.sh
source process_functions.sh


# process to generate de file for next week processing
generate_to_de_file(){
    if ! hdfs dfs -test -e $HDFS_DE_PATH/$WEEK_START_DATE/to_de.csv && [ $RUNDATE == $WED_DATE ] && [ $(date +%H) == "11" ];    
    then
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P020_1_Taxonomy_Suppression.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P020_2_to_de.py
        create_gzipped_file to_de $DATA_TO_STS_TEMP
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_TO_STS_TEMP -fp to_de
        move_file locals to_de $DATA_TO_STS_TEMP $DATA_TO_STS/vendors
        chown -R unity:stsgrp $DATA_TO_STS/vendors && echo "updated ownership of to_de file"
        chmod -R 774 $DATA_TO_STS/vendors && echo "updated file permissions of to_de file"
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S01_taxonomy_suppress_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S02_to_ip_supplier_stats.py
    else
        if hdfs dfs -test -e $HDFS_DE_PATH/$WEEK_START_DATE/to_de.csv
        then
            echo "to_de.csv already exists in $HDFS_DE_PATH/$WEEK_START_DATE"
        elif [ $RUNDATE != $WED_DATE ]
        then
            echo "Today is not $WED_DATE"
        else
            echo "Time is not 11 AM"
        fi
    fi
}


# process to generate id5 file for processing
generate_to_id5_file(){
    if ! hdfs dfs -test -e $HDFS_ID5_PATH/$WEEK_START_DATE/to_id5.csv && [ $RUNDATE == $WEEK_START_DATE ] && [ $(date +%H) == "11" ];
    then
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P015_to_id5.py
        create_gzipped_file to_id5 $DATA_TO_STS_TEMP
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_TO_STS_TEMP -fp to_id5
        move_file locals to_id5 $DATA_TO_STS_TEMP $DATA_TO_STS/vendors
        chown -R unity:stsgrp $DATA_TO_STS/vendors && echo "updated ownership of to_id5 file"
        chmod -R 774 $DATA_TO_STS/vendors && echo "updated file permissions of to_id5 file"
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S08_to_uid_supplier_stats.py
    else
        if hdfs dfs -test -e $HDFS_ID5_PATH/$WEEK_START_DATE/to_id5.csv
        then
            echo "to_id5.csv already exists in $HDFS_ID5_PATH/$WEEK_START_DATE"
        elif [ $RUNDATE != $WEEK_START_DATE ]
        then
            echo "Today is not $WEEK_START_DATE"
        else
            echo "Time is not 11 AM"
        fi
    fi
}


#taxonomy pre-processing
trigger_taxonomy_preprocessing(){
    if is_file_ingestible "$DATA_FROM_STS/internal" "Digital_taxonomy.txt" && [ $RUNDATE == $WED_DATE ] && [ $(date +%H) == "05" ];    
    then
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_FROM_STS/internal -fp Digital_taxonomy
        transfer_file_to_hdfs Digital_taxonomy $DATA_FROM_STS/internal $HDFS_TAXONOMY_PATH/$RUNDATE
        move_file locals Digital_taxonomy $DATA_FROM_STS/internal $DATA_FROM_STS_BACKUP
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P010_1_Digital_Taxonomy_Ingest.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P010_2_Taxonomy_Process.py
        if [ $? -ne 0 ]; then
            echo "Error in preprocessing taxonomy data for $RUNDATE"
            return 1
        fi
#        hdfs dfs -touchz "$IDG_FLAG_PATH/idg_taxonomy_${RUNDATE}_inprogress" && echo "created flag for idgraph taxonomy trigger" commented out due to infra contraints
        hdfs dfs -touchz "$IDG_FLAG_PATH/p39_taxonomy_${RUNDATE}_inprogress" && echo "created flag for peer39 taxonomy trigger"
    else
        if ! compgen -G "$DATA_FROM_STS/internal/*Digital_taxonomy*.txt"
        then
            echo "taxonomy file doesn't exist in $DATA_FROM_STS/internal"
        elif [ $RUNDATE != $WED_DATE ]
        then
            echo "Today is not $WED_DATE"
        else      
            echo "Time is not 05 AM"        
        fi
    fi
}


# from de preprocessing
trigger_from_de_preprocessing(){
    if is_file_ingestible "$DATA_FROM_STS/vendors" "from_de" && ! hdfs dfs -test -e $HDFS_DE_PATH/$WEEK_START_DATE/from_de__*.gz
    then        
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_FROM_STS/vendors -fp from_de
        transfer_file_to_hdfs from_de $DATA_FROM_STS/vendors $HDFS_DE_PATH/$WEEK_START_DATE
        move_file locals from_de $DATA_FROM_STS/vendors $DATA_FROM_STS_BACKUP
        # spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P030_0_DE_Fix.py        
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P030_1_DE_Ingest.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P030_2B_DE_Process.py        
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P035_1_DE_Add_CB_KEY.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P035_2_DE_ChV_Flag.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P035_3_1_DE_Pub_Flag.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P035_3_2_DE_Aud_Flag.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P035_4_Split.py
    else
        if hdfs dfs -test -e $HDFS_DE_PATH/$WEEK_START_DATE/from_de__*.gz
        then
            echo "Latest DE file already exists in HDFS"
        else
            echo "No file received from DE yet"
        fi
    fi
}


trigger_chv_mk2_creation(){
    if hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/DE_rows_to_keep.parquet && ! hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/chv_mk2.parquet && [ $RUNDATE == $FRI_DATE ] && [ $(date +%H) == "11" ]
    then
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P036_1B_ChV_MK2.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P036_2_ChV_MK2_backlog.py              
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S05b_check_vs_pub_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S07_chv_mk2_stats.py
    else
        if ! hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/DE_rows_to_keep.parquet
        then
            echo "Latest DE_rows_to_keep.parquet file doesn't exist in $HDFS_PROCESS_PATH/$WEEK_START_DATE"
        elif hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/chv_mk2.parquet
        then
            echo "Latest chv_mk2.parquet already exists in $HDFS_PROCESS_PATH/$WEEK_START_DATE"            
        elif [ $RUNDATE != $FRI_DATE ]
        then
            echo "Today is not $FRI_DATE"
        else      
            echo "Time is not 09 AM"
        fi
    fi
}


trigger_id5_optout_ingestion(){    
    local FILE_DATE=$(date -d "$RUNDATE -1 day" +%Y%m%d)
    local OPT_DATE=$(date -d "$FILE_DATE" +%Y-%m-%d)
    if is_file_ingestible "$DATA_FROM_STS/vendors/id5/optouts" "id5opt_${FILE_DATE}" && ! hdfs dfs -test -e $HDFS_ID5_PATH/$OPT_DATE/id5_optouts.csv && [ $(date +%H) == "10" ]
    then        
        if test -e "$DATA_FROM_STS/vendors/id5_optouts__${OPT_DATE}.csv" > /dev/null
        then
            echo "Latest id5 optout file already exists in ${DATA_FROM_STS}/vendors: id5_optouts__${OPT_DATE}.csv" | mailx -r "process_monitor@experian.com" -s "ID5 Optout File Ingestion - Error" $MAILLIST            
            return 1
        else
            echo "No id5 optout file exists for $UC_DATE"
            mkdir -p "$DATA_FROM_STS/vendors/id5_optouts__${OPT_DATE}.csv" && echo "created id5_optouts__${OPT_DATE}.csv folder"
            echo "Ingesting latest ID5 Optout file from $DATA_FROM_STS/id5/optouts"
        fi                
        move_file locals "id5opt_${FILE_DATE}" "$DATA_FROM_STS/vendors/id5/optouts" "$DATA_FROM_STS/vendors/id5_optouts__${OPT_DATE}.csv"
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_FROM_STS/vendors -fp "id5_optouts__${OPT_DATE}.csv"
        transfer_file_to_hdfs "id5_optouts__${OPT_DATE}.csv" $DATA_FROM_STS/vendors $HDFS_ID5_PATH/$OPT_DATE
        mv "$DATA_FROM_STS/vendors/id5_optouts__${OPT_DATE}.csv" $DATA_FROM_STS_BACKUP
        echo "Created ID5 optouts file successfully"
    else
        if hdfs dfs -test -e $HDFS_ID5_PATH/$OPT_DATE/id5_optouts__${OPT_DATE}.csv
        then
            echo "Latest id5 optout file already exists in HDFS"          
        elif [ $(date +%H) != "10" ]
        then
            echo "Time is not 10 AM"
        else
            echo "No optout file received from id5 yet"
        fi     
    fi
}


trigger_from_id5_preprocessing(){    
    if is_file_ingestible "$DATA_FROM_STS/vendors/id5" "gz.parquet" && ! hdfs dfs -test -e $HDFS_ID5_PATH/$WEEK_START_DATE/from_id5__*.parquet && [ $(date +%H) -ge 1 ]
    then
        if test -e "$DATA_FROM_STS/vendors/id5/from_id5__${RUNDATE}.parquet" > /dev/null
        then
            echo "Latest from ID5 file already exists: ${DATA_FROM_STS}/vendors/id5/from_id5__${RUNDATE}.parquet" | mailx -r "process_monitor@experian.com" -s "ID5 File Ingestion - Error" $MAILLIST
            return 1
        else
            echo "No from ID5 file for $RUNDATE exists"
        fi
        mkdir $DATA_FROM_STS/vendors/id5/from_id5__$RUNDATE.parquet && echo "created from_id5__$RUNDATE.parquet folder"
        move_file locals gz.parquet $DATA_FROM_STS/vendors/id5 $DATA_FROM_STS/vendors/id5/from_id5__$RUNDATE.parquet
        mv $DATA_FROM_STS/vendors/id5/from_id5__$RUNDATE.parquet $DATA_FROM_STS/vendors && echo "moved from_id5__$RUNDATE.parquet into $DATA_FROM_STS/vendors"
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_FROM_STS/vendors -fp from_id5
        transfer_file_to_hdfs from_id5 $DATA_FROM_STS/vendors $HDFS_ID5_PATH/$WEEK_START_DATE
        mv $DATA_FROM_STS/vendors/from_id5__$RUNDATE.parquet $DATA_FROM_STS_BACKUP && echo "moved from_id5__$RUNDATE.parquet to $DATA_FROM_STS_BACKUP"
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P040_0A_ID5_Optouts.py
        spark-submit --master yarn --queue $QUEUE1 --archives "hdfs:///user/unity/environments/tcfenv_2.zip#venv" $SCRIPTDIR/P040_0B_ID5_TCF_Decoder.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P040_ID5_Ingest.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P041_ID5_Apply_Optouts.py
    else
        if hdfs dfs -test -e $HDFS_ID5_PATH/$WEEK_START_DATE/from_id5__*.parquet
        then
            echo "Latest ID5 file already exists in HDFS"
        else
            echo "No file received from ID5 yet"
        fi
        
    fi
}


trigger_uc_optout_ingestion(){
    local UC_ORIG_DATE=$(date -d "$RUNDATE -2 day" +%Y%m%d)
    local UC_DATE=$(date -d "$RUNDATE -1 day" +%Y-%m-%d)
    if is_file_ingestible "$DATA_FROM_STS/vendors/unacast/optouts" "${UC_ORIG_DATE}__data.csv.gz" && ! hdfs dfs -test -e $HDFS_UC_PATH/$UC_DATE/from_unacast_optout__*.csv.gz
    then        
        if test -e "$DATA_FROM_STS/vendors/from_unacast_optout__${UC_DATE}_*.csv" > /dev/null
        then
            echo "Latest unacast optout file already exists in ${DATA_FROM_STS}/vendors: from_unacast_optout__${UC_DATE}_*.csv" | mailx -r "process_monitor@experian.com" -s "Unacast Optout File Ingestion - Error" $MAILLIST            
            return 1
        else
            echo "No unacast optout file exists for $UC_DATE"
            mkdir -p "$DATA_FROM_STS/vendors/from_unacast_optout__${UC_DATE}.csv.gz" && echo "created from_unacast_optout__${UC_DATE}.csv.gz folder"
            echo "Ingesting latest UC Optout file from $DATA_FROM_STS/vendors/unacast"
        fi                
        move_file locals "${UC_ORIG_DATE}__data.csv.gz" "$DATA_FROM_STS/vendors/unacast/optouts" "$DATA_FROM_STS/vendors/from_unacast_optout__${UC_DATE}.csv.gz"
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_FROM_STS/vendors -fp "from_unacast_optout__${UC_DATE}.csv.gz"
        transfer_file_to_hdfs "from_unacast_optout__${UC_DATE}.csv.gz" $DATA_FROM_STS/vendors $HDFS_UC_PATH/$UC_DATE
        mv "$DATA_FROM_STS/vendors/from_unacast_optout__${UC_DATE}.csv.gz" $DATA_FROM_STS_BACKUP
        echo "Created master UC optouts successfully"
    else
        if hdfs dfs -test -e $HDFS_UC_PATH/$UC_DATE/from_unacast_optout__*.csv.gz
        then
            echo "Latest unacast optout file already exists in HDFS"
        else
            echo "No optout file received from unacast yet"
        fi        
    fi
}


trigger_from_uc_preprocessing(){
    UC_ORIG_DATE=$(date -d "$RUNDATE -2 day" +%Y-%m-%d)
    UC_DATE=$(date -d "$RUNDATE -1 day" +%Y-%m-%d)
    UC_DATETIME=${UC_DATE}_$(date +%H%M)
    if is_file_ingestible "$DATA_FROM_STS/vendors/unacast" "${UC_ORIG_DATE}__" && [ $(date +%H) == "10" ]
    then
        if test -e "$DATA_FROM_STS/vendors/unacast/from_unacast__${UC_DATE}_*.csv" > /dev/null
        then
            echo "Latest unacast file already exists in ${DATA_FROM_STS}/vendors/unacast: from_unacast__${UC_DATE}_*.csv" | mailx -r "process_monitor@experian.com" -s "Unacast File Ingestion - Error" $MAILLIST            
            return 1
        else
            echo "No from unacast file for $UC_DATE exists"
        fi
        mkdir "$DATA_FROM_STS/vendors/unacast/from_unacast__${UC_DATETIME}.csv" && echo "created from_unacast__${UC_DATETIME}.csv folder"
        move_file locals "${UC_ORIG_DATE}___SUMMARY" $DATA_FROM_STS/vendors/unacast $DATA_FROM_STS_BACKUP
        move_file locals $UC_ORIG_DATE $DATA_FROM_STS/vendors/unacast "$DATA_FROM_STS/vendors/unacast/from_unacast__${UC_DATETIME}.csv"
        mv "$DATA_FROM_STS/vendors/unacast/from_unacast__${UC_DATETIME}.csv" $DATA_FROM_STS/vendors && echo "moved from_unacast__${UC_DATETIME}.csv into $DATA_FROM_STS/vendors"
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_FROM_STS/vendors -fp from_unacast__
        transfer_file_to_hdfs from_unacast $DATA_FROM_STS/vendors $HDFS_UC_PATH/$UC_DATE
        mv "$DATA_FROM_STS/vendors/from_unacast__${UC_DATETIME}.csv" $DATA_FROM_STS_BACKUP && echo "moved from_unacast__${UC_DATETIME}.csv to $DATA_FROM_STS_BACKUP"
        if [ $RUNDATE == $FRI_DATE ]; then
            spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P024_UC_Master_Optouts.py
            spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P025_UC_Preprocess.py
        fi
    else
        if compgen -G "$DATA_FROM_STS/vendors/unacast/*inprogress" > /dev/null
        then
            echo "STS is still copying files, exiting"
        elif ! compgen -G "$DATA_FROM_STS/vendors/unacast/${UC_ORIG_DATE__}*csv.gz" > /dev/null
        then
            echo "Files with $UC_ORIG_DATE prefix not available in $DATA_FROM_STS/vendors/unacast"
        elif hdfs dfs -test -e $HDFS_UC_PATH/$UC_DATE/from_unacast__*.csv
        then
            echo "Latest Unacast file already exists in HDFS"
        else
            echo "New unacast inbound file not found"
        fi
    fi
}


trigger_ipsup_stats(){
    if hdfs dfs -test -e $HDFS_UC_PATH/$WEEK_START_DATE/process/unacast_hh_ip_flags.parquet && hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/DE_rows_to_keep.parquet && ! hdfs dfs -test -e $HDFS_STATS_PATH/$WEEK_START_DATE/from_ip_supplier_stats__$WEEK_START_DATE.csv
    then
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S03_from_ip_supplier_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S04_check_vs_chv_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S05a_check_vs_pub_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S06_ip_supplier_data_deletion_stats.py
    else
        if ! hdfs dfs -test -e $HDFS_UC_PATH/$WEEK_START_DATE/process/unacast_hh_ip_flags.parquet
        then
            echo "Latest unacast_hh_ip_flags file doesn't exist in $HDFS_UC_PATH/$WEEK_START_DATE/process"
        elif ! hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/DE_rows_to_keep.parquet
        then
            echo "Latest DE_rows_to_keep file doesn't exist in $HDFS_PROCESS_PATH/$WEEK_START_DATE"
        else
            echo "Latest stats file already exists in $HDFS_STATS_PATH/$WEEK_START_DATE"
        fi
    fi    
}


trigger_pubmatic_processing(){
    if hdfs dfs -test -e $HDFS_ID5_PATH/$WEEK_START_DATE/from_id5__*.parquet && hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/chv_mk2.parquet && ! hdfs dfs -test -e $HDFS_PUBMATIC_PATH/$WEEK_START_DATE/pubmatic.csv && [ $RUNDATE == $FRI_DATE ] && [ $(date +%H) == "13" ]
    then        
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P050_1_Merge_ID5_DE.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P050_3_UID_S_Code.py
        sleep 120
        check_file_exists $HDFS_PROCESS_PATH/$WEEK_START_DATE uid_s_code_cal.parquet "P50_3"
        if [ $? -ne 0 ]; then
            echo "Error in creating Pubmatic data for $RUNDATE"
            return 1
        fi
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P051_1_Merge_MAIDS_DE.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P051_3_MAIDS_S_Code.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P052_1_ChV_S_Code.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P070_Pubmatic.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P075_Pubmatic_Backlog.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P076_HH_IP.py
        sleep 300
        hdfs dfs -copyToLocal $HDFS_PUBMATIC_PATH/$WEEK_START_DATE/pubmatic.csv/* $DATA_TO_STS_TEMP && echo "copied files from hdfs to local temp"        
        sleep 300
        create_pubmatic_file part- $DATA_TO_STS_TEMP $DATA_TO_STS_TEMP "${PUBMATIC_ID}_replaceseg"
        sleep 300
        spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_TO_STS_TEMP -fp $PUBMATIC_ID
        hdfs dfs -copyFromLocal $DATA_TO_STS_TEMP/$PUBMATIC_ID* datascience/marketplace_files
        sleep 60
        move_file locals $PUBMATIC_ID $DATA_TO_STS_TEMP $DATA_TO_STS/marketplaces/pubmatic
        chown -R unity:stsgrp $DATA_TO_STS/marketplaces/pubmatic && echo "updated ownership of all pubmatic files"
        chmod -R 774 $DATA_TO_STS/marketplaces/pubmatic && echo "updated file permissions of all pubmatic files"
        sleep 60
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S09_from_uid_supplier_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S10_combiner_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S11_to_marketplace_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S11_1_pubmatic_marketplace_stats.py
        spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S99_stakeholder_dashboard_stats.py
        # hdfs dfs -touchz "$IDG_FLAG_PATH/chv_mk2_180_${RUNDATE}_inprogress" && echo "created flag for idgraph chvmk2180 trigger"
    else
        if ! hdfs dfs -test -e $HDFS_ID5_PATH/$WEEK_START_DATE/from_id5__*.parquet
        then
            echo "Latest file from id5 not found in $HDFS_ID5_PATH/$WEEK_START_DATE"
        elif ! hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/chv_mk2.parquet
        then
            echo "Latest chv mk2 file not found in $HDFS_PROCESS_PATH/$WEEK_START_DATE"
        else
            echo "Latest pubmatic file already generated, no need to run process for this week"        
        fi
    fi
}


trigger_taxonomy_lookup_generation(){
    if compgen -G "$SOURCE_MKP_FILES/Digital_Taxonomy_Master_Layout*.xlsx" > /dev/null
    then
        mkdir -p $TAXONOMY_LOOKUP_PATH
        spark-submit $SP_SCRIPT_DIR/format_taxonomy_lookup.py -m $RUNDATE -if $SOURCE_MKP_FILES -of $TAXONOMY_LOOKUP_PATH
        if compgen -G "$TAXONOMY_LOOKUP_PATH/taxonomy_lookup__${RUNDATE}.csv" > /dev/null
        then
            if hdfs dfs -test -e match2/taxonomy_lookup__*
            then
                hdfs dfs -rm -r match2/taxonomy_lookup__* && echo "removed older lookups"
            else
                echo "Older taxonomy lookup file not found"
            fi
            hdfs dfs -copyFromLocal "$TAXONOMY_LOOKUP_PATH/taxonomy_lookup__${RUNDATE}.csv" match2 && echo "copied $TAXONOMY_LOOKUP_PATH/taxonomy_lookup__${RUNDATE}.csv to match2"
            move_file locals Digital_Taxonomy_Master_Layout $SOURCE_MKP_FILES $DATA_FROM_STS_BACKUP
        else
            echo "No taxonomy lookup file created"
        fi
    else
        echo "Latest taxonomy lookup file not found"
    fi
}


trigger_eyeota_processing(){
    if [ $RUNDATE == $SUN_DATE ] && [ $(date +%H) == "21" ] && hdfs dfs -test -e $HDFS_PROCESS_PATH/$WEEK_START_DATE/uid_s_code_cal.parquet
    then
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P270_eyeota_subset.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P271_eyeota_format.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P272_eyeota_backlog.py
        spark-submit --master yarn --queue $QUEUE1 $SCRIPTDIR/P273_eyeota_output.py
        sleep 10
        #spark-submit --master yarn --queue $QUEUE2 $STATSSCRIPTDIR/S11_1_eyeota_marketplace_stats.py
        file_date=$(date -d $RUNDATE +%Y%m%d)
        if compgen -G "$DATA_TO_STS_TEMP/eyeota/experian_r89cb20_${file_date}-*.csv.gz" > /dev/null
        then
            spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip $DATA_TO_STS_TEMP/eyeota -fp experian_r89cb20
            # commented out due to consent issues 14/11/25
            #move_file locals experian_r89cb20 $DATA_TO_STS_TEMP/eyeota $DATA_TO_STS/marketplaces/eyeota
            #chown -R unity:stsgrp $DATA_TO_STS/marketplaces/eyeota && echo "updated ownership of all eyeota files"
            #chmod -R 774 $DATA_TO_STS/marketplaces/eyeota && echo "updated file permissions of all eyeota files"            
        else
            echo "Eyeota files not found in $DATA_TO_STS_TEMP" | mailx -r "process_monitor@experian.com" -s "Eyeota output files in local - not found" $MAILLIST
        fi
    else
        if [ $RUNDATE != $SUN_DATE ]
        then
            echo "Today is not Sunday"
        elif [ $(date +%H) != "21" ]
        then
            echo "Time is not 9PM"
        else
            echo "uid_s_code_cal.parquet doesn't exist in $HDFS_PROCESS_PATH/$WEEK_START_DATE"
        fi
    fi
}


trigger_dashboard_restart(){
    echo "restarting dashboard in nn01 server"
    ssh ukfhpapmtc01 -t "cd /home/unity/match-dashboard && ./match-dashboard-service.sh stop && echo 'stopped' && sleep 20 && cd /home/unity/match-dashboard && ./match-dashboard-service.sh start && echo 'started'"
    echo "dashboard started in nn01 server"
}


# check if previous flow is running
function can_pubm_process_run(){
    if [ -f $PUBM_LOCK_FILE ]
    then
        return 1
    else
        return 0
    fi
}


#check if previous flow has finished, if so then run other scripts
run_pubm_process_flow() {
    if can_pubm_process_run
    then        
        printf '#%.0s' {1..100}; echo
        touch $PUBM_LOCK_FILE && echo "created pubm lock file"
        # start time
        starttime=$(date +%s)
        echo "Started Match2 Process"
        # process starts here
        trigger_taxonomy_preprocessing
        generate_to_de_file
        trigger_from_de_preprocessing
        trigger_uc_optout_ingestion
        trigger_from_uc_preprocessing
        trigger_ipsup_stats
        trigger_chv_mk2_creation
        generate_to_id5_file
        trigger_id5_optout_ingestion
        trigger_from_id5_preprocessing
        trigger_pubmatic_processing
        trigger_taxonomy_lookup_generation
        trigger_eyeota_processing        
        # finished process
        endtime=$(date +%s)
        duration=$(($endtime - $starttime))
        echo "Finished Match2 Process, took $duration seconds to complete"
        rm $PUBM_LOCK_FILE && echo "removed pubm lock file"        
    else
        echo "Previous process flow hasn't finished. Will retry later"
    fi
}

run_pubm_process_flow 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/datatransfer-$RUNDATE.log"
trigger_dashboard_restart


