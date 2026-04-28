#!/bin/bash

source conf.sh
source /etc/profile.d/hadoop.sh

# Required variables
# SRC is the source cluster
SRC_NAMENODES="ukfhpapmtc01.gdc.local"
# Dest is the destination cluster
DEST_NAMENODES="ukbldapmtc01.gdc.local"


# Functions

# Function for creating directory if it doesnt exist
mknewdir(){
    if $(hdfs dfs -test -d $1) ; then echo "Directory already exists"; else hdfs dfs -mkdir -p $1; fi
}

# Retry and failure reporting function
distcp_retry(){
  COMMAND="$1"
  LOG_NAME="$2"
  COUNT=0
  while [[ $COUNT < 2 ]] ; do
    $COMMAND >> $LOG_NAME 2>&1
    if [[ $? -ne 0 ]] && [[ $COUNT < 1 ]] ; then
      ((COUNT++))
    elif [[ $? -ne 0 ]] && [[ $COUNT = 1 ]] ; then
      echo "Data sync failure on $COMMAND" | mailx -r "process_monitor@experian.com" -s "Data Sync Failure: $RUNDATE" $MAILLIST
      break
    else
      break
    fi
  done
}


# Process starts here

# Exit if not run by unity user
if [ "$USER" != "unity" ] ; then
    echo "Not unity user, not running DEST sync"
    exit 0
fi


# Get HDFS paths for Source and Destination
for i in $SRC_NAMENODES; do
    SRC_NAME=`echo $i | cut -d'.' -f1`
    if hdfs dfs -test -e "hdfs://${SRC_NAME}/" ; then
        SRC="hdfs://${SRC_NAME}:8020"
        SRC_BARE="hdfs://${SRC_NAME}:8020"
    fi
done

for i in $DEST_NAMENODES; do
    DEST_NAME=`echo $i | cut -d'.' -f1`
    if hadoop fs -test -e "hdfs://${DEST_NAME}/" ; then
        DEST="hdfs://${DEST_NAME}:8020"
        DEST_BARE="hdfs://${DEST_NAME}:8020"
    fi
done


echo "Sync from source to destination has been started"
echo "Source: $SRC"
echo "Destination: $DEST"


# Exit if there is no destination
if [ -z "$DEST/user/$USER" ] ; then
    echo "There is no current destination Environment"
    echo "Sync failure, no destination environment has been found, stopping sync" | mailx -r "process_monitor@experian.com" -s "Data Sync Failure: $RUNDATE" $MAILLIST
    exit 0
fi


# Create LOGS_DIR directory if it doesnt exist
if [[ ! -e $LOGS_DIR ]]
then
    echo No LOGS_DIR directory, creating
    mkdir $LOGS_DIR
fi


# Function to sync data, Arguments required:-
# 1. The parent path of data to be synced in source, 2. The folder pattern or date, 3. Process Name
sync_data(){
  SOURCE_PATH="$1"  
  FOLDER_PATTERN="$2"
  PROCESS="$3"
  UPDATED_PATH="${SOURCE_PATH:1}"

  FOLDER_NAME=$(basename $(hdfs dfs -ls $SOURCE_PATH | grep -E $FOLDER_PATTERN | awk '{print$8}'))
  if [[ $? -ne 0 ]]; then
    echo "Failed to get folder name from HDFS for source path: $SOURCE_PATH and folder pattern: $FOLDER_PATTERN" >> "$LOGS_DIR"/data_sync_"$RUNDATE".log
    return 1
  fi
  SRC_PATH="$SRC/$UPDATED_PATH/$FOLDER_NAME"
  DST_PATH="$DEST/$UPDATED_PATH/$FOLDER_NAME" 
  if hdfs dfs -test -e "$SRC_PATH"
  then
    distcp_retry "hadoop distcp -Ddfs.replication=2 -pt -update -delete -filters ./distcp_exclude.list $SRC_PATH $DST_PATH" "$LOGS_DIR"/data_sync_"$RUNDATE".log
    echo "Synced $PROCESS Data"
  else
    echo "$SRC_PATH doesn't exist" >> "$LOGS_DIR"/data_sync_"$RUNDATE".log
  fi
}

# Sync processes
echo "########################################################################################################"
starttime=$(date +%s)
echo "Started data sync process at $(date '+%Y-%m-%d %H:%M:%S')" >> $LOGS_DIR/data_sync_$RUNDATE.log

## Pubmatic process syncing
echo "Syncing DE data"
sync_data $HDFS_DE_PATH /$WEEK_START_DATE$ "DE"

echo "Syncing UC data"
sync_data $HDFS_UC_PATH /$RUNDATE$ "UC"

echo "Syncing ID5 data"
sync_data $HDFS_ID5_PATH /$WEEK_START_DATE$ "ID5"

echo "Syncing taxonomy input"
sync_data $HDFS_TAXONOMY_PATH /$RUNDATE$ "Taxonomy"

echo "Syncing process outputs"
sync_data $HDFS_PROCESS_PATH /$WEEK_START_DATE$ "Process Output"

echo "Syncing udprn data"
sync_data $HDFS_UDPRN_PATH /$RUNDATE$ "UDPRN"

echo "Syncing marketplace outputs"
sync_data $HDFS_PUBMATIC_PATH /$WEEK_START_DATE$ "Pubmatic Output"
sync_data $HDFS_EYEOTA_PATH /$WEEK_START_DATE$ "Eyeota Output"

echo "Syncing stats"
sync_data $HDFS_STATS_PATH /$WEEK_START_DATE$ "Stats"

## ID_graph process syncing
echo "Syncing cv data"
sync_data $IDG_HDFS_PATH/cv /$RUNDATE_MONTHYEAR$ "CV Input"

echo "Syncing fpid data"
sync_data $IDG_HDFS_PATH/fpid /$RUNDATE$ "Fpid Intermediate Files"

echo "Syncing hems data"
sync_data $IDG_HDFS_PATH/hems /$RUNDATE_MONTHYEAR$ "Hems Input"

echo "Syncing ip data"
sync_data $IDG_HDFS_PATH/ip /$RUNDATE$ "IP Intermediate Files"

echo "Syncing mobile data"
sync_data $IDG_HDFS_PATH/mobile /$RUNDATE_MONTHYEAR$ "Mobile Input"

echo "Syncing pc data"
sync_data $IDG_HDFS_PATH/pc /$RUNDATE_YEAR$ "PC Input"

echo "Syncing uid data"
sync_data $IDG_HDFS_PATH/uid /$RUNDATE$ "UID Intermediate Files"

echo "Syncing maids data"
sync_data $IDG_HDFS_PATH/maids /$RUNDATE$ "Maids Intermediate Files"

if [ $RUNDATE == $WEEK_START_DATE ]
then
  echo "Syncing graphs"
  sync_data $IDG_HDFS_PATH/graphs/fpid "fpid_" "Fpid Graph"
  sync_data $IDG_HDFS_PATH/graphs/hems "hems_" "Hems Graph"
  sync_data $IDG_HDFS_PATH/graphs/ip "ip_" "IP Graph"
  sync_data $IDG_HDFS_PATH/graphs/maids "maids_" "Maids Graph"
  sync_data $IDG_HDFS_PATH/graphs/mobile "mobile_" "Mobile Graph"
  sync_data $IDG_HDFS_PATH/graphs/pc "pc_" "PC Graph"
  sync_data $IDG_HDFS_PATH/graphs/uid "uid_" "UID Graph"
  sync_data $IDG_HDFS_PATH/graphs/apnids "apnids_" "ApnId Graph"
  sync_data $IDG_HDFS_PATH/graphs/ttdids "ttdids_" "TTDId Graph"
  echo "Syncing idgraph stats"
  sync_data $IDG_HDFS_PATH/stats/crossover "idg_hh_crossover_stats__" "Crossover Stats"
  sync_data $IDG_HDFS_PATH/stats/crossover "idg_hh_exclusive_stats__" "Crossover Exclusive Stats"
  sync_data $IDG_HDFS_PATH/stats/fpid/standard "idg_fpid_stats__" "Fpid Standard Stats"
  sync_data $IDG_HDFS_PATH/stats/fpid/distribution "idg_fpid_id_count_distribution_stats__" "Fpid-Id Distribution Stats"
  sync_data $IDG_HDFS_PATH/stats/fpid/distribution "idg_id_fpid_count_distribution_stats__" "Id-Fpid Distribution Stats"
  sync_data $IDG_HDFS_PATH/stats/hems/standard "idg_hems_stats__" "Hems Stats"
  sync_data $IDG_HDFS_PATH/stats/ip/standard "idg_ip_stats__" "IP Stats"
  sync_data $IDG_HDFS_PATH/stats/maids/standard "idg_maids_stats__" "Maids Stats"
  sync_data $IDG_HDFS_PATH/stats/pc/standard "idg_pc_stats__" "PC Stats"
  sync_data $IDG_HDFS_PATH/stats/uid/standard "idg_uid_stats__" "UID Stats"
  sync_data $IDG_HDFS_PATH/stats/mobile/standard "idg_mobile_stats__" "Mobile Stats"
  sync_data $IDG_HDFS_PATH/stats/apnids/standard "idg_apnids_stats__" "ApnId Stats"
  sync_data $IDG_HDFS_PATH/stats/ttdids/standard "idg_ttdids_stats__" "TTDId Stats"
else
  echo "Today is not $WEEK_START_DATE so will not sync graphs"
fi

echo "Syncing idg lookup"
sync_data ${IDG_HDFS_PATH}/lookup match_id_keys "Match ID Secret Keys"
sync_data $IDG_HDFS_PATH/lookup/experian match_id_lookup_experian_"$IDG_RUNDATE" "Lookup Data"
sync_data $IDG_HDFS_PATH/lookup/evorra match_id_lookup_evorra_"$IDG_RUNDATE" "Evorra match_id Lookup Data"
sync_data $IDG_HDFS_PATH/lookup/evorra cbk_hh_lookup_evorra_"$IDG_RUNDATE" "Evorra cbk Lookup Data"
sync_data $IDG_HDFS_PATH/lookup/audigent match_id_lookup_audigent_"$IDG_RUNDATE" "Audigent Lookup Data"

echo "Syncing custom segment data"
sync_data $CUSTOM_SEG_PATH/data /$WEEK_START_DATE$ "Custom Data"
sync_data $CUSTOM_SEG_PATH/process /$WEEK_START_DATE$ "Custom Process Data"


echo "Syncing Consumer Sync Data"
sync_data $HDFS_PMAX_PATH/raw pulse_max_raw_"$RUNDATE" "Pulse Max Raw Data"
sync_data $HDFS_PMAX_PATH/clean pulse_max_"$RUNDATE" "Pulse Max Clean Data"


#finished process
endtime=$(date +%s)
duration=$(($endtime - $starttime))
echo "Finished data sync process at $(date '+%Y-%m-%d %H:%M:%S'), took $duration seconds to complete" >> $LOGS_DIR/data_sync_$RUNDATE.log
echo "Data sync completed"

