#!/bin/bash

source /etc/profile.d/hadoop.sh
source conf.sh

#function to check if the process is running or not
function check_process_running(){
    if [ -f $MKP_LOCK_FILE ]
    then
        return 0
    else
        return 1
    fi
}

#arguments: 1. input folder, 2. filename or pattern, 3. destination folder
function move_files(){
    INPUT_FILES="$1/*$2*"
    mkdir -p $3
    for i in $INPUT_FILES
    do
        filename=$(basename $i)
        if test -f $i;then
            if test -f $3/$filename; then
                echo "$filename already exists in $3, cannot move"
            else
                mv $i $3
                echo "moved $i into $3"
            fi
        else
            echo "$i doesn't exist"
        fi
    done
}

#function to move marketplace segment files to their own dedicated folders
function move_mkp_files(){
    for mkp in ${MARKETPLACES[@]}
    do        
        move_files $SOURCE_MKP_FILES $mkp_custom_create $DEST_MKP_FILES/$mkp/custom/create
        move_files $SOURCE_MKP_FILES $mkp_custom_volume $DEST_MKP_FILES/$mkp/custom/create
        move_files $SOURCE_MKP_FILES $mkp_custom_replace $DEST_MKP_FILES/$mkp/custom/replace
        move_files $SOURCE_MKP_FILES $mkp_standard_add $DEST_MKP_FILES/$mkp/standard/add
        move_files $SOURCE_MKP_FILES $mkp_standard_replace $DEST_MKP_FILES/$mkp/standard/replace
    done
}


if check_process_running
then
    echo "process is still running"
else    
    move_mkp_files  >> "$LOGS_DIR/mkp_segment_files-$RUNDATE.log"
    touch $MKP_LOCK_FILE
    if compgen -G $DEST_MKP_FILES/pubmatic/custom/create/*.xlsx > /dev/null
    then        
        spark-submit --master yarn --queue unity /data/griffin/match3_mvp1/code/process/scripts/P110_1_Custom_Segement.py >> "$LOGS_DIR/custom_segment-$RUNDATE.log"
    fi
    echo "running"
    sleep 5
    rm $MKP_LOCK_FILE
fi