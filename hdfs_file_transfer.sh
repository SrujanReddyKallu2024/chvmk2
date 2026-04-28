#!/bin/bash

source conf.sh
source /etc/profile.d/hadoop.sh

DS_AUD_LOCAL_DIR="${FT_SRC_DIR}/internal/data_office/audigent"
DS_IDG_LOCAL_DIR="${FT_SRC_DIR}/internal/data_office/id_graph"
DS_DATE=$(date -d "$RUNDATE" +%Y%m%d)
YESTERDAY_DATE=$(date -d "$RUNDATE -1 day" +%Y%m%d)

declare -A aud_naming_convention=(
    ["appnexus"]="apnids"
    ["thetradedesk"]="ttdids"
	["sonobi"]="sonobiids"
	["pubmatic"]="pubmaticids"
	["openx"]="openxids"    
)


# arguments:
# 1. local input directory
# 2. file name pattern to match
run_pipeline_stats() {
	local indir="$1"
	local filepatt="$2"
	echo "Running pipeline stats script at $(date)"
	spark-submit $SP_SCRIPT_DIR/get_stats.py -m $RUNDATE -ip "$indir" -fp "$filepatt" -et $MAILLIST
	if [ $? -eq 0 ]; then
		echo "Completed pipeline stats script at $(date)"
	else
		echo "Failed to run pipeline stats script at $(date)"
	fi
}

# arguments:
# 1. regex pattern for basename
# 2. local input directory
# 3. HDFS destination directory
# 4. file_type: file | folder | both
transfer_files_to_hdfs_regex(){
    local regex_pattern="$1"
    local input_dir="$2"
    local hdfs_dir="$3"
    local file_type="${4:-both}"	
    local on_failure_fn="$5"
    
    local fail_count=0
    local find_type_arg=""
    case "$file_type" in
        file)   find_type_arg="-type f" ;;
        folder) find_type_arg="-type d" ;;
        both)   find_type_arg="" ;;
        *)
            echo "ERROR: file_type must be file, folder, or both"
            return 1
            ;;
    esac

    hdfs dfs -mkdir -p "$hdfs_dir"
    echo "searching for $file_type matching '$regex_pattern' in $input_dir"

    while IFS= read -r path; do
        local name
        name=$(basename "$path")
		echo "Found $file_type: $name at $path"
		if [[ "$input_dir" == *"audigent"* ]]; then ## Only apply renaming for Audigent files
			for key in "${!aud_naming_convention[@]}"; do
				if [[ "$name" == *"$key"* ]]; then
					name="${name//$key/${aud_naming_convention[$key]}}"
					echo "Updated filename to $name"
					break
				fi
			done
			name=$(echo "$name" | sed -E 's/([0-9]{4})([0-9]{2})([0-9]{2})/\1-\2-\3/') ## Reformat date to YYYY-MM-DD for Audigent files
			mv "$path" "$(dirname "$path")/$name" && wait $! && echo "Renamed file to $name" ## Rename the file and wait for the move to complete before proceeding
			path="$(dirname "$path")/$name"
		fi	
		
        local hdfs_target="$hdfs_dir/$name"

        if hdfs dfs -test -e "$hdfs_target"; then
            echo "$name already exists in $hdfs_dir, skipping"
            continue
        fi

		run_pipeline_stats "$input_dir" "$name"

        if [ -f "$path" ]; then
            echo "copying file: $name"
        elif [ -d "$path" ]; then
            echo "copying directory: $name"
        fi

        if hdfs dfs -copyFromLocal "$path" "$hdfs_dir/" && echo "copied copied $name to $hdfs_dir"; then            
            if rm -rf "$path"; then
                echo "Removed local copy of $name"
            else
                echo "WARNING: Failed to remove local copy of $name at $path"
                fail_count=$((fail_count + 1))
            fi            
        else
            echo "Failed to copy $name to $hdfs_dir"
            fail_count=$((fail_count + 1))            
        fi

        if [ $fail_count -gt 0 ] && [ -n "$on_failure_fn" ]; then
            "$on_failure_fn" "$name"
        fi
    done < <(find "$input_dir" -mindepth 1 -maxdepth 1 $find_type_arg -regextype posix-extended -regex ".*/$regex_pattern")

    echo "End transfer of pattern '$regex_pattern' to $hdfs_dir"
}


transfer_files() {    
    local -a failed_names=()

    increment_failure_count() {
        local failed_name="$1"        
        failed_names+=("$failed_name")
    }

	echo "####################################################"
	echo "Starting file transfer to HDFS at $(date)"
	# Audigent Data
	transfer_files_to_hdfs_regex ".*preprocessed_${YESTERDAY_DATE}\\.parquet" "$DS_AUD_LOCAL_DIR" "${AUD_HDFS_DIR}/${RUNDATE}" "file" "increment_failure_count"
	transfer_files_to_hdfs_regex ".*lookup_${YESTERDAY_DATE}\\.parquet" "$DS_AUD_LOCAL_DIR" "${AUD_HDFS_DIR}/${RUNDATE}" "file" "increment_failure_count"

    # ID Graphs
    transfer_files_to_hdfs_regex "ip_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/ip" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "hems_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/hems" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "mobile_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/mobile" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "pc_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/pc" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "uid_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/uid" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "maids_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/maids" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "ttdids_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/ttdids" "folder" "increment_failure_count"
    transfer_files_to_hdfs_regex "apnids_${DS_DATE}[0-9]{4}\\.parquet" "$DS_IDG_LOCAL_DIR" "${IDG_HDFS_PATH}/graphs/apnids" "folder" "increment_failure_count"

	echo "Completed file transfer to HDFS at $(date)"

    if [ ${#failed_names[@]} -ne 0 ]; then
        {
            echo -e "Failed to transfer the following files/directories:\n"
            printf '%s\n' "${failed_names[@]}"
        } | mailx -r "emsactivate@experian.com" -s "AWS to HDFS Transfer Failures" "$MAILLIST"
        return 1
    fi
}


transfer_files 2>&1 | grep -v 'INFO\|_JAVA_OPTIONS' | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush(); }' >> "$LOGS_DIR/hdfs_transfer-$RUNDATE.log"