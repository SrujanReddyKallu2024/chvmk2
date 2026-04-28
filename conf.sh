CURRENT_DIR="/home/unity/match-datatransfer"
LOGS_DIR="${CURRENT_DIR}/logs"
SP_SCRIPT_DIR="${CURRENT_DIR}/sparkscripts"
LISTENER_PATH="/data/datahub-filelistener"
DATA_FROM_STS="/data/data_from_sts"
DATA_FROM_STS_BACKUP="/data/data_from_sts_backup"
DATA_TO_STS="/data/data_to_sts"
DATA_TO_STS_TEMP="/data/data_to_sts/temp"
HDFS_TAXONOMY_PATH="/user/unity/match2/taxonomy"
HDFS_DE_PATH="/user/unity/match2/de"
HDFS_ID5_PATH="/user/unity/match2/id5"
HDFS_PUBMATIC_PATH="/user/unity/match2/marketplaces/pubmatic"
HDFS_EYEOTA_PATH="/user/unity/match2/marketplaces/eyeota"
HDFS_PROCESS_PATH="/user/unity/match2/process"
HDFS_STATS_PATH="/user/unity/match2/stats"
HDFS_MC_PATH="/user/unity/match2/mc"
HDFS_UDPRN_PATH="/user/unity/match2/udprn"
TEMP_PATH="/user/unity/match2/temp"
PUBMATIC_ID=857
SCRIPTDIR="/home/unity/match3_mvp1/code/process/scripts"
STATSSCRIPTDIR="/home/unity/match3_mvp1/code/stats/scripts"
TAXONOMY_LOOKUP_PATH="/data/taxonomy_lookup"
HDFS_UC_PATH="/user/unity/match2/unacast"
PUBM_LOCK_FILE="/tmp/process_flow.lock"
CUSTOM_SEG_PATH="/user/unity/match2/custom"

MAILLIST="emsactivatealerts@experian.com"

# Queue names for Spark job execution
QUEUE1="andy"
QUEUE2="adam"
QUEUE3="unity"

# Depending on the $(hostname) set the SERVER variable to live, stage or dev
if [ $(hostname) == "ukfhpapmtc02" ]; then
    SERVER="live"
elif [ $(hostname) == "ukbldapmtc02" ]; then
    SERVER="stage"
elif [ $(hostname) == "ukbltapmtc01" ]; then
    SERVER="dev"
fi

# ========================
# Date Variables
# ========================
if [ $# -eq 0 ]; then
    #echo "No arguments provided"
    RUNDATE=$(date +%Y-%m-%d)
    RUNDATETIME=$(date)
else
    RUNDATE=$1
    RUNDATETIME=$(date -d "$1")
fi

YYYY_MM_DD_HH_MM=$(date -d "$RUNDATETIME" +"%Y-%m-%d-%H-%M")
YYYY_MM_DD_HH=$(date -d "$RUNDATETIME" +%Y-%m-%d-%H)
YYYY_MM_DD=$(date -d "$RUNDATETIME" +%Y-%m-%d)

# Calculate the day of the week (1=Monday, ..., 7=Sunday)
day_of_week=$(date -d "$RUNDATE" +%u)

# Calculate the dates for the start of the week, Tuesday, Wednesday, Friday, Saturday, and Sunday
WEEK_START_DATE=$(date -d "$RUNDATE -$((day_of_week - 1)) days" +%Y-%m-%d)
TUE_DATE=$(date -d "$RUNDATE -$((day_of_week - 2)) days" +%Y-%m-%d)
WED_DATE=$(date -d "$RUNDATE -$((day_of_week - 3)) days" +%Y-%m-%d)
FRI_DATE=$(date -d "$RUNDATE -$((day_of_week - 5)) days" +%Y-%m-%d)
SAT_DATE=$(date -d "$RUNDATE -$((day_of_week - 6)) days" +%Y-%m-%d)
SUN_DATE=$(date -d "$RUNDATE -$((day_of_week - 7)) days" +%Y-%m-%d)

# Calculate the start date of the next week
NEXT_WEEK_START_DATE=$(date -d "$WEEK_START_DATE +7 days" +%Y-%m-%d)

# Get the year from the RUNDATE
DYEAR=$(date -d "$RUNDATE" +%Y)

LOCAL_HOME="/home/unity"

# ========================
# Custom Segment Configs
# ========================

MARKETPLACES=("pubmatic") #add new marketplaces separated by space
MKP_LOCK_FILE="/tmp/custom_marketplace_process.lock"
SOURCE_MKP_FILES="/data/data_from_sts/marketplace_input_files"
DEST_MKP_FILES="/data/marketplace_files"


# ========================
# ID GRAPH
# ========================
IDG_SCRIPTDIR="/home/unity/match-id-graph/process"
IDG_CONFIDENCE_SCRIPTDIR="/home/unity/match-id-graph-confidence-score/scripts"
IDG_STATSSCRIPTDIR="/home/unity/match-id-graph/stats"
IDG_DATA_FROM_STS="/data/idg_inputs"
IDG_HDFS_PATH="/user/unity/id_graph"
HDFS_IDG_LOOKUP_DIR="${IDG_HDFS_PATH}/lookup"
IDG_LOCK_FILE="/tmp/idg_process_flow.lock"
IDG_FLAG_PATH="/user/unity/id_graph/flags"
IDG_RUNDATE=$(date -d "$RUNDATE" +%Y%m%d)
RUNDATE_MONTHYEAR=$(date -d "$RUNDATE" +%Y-%m)
RUNDATE_YEAR=$(date -d "$RUNDATE" +%Y)
IDG_INSIGHTS_SCRIPTDIR="/home/unity/match-id-graph/insights"


# ========================
# Marketplaces
# ========================

# MKT_REPO="/home/unity/match-marketplaces"
# MKT_HDFS_PATH="/user/unity/match2/marketplaces"
# MKP_GEN_LOCK_FILE="/tmp/mkp_gen_process_flow.lock"
# MKP_LOCAL_PATH="/data/data_to_sts/marketplaces"
# # Audigent
# AUD_SCRIPT_DIR="$MKT_REPO/audigent"
# AUD_MKT_HDFS_PATH="$MKT_HDFS_PATH/audigent"
# AUD_IDS=( "ip" "hems" "maids" "uid" "ttdids" "apnids" )
# AUD_LOCAL_TEMP_PATH="$DATA_TO_STS_TEMP/audigent"
# AUD_LOCAL_DATA_PATH="$MKP_LOCAL_PATH/audigent/data"
# AUD_LOCAL_METADATA_PATH="$MKP_LOCAL_PATH/audigent/metadata"


# ========================
# CONSUMER SYNC
# ========================


CS_HDFS="/user/unity/consumersync"
CS_REPO="${LOCAL_HOME}/match-consumer-sync"
CS_REPO_PROCESS="${LOCAL_HOME}/consumer-sync-%s" # INSIGHTS_REPO=$(printf "$CS_REPO_PROCESS" "insights") --> consumer-sync-insights
CS_LOCAL="/data/consumersync"
CS_DATA_FROM_STS="${DATA_FROM_STS}/consumersync"
MKT_IN_DATA_FROM_STS="${DATA_FROM_STS}/internal/onboarding"
CS_ONBOARD_ERRORS_LOCALDIR="${DATA_FROM_STS}/errors"
CS_PY_SCRIPT_DIR="${CS_REPO}/scripts"
CS_EU_REPO="${LOCAL_HOME}/consumer-sync-eu"
CS_EU_PY_SCRIPT_DIR="${CS_EU_REPO}/scripts"
CS_EVENT_SCRIPT_DIR="${CURRENT_DIR}/csync_events"
CS_RESOURCES_DIR="${CS_REPO}/resources"
CS_ONBOARDING_DIR="${CS_RESOURCES_DIR}/onboarding"
CS_CLIENT_DATA_DIR="${CS_HDFS}/client"
CS_CLIENT_RAW_DIR="${CS_CLIENT_DATA_DIR}/raw"
CS_CLIENT_CLEAN_DIR="${CS_CLIENT_DATA_DIR}/clean"
CS_CLIENT_ARCHIVE_DIR="${CS_CLIENT_DATA_DIR}/archive"
CS_CLIENT_ERRORS_DIR="${CS_CLIENT_DATA_DIR}/errors"
CS_LOCAL_ERRORS_DIR="${CS_LOCAL}/errors" # Client Data which fails validation
CS_STATS="${CS_HDFS}/stats"
CS_LOCK_FILE="/tmp/csync_process_flow.lock" # Lock file to ensure only one process runs at a time
HDFS_PM_PATH="${CS_HDFS}/pulse_max"
CS_LOG_FILE="${LOGS_DIR}/csync_process_flow/csync_process_flow-${YYYY_MM_DD}.log" # needs to be unique file for each cron trigger of csync_process_flow.sh script
WV_LOCAL_IN_DIR="${DATA_FROM_STS}/internal/data"
WV_RAW_HDFS_DIR="${CS_HDFS}/eu/wv/raw"
WV_CLEAN_HDFS_DIR="${CS_HDFS}/eu/wv/clean"


# ECS Truthset Ingestion
ECS_LOCK_FILE="/tmp/ecs_process_flow.lock"
ECS_SOURCE_PATH="${DATA_FROM_STS}/internal/data"
ECS_HDFS_PATH="/user/unity/match2/ecs"

# Audigent Ingestion
AUD_IDS=("appnexus" "ipv4" "ipv6" "openx" "pubmatic" "sonobi" "thetradedesk")
AUD_VISITS_DIR="${DATA_FROM_STS}/audigent/visits"
AUD_IDMATCH_DIR="${DATA_FROM_STS}/audigent/idmatch"
AUD_HDFS_DIR="/user/unity/id_graph/audigent"

# Data office paths
DATA_OFFICE_LOCAL_DIR="/data_office"
DIGITAL_TAX_INDIR="/data/digitaltaxonomy/input"
DIGITAL_TAX_BASEDIR="/data/digitaltaxonomy/base"
DIGITAL_TAX_HDFS_INDIR="/user/unity/match2/digitaltaxonomy/input"
GRAPH_INDIR="/data/idg_inputs/internal/identifiers"
AD_HOC_DIR="/data/data_from_sts/internal/misc"
ANALYTICS_INDIR="/data/data_from_sts/internal/analytics"

# File Transfer Paths
FT_SRC_DIR="/data/aws/from_s3"