# Introduction 
This repo will contain scripts required for data transfer or process tracking for Match 2.0

# Structure
    -   all config parameters for bash scripts are set in config.sh
    -   all bash functions are setup in process_functions.sh
    -   all scripts are run via process_flow.sh
    -   all logs will be stored in logs folder
    -   all pyspark scripts are stored in sparkscripts folder

# Usage

Scripts usage in sparkscripts folder

1. *config.py*
    This script contains all configurable parameters requiured for spark scripts like mask, paths etc.

2. *helper_functions.py*
    This script contains all functions that are generic and can be reused/shared amongst multiple spark scripts

3. *split_file.py*
    This script splits a local file into partitioned files in HDFS for improving performance of subsequent spark queries.
    Arguments:
    1. mask (-m) - the date for which stats is run, default is today
    2. input_folder (-if) - the folder in local containing files whose stats is required
    3. output_folder (-of) - the folder in HDFS where files will be output
    4. (optional) file_pattern (-fp) - the file name pattern to search for in the given input directory, default is False
    5. (optional) islocal (-l) - flag to check if script needs to run for files in local or HDFS path, default is "True"


4. *get_stats.py*
    This script can generate the below stats on local or files in HDFS. It takes the following arguments: -
    Arguments:
    1. mask (-m)  - the date for which stats is run, default is today
    2. input_folder (-ip) - the folder in local or hdfs containing files whose stats is required
    3. (optional) file_pattern (-fp) - the file name pattern to search for in the given input directory, default is False
    4. (optional) stats_path (-sp) - the path to output stats, default is in HDFS in server and runlocal folder otherwise
    5. (optional) islocal (-l) - flag to denote if script needs to read files in local or HDFS, default is "True"
    6. (optional) email_to (-et) - list of email receipients, default is specified in the config file

    Stats:
    "date": date of stats run
    "file_name": name of file
    "file_received_time" file creation time
    "file_size" : file size in bytes, K, M, G, T
    "record_counts" : count of records in each file

5.  *format_taxonomy_lookup.py*
    This script reformats the taxonomy lookup file sent by Donovan's team and converts multi tiered headers into a single header.
    It looks for the latest file called "Digital_Taxonomy_Master_Layout_created_*.xlsx" and formats it based on requirements and uploads it into a folder in local.It takes the following arguments: -
    1. mask (-m)  - the date for which stats is run, default is today
    2. input_folder (-if) - the folder in local containing taxonomy lookup file
    3. output_folder (-of) - the folder in local where file will be output

# How to run

1. Windows:
    run the spark scripts - gets_stats and split_huge_file via spark-submit
    eg: spark submit get_stats.py -ip <folder>

2. Server/Linux
    add the required script in process_flow.sh and run it via ./process_flow.sh OR
    run the spark scripts via spark-submit


# Versions

# Version 0.1 - 15 Nov 2022
    - initial setup of repository with all scripts and folders.

# Version 0.2 - 21 Nov 2022
    - added stats script alongwith helper and config files.
    - moved all spark scripts into sparkscripts folder
    - added emailing functionality via python
    - added usage in readme file

# Version 0.3 - 25 Nov 2022
    - added check to not send email or write out stats for exactly the same files
    - added function to compare two spark dataframes
    - added week start date in stats, stat filename, stat foldername
    - added check to not send emails continuously for all files

# Version 0.4 - 29 Nov 2022
    - added testing structure, dummy input data, expected outputs, test scripts for both bash and spark scripts

# Version 0.5 - 07 Dec 2022
    - changed split script output to parquet for easy storage and better performance, updated tests
    - added bash function to remove old file from given directory
    - updated filesplit to split files in HDFS as well
    - created process flow bash script for actual process

# Version 0.6 - 27 Dec 2022
    - fixed issues with create_pubmatic_file bash function
    - updated get_stats script, config, helper_functions to run stats for tsv and gz files
    - updated process_flow script with actual script names
    - added in process_flow_test.sh and conf_test.sh in testing for testing in server

# Version 0.7 - 02 Feb 2023
    - added bash script for getting pipeline stats for external files, transfer them to their respective paths in hdfs and then move them to local backup
    - added timing for "to_de" stats generation

# Version 0.8 - 24 Feb 2023
    - setup dr sync script to sync between stage and live servers

# Version 0.9 - 07 Aug 2023
    - setup from_id5 function process_flow script to move split input files from id5 into required format and upload to HDFS

# Version 1.0 - 05 Oct 2023
    - setup from_unacast function in process_flow script to move split input files from unacast into required format and upload to HDFS

# Version 1.1 - 19 Oct 2023
    - added pubmatic uid backlog process and stats script to process_flow
    - updated marketplace name to be lowercase in config
    - updated custom script to look for presence of any xlsx files even if multiple
    - fixed get_stats for running on tuesdays

# Version 1.2 - 06 Nov 2023    
    - updated pubmatic process to use pigz for faster gzipping
    - split pubmatic files and added counts to the end of file as per their requirements

# Version 1.3 - 14 Nov 2023  
    - added dashboard restart as a function so that the dashboard is refreshed every hour
    - moved s06 stats to from de function instead of after chv_mk2 creation

# Version 1.4 - 20 Dec 2023
    - added statement to unacast trigger to prevent it from picking up inprogress files
    - added unacast automation, moved ip supplier stats to run after unacast and de files have been processed
    - added maids scripts in pubmatic processing

# Version 1.5 - 19 Jan 2024
    - added checks to trigger conditions so that they are not re-triggered if latest file already exists in hdfs
    - fixed taxonomy lookup function to be able to access two different date formats i.e. YYYY-MM-DD and YYYYMMDD, updated pubmatic processing time trigger to 2PM
    - added maids scripts in pubmatic processing

# Version 1.6 - 26 Jan 2024
    - added eyteota trigger function
    - updated unacast trigger to run at 9pm instead of checking the latest file is present as two batches of files may exist, added move file statement for eyeota trigger to move the files from temp to live sts path

# Version 1.7 -  12 Mar 2024
    - added script for idgraph triggers, updated process_flow.sh to create trigger flags for idgraphs, added bash configs for idgraph
    - updated logging and optimized the final process flow function    

# Version 1.8 -  22 Mar 2024
    - enclosed the date folders in such a way that the data is synced when there is an exact match and not partial matches
    - updated sync_data script to sync id_graph data as well, updated idg_process_flow.sh to fix the dates used in idgraph based on latest standardized process, removed redundant file

# Version 1.9 -  17 Apr 2024
    - updated sync data script to sync custom data

# Version 2.0 - 14 May 2024
    - added P36_2 trigger
    - split s05 into s05a and s05b, s05a will need to be run before de and uc data are deleted, s05b will run during chv mk2 creation
    - updated rules for idgraph creation process
    - updated condition for hems,mobile,cv,pc graph creation to run as part of weekly process or when a new file comes in
    - updated paths of stats from id_graph/stats to id_graph/stats/standard

# Version 2.1 - 29 Jul 2024
    - added run of crossover stats scripts once all graphs have been updated
    - idg_process_flow: added triggers for ttdids and apnids, added both triggers to the trigger_all_graphs function but left commented out, this is to be commented back in once process scripts are complete
    - added netacuity ingestion process

