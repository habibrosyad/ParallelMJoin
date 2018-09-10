#!/bin/bash

# This script assume ParallelMJoin.jar is in the same directory

# Date for output suffix
now=`date +%Y-%m-%d.%H:%M:%S`

# Dataset path
data_path=${1}
# Output path
output_path="output"
# Output file name
output="$output_path/ParallelMJoin_$now"
# Number of threads
n_threads=${2}
# Number of trials
n_trials=${3}

# Check dataset path
if [[ ! -d $data_path ]]; then
	exit 1
fi

# Create output directory if it doesn't exist
if [[ ! -d $output_path ]]; then
	mkdir $output_path
fi

# Run the experiments n_trials time
for (( i=0; i<$n_trials; i++ )); do
	echo "TRIAL INSTANCE $i" >> $output
	java -jar ParallelMJoin.jar $data_path >> $output
	sleep 1
done

# Send mail as notification that the task is finished. 
# This assume that 'mail' is intalled and configured.
ip=`hostname -I`
echo "ParallelMJoin experiment is done on $ip" | mail -s "ParallelMJoin Experiment on $ip" habib.ryd@gmail.com