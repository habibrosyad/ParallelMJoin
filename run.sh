#!/bin/bash

# This script assume ParallelMJoin.jar is in the same directory

# Trial id
trial=${1}
# Dataset path
data_path=${2}
# Rates per second
rates="1000 3000 5000"
# Window size in milis
windows="60000 300000 600000"
# Output path
output_path="output"
# Output file name
output="$output_path/output_${1}_${3}" # ${3} should be a date

# Check dataset path
if [[ ! -d $data_path ]]; then
	exit 1
fi

# Create output directory if it doesn't exist
if [[ ! -d $output_path ]]; then
	mkdir $output_path
fi

# Run the experiments n_trials time
for r in $rates; do
	for w in $windows; do
		# [trial_id, experiment_id, threads, window_ms, rate_s, latency_ms, processed_s, processed_avg_s, output_s, comparison_s, comparison_avg_s]
		java -jar ParallelMJoin.jar $r $w $data_path | while read line; do echo "$trial,Scenario4a3,$line"; done >> $output
		sleep 2
	done
done

# Send mail as notification that the task is finished. 
# This assume that 'mail' is intalled and configured.
ip=`hostname -I`
echo "ParallelMJoin experiment is done on $ip" | mail -s "ParallelMJoin Experiment on $ip" habib.ryd@gmail.com