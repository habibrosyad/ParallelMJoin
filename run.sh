#!/bin/bash

# This script assume ParallelMJoin.jar is in the same directory

# Dataset path
data_path=${1}
# Rates per second
rates="1000 2000 4000 8000 16000 32000"
# Window size in milis
windows="60000 120000 300000 600000 900000"
# Output path
output_path="output"
# Output file name
output="$output_path/output_${2}" # ${2} should be a date

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
		# [experiment_id, trial_id, threads, window_ms, rate_s, latency_ms, processed_s, output_s, comparison_s, comparison_avg_s]
		java -jar ParallelMJoin.jar $r $w $data_path | while read line; do echo "Scenario4a3,$line"; done >> $output
		sleep 2
	done
done

# Send mail as notification that the task is finished. 
# This assume that 'mail' is intalled and configured.
ip=`hostname -I`
echo "ParallelMJoin experiment is done on $ip" | mail -s "ParallelMJoin Experiment on $ip" habib.ryd@gmail.com