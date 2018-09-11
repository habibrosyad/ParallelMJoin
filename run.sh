#!/bin/bash

# This script assume ParallelMJoin.jar is in the same directory

# Dataset path
data_path=${1}
# Number of trials
n_trials=${2}
# Output path
output_path="output"
# Output file name
output="$output_path/out_${3}" # ${3} should be a date

# Check dataset path
if [[ ! -d $data_path ]]; then
	exit 1
fi

# Get number of dataset as number of cpu
n_cpus=$((`ls -l $data_path | wc -l`-1))

# Create output directory if it doesn't exist
if [[ ! -d $output_path ]]; then
	mkdir $output_path
fi

# Structure of the output file
# [algorithm,experiment,cpus,elapsed_s,initial_response_ms,output_total,output_s,comparison_total,comparison_s]

# Run the experiments n_trials time
for (( i=0; i<$n_trials; i++ )); do
	echo -n "ParallelMJoin,EquiJoinCommonShj,$n_cpus," >> $output
	java -jar ParallelMJoin.jar $data_path >> $output
	sleep 2
done

# Send mail as notification that the task is finished. 
# This assume that 'mail' is intalled and configured.
ip=`hostname -I`
echo "ParallelMJoin experiment is done on $ip" | mail -s "ParallelMJoin Experiment on $ip" habib.ryd@gmail.com