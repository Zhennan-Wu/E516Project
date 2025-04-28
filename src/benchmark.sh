#!/bin/bash

# === Parameters ===
FILES=("NA_forcingGEN_pnetcdf_collective_block_time.py" "NA_forcingGEN_pnetcdf_independent_unblock_time.py")
# FILES=("NA_forcingGEN_pnetcdf_independent_unblock_time.py")

NUM_PROCESSES=(8 16 32 64)
MULTI=0  # 0 for single node

# NUM_PROCESSES=(128)
# MULTI=1  # 1 for multiple nodes

REPEAT=8
SLEEP_TIME=10   # Seconds to sleep between runs

# === Output log ===
# CSVFILE="benchmark_results.csv"
CSVFILE="benchmark_results_multiple.csv"
echo "File,Processes,Trial,ReadTime,WriteTime,TotalTime" > $CSVFILE

# === Main loop ===
for FILE in "${FILES[@]}"; do
    for PROCS in "${NUM_PROCESSES[@]}"; do
        for ((i=1; i<=$REPEAT; i++)); do
            echo "Running $FILE with $PROCS processes (trial $i)"
            
            # Run your script and capture output
            ./run.sh $FILE $PROCS 1 -1 $MULTI > tmp_output.txt 2>&1

            # Extract relevant lines
            read_line=$(grep "Read time" tmp_output.txt)
            total_line=$(grep "Group 0: Processing" tmp_output.txt)

            # Parse the times
            if [[ $read_line && $total_line ]]; then
                # Extract read time
                read_time=$(echo $read_line | sed -n 's/.*Read time = \([0-9.]*\)s.*/\1/p')
                
                # Extract write time
                write_time=$(echo $read_line | sed -n 's/.*Write time = \([0-9.]*\)s.*/\1/p')
                
                # Extract total processing time
                total_time=$(echo $total_line | sed -n 's/.*took \([0-9.]*\) seconds.*/\1/p')
                
                # Append to CSV
                echo "$FILE,$PROCS,$i,$read_time,$write_time,$total_time" >> $CSVFILE
            else
                echo "Warning: Missing output for $FILE with $PROCS processes on trial $i" >> $CSVFILE
            fi

            # Sleep between runs
            sleep $SLEEP_TIME
        done
    done
done
