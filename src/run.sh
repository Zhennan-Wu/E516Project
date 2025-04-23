#!/bin/bash

# === CONFIG ===
CONDA_ENV="elm"
INPUT_PATH="../dataset"
OUTPUT_PATH="../output"
# ==============

# Help message
if [ "$#" -ne 4 ] || [ "$1" == "--help" ]; then
    echo "Usage: ./run_na_forcinggen.sh <python_script> <M> <N> <timesteps>"
    echo "  <python_script>: The Python script to run (e.g., NA_forcingGEN_PNetCDF.py)"
    echo "  <M>: Number of processes per file (time splitting)"
    echo "  <N>: Number of files to process simultaneously"
    echo "  <timesteps>: Timesteps to process, or -1 for all"
    exit 1
fi

SCRIPT=$1
M=$2
N=$3
TIMESTEPS=$4
TOTAL_PROCS=$((M * N))

# === Conda setup ===
# Source conda so we can use `conda activate` in this script
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

# === Run command ===
echo "Running '$SCRIPT' using $TOTAL_PROCS MPI processes in conda env '$CONDA_ENV'"
echo "Command: mpiexec -n $TOTAL_PROCS python $SCRIPT $INPUT_PATH $OUTPUT_PATH $TIMESTEPS $M $N"

mpiexec -n "$TOTAL_PROCS" python "$SCRIPT" "$INPUT_PATH" "$OUTPUT_PATH" "$TIMESTEPS" "$M" "$N"
