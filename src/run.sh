#!/bin/bash

# === CONFIG ===
CONDA_ENV="elm"
INPUT_PATH="/home/exouser/shared_data/final_project/dataset"
OUTPUT_PATH="/home/exouser/shared_data/final_project/output"
HOSTFILE_PATH="/home/exouser/shared_data/final_project/src/hostfile.txt"
SCRIPT_PATH="/home/exouser/shared_data/final_project/src/"
# ==============

# Help message
if [ "$#" -ne 5 ] || [ "$1" == "--help" ]; then
    echo "Usage: ./run_na_forcinggen.sh <python_script> <M> <N> <timesteps> <multi_node_flag>"
    echo "  <python_script>: The Python script to run (e.g., NA_forcingGEN_PNetCDF.py)"
    echo "  <M>: Number of processes per file (time splitting)"
    echo "  <N>: Number of files to process simultaneously"
    echo "  <timesteps>: Timesteps to process, or -1 for all"
    echo "  <multi_node_flag>: 0 for single node, 1 for multi-node using hostfile"
    exit 1
fi

# === Parse arguments ===
SCRIPT="$SCRIPT_PATH$1"
M=$2
N=$3
TIMESTEPS=$4
MULTI_NODE=$5
TOTAL_PROCS=$((M * N))

# === Conda setup ===
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

# === Find Python executable ===
PYTHON_PATH="$(which python)"

# === Build mpiexec command ===
if [ "$MULTI_NODE" -eq 1 ]; then
    # Multi-node mode: must use absolute python path to ensure correct env
    MPI_CMD="mpiexec -f $HOSTFILE_PATH -n $TOTAL_PROCS $PYTHON_PATH $SCRIPT $INPUT_PATH $OUTPUT_PATH $TIMESTEPS $M $N"
else
    # Single-node mode
    MPI_CMD="mpiexec -n $TOTAL_PROCS $PYTHON_PATH $SCRIPT $INPUT_PATH $OUTPUT_PATH $TIMESTEPS $M $N"
fi

# === Run ===
echo "Running '$SCRIPT' using $TOTAL_PROCS MPI processes"
echo "Conda env: '$CONDA_ENV'"
echo "Python: '$PYTHON_PATH'"
echo "Command: $MPI_CMD"

eval "$MPI_CMD"
