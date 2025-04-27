#!/bin/bash

# === CONFIG ===
CONDA_ENV="elm"
HOSTFILE_PATH="/home/exouser/shared_data/final_project/src/hostfile.txt"
SCRIPT_PATH="/home/exouser/shared_data/final_project/src/"
# ==============

# Help message
if [ "$#" -ne 3 ] || [ "$1" == "--help" ]; then
    echo "Usage: ./run_na_forcinggen.sh <python_script> <M> <N> <timesteps> <multi_node_flag>"
    echo "  <python_script>: The Python script to run (e.g., NA_forcingGEN_PNetCDF.py)"
    echo "  <M>: Number of processes per file (time splitting)"
    echo "  <multi_node_flag>: 0 for single node, 1 for multi-node using hostfile"
    exit 1
fi

# === Parse arguments ===
SCRIPT="$SCRIPT_PATH$1"
TOTAL_PROCS=$2
MULTI_NODE=$3

# === Conda setup ===
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

# === Find Python executable ===
PYTHON_PATH="$(which python)"

# === Build mpiexec command ===
if [ "$MULTI_NODE" -eq 1 ]; then
    # Multi-node mode: must use absolute python path to ensure correct env
    MPI_CMD="mpiexec -f $HOSTFILE_PATH -n $TOTAL_PROCS $PYTHON_PATH $SCRIPT"
else
    # Single-node mode
    MPI_CMD="mpiexec -n $TOTAL_PROCS $PYTHON_PATH $SCRIPT"
fi

# === Run ===
echo "Running '$SCRIPT' using $TOTAL_PROCS MPI processes"
echo "Conda env: '$CONDA_ENV'"
echo "Python: '$PYTHON_PATH'"
echo "Command: $MPI_CMD"

eval "$MPI_CMD"
