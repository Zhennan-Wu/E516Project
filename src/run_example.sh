#!/bin/bash

# === CONFIG ===
CONDA_ENV="elm"
HOSTFILE_PATH="/home/exouser/shared_data/final_project/src/hostfile.txt"   # <-- Default hostfile for multi-node run
SCRIPT_PATH="/home/exouser/shared_data/final_project/src/"
# ==============

# Help message
if [ "$#" -ne 2 ] || [ "$1" == "--help" ]; then
    echo "Usage: ./run_na_forcinggen.sh <python_script> <M> <N> <timesteps> <multi_node_flag>"
    echo "  <python_script>: The Python script to run (e.g., NA_forcingGEN_PNetCDF.py)"
    echo "  <multi_node_flag>: 0 for single node, 1 for multi-node using hostfile"
    exit 1
fi

SCRIPT="$SCRIPT_PATH$1"
MULTI_NODE=$2

# === Conda setup ===
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV"

# === Build mpiexec command ===
if [ "$MULTI_NODE" -eq 1 ]; then
    # Multi-node mode
    MPI_CMD="mpiexec -f $HOSTFILE_PATH -n $TOTAL_PROCS env "PATH=/home/exouser/miniforge3/envs/$CONDA_ENV/bin:$PATH" python $SCRIPT"
else
    # Single-node mode
    MPI_CMD="mpiexec -n $TOTAL_PROCS python $SCRIPT"
fi

# === Run ===
echo "Running '$SCRIPT' using $TOTAL_PROCS MPI processes in conda env '$CONDA_ENV'"
echo "Command: $MPI_CMD"

eval "$MPI_CMD"
