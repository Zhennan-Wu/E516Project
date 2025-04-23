#!/bin/bash
#SBATCH -J NA_forcingGEN_2nodes_64processes
#SBATCH -p general
#SBATCH -o output_%j.txt
#SBATCH -e err_%j.err
#SBATCH --mail-type=ALL
#SBATCH --mail-user=xinhding@iu.edu
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=64
#SBATCH --time=02:00:00
#SBATCH --exclusive
#SBATCH -A r01156
#Load any modules that your program needs
module load conda
conda activate elm

#Run your program
mpiexec -n 128 python /N/u/xinhding/BigRed200/kiloCraft/NA_forcingGEN_pnetcdf_independent_unblock_time.py /N/project/hpc_innovation_slate/ELM_Dataset/kilocraft_input_cdf5_test /N/project/hpc_innovation_slate/ELM_Dataset/kilocraft_output_cdf5 -1 128 1
