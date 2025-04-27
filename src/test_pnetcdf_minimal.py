from mpi4py import MPI
import pnetcdf as pnc
import numpy as np
import os

# Get the global MPI communicator
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Define file name
output_file = "test_parallel_pnetcdf.nc"

# Each process prepares its own little data
local_data = np.full((10,), rank, dtype=np.float32)  # Each rank has 10 elements set to its rank ID

# Create the output NetCDF file
if os.path.exists(output_file) and rank == 0:
    os.remove(output_file)  # Clean old file (only rank 0)

comm.Barrier()  # Synchronize before creating file

# Open file in write mode with PNetCDF
ncfile = pnc.File(filename=output_file, mode='w', format='NC_64BIT_DATA', comm=comm)

# Define dimensions (global size = size * 10)
ncfile.def_dim('x', size * 10)

# Define a single variable
var_x = ncfile.def_var('var', pnc.NC_FLOAT, ['x'])

# End define mode
ncfile.enddef()

# Each rank writes its part
start = [rank * 10]
count = [10]

req = var_x.iput_var(start=start, count=count, data=local_data)

# Wait for all non-blocking write operations to complete
ncfile.wait_all([req])

# Close the file
ncfile.close()

if rank == 0:
    print("✅ Successfully created test_parallel_pnetcdf.nc with", size, "processes.")
