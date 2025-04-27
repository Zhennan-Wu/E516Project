from mpi4py import MPI
import pnetcdf
from pnetcdf import constants as pnetcdf_const

# Get MPI communicator, rank, size
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    print(f"Running with {size} MPI processes")

# Create a new NetCDF file (parallel)
filename = "testfile.nc"
cmode = pnetcdf_const.NC_CLOBBER  # overwrite if exists

# Open file collectively
ncfile = pnetcdf.File.create(filename, cmode=cmode, comm=comm)

# Define a dimension
dim_name = "x"
dim_size = 4
ncfile.def_dim(dim_name, dim_size)

# Define a variable
var_name = "var"
var_id = ncfile.def_var(var_name, pnetcdf_const.NC_INT, (dim_name,))

# End define mode
ncfile.enddef()

# Each rank writes one value
local_value = rank * 10

# Define the write offset
start = [rank]
count = [1]

# Do non-blocking put
req = ncfile.iput_vara(var_id, start, count, [local_value])

# Wait for completion
ncfile.wait_all([req])

# Close file
ncfile.close()

if rank == 0:
    print(f"Finished writing {filename}")
