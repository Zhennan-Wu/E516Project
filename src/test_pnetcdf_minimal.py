from mpi4py import MPI
import pnetcdf
from pnetcdf import _pnetcdf  # hidden internal module

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    print(f"Running with {size} MPI processes")

filename = "testfile.nc"
cmode = _pnetcdf.NC_CLOBBER

ncfile = pnetcdf.File.create(filename, cmode=cmode, comm=comm)

dim_name = "x"
dim_size = 4
ncfile.def_dim(dim_name, dim_size)

var_name = "var"
var_id = ncfile.def_var(var_name, _pnetcdf.NC_INT, (dim_name,))

ncfile.enddef()

local_value = rank * 10

start = [rank]
count = [1]

req = ncfile.iput_vara(var_id, start, count, [local_value])

ncfile.wait_all([req])

ncfile.close()

if rank == 0:
    print(f"Finished writing {filename}")
