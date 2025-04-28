from mpi4py import MPI
import pnetcdf as pnc
import sys


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    print(f"Running with {size} MPI processes")


local_rank = comm.Get_rank()  # Rank within the sub-communicator for this file

source_file = '/home/exouser/shared_data/final_project/dataset/clmforc.Daymet4.1km.PRECTmms.2014-01.nc'

# Open with PNetCDF
src = pnc.File(filename=source_file, mode='r', comm=comm)