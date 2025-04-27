try:
    from mpi4py import MPI
    HAS_MPI4PY = True
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

except ImportError:
    HAS_MPI4PY = False
    print("mpi4py is required for this script")
    rank = -1
    size =-1

# Print a message from each process
print(f"Hello from rank {rank} of {size} processes.")
