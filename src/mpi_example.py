from mpi4py import MPI

# Get the rank and size of the current process
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Print a message from each process
print(f"Hello from rank {rank} of {size} processes.")
