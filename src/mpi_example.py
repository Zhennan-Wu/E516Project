try:
    from mpi4py import MPI
    HAS_MPI4PY = True

except ImportError:
    HAS_MPI4PY = False
    print("mpi4py is required for this script")
    rank = -1
    size =-1


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    proc_name = MPI.Get_processor_name()

    # Print a message from each process
    print(f"Hello from rank {rank} of {size} processes of name {proc_name}.")

if __name__ == "__main__":
    if HAS_MPI4PY:
        main()
    else:
        print("mpi4py is not available. This script requires mpi4py to run.")