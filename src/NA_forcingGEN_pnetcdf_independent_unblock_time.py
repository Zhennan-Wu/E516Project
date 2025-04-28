# data_partition module for batch processing
# based on array_split and function definition - Modified for MxN parallelism with PNetCDF

import os, sys
import glob
import math
import numpy as np
from time import process_time
from datetime import datetime
from pyproj import Proj
from pyproj import Transformer
from pyproj import CRS


try:
    from mpi4py import MPI
    HAS_MPI4PY = True
except ImportError:
    HAS_MPI4PY = False
    print("mpi4py is required for this script")
    sys.exit(1)

try:
    import pnetcdf as pnc
    HAS_PNETCDF = True
except ImportError:
    HAS_PNETCDF = False
    print("pnetcdf-python is required for this script")
    sys.exit(1)

# Get current date
current_date = datetime.now()
# Format date to mmddyyyy
formatted_date = current_date.strftime('%m-%d-%Y')

def get_node_rank(comm):
    """Get the node-local rank for the current process."""
    # Get the processor name (node identifier)
    proc_name = MPI.Get_processor_name()
    
    # Gather all processor names to rank 0
    all_proc_names = comm.allgather(proc_name)
    
    # Create a dictionary mapping node names to node ranks
    node_dict = {}
    node_rank = 0
    for name in all_proc_names:
        if name not in node_dict:
            node_dict[name] = node_rank
            node_rank += 1
    
    # Get the node rank for the current process
    return node_dict[proc_name]

def forcing_save_1dNA(input_path, file, var_name, period, time_steps, output_path, comm, M):
    """
    Convert NetCDF file to PNetCDF format with M processes
    handling a single file, splitting work along the time dimension.
    Using nonblocking collective I/O with PNetCDF.
    
    Args:
        input_path: Path to input files
        file: Name of the file to process
        var_name: Variable name
        period: Period string
        time_steps: Number of timesteps to process (-1 for all)
        output_path: Path for output files
        comm: MPI communicator for this file's M processes
        M: Number of processes for this file
    """
    # === Start read timing ===
    start_read_time = process_time()

    local_rank = comm.Get_rank()  # Rank within the sub-communicator for this file
    
    # Open the source file (all processes)
    source_file = os.path.join(input_path, file)

    # Open with PNetCDF
    src = pnc.File(filename=source_file, mode='r', comm=comm)

    if local_rank == 0:
        print(f"Successfully opened file: {source_file}\n")
    total_rows = len(src.dimensions['x'])
    total_cols = len(src.dimensions['y'])
    total_time = len(src.dimensions['time'])

    # If time_steps is -1, use all time steps
    if time_steps == -1:
        time_steps = total_time
    else:
        time_steps = min(time_steps, total_time)
    
    # Calculate time slice for each process
    base_time_per_process = time_steps // M
    time_remainder = time_steps % M
    
    # Calculate the start and end indices for the current process
    if local_rank < time_remainder:
        local_start_time = local_rank * (base_time_per_process + 1)
        local_count_time = base_time_per_process + 1
    else:
        local_start_time = local_rank * base_time_per_process + time_remainder
        local_count_time = base_time_per_process
    
    local_end_time = local_start_time + local_count_time
    
    # print(f"Total time steps: {time_steps}, Rank: {local_rank}, Processing steps: {local_start_time} to {local_end_time-1}\n")
    
    # Read data using PNetCDF API (avoiding Python slicing)
    # Create start and count arrays for data reading
    start_time = [local_start_time, 0, 0]
    count_time = [local_count_time, total_cols, total_rows]


    # Read only my portion of the data using PNetCDF
    local_data = np.zeros((local_count_time, total_cols, total_rows), dtype=np.float32)
    req_var = src.variables[var_name].get_var_all(start=start_time, count=count_time, data=local_data)

    # start=start_time, count=count_time, 
    # src.wait_all(requests=[req_var])
    # print(f"Process {local_rank}: Successfully loaded data from time {local_start_time} to {local_end_time-1}\n")
    
    # req_read_list = []
    # Read x and y dimensions
    # x_dim = np.zeros(total_rows, dtype=np.float32)
    # req_read_list.append(src.variables['x'].iget_var(data=x_dim))
    # y_dim = np.zeros(total_cols, dtype=np.float32)
    # req_read_list.append(src.variables['y'].iget_var(data=y_dim))
    x_dim = np.zeros(total_rows, dtype=np.float32)
    req_x = src.variables['x'].get_var_all(data=x_dim)
    y_dim = np.zeros(total_cols, dtype=np.float32)
    req_y = src.variables['y'].get_var_all(data=y_dim)

    # Time handling - each process reads its own time portion
    local_data_time = np.zeros(local_count_time, dtype=np.float32)
    req_time = src.variables['time'].get_var_all(start=[local_start_time], count=[local_count_time], data=local_data_time)

    # Create a request list for all non-blocking operations
    req_read_list = [req_var, req_x, req_y, req_time]

    # Set up projection transformations
    geoxy_proj_str = "+proj=lcc +lon_0=-100 +lat_0=42.5 +lat_1=25 +lat_2=60 +x_0=0 +y_0=0 +R=6378137 +f=298.257223563 +units=m +no_defs"
    geoxyProj = CRS.from_proj4(geoxy_proj_str)
    lonlatProj = CRS.from_epsg(4326)
    Txy2lonlat = Transformer.from_proj(geoxyProj, lonlatProj, always_xy=True)

    # Wait for all read requests to complete
    src.wait_all(requests=req_read_list)
    x_dim = x_dim.astype(np.float64)
    y_dim = y_dim.astype(np.float64)

    grid_x, grid_y = np.meshgrid(x_dim, y_dim)
    lonxy, latxy = Txy2lonlat.transform(grid_x, grid_y)  


    local_data_time = local_data_time.astype(np.float64)

    # Get time unit attribute
    tunit = src.variables['time'].get_att('units')
    
    # === End read timing ===
    end_read_time = process_time()

    # Process the time units
    t0 = str(tunit.lower()).strip('days since')
    t0 = datetime.strptime(t0, '%Y-%m-%d %X')
    
    # Calculate year, month, day for the time values
    iyr = t0.year + np.floor(local_data_time/365.0)
    data_time0 = datetime.strptime(str(int(iyr[0]))+'-01-01', '%Y-%m-%d')
    data_time0 = (data_time0-t0).total_seconds()/86400.0
    
    iday = local_data_time - data_time0
    imm = np.zeros_like(iday)
    mdoy = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    
    for m in range(1, 13):
        tpts = np.where((iday > mdoy[m-1]) & (iday <= mdoy[m]))
        if (len(tpts[0]) > 0):
            imm[tpts] = m
            iday[tpts] = iday[tpts] - mdoy[m-1]
    
    local_data_time = iday
    
    # Update time units
    tunit = tunit.replace(str(t0.year).zfill(4)+'-', str(int(iyr[0])).zfill(4)+'-')
    tunit = tunit.replace('-'+str(t0.month).zfill(2)+'-', '-'+str(int(imm[0])).zfill(2)+'-')
    if(tunit.endswith(' 00') and not tunit.endswith(' 00:00:00')):
        tunit = tunit + ':00:00'
    
    
    # Create land mask from first time slice
    mask = local_data[0].copy()
    mask = np.where(~np.isnan(mask), 1, np.nan)
    
    # Create gridIDs
    total_gridcells = total_rows * total_cols
    grid_ids = np.linspace(0, total_gridcells-1, total_gridcells, dtype=int)
    grid_ids = grid_ids.reshape(total_cols, total_rows)
        
    # create an flattened list of land gridID and reduce the size of gridIDs array
    grid_ids = np.multiply(mask, grid_ids)
    grid_ids = grid_ids[~np.isnan(grid_ids)]
        
    # extract the data over land gridcells
    number_landcells = len(grid_ids)    
    local_data = np.multiply(mask, local_data)
    local_data = local_data[~np.isnan(local_data)]
    local_data = np.reshape(local_data,(local_count_time,number_landcells))

    latxy = np.multiply(mask, latxy)
    latxy = latxy[~np.isnan(latxy)]
    lonxy = np.multiply(mask, lonxy)
    lonxy = lonxy[~np.isnan(lonxy)]

    # convert local grid_id_lists into an array
    grid_id_arr = np.array(grid_ids)
    local_data_arr = np.array(local_data)
    lonxy_arr = np.array(lonxy)
    latxy_arr = np.array(latxy)

    # Create output filename
    dst_name = os.path.join(output_path, f'clmforc.Daymet4.1km.p1d.{var_name}.{period}.1step1process.nc')
    
    # === Start write timing ===
    start_write_time = process_time()
    
    # Create the output file with PNetCDF
    dst = pnc.File(filename=dst_name, mode='w', format='NC_64BIT_DATA', comm=comm)
    
    # Add file title attribute
    dst.put_att('title', var_name + '('+period+') created from '+ input_path +' on ' + formatted_date)
    
    # Define dimensions
    dst.def_dim('time', time_steps)
    dst.def_dim('ni', number_landcells)
    dst.def_dim('nj', 1)
    
    # Define variables using PNetCDF API
    var_id = dst.def_var('gridID', pnc.NC_INT, ['nj', 'ni'])
    var_time = dst.def_var('time', pnc.NC_DOUBLE, ['time'])
    var_lat = dst.def_var('LATIXY', pnc.NC_DOUBLE, ['nj', 'ni'])
    var_lon = dst.def_var('LONGXY', pnc.NC_DOUBLE, ['nj', 'ni'])
    var_main = dst.def_var(var_name, pnc.NC_FLOAT, ['time', 'nj', 'ni'])
    
    var_id.put_att('long_name', "gridId in the NA domain")
    var_id.put_att('decription', "Covers all land and ocean gridcells, with #0 at the upper left corner of the domain")
    # Copy attributes
    for attr_name in src.variables[var_name].ncattrs():
        value = src.variables[var_name].get_att(attr_name)
        var_main.put_att(attr_name, value)

    for attr_name in src.variables['time'].ncattrs():
        if attr_name == 'units':
            var_time.put_att('units', tunit)
        else:
            value = src.variables['time'].get_att(attr_name)
            var_time.put_att(attr_name, value)
    
    # Copy lat/lon attributes if they exist
    if 'lat' in src.variables:
        for attr_name in src.variables['lat'].ncattrs():
            value = src.variables['lat'].get_att(attr_name)
            var_lat.put_att(attr_name, value)
    
    if 'lon' in src.variables:
        for attr_name in src.variables['lon'].ncattrs():
            value = src.variables['lon'].get_att(attr_name)
            var_lon.put_att(attr_name, value)
    
    # End define mode
    dst.enddef()
    
    # Write main variable data using nonblocking independent I/
    req_write_list = []

    start_var = [local_start_time, 0, 0]
    count_var = [local_count_time, 1, number_landcells]
    req_write_list.append(var_main.iput_var(start=start_var, count=count_var, data=local_data_arr.reshape(local_count_time, 1, number_landcells)))
    
    # Write time data
    start_time = [local_start_time]
    count_time = [local_count_time]
    req_write_list.append(var_time.iput_var(start=start_time, count=count_time, data=local_data_time))
    
    # Calculate landcells slice for each process
    base_lancells_per_process = number_landcells // M
    landcells_remainder = number_landcells % M
    
    # Calculate the start and end indices for the current process
    if local_rank < landcells_remainder:
        local_start_landcells = local_rank * (base_lancells_per_process + 1)
        local_count_landcells = base_lancells_per_process + 1
    else:
        local_start_landcells = local_rank * base_lancells_per_process + landcells_remainder
        local_count_landcells = base_lancells_per_process
    
    local_end_landcells = local_start_landcells + local_count_landcells

    # Write lat/lon data
    req_write_list.append(var_lat.iput_var(start=[0, local_start_landcells], count=[1, local_count_landcells], data=latxy_arr.reshape(1, -1)[:, local_start_landcells:local_end_landcells]))
        
    req_write_list.append(var_lon.iput_var(start=[0, local_start_landcells], count=[1, local_count_landcells], data=lonxy_arr.reshape(1, -1)[:, local_start_landcells:local_end_landcells]))

    req_write_list.append(var_id.iput_var(start=[0, local_start_landcells], count=[1, local_count_landcells],  data=grid_id_arr.reshape(1, -1)[:, local_start_landcells:local_end_landcells]))

    # Wait for all write operations to complete
    dst.wait_all(requests=req_write_list)

    # Close files
    src.close()
    dst.close()
    
    # === End write timing ===
    end_write_time = process_time()

    if local_rank == 0:
        read_elapsed = end_read_time - start_read_time
        write_elapsed = end_write_time - start_write_time
        print(f"Successfully processed {file}\n")
        print(f"File {file}: Read time = {read_elapsed:.2f}s, Write time = {write_elapsed:.2f}s")
        
def get_files(input_path, ncheader='clmforc'):
    """Get the list of NetCDF files to process."""
    print(input_path + ncheader)
    files = glob.glob("%s*.%s" % (input_path + ncheader, 'nc'))
    files.sort()
    print("Total " + str(len(files)) + " files need to be processed")
    return files

def main():
    args = sys.argv[1:]

    if len(sys.argv) != 6 or sys.argv[1] == '--help':  # sys.argv includes the script name as the first argument
        print("Example use: python NA_forcingGEN_PNetCDF.py <input_path> <output_path> <time steps> <M> <N>")
        print(" <input_path>: path to the 2D source data directory")
        print(" <output_path>: path for the 1D forcing data directory")
        print(" <time steps>: timesteps to be processed or -1 (all time series)")
        # M processes will handle a single file, splitting work along the time dimension
        print(" <M>: Number of processes per file (time dimension splitting)")
        # N files will be processed in parallel, each with M processes, for a total of M*N processes
        print(" <N>: Number of files to process simultaneously")
        print(" The code converts NetCDF to Parallel NetCDF with MxN parallelism and nonblocking collective I/O")              
        exit(0)

    input_path = args[0]
    if not input_path.endswith('/'): input_path = input_path + '/'
    
    output_path = args[1]
    if not output_path.endswith('/'): output_path = output_path + '/'
    
    time_steps = int(args[2])
    M = int(args[3])  # Processes per file
    N = int(args[4])  # Files in parallel
    
    # Initialize MPI
    if not HAS_MPI4PY:
        print("mpi4py is required for this script")
        sys.exit(1)
        
    if not HAS_PNETCDF:
        print("pnetcdf-python is required for this script")
        sys.exit(1)

    world_comm = MPI.COMM_WORLD
    world_size = world_comm.Get_size()
    world_rank = world_comm.Get_rank()

    # Check if world_size is consistent with M*N
    if world_size != M * N:
        if world_rank == 0:
            print(f"Error: Total MPI processes ({world_size}) must equal M*N ({M}*{N}={M*N})")
        sys.exit(1)
    
    # Get list of files to process
    files_nc = get_files(input_path)
    n_files = len(files_nc)
    
    # Determine which files to process
    # We want to split the world communicator into N groups, each with M processes
    
    # Calculate group ID and rank within group
    file_group = world_rank // M  # Which file group this process belongs to
    group_rank = world_rank % M   # Rank within the file group
    
    # Create communicators for each file group
    file_comm = world_comm.Split(file_group, group_rank)

    # if world_rank == 0:
    #     print('Node_world_rank', get_node_rank(world_comm))
    print(f'Group {file_group} Local_rank {group_rank}')

    # Calculate file distribution among groups, handling uneven division
    base_files_per_group = n_files // N  # Integer division
    remainder = n_files % N

    # Calculate start and end indices for files to be processed by this group
    # First 'remainder' groups get (base_files_per_group + 1) files
    # Remaining groups get base_files_per_group files
    if file_group < remainder:
        start_file_idx = file_group * (base_files_per_group + 1)
        end_file_idx = start_file_idx + base_files_per_group + 1
    else:
        start_file_idx = (remainder * (base_files_per_group + 1)) + ((file_group - remainder) * base_files_per_group)
        end_file_idx = start_file_idx + base_files_per_group    
    # Safety check to ensure we don't exceed the number of files
    end_file_idx = min(end_file_idx, n_files)
    
    # Process each file assigned to this group
    for file_idx in range(start_file_idx, end_file_idx):
        f = files_nc[file_idx]
        if not os.path.basename(f).startswith('clmforc'):
            continue
        
        # Extract variable name and period from the filename
        var_name = os.path.basename(f).split('.')[-3]
        period = os.path.basename(f).split('.')[-2]
        
        if group_rank == 0:
            print(f'Group {file_group} processing {var_name} ({period}) in the file {f}')
        
        start_time = process_time()
        
        # Process the file with the file_comm
        forcing_save_1dNA(input_path, os.path.basename(f), var_name, period, time_steps, output_path, file_comm, M)
        
        end_time = process_time()
        
        if group_rank == 0:
            print(f"Group {file_group}: Processing {f} took {end_time - start_time:.2f} seconds")
    
    # Cleanup
    file_comm.Free()
    
    # Wait for all processes to finish
    world_comm.Barrier()
    
    if world_rank == 0:
        print("All files have been processed successfully")

if __name__ == '__main__':
    main()
