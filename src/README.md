## Code Instruction
- Log in on to both master node and worker node, both activate conda environment "elm".
### Single node
```
./run.sh NA_forcingGEN_pnetcdf_collective_block_time.py M N T 0
```
### Multi-node
```
./run.sh NA_forcingGEN_pnetcdf_collective_block_time.py M N T 1
```
### Note
- M: Number of processes per file (time dimension splitting)
  
- N: Files will be processed in parallel (each with M processes, for a total of M*N processes)
  
- T: timesteps to be processed or -1 (all time series)

- hostfile.txt specify the number of cpus each node provides, might need to be modified in experiments