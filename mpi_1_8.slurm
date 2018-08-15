#!/bin/bash
#SBATCH -p physical
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
module load Python/3.4.3-goolf-2015a
mpirun -np 8 python mpi.py