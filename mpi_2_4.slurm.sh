#!/bin/bash
#SBATCH -p physical
#SBATCH --time=00:30:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
module load Python/3.4.3-goolf-2015a
mpirun -np 8 python mpi_2_4.py