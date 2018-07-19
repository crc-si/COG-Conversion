#!/bin/bash
#PBS -q express
#PBS -l walltime=24:00:00
#PBS -l jobfs=100GB
#PBS -l ncpus=1,mem=64GB
#PBS -l wd

module use /g/data/v10/public/modules/modulefiles/
module load agdc-py3-prod

# Create the JSONs for specified tiles. It overrides the value given in the config file.
# ALL = all tiles in the output_dir. 
# Usage: './netcdf_cog.sh 15_-40' 
#	or './netcdf_cog.sh 15_-40,-15_-40,18_-28' 
#	or './netcdf_cog.sh ALL' 
#	or './netcdf_cog.sh' (Def: ALL)
export TILES=$1
if [ ! $TILES ]
then
	export TILES="ALL"
fi	
/g/data/u46/users/sa9525/avs/STAC/COG-Conversion/netcdf_cog.py
