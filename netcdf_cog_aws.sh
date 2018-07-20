#!/bin/bash
#PBS -q copyq
#PBS -l walltime=10:00:00
#PBS -l ncpus=1,mem=31GB
#PBS -l wd

module use /g/data/v10/public/modules/modulefiles/
module load dea

# Usage: './netcdf_cog_aws.sh Subdir' 
# e.g.	 './netcdf_cog_aws.sh FC' 
export SUBDIR=$1
if [ ! $SUBDIR ]
then
	echo "ERROR: Mandatory param missing: SUBDIR"
	exit
fi	
# Upload the created COGs and STAC catalogs to s3
aws s3 sync /g/data/u46/users/sa9525/avs/STAC/FC/Test/ s3://dea-public-data-dev/$SUBDIR --exclude '*.yaml' --exclude '*.xml'

# Stac browser compilation
cd /g/data/u46/users/sa9525/avs/STAC/Stac_browser; NODE_ENV=development CATALOG_URL=http://dea-public-data-dev.s3-website-ap-southeast-2.amazonaws.com/$SUBDIR/catalog.json PATH_PREFIX=/FC_AVS/ yarn parcel build --public-url http://dea-public-data-dev.s3-website-ap-southeast-2.amazonaws.com/$SUBDIR/ index.html

# Copy the Stac browser files to s3
aws s3 sync /g/data/u46/users/sa9525/avs/STAC/Stac_browser/dist s3://dea-public-data-dev/$SUBDIR
