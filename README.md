# Cloud Optimized GeoTIFF Summary

# TL;DR
    - The two main organization techniques that Cloud Optimized GeoTIFFs use are Tiling and Overviews.
    - HTTP Version 1.1 introduced a very cool feature called Range requests.
        - This document describes the two technologies to show how the two work together.
    - Convert the NetCDFs and save them as COGs to the output path provided.
        - Usage: netcdf-cog.py [OPTIONS]
    - GeoTIFF to COG conversion
        - Usage: geotiff-cog.py -p input_path -o output_path
    - Validate the GeoTIFFs using the GDAL script
        - Usage: validate_cloud_optimized_geotiff.py [-q] test.tif 
    - Verify all GeoTIFFs
        - Usage: verify_cog.py -p input_path
    - Upload data to AWS S3 Bucket
        - Usage: aws s3 sync path s3://path --exclude '*.yaml' --exclude '*.xml'
    - Create the STAC catalogs
        - Usage: netcdf_cog.py -p input_path -o output_path -b base_url -r product_name -s tile_id
    - Upload STAC catalogs to AWS S3 Bucket
        - Usage: aws s3 sync stac_path s3://path --exclude '*.yaml'

    
#### Cloud Optimized GeoTIFF rely on two complementary pieces of technology.

    The first is the ability of GeoTIFF's to store not just the raw pixels of the image, but to organize those pixels in particular         ways. 
    The second is HTTP GET range requests, that let clients ask for just the portions of a file that they need. Using the first organizes the GeoTIFF so the latter's requests can easily select the parts of the file that are useful for processing.

## GeoTIFF Organization

     The two main organization techniques that Cloud Optimized GeoTIFF's use are Tiling and Overviews.
     And the data is also compressed for more efficient passage online.

     Tiling creates a number of internal `tiles` inside the actual image, instead of using simple `stripes` of data.
     With a stripe of data then the whole file needs to be read to get the key piece.
     With tiles much quicker access to a certain area is possible, so that just the portion of the file that needs to
     be read is accessed.

     Overviews create down sampled versions of the same image. This means it's `zoomed out` from the original image -
     it has much less detail (1 pixel where the original might have 100 or 1000 pixels), but is also much smaller.
     Often a single GeoTIFF will have many overviews, to match different zoom levels. These add size to the overall file,
     but are able to be served much faster, since the renderer just has to return the values in the overview instead of
     figuring out how to represent 1000 different pixels as one.

     These, along with compression of the data, are general best practices for enabling software to quickly access imagery.
     But they are even more important to enable the HTTP GET Range requests to work efficiently.

## HTTP Get Range requests

    HTTP Version 1.1 introduced a very cool feature called Range requests. It comes into play in GET requests,
    when a client is asking a server for data. If the server advertises with an Accept-Ranges: bytes header in its
    response it is telling the client that bytes of data can be requested in parts, in whatever way the client wants.
    This is often called Byte Serving.
    The client can request just the bytes that it needs from the server.
    On the broader web it is very useful for serving things like video, so clients don't have to download
    the entire file to begin playing it.

    The Range requests are an optional field, so web servers are not required to implement it.
    But most all the object storage options on the cloud (Amazon, Google, Microsoft, OpenStack etc) support the field on
    data stored on their servers. So most any data that is stored on the cloud is automatically able to serve up parts of
    itself, as long as clients know what to ask for.

## Bringing them together

    Describing the two technologies probably makes it pretty obvious how the two work together.
    The Tiling and Overviews in the GeoTIFF put the right structure on the files on the cloud so that the Range queries
    can request just the part of the file that is relevant.

    Overviews come into play when the client wants to render a quick image of the whole file - it doesn't have to download
    every pixel, it can just request the much smaller, already created, overview. The structure of the GeoTIFF file on an
    HTTP Range supporting server enables the client to easily find just the part of the whole file that is needed.

    Tiles come into play when some small portion of the overall file needs to be processed or visualized.
    This could be part of an overview, or it could be at full resolution. But the tile organizes all the relevant bytes
    of an area in the same part of the file, so the Range request can just grab what it needs.

    If the GeoTIFF is not cloud optimized with overviews and tiles then doing remote operations on the data will still work.
    But they may download the whole file or large portions of it when only a very small part of the data is actually needed.


# NETCDF to COG conversion
 
 NetCDF to COG conversion from NCI file system:

- Convert the netcdfs that are on NCI g/data file system and save them to the output path provided
- To use python script to convert to COG:

```
> $ python netcdf-cog.py --help

  Usage: netcdf-cog.py [OPTIONS]

  Convert netcdf to Geotiff and then to Cloud Optimized Geotiff using gdal.
  Mandatory Requirement: GDAL version should be <=2.2

  Options:
    -p, --path PATH    Read the NetCDF's from this folder  [required]
    -o, --output PATH  Write COG's into this folder  [required]
    --help             Show this message and exit.
```

# GeoTIFF to COG conversion

 GeoTIFF to COG conversion from NCI file system:
 
- Convert the Geotiff that are on NCI g/data file system and save them to the output path provided 
- To use python script to convert Geotiffs to COG data:
```
> $ python geotiff-cog.py --help

  Usage: geotiff-cog.py [OPTIONS]

  Convert Geotiff to Cloud Optimized Geotiff using gdal. Mandatory
  Requirement: GDAL version should be <=2.2

  Options:
    -p, --path PATH    Read the Geotiffs from this folder  [required]
    -o, --output PATH  Write COG's into this folder  [required]
    --help             Show this message and exit.
```

# Validate the GeoTIFFs using the GDAL script

- How to use the Validate_cloud_Optimized_Geotiff:  
```
> $ python validate_cloud_optimized_geotiff.py --help  

Usage: validate_cloud_optimized_geotiff.py [-q] test.tif  

```
# Verify all GeoTIFFs
```
> $python verify_cog.py --help

  Usage: verify_cog.py [OPTIONS]

  Verify the converted Geotiffs are Cloud Optimized Geotiffs. Mandatory
  Requirement: validate_cloud_optimized_geotiff.py gdal file

Options:
  -p, --path PATH  Read the Geotiffs from this folder  [required]
  --help           Show this message and exit.
```

# Upload GeoTIFFs to AWS S3 Bucket

- Run the bash script, compute_sync.sh, as a PBS job and update more profile use case

- To run the script/submit job - qsub compute-sync.sh

- Usage:
  ``` aws s3 sync {from_folder} {to_folder} --includes {include_specific_files} --excludes {exclude_specific_extension_files}
      {from_folder} : Will sync all the folders, subfolders and files in the given path excluding the path to foldername
      {to_folder} : Provide S3 URL as in s3://{bucket_name}/{object_path}. If the object path is not present the path specified
                    in {from_folder} is duplicated in S3 bucket
       --include (string) Don't exclude files or objects in the command that match the specified pattern.
       --exclude (string) Exclude all files or objects from the command that matches the specified pattern.

  ```
# Create the STAC catalogs

"The SpatioTemporal Asset Catalog (STAC) specification aims to standardize the way geospatial assets are exposed online and queried."[[1](https://github.com/radiantearth/stac-spec)]

## Objectives

- Create the JSON files for STAC from the datasets
- Upload them to the publicly available DEA data staging area

## Program Structure

The program is written in Python, and is set to use the YAML files created by the GeoTIFF to COG conversion described above. The YAML file represents one dataset (NetCDF) and will contain the necessary info to create one a STAC JSON file for each. These JSON files, termed 'item catalogs', will be grouped together in a 'tile catalog' which in turn will be grouped together in a 'root catalog' as the hierarchy below shows.

- Root
    - Tiles
        - Items
            - Assets
            
### Process Flow

The code to create the STAC catalogs is run for each product (e.g. Fractionl Cover) after all tiles in it are processed to convert from NetCDF to GeoTIFF to COGs. It can be run as part of the above step or independently after the COGs and YAMLs are generated.

### How to Run

    - /g/data/u46/users/sa9525/avs/STAC/COG-Conversion/netcdf_cog.py -p /g/data/fk4/datacube/002/FC/LS8_OLI_FC/ -o /g/data/u46/users/sa9525/avs/STAC/FC/Tiles -b https://s3-ap-southeast-2.amazonaws.com/dea-public-data-dev -r FC -s -15_-40'
    
where -p = input directory; -o = output directory; -b = base URL; -r = product code -s = tile ID

**NOTES**

    - Change the value to "No" in the following netcdf_cog.py line (534) if COGs have already been created.
        - create_cog = "Yes"
        
    - If only intending to create the COGs, then comment out the line 565:
        -     create_jsons(base_url, output_dir, product, verbose, subfolder)
        
    - In order to be able to run in parallel, each tile is processed separately. 
        - Hence, it is necessary to have all tiles processed before running the STAC component of the program

# Upload STAC catalogs to AWS S3 Bucket

Uploading the files follow the same method as shown above. Given below is a sample shell script to upload the STAC catalogs. The s3 bucket name must be changed to denote the correct one for DEA Staging.

```
#!/bin/bash
#PBS -q copyq
#PBS -l walltime=10:00:00
#PBS -l ncpus=1,mem=31GB
#PBS -l wd


module use /g/data/v10/public/modules/modulefiles/
module load agdc-py3-prod

aws s3 cp /g/data/u46/users/sa9525/avs/STAC/FC/Test/catalog.json s3://dea-public-data-dev/FC/
aws s3 sync /g/data/u46/users/sa9525/avs/STAC/FC/Test/ s3://dea-public-data-dev/FC --exclude '*.yaml'
```


