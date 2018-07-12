#!/bin/env python
"""
The first part of this program converts NetCDF to GeoTIFF and then to
Cloud Optimized GeoTIFF (COG) using GDAL. The resulting *.yaml file
is then used to create STAC catalogs and JSONs.

Typically, the input directory has many tiles as sub-directories and one or
more NetCDF files in them. These subdirs will be recreated in the output
directory and the NetCDFs are written out as *.tif, *.xml and *.yaml files.
Each NetCDF file will result in one *.yaml file and many *.tif and *.xml files.

Only the *.yaml file is required for the second part of STAC catalog creations.
However, all the *.tif files must be uploaded to the web.
The *.xml files are not required for the STAC.
"""
import glob
import json
import logging
import os
from os.path import join as pjoin, basename, dirname, exists, splitext
import subprocess
from subprocess import check_call
import tempfile

import datetime
import click
import xarray
import yaml
from yaml import CLoader as Loader, CDumper as Dumper
from osgeo import gdal
from pyproj import Proj, transform

from dateutil.parser import parse
import requests

# ------------------------------------------------------------------------------
# AVS debugging modules
# ------------------------------------------------------------------------------
start_time = datetime.datetime.now()
prev_time = start_time


def time_it(step):
    global prev_time
    now = datetime.datetime.now()
    elapsed = now - prev_time
    print("**** {}. Elapsed: {} sec".format(step,elapsed))
    prev_time = now
    

# ------------------------------------------------------------------------------
# CORE FUNCTIONS
# ------------------------------------------------------------------------------
def create_item_dict(item, ard_metadata, base_url, ard_metadata_file,
                     item_json_file):
    """
    Create a dictionary structure of the required values.

    This will be written out as the 'output_dir/subdir/item_STAC.json'

    These output files are STAC compliant and must be viewable with any
    STAC browser.
    """
    geodata = create_geodata(ard_metadata['grid_spatial']
                             ['projection']['valid_data']
                             ['coordinates'])

    # Convert the date to add time zone.
    center_dt = parse(ard_metadata['extent']['center_dt'])
    time_zone = center_dt.tzinfo
    if not time_zone:
        center_dt = center_dt.replace(tzinfo=datetime.timezone.utc).isoformat()
    else:
        center_dt = center_dt.isoformat()
    item_dict = {
        'id': ard_metadata['id'],
        'type': 'Feature',
        'bbox': [ard_metadata['extent']['coord']['ll']['lon'],
                 ard_metadata['extent']['coord']['ll']['lat'],
                 ard_metadata['extent']['coord']['ur']['lon'],
                 ard_metadata['extent']['coord']['ur']['lat']],
        'geometry': geodata['geometry'],
        'properties': {
            'datetime': center_dt,
            'provider': 'Geoscience Australia',
            'license': 'PDDL-1.0'
        },
        'links': {
            "self": {
                'rel': 'self',
                'href': base_url + item + "/" + item_json_file
            }
        },
        'assets': {
            'map': {
                'href': base_url + item + '/map.html',
                'required': 'true',
                'type': 'html'
            },
            'metadata': {
                'href': base_url + item + "/" + ard_metadata_file,
                "required": 'true',
                "type": "yaml"
            }
        }
    }
    bands = ard_metadata['image']['bands']
    for band_num, key in enumerate(bands):
        path = ard_metadata['image']['bands'][key]['path']
        item_dict['assets'][key] = {
            'href': path,
            "required": 'true',
            "type": "GeoTIFF",
            "eo:band": [band_num]}
    return item_dict


def create_geodata(valid_coord):
    """
    The polygon coordinates come in Albers' format, which must be converted to
    lat/lon as in universal format in EPSG:4326
    """
    albers = Proj(init='epsg:3577')
    geo = Proj(init='epsg:4326')
    for i in range(len(valid_coord[0])):
        j = transform(albers, geo, valid_coord[0][i][0], valid_coord[0][i][1])
        valid_coord[0][i] = list(j)

    return {
        'geometry': {
            "type": "Polygon",
            "coordinates": valid_coord
        }
    }


def create_catalogs(base_url, pr_tile_item, tiles_list, verbose):
    """
    There are several catalogs to be craeted as below.

    1. One for each item in the same directory where the COGs are.

    2. Parent catalog for the item(s) in the immediate parent dir. Create once.

    3. Parent catalog for the product (e.g. FCP). Same as (2), unless a new
    subdir is used. Currently they reside in the base_url.

    4. Root catalog. Same as (2).

    """
    product = pr_tile_item[0]
    item = pr_tile_item[1]
    item_json_file = pr_tile_item[2]

#  Create a catalog.json for each item in the same directory as the GeoTIFFs
    name = item_json_file.replace('_STAC.json', '')
    catalog = {
        "name": name,
        "links":
        [
            {
                "href": base_url + product + "/" + item + '/catalog.json',
                "rel": "self"
            },
            {
                "href": base_url + product + '/catalog.json',
                "rel": "parent"
            },
            {
                "href": base_url + product + '/catalog.json',
                "rel": "root"
            },
            {
                "href": item_json_file,
                "rel": "item"
            }
        ]
    }
    with open('catalog.json', 'w') as dest:
        json.dump(catalog, dest, indent=1)

# Parent catalog for the item(s) in the immediate parent dir. Create once.
    parent_catalog = '../catalog.json'
    if not os.path.exists(parent_catalog):
        parents_n_children = [
            {
                "href": base_url + product + '/catalog.json',
                "rel": "self"
            },
            {
                "href": base_url + product + '/catalog.json',
                "rel": "parent"
            },
            {
                "href": base_url + product + '/catalog.json',
                "rel": "root"
            }
        ]
        for tile in tiles_list:
            tile_catalog = tile + "/catalog.json"
            parents_n_children.append({"href": tile_catalog, "rel": "child"})
        catalog = {
            "name": product,
            "links": parents_n_children
        }
        with open(parent_catalog, 'w') as dest:
            json.dump(catalog, dest, indent=1)

    else:
        if verbose:
            print("""Parent catalog exists. Not overwriting!""")


def create_one_json(base_url, output_dir, tile, product, verbose):
    """
    Iterate through all tile directories and create a JSON file for each
    YAML file in there. These JSONs will be saved as *_STAC.json.

    A 'catalog.json' will be created in the tile directory, and its parent
    is written in the directory above it. This will list all tiles as child.

    This function is called after the COGs are created in output_dir.
    """
#    tiles_list = os.listdir(output_dir)
#    for tile in tiles_list:
    #  Each item in the input_dir is a tile.
    #  Find the list of files in its subdir.
    tile_dir = os.path.join(output_dir, tile, "")
    if os.path.exists(tile_dir):
        if verbose:
            print("Analysing {}".format(tile_dir))
        os.chdir(tile_dir)
        for ard_metadata_file in glob.glob("*.yaml"):
            item_json_file = ard_metadata_file.replace('.yaml',
                                                       "_STAC.json")
            try:
                with open(ard_metadata_file) as ard_src:
                    ard_metadata = yaml.safe_load(ard_src)

                if verbose:
                    print("Creating the JSON dictionary structure.")
                item_dict = create_item_dict(tile, ard_metadata,
                                             base_url, ard_metadata_file,
                                             item_json_file)

                #  Write out the Item JSON file.
                if verbose:
                    print("Writing: {}/{}".format(tile, item_json_file))
                with open(item_json_file, 'w') as dest:
                    json.dump(item_dict, dest, indent=1)

                #  Write out the 'catalog.json' files for item and parents.
                if verbose:
                    print("Writing the catalog.json for: {}".format(tile))
                create_catalogs(base_url, [product, tile, item_json_file],
                                tiles_list, verbose)

            except NameError:
                print("*** ERROR: *** Some variable(s) not defined")

def create_jsons(base_url, output_dir, product, verbose):
    """
    Iterate through all tile directories and create a JSON file for each
    YAML file in there. These JSONs will be saved as *_STAC.json.

    A 'catalog.json' will be created in the tile directory, and its parent
    is written in the directory above it. This will list all tiles as child.

    This function is called after the COGs are created in output_dir.
    """
    tiles_list = os.listdir(output_dir)
    for tile in tiles_list:
        #  Each item in the input_dir is a tile.
        #  Find the list of files in its subdir.
        tile_dir = os.path.join(output_dir, tile, "")
        if os.path.exists(tile_dir):
            if verbose:
                print("Analysing {}".format(tile_dir))
            os.chdir(tile_dir)
            for ard_metadata_file in glob.glob("*.yaml"):
                item_json_file = ard_metadata_file.replace('.yaml',
                                                           "_STAC.json")
                try:
                    with open(ard_metadata_file) as ard_src:
                        ard_metadata = yaml.safe_load(ard_src)

                    if verbose:
                        print("Creating the JSON dictionary structure.")
                    item_dict = create_item_dict(tile, ard_metadata,
                                                 base_url, ard_metadata_file,
                                                 item_json_file)

                    #  Write out the Item JSON file.
                    if verbose:
                        print("Writing: {}/{}".format(tile, item_json_file))
                    with open(item_json_file, 'w') as dest:
                        json.dump(item_dict, dest, indent=1)

                    #  Write out the 'catalog.json' files for item and parents.
                    if verbose:
                        print("Writing the catalog.json for: {}".format(tile))
                    create_catalogs(base_url, [product, tile, item_json_file],
                                    tiles_list, verbose)

                except NameError:
                    print("*** ERROR: *** Some variable(s) not defined")
        break  # Comment this out to process all tiles in the directory.


# ------------------------------------------------------------------------------
# Code below is functionally the original, but has been altered to
# eliminate several pylint warnings and errors.
# ------------------------------------------------------------------------------
def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=work_dir)
    except subprocess.CalledProcessError as error:
        raise RuntimeError("command '{}' return with error (code {}): {}"
                           .format(error.cmd, error.returncode, error.output))


def check_dir(fname):
    """
    Get the filename part without the extension.
    """
    file_name = fname.split('/')
    rel_path = pjoin(*file_name[-2:])
    file_wo_extension, extension = splitext(rel_path)
    logging.info("Ext: %s", extension)  # Just to avoid a pylint warning
    return file_wo_extension


def getfilename(fname, outdir):
    """ To create a temporary filename to add overviews and convert to COG
        and create a file name just as source but without '.TIF' extension
    """
    file_path = check_dir(fname)
    out_fname = pjoin(outdir, file_path)
    if not exists(dirname(out_fname)):
        os.makedirs(dirname(out_fname))
    return out_fname, file_path


def get_bandname(filename):
    """
    Get the band name, BS_PC_10, from the NetCDF example as below.
    NETCDF:"/g/data/.../file.nc":BS_PC_10
    """
    return (filename.split(':'))[-1]


def add_image_path(bands, fname, rastercount, count):
    """
    Add the full path to the image filename.
    """
    for key, value in bands.items():
        value['layer'] = '1'
        if rastercount > 1:
            value['path'] = basename(fname) + '_' + str(count + 1) + \
                            '_' + key + '.tif'
        else:
            value['path'] = basename(fname) + '_' + key + '.tif'
    return bands


def _write_dataset(fname, file_path, outdir, rastercount):
    """ Write the dataset which is in indexable format to datacube and update
    the format name too GeoTIFF"""
    dataset_array = xarray.open_dataset(fname)
    for count in range(rastercount):
        if rastercount > 1:
            y_fname = file_path + '_' + str(count + 1) + '.yaml'
            dataset_object = (dataset_array.dataset.item(count)). \
                decode('utf-8')
        else:
            y_fname = file_path + '.yaml'
            dataset_object = (dataset_array.dataset.item()).decode('utf-8')
        yaml_fname = pjoin(outdir, y_fname)
        dataset = yaml.load(dataset_object, Loader=Loader)
        bands = dataset['image']['bands']
        dataset['image']['bands'] = add_image_path(bands, file_path,
                                                   rastercount, count)
        dataset['format'] = {'name': 'GeoTIFF'}
        dataset['lineage'] = {'source_datasets': {}}
        with open(yaml_fname, 'w') as fileout:
            yaml.dump(dataset, fileout, default_flow_style=False,
                      Dumper=Dumper)
            logging.info("Writing dataset Yaml to %s", basename(yaml_fname))


def _write_cogtiff(out_f_name, outdir, subdatasets, rastercount):
    """ Convert the Geotiff to COG using gdal commands
        Blocksize is 512
        TILED <boolean>: Switch to tiled format
        COPY_SRC_OVERVIEWS <boolean>: Force copy of overviews of source dataset
        COMPRESS=[NONE/DEFLATE]: Set the compression to use. DEFLATE is only
        available if NetCDF has been compiled with NetCDF-4 support.
        NC4C format is the default if DEFLATE compression is used.

        ZLEVEL=[1-9]: Set the level of compression when using DEFLATE
        compression. A value of 9 is best, and 1 is least compression.
        The default is 1, which offers the best time/compression ratio.

        BLOCKXSIZE <int>: Tile Width
        BLOCKYSIZE <int>: Tile/Strip Height
        PREDICTOR <int>: Predictor Type (1=default, 2=horizontal differencing,
        3=floating point prediction)
        PROFILE <string-select>: possible values: GDALGeoTIFF,GeoTIFF,BASELINE,
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        for netcdf in subdatasets[:-1]:
            print(netcdf)
#            time_it(2)
            for count in range(1, rastercount + 1):
                band_name = get_bandname(netcdf[0])
                if band_name.endswith('_observed_date') or band_name.endswith('_source'):
                    continue
                    
                if rastercount > 1:
                    out_fname = out_f_name + '_' + str(count) + '_' + \
                                band_name + '.tif'
                else:
                    out_fname = out_f_name + '_' + band_name + '.tif'

                env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
                       'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
                subprocess.check_call(env, shell=True)

                # copy to a tempfolder
                temp_fname = pjoin(tmpdir, basename(out_fname))
                to_cogtif = [
                    'gdal_translate',
                    '-b',
                    str(count),
                    netcdf[0],
                    temp_fname]
                run_command(to_cogtif, tmpdir)

                # Add Overviews
                # gdaladdo - Builds or rebuilds overview images.
                # 2, 4, 8,16,32 are levels which is a list of integral
                # overview levels to build.
                add_ovr = [
                    'gdaladdo',
                    '-r',
                    'average',
                    '--config',
                    'GDAL_TIFF_OVR_BLOCKSIZE',
                    '512',
                    temp_fname,
                    '2',
                    '4',
                    '8',
                    '16',
                    '32']
                run_command(add_ovr, tmpdir)

                # Convert to COG
                cogtif = [
                    'gdal_translate',
                    '-co',
                    'TILED=YES',
                    '-co',
                    'COPY_SRC_OVERVIEWS=YES',
                    '-co',
                    'COMPRESS=DEFLATE',
                    '-co',
                    'ZLEVEL=9',
                    '--config',
                    'GDAL_TIFF_OVR_BLOCKSIZE',
                    '512',
                    '-co',
                    'BLOCKXSIZE=512',
                    '-co',
                    'BLOCKYSIZE=512',
                    '-co',
                    'PREDICTOR=1',
                    '-co',
                    'PROFILE=GeoTIFF',
                    temp_fname,
                    out_fname]
                run_command(cogtif, outdir)


def sanity_check(base_url, product):
    """
    Check that the URL exists and is accessible.
    """
    base_url += product + "/"
    request = requests.get(base_url)
    if request.status_code != 200:
        print('**** WARNING: Web site does not exist:', base_url)


@click.command(help="""\b Convert netcdf to Geotiff and then to Cloud
                    Optimized Geotiff using gdal."""
                    " Mandatory Requirement: GDAL version should be >=2.2")
@click.option('--netcdf_path', '-p', required=True,
              help="Read the netcdfs from this folder.",
              type=click.Path(exists=True, readable=True))
@click.option('--output_dir', '-o', required=True,
              help="Write COG's into this folder.",
              type=click.Path(exists=True, writable=True))
@click.option('--base_url', '-b', required=True,
              help="""Base URL for the json and yaml. It can be the root URL
                   or a products subdir. Give as https://""")
@click.option('--product', '-r', required=True,
              help="""Product name. e.g. FCP, FC_Percentile, FC_Medoid, etc.
                    There must be a subdir for these in 'output' as well as
                    on the public website.""")
@click.option('--subfolder', '-s', required=True, help="Tile dir for this task",
              type=str)
def main(netcdf_path, output_dir, base_url, product, subfolder):
    """
    The main function. This converts NetCDF to GeoTiff and then to Cloud
    Optimized Geotiff. A *.yaml file is created for each NetCDF. These YAMLs
    are used by the STAC creation module to create 'item.json' for each *.nc.
    """
    verbose = 1
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    # Add the ending slash if not present
    netcdf_path = os.path.join(netcdf_path, subfolder)
    print(netcdf_path)
    output_dir = os.path.join(output_dir, '')
    base_url = os.path.join(base_url, '')
    if verbose:
        print("Input dir:", netcdf_path)
        print("Output dir:", output_dir)
        print("Base URL:", base_url)
        print("Product:", product)

    create_cog = "Yes"
    if "Yes" in create_cog:
        for this_path, subdirs, files in os.walk(netcdf_path):
            for fname in files:
                fname = pjoin(this_path, fname)
                logging.info("Sub-dirs: %s; Reading %s", subdirs,
                             basename(fname))
                gtiff_fname, file_path = getfilename(fname, output_dir)
                dataset = gdal.Open(fname, gdal.GA_ReadOnly)
                subdatasets = dataset.GetSubDatasets()
                print(subdatasets)
                # ---To Check if NETCDF is stacked or unstacked --
                rastercount = gdal.Open(subdatasets[0][0]).RasterCount
                dataset = None
                stac_file = output_dir + file_path + "_STAC.json"
                if not os.path.exists(stac_file):
                    _write_dataset(fname, file_path, output_dir, rastercount)
                    _write_cogtiff(gtiff_fname, output_dir, subdatasets,
                                   rastercount)
                    logging.info("Writing COG to %s %s", file_path,
                                 basename(gtiff_fname))
                    time_it(1)
                    return
#                    create_one_json(base_url, output_dir, subfolder, product, verbose)
                else:
                    logging.info("File exists: %s", stac_file)

    # Create the STAC json and catalogs
#    create_jsons(base_url, output_dir, product, verbose)
# ACT Tiles: -15_-40 -15_-41
# ACT neighbours:  -14_-40 -14_-41

if __name__ == "__main__":
    main()
