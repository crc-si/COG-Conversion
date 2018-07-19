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
import sys
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

from xml.dom import minidom
import re


# ------------------------------------------------------------------------------
# AVS debugging modules. Not required in production version.
# If deleting, be sure to delete the function calls, 'time_it(n)'
# ------------------------------------------------------------------------------
start_time = datetime.datetime.now()
prev_time = start_time
cumul_time = 0

def time_it(step):
    """
    Function to profile the execution times.
    """
    global prev_time, cumul_time
    now = datetime.datetime.now()
    elapsed = now - prev_time
    elapsed = round(elapsed.total_seconds())
    cumul_time += elapsed
    disp_time = 0
    unit = 'seconds'

    if cumul_time > 3600:
        disp_time = float(cumul_time / 3600) # Hours
        unit = 'hr.'
    elif cumul_time > 60:
        disp_time = float(cumul_time / 60) # Minutes
        unit = 'min.'
    else:
        disp_time = float(cumul_time) # Seconds
        unit = 'sec.'

    total_time = "{0:.2f}".format(disp_time)
    print("    **** {}. Elapsed: {} sec. Cumul time: {} {}".format\
        (step, elapsed, total_time, unit))
    prev_time = now

# ------------------------------------------------------------------------------
# CORE FUNCTIONS
# ------------------------------------------------------------------------------
def create_item_dict(item, ard_metadata, base_url, ard_metadata_file,
                     item_json_file, config_json):
    """
    Create a dictionary structure of the required values.

    This will be written out as the 'output_dir/subdir/item_STAC.json'

    These output files are STAC compliant and must be viewable with any
    STAC browser.
    """
    geodata = create_geodata(ard_metadata['grid_spatial']
                             ['projection']['valid_data']
                             ['coordinates'])

    # Create the variables that go into item.json
    provider = "Commonwealth of Australia (Geoscience Australia)"
    license = 'CC BY Attribution 4.0 International License'
    copyright = "DEA, Geoscience Australia"
    product_type = ard_metadata['product_type']
    keywords = [
        "AU",
        "GA",
        "NASA",
        "GSFC",
        "SED",
        "ESD",
        "LANDSAT",
        "REFLECTANCE",
        "ETM",
        "TM",
        "OLI",
        "EARTH SCIENCE"
    ]
    homepage = "http://www.ga.gov.au/"
    provider = {
        "scheme": "s3",
        "region": "ap-southeast-2",
        "requesterPays": "False"
    },

    if config_json:
        provider_name = config_json['contact']['name']
        license = config_json['license']['name']
        copyright = config_json['license']['copyright']
        product_type = config_json['product']['name']
        keywords = config_json['keywords']
        homepage = config_json['homepage']
        provider = config_json['provider']
        
    # Convert the date to add time zone.
    center_dt = parse(ard_metadata['extent']['center_dt'])
    center_dt = center_dt.replace(microsecond=0)
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
            'provider': provider_name,
            'license': license,
            'copyright': copyright,
            'product_type': product_type,
            'homepage': homepage
        },
        'provider': provider,
        'links': {
            "self": {
                'rel': 'self',
                'href': base_url + item + "/" + item_json_file
            }
        },
        'assets': {
        }
    }
    bands = ard_metadata['image']['bands']
    for key in bands:
        path = ard_metadata['image']['bands'][key]['path']
        if config_json:
            key = config_json['bands'][key] + ' GeoTIFF'

        item_dict['assets'][key] = {
            'href': path,
            "required": 'true',
            "type": "GeoTIFF"
        }
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


def create_catalogs(base_url, output_dir, pr_tile_item, tiles_list, config_json):
    """
    There are several catalogs to be craeted as below.

    1. One for each item in the same directory where the COGs are.

    2. Parent catalog for the item(s) in the immediate parent dir. Create once.

    3. Parent catalog for the product (e.g. FCP). Same as (2), unless a new
    subdir is used. Currently they reside in the base_url.

    4. Root catalog. Same as (2).

    """   
    for tile in tiles_list:
        tile_dir = os.path.join(output_dir, tile, "")
        items_list = os.listdir(tile_dir)
        item_jsons = []
    
        # Item catalog. Each dataset is an item. 
        for item_json in items_list:
            if ".json" in item_json and "catalog" not in item_json:
                item_jsons.append({"href": item_json, "rel": "item"})
        product = config_json['product']['code']
        catalog = {
            "name": tile,
            "description": "Fractional Cover - List of items",
            "links":
            [
                {
                    "href": base_url + product + "/" + tile + '/catalog.json',
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
        }
        for item_json in item_jsons:
            catalog['links'].append(item_json)
        with open('catalog.json', 'w') as dest:
            json.dump(catalog, dest, indent=1)

# Root catalog for the tiles. Each tile is a child catalog.
    keywords = ''
    description = ''
    product_name = product
    if config_json:
        # Take description and keywords from config_json
        description = config_json['description']
        description = ' '.join([str(x) for x in description])
#        description = "Fractional Cover - Testing the desc"
        keywords = config_json['keywords']
        product_name = config_json['product']['name']
        license = config_json['license']
        contact = config_json['contact']
        formats = config_json['formats']
        homepage = config_json['homepage']
        provider = config_json['provider']
        
    else:
        # Get the description from one XML file. They are the same in all Tifs
        print(items_list[0])
        if ".aux.xml" in items_list[0]:
            xml_1_file = items_list[0]
        else:
            xml_1_file = items_list[0] + ".aux.xml"
        xmldoc = minidom.parse(xml_1_file)
        itemlist = xmldoc.getElementsByTagName('MDI')
        for s in itemlist:
            key_value = s.attributes['key'].value
            if "NC_GLOBAL#summary" in key_value:
                description = s.childNodes[0].nodeValue
                description = description.replace("\"", "")
                description = description.replace("\n", " ")
            if "NC_GLOBAL#keywords" in key_value and not keywords:
                keywords = s.childNodes[0].nodeValue
                keywords = keywords.replace("/", ",")
                keywords = keywords.replace(" ,", ",")
                keywords = keywords.split(",")   
    parent_catalog = '../catalog.json'
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

    tiles_list = [name for name in os.listdir(output_dir)
                  if os.path.isdir(os.path.join(output_dir, name))]
    for tile in tiles_list:
        tile_catalog = tile + "/catalog.json"
        parents_n_children.append({"href": tile_catalog, "rel": "child"})
    if config_json:
        catalog = {
            "name": product_name,
            "description": description,
            "contact": contact,
            "license": license,
            "formats": formats,
            "keywords": keywords,
            "homepage": homepage,
            "provider": provider,
            "links": parents_n_children
        }
        
    else:        
        catalog = {
            "name": product_name,
            "description": description,
            "license": {
              "name": "CC BY Attribution 4.0 International License",
              "copyright": "DEA, Geoscience Australia"
            },
            "contact": {
                "name": "Commonwealth of Australia (Geoscience Australia)",
                "email": "sales@ga.gov.au",
                "phone": "+61 2 6249 9966",
                "url": "http://www.ga.gov.au"
            },
            "formats": [
                "geotiff",
                "cog"
            ],
            "keywords": keywords,
            "homepage": "http://www.ga.gov.au/",
            "provider": {
                "scheme": "s3",
                "region": "ap-southeast-2",
                "requesterPays": "False"
            },
            "links": parents_n_children
        }
    with open(parent_catalog, 'w') as dest:
        json.dump(catalog, dest, indent=1)


def create_catalogs_0(base_url, output_dir, pr_tile_item, config_json):
    """
    There are several catalogs to be craeted as below.

    1. One for each item in the same directory where the COGs are.

    2. Parent catalog for the item(s) in the immediate parent dir. Create once.

    3. Parent catalog for the product (e.g. FCP). Same as (2), unless a new
    subdir is used. Currently they reside in the base_url.

    4. Root catalog. Same as (2).

    """
    
    product = pr_tile_item[0]
    tile = pr_tile_item[1]

    tile_dir = os.path.join(output_dir, tile, "")
    items_list = os.listdir(tile_dir)
    item_jsons = []

    # Item catalog. Each dataset is an item. 
    for item_json in items_list:
        if ".json" in item_json and "catalog" not in item_json:
            item_jsons.append({"href": item_json, "rel": "item"})
    catalog = {
        "name": tile,
        "description": "Fractional Cover - List of items",
        "links":
        [
            {
                "href": base_url + product + "/" + tile + '/catalog.json',
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
    }
    for item_json in item_jsons:
        catalog['links'].append(item_json)
    with open('catalog.json', 'w') as dest:
        json.dump(catalog, dest, indent=1)

# Root catalog for the tiles. Each tile is a child catalog.
    keywords = ''
    description = ''
    product_name = product
    if config_json:
        # Take description and keywords from config_json
        description = config_json['description']
        description = ' '.join([str(x) for x in description])
#        description = "Fractional Cover - Testing the desc"
        keywords = config_json['keywords']
        product_name = config_json['product']['name']
        license = config_json['license']
        contact = config_json['contact']
        formats = config_json['formats']
        homepage = config_json['homepage']
        provider = config_json['provider']
        
    else:
        # Get the description from one XML file. They are the same in all Tifs
        print(items_list[0])
        if ".aux.xml" in items_list[0]:
            xml_1_file = items_list[0]
        else:
            xml_1_file = items_list[0] + ".aux.xml"
        xmldoc = minidom.parse(xml_1_file)
        itemlist = xmldoc.getElementsByTagName('MDI')
        for s in itemlist:
            key_value = s.attributes['key'].value
            if "NC_GLOBAL#summary" in key_value:
                description = s.childNodes[0].nodeValue
                description = description.replace("\"", "")
                description = description.replace("\n", " ")
            if "NC_GLOBAL#keywords" in key_value and not keywords:
                keywords = s.childNodes[0].nodeValue
                keywords = keywords.replace("/", ",")
                keywords = keywords.replace(" ,", ",")
                keywords = keywords.split(",")   
    parent_catalog = '../catalog.json'
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

    tiles_list = [name for name in os.listdir(output_dir)
                  if os.path.isdir(os.path.join(output_dir, name))]
    for tile in tiles_list:
        tile_catalog = tile + "/catalog.json"
        parents_n_children.append({"href": tile_catalog, "rel": "child"})
    if config_json:
        catalog = {
            "name": product_name,
            "description": description,
            "contact": contact,
            "license": license,
            "formats": formats,
            "keywords": keywords,
            "homepage": homepage,
            "provider": provider,
            "links": parents_n_children
        }
        
    else:        
        catalog = {
            "name": product_name,
            "description": description,
            "license": {
              "name": "CC BY Attribution 4.0 International License",
              "copyright": "DEA, Geoscience Australia"
            },
            "contact": {
                "name": "Commonwealth of Australia (Geoscience Australia)",
                "email": "sales@ga.gov.au",
                "phone": "+61 2 6249 9966",
                "url": "http://www.ga.gov.au"
            },
            "formats": [
                "geotiff",
                "cog"
            ],
            "keywords": keywords,
            "homepage": "http://www.ga.gov.au/",
            "provider": {
                "scheme": "s3",
                "region": "ap-southeast-2",
                "requesterPays": "False"
            },
            "links": parents_n_children
        }
    with open(parent_catalog, 'w') as dest:
        json.dump(catalog, dest, indent=1)


def create_jsons(base_url, output_dir, product, verbose, tiles_list, config_json):
    """
    Iterate through all tile directories and create a JSON file for each
    YAML file in there. These JSONs will be saved as *_STAC.json.

    A 'catalog.json' will be created in the tile directory, and its parent
    is written in the directory above it. This will list all tiles as child.

    This function is called after the COGs are created in output_dir.
    """
    for tile in tiles_list:
        # Process only the specified tile(s), just as in creating COGs
        if re.match(r'[^\d-]', tile):
            continue
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
    
                    item_dict = create_item_dict(tile, ard_metadata,
                                                 base_url, ard_metadata_file,
                                                 item_json_file, config_json)
    
                    #  Write out the Item JSON file.
                    if verbose:
                        print("Writing: {}/{}".format(tile, item_json_file))
                    with open(item_json_file, 'w') as dest:
                        json.dump(item_dict, dest, indent=1)
    
                except NameError as e:
                    print("*** ERROR: *** ",e)


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
    logging.info("Ext: %s", extension)  # Keep it to avoid a pylint warning
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
            for count in range(1, rastercount + 1):
                band_name = get_bandname(netcdf[0])

                # In the case of FC Percentile, skip two bands as below.
                # It does not apply in FC Products
                if band_name.endswith('_observed_date')\
                or band_name.endswith('_source'):
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
@click.option('--netcdf_path', '-p', required=False,
              help="Read the netcdfs from this folder.")
@click.option('--output_dir', '-o', required=False,
              help="Write COG's into this folder.")
@click.option('--base_url', '-b', required=False,
              help="""Base URL for the json and yaml. It can be the root URL
                   or a products subdir. Give as https://""")
@click.option('--product', '-r', required=False,
              help="""Product name. e.g. FCP, FC_Percentile, FC_Medoid, etc.
                    There must be a subdir for these in 'output' as well as
                    on the public website.""")
@click.option('--subfolder', '-s', required=False, help="Tile for this task",
              type=str)
def main(netcdf_path, output_dir, base_url, product, subfolder):
    """
    The main function. This converts NetCDF to GeoTiff and then to Cloud
    Optimized Geotiff. A *.yaml file is created for each NetCDF. These YAMLs
    are used by the STAC creation module to create 'item.json' for each *.nc.
    """
    # These can be overridden in 'netcdf_cog.json'
    create_cog = "No"
    create_stac = "Yes"
    verbose = 1
    # Config file: netcdf_cog.json in CWD or Program dir
    config_file = './netcdf_cog.json'
    config_json = ''
    if(not netcdf_path or not output_dir or not base_url or not product or not subfolder):
        if os.path.exists(config_file):
            pass
        else:
            mypath = os.path.dirname(os.path.realpath(__file__))
            config_file = mypath + '/netcdf_cog.json'
        print("config_file = ", config_file)
        if os.path.exists(config_file):
            with open(config_file) as f:
                config_json = json.load(f)
        netcdf_path = config_json['input_dir']
        output_dir = config_json['output_dir']
        base_url = config_json['base_url']
        product = config_json['product']['code']
        tiles = config_json['tiles']
        create_cog = config_json['create_cog']
        create_stac = config_json['create_stac']
        verbose = config_json['verbose']
        
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        level=logging.INFO)
    # Add the ending slash if not present
    output_dir = os.path.join(output_dir, '')
    base_url = os.path.join(base_url, '')
    if verbose:
        print("Input dir:", netcdf_path)
        print("Output dir:", output_dir)
        print("Base URL:", base_url)
        print("Product:", product)

    if "Yes" in create_cog:
        netcdf_path = os.path.join(netcdf_path, subfolder)
        for this_path, subdirs, files in os.walk(netcdf_path):
            for fname in files:

                # Normally all files in this dir are NetCDF (*.nc). Be safe!
                if ".nc" not in fname:
                    continue
                fname = pjoin(this_path, fname)
                logging.info("Sub-dirs: %s; Reading %s", subdirs,
                             basename(fname))
                gtiff_fname, file_path = getfilename(fname, output_dir)
                subdatasets = gdal.Open(fname, gdal.GA_ReadOnly).GetSubDatasets()
                # ---To Check if NETCDF is stacked or unstacked --
                rastercount = gdal.Open(subdatasets[0][0]).RasterCount
                # Create the YAML after creating the Tiffs.
                # This allows to skip the datasets that are already processed.
                yaml_file = output_dir + file_path + ".yaml"
                if not os.path.exists(yaml_file):
                    logging.info("Writing COG to %s %s", file_path,
                                 basename(gtiff_fname))
                    _write_cogtiff(gtiff_fname, output_dir, subdatasets,
                                   rastercount)
                    _write_dataset(fname, file_path, output_dir, rastercount)
                    time_it(1)
                else:
                    logging.info("File exists: %s", yaml_file)
                    time_it(1)
    # Create the STAC json and catalogs
#    print("create_stac: ", create_stac)
    if "Yes" in create_stac:
        # Take the tile numbers as an env variable. ALL = all tiles
        tiles_list = []
        try:
            tiles_list = os.environ['TILES'].split(',')
        except:
            pass

        # If not from env, take the tile numbers from config file
        if not tiles_list:
            tiles_list = config_json['tiles'].split(',')
            
        if "ALL" in tiles_list[0]:
            tiles_list = [name for name in os.listdir(output_dir)
                          if os.path.isdir(os.path.join(output_dir, name))]
        print("tiles_list: ", tiles_list)                          
        create_jsons(base_url, output_dir, product, verbose, tiles_list, config_json)
        time_it(3)
        if verbose:
            print("Writing the catalog.json for: {}".format(subfolder))
        create_catalogs(base_url, output_dir, [product, subfolder], tiles_list, config_json)
        time_it(4)
# ACT Tiles: -15_-40 -15_-41
# ACT neighbours:  -14_-40 -14_-41
# QLD: 18_-28
# Sea: 9_-45

if __name__ == "__main__":
    main()
