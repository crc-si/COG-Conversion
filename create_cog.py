#!/usr/bin/env python
import glob
import json
import logging
import os
from os.path import join as pjoin, basename, dirname, exists, splitext
import subprocess
from subprocess import check_call
from subprocess import Popen, PIPE
import tempfile

import datetime
import re
import click
import xarray
import yaml
from yaml import CLoader as Loader, CDumper as Dumper
from osgeo import gdal
from pyproj import Proj, transform

from dateutil.parser import parse
import requests


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
            print("Written YAML:", yaml_fname)

def run_gdal(out_f_name, outdir, netcdf, subdatasets, rastercount, tmpdir):
    for count in range(1, rastercount + 1):
        band_name = get_bandname(netcdf[0])
        print("0: ****: ")

        # In the case of FC Percentile, skip two bands as below.
        # It does not apply in FC Products
        if band_name.endswith('_observed_date') or band_name.endswith('_source'):
            continue


        if rastercount > 1:
            out_fname = out_f_name + '_' + str(count) + '_' + band_name + '.tif'
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
        print("1: ****: ", to_cogtif)
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
        print("2: ****: ", add_ovr)
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
        print("3: ****: ", cogtif)
        run_command(cogtif, outdir)


def _write_cogtiff(out_f_name, outdir, subdatasets, rastercount):
    """ 
    Convert the Geotiff to COG using gdal commands
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
#    with tempfile.TemporaryDirectory() as tmpdir:
    for netcdf in subdatasets[:-1]:
        print("************************ NetCDF:", netcdf[0], len(subdatasets), rastercount)
        rastercount_str = str(rastercount)
        process = Popen(["./write_cogtiff_parallel.pl", out_f_name, outdir, netcdf[0], rastercount_str], stdout=PIPE)
        output = process.communicate()[0]
        print("OK")
        return
#            run_gdal(out_f_name, outdir, netcdf, subdatasets, rastercount, tmpdir)

@click.command(help="""\b Convert netcdf to Geotiff and then to Cloud
                    Optimized Geotiff using gdal."""
                    " Mandatory Requirement: GDAL version should be >=2.2")
@click.argument('fname', type=str)
@click.argument('output_dir', type=str)
def main(fname, output_dir):
    logging.info("Reading %s", basename(fname))
    gtiff_fname, file_path = getfilename(fname, output_dir)
#    print("fname: ", fname)
    subdatasets = gdal.Open(fname, gdal.GA_ReadOnly).GetSubDatasets()
    # ---To Check if NETCDF is stacked or unstacked --
    rastercount = gdal.Open(subdatasets[0][0]).RasterCount
#    print("**** rastercount:", rastercount)
#    return
    # Create the YAML after creating the Tiffs.
    # This allows to skip the datasets that are already processed.
    yaml_file = output_dir + file_path + ".yaml"
    if not os.path.exists(yaml_file):
        logging.info("Writing COG to %s %s", file_path,
                     basename(gtiff_fname))
        _write_cogtiff(gtiff_fname, output_dir, subdatasets,
                       rastercount)
#        _write_dataset(fname, file_path, output_dir, rastercount)
    else:
        logging.info("File exists: %s", yaml_file)

if __name__ == "__main__":
    main()

