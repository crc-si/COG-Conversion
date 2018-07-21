#!/usr/bin/env python
import glob
import json
import logging
import os
from os.path import join as pjoin, basename, dirname, exists, splitext
import subprocess
from subprocess import check_call
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


def get_bandname(filename):
    """
    Get the band name, BS_PC_10, from the NetCDF example as below.
    NETCDF:"/g/data/.../file.nc":BS_PC_10
    """
    return (filename.split(':'))[-1]


@click.command(help="""\b Convert netcdf to Geotiff and then to Cloud
                    Optimized Geotiff using gdal."""
                    " Mandatory Requirement: GDAL version should be >=2.2")
@click.argument('out_f_name', type=str)
@click.argument('outdir', type=str)
@click.argument('netcdf', type=str)
@click.argument('count', type=int)
@click.argument('rastercount', type=int)
def main(out_f_name, outdir, netcdf, count, rastercount):
    band_name = get_bandname(netcdf)
    print("0: ****: ")

    if rastercount > 1:
        out_fname = out_f_name + '_' + str(count) + '_' + band_name + '.tif'
    else:
        out_fname = out_f_name + '_' + band_name + '.tif'

    env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
           'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
    subprocess.check_call(env, shell=True)

    with tempfile.TemporaryDirectory() as tmpdir:
#        print("**** tmpdir: ", tmpdir)
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
#    tmpdir = tempfile.TemporaryDirectory()
    # copy to a tempfolder
        temp_fname = pjoin(tmpdir, basename(out_fname))
        to_cogtif = [
            'gdal_translate',
            '-b',
            str(count),
            netcdf,
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

if __name__ == "__main__":
    main()

