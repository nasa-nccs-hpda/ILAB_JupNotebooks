"""
process to download several dates/tiles from laads without having to download all
https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/
splits process into 24 threads

Written by Jordan Caraballo-Vega, modified my Melanie Frost 4/19/2024

-----------------------
To run

ssh ilab206
screen -S modis_download

cd {filepath}

module load anaconda
conda activate ilab-pytorch

python modis_download.py
------------------------
Can check on status by opening new screen, navigating to dir and 
watch -n.1 'ls *.hdf|wc'
-----------------------
May have to update Authroization Bearer code
----------------------
wget flag guide
-np = no parent
-A = files to download 
-nH = no host direcotreis
-nd = no directory structure (alternately, can do "-nH --cut-dirs=3" to separate by product/year/day)
-r = recursive
-R = files to exclude (ex: -R .html,.tmp)
-nc = continues if interrupted
-m = mirror

"""

import os
import sys
import time
import argparse
import logging
import numpy as np
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

modis_product = 'MODTBGA'

def download_file(download_url: str):
    os.system(download_url)
    return


def main():

    intervals_2019 = [f'2019/{str(day).zfill(3)}' for day in range(65, 366)]
    intervals_2020 = [f'2020/{str(day).zfill(3)}' for day in range(1, 65)]
    intervals = intervals_2019 + intervals_2020

    # List of files to download with their respective url
    download_urls = []

    # Iterate over each tile
    for time_interval in tqdm(intervals):

        url = \
            'wget -e robots=off -m -np -A "*h08v04*.hdf, *h08v05*.hdf, *h09v04*.hdf, *h09v05*.hdf, *h10v04*.hdf, *h10v05*.hdf, *h10v06*.hdf, *h11v02*.hdf, *h11v03*.hdf, *h11v04*.hdf, *h11v05*.hdf, *h11v08*.hdf, *h11v09*.hdf, *h11v10*.hdf, *h12v01*.hdf, *h12v02*.hdf, *h12v03*.hdf, *h12v04*.hdf, *h12v05*.hdf, *h12v09*.hdf, *h12v10*.hdf, *h12v12*.hdf, *h13v01*.hdf, *h13v02*.hdf, *h13v10*.hdf, *h13v11*.hdf, *h13v12*.hdf, *h16v01*.hdf, *h17v05*.hdf, *h18v03*.hdf, *h18v04*.hdf, *h18v07*.hdf, *h19v04*.hdf, *h19v07*.hdf, *h19v08*.hdf, *h19v09*.hdf, *h19v10*.hdf, *h19v11*.hdf, *h19v12*.hdf, *h20v02*.hdf, *h20v03*.hdf, *h20v04*.hdf, *h20v06*.hdf, *h20v08*.hdf, *h20v09*.hdf, *h20v10*.hdf, *h20v11*.hdf, *h21v01*.hdf, *h21v02*.hdf, *h21v04*.hdf, *h21v05*.hdf, *h21v06*.hdf, *h21v10*.hdf, *h22v03*.hdf, *h22v04*.hdf, *h23v02*.hdf, *h23v03*.hdf, *h24v02*.hdf, *h24v03*.hdf, *h24v04*, *h26v06*.hdf, *h27v04*.hdf, *h27v06*, *h27v07*.hdf, *h28v11*.hdf, *h29v11*.hdf, *h29v12*.hdf, *h30v12*.hdf, *h31v11*.hdf" -R .html,.tmp -nd -r ' + \
            f'"https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/{modis_product}/{time_interval}" ' + \
            '--header "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbF9hZGRyZXNzIjoibWVsYW5pZWpmcm9zdEBnbWFpbC5jb20iLCJpc3MiOiJBUFMgT0F1dGgyIEF1dGhlbnRpY2F0b3IiLCJpYXQiOjE3MTIzMzY4ODUsIm5iZiI6MTcxMjMzNjg4NSwiZXhwIjoxODcwMDE2ODg1LCJ1aWQiOiJtZWxmcm9zdHkiLCJ0b2tlbkNyZWF0b3IiOiJtZWxmcm9zdHkifQ.Nk1bPwrgLrO9wnajdxJm6JUb7VKZ5tRUq5xivED7vK4" -P .'
        download_urls.append(url)

    # print(download_urls)

    # Set pool, start parallel multiprocessing
    p = Pool(processes=24)
    p.map(download_file, download_urls)
    p.close()
    p.join()


# -----------------------------------------------------------------------------
# Invoke the main
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    sys.exit(main())
