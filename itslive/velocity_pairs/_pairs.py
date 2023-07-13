import datetime
import os
import pathlib
from time import sleep
from typing import Any, List, Union

import earthaccess
import requests
from pqdm.threads import pqdm
from tqdm import tqdm

_API_ENDPOINT = "https://nsidc.org/apps/itslive-search/velocities"


def find(
    bbox: Union[List[float], None],
    polygon: Union[List[float], None] = None,
    percent_valid_pixels: int = 1,
    version: int = 2,
    mission: Union[None, str] = None,
    start: Union[None, datetime.date] = None,
    end: Union[None, datetime.date] = None,
    min_interval: Union[None, int] = None,
    max_interval: Union[None, int] = None,
) -> List[str]:
    """Returns a list velocity netcdf files based on the provided parameters"""
    urls = []
    if polygon is None and bbox is None:
        print("Search needs either bbox or polygon geometries")
        return []
    params = {
        "percent_valid_pixels": percent_valid_pixels,
        "version": version,
        "mission": mission,
        "start": start,
        "end": end,
        "min_interval": min_interval,
        "max_interval": max_interval,
    }
    if polygon is not None:
        params["polygon"] = ",".join([str(coord) for coord in polygon])

    if bbox is not None:
        if "polygon" in params:
            params.pop("polygon")
        params["bbox"] = ",".join([str(coord) for coord in bbox])
    called = False
    response = None
    print("Finding matching velocity pairs... ")
    with tqdm(total=120) as pbar:
        if not called:
            response = requests.get(
                f"{_API_ENDPOINT}/urls/", params=params, timeout=120
            ).json()
            called = True
            pbar.update(1)
        pbar.update(1)
        if response:
            pbar.update(120)
        sleep(1)

    for pair in response:
        if "url" in pair:
            urls.append(pair["url"])
    print(f"{len(urls)} pairs found")
    return urls


def coverage(
    bbox: List[float],
    polygon: List[float],
    percent_valid_pixels: int = 1,
    version: int = 2,
    mission: Union[None, str] = None,
    start: Union[None, datetime.date] = None,
    end: Union[None, datetime.date] = None,
    min_interval: Union[None, int] = None,
    max_interval: Union[None, int] = None,
) -> List[Any]:
    """Returns a list of velocity files counts by year on a given area"""
    params = {
        "bbox": bbox,
        "polygon": polygon,
        "percent_valid_pixels": percent_valid_pixels,
        "version": version,
        "mission": mission,
        "start": start,
        "end": end,
        "min_interval": min_interval,
        "max_interval": max_interval,
    }
    response = requests.get(f"{_API_ENDPOINT}/coverage/", params=params, timeout=120)
    return response


def _download_aws(urls: List[str], path: str) -> List[str]:

    # Closure!
    def _download_file_aws(url: str) -> str:
        local_filename = pathlib.Path(path) / pathlib.Path(url.split("/")[-1])
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename

    results = pqdm(urls, _download_file_aws, n_jobs=4)
    return results


def _download_nsidc(urls: List[str], path: str) -> List[str]:
    auth = earthaccess.login()
    if auth.auhtenticated:
        results = earthaccess.download(urls, path)
        return results


def download(urls: List[str], path: str, limit: int = 2000) -> List[str]:
    """Download ITS_LIVE velocity pairs using a list of URLs"""
    os.makedirs(path, exist_ok=True)
    if urls[0].startswith("https://its-live-data.s3.amazonaws.com"):
        files = _download_aws(urls, path)
    else:
        files = _download_nsidc(urls, path)
    return files
