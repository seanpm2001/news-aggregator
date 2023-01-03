import json
from multiprocessing import Pool
from typing import List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from structlog import get_logger

import config
import image_processor_sandboxed
from config import (
    CONCURRENCY,
    FAVICON_LOOKUP_FILE,
    PCDN_URL_BASE,
    PRIV_S3_BUCKET,
    PUB_S3_BUCKET,
    USER_AGENT,
)
from utils import get_all_domains, upload_file, uri_validator

logger = get_logger()
im_proc = image_processor_sandboxed.ImageProcessor(
    PRIV_S3_BUCKET, s3_path="brave-today/favicons/{}.pad", force_upload=True
)

# In seconds. Tested with 5s but it's too low for a bunch of sites (I'm looking
# at you https://skysports.com).
REQUEST_TIMEOUT = 15


def get_favicon(domain: str) -> Tuple[str, str]:
    # Set the default favicon path. If we don't find something better, we'll use
    # this.
    icon_url = "/favicon.ico"

    try:
        response = requests.get(
            domain, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}
        )
        soup = BeautifulSoup(response.text, features="lxml")
        icon = soup.find("link", rel="icon")

        # Some sites may use an icon with a different rel.
        if not icon:
            icon = soup.find("link", rel="shortcut icon")
        if not icon:
            icon = soup.find("link", rel="apple-touch-icon")

        # Check if the icon exists, and the href is not empty. Surprisingly,
        # some sites actually do this (https://coinchoice.net/ + more).
        if icon and icon.get("href"):
            icon_url = icon.get("href")
    except Exception as e:
        logger.info(
            f"Failed to download HTML for {domain} with exception {e}. Using default icon path {icon_url}"
        )

    # We need to resolve relative urls, so we send something sensible to the client.
    icon_url = urljoin(domain, icon_url)

    if not uri_validator(icon_url):
        icon_url = None

    return domain, icon_url


def process_favicons_image(item):
    domain = ""
    padded_icon_url = None
    try:
        domain, icon_url = item
        try:
            cache_fn = im_proc.cache_image(icon_url)
        except Exception as e:
            cache_fn = None
            logger.error(f"im_proc.cache_image failed [e]: {icon_url}")
        if cache_fn:
            if cache_fn.startswith("https"):
                padded_icon_url = cache_fn
            else:
                padded_icon_url = f"{PCDN_URL_BASE}/brave-today/favicons/{cache_fn}.pad"
        else:
            padded_icon_url = None

    except ValueError as e:
        logger.info(f"Tuple unpacking error {e}")

    return domain, padded_icon_url


if __name__ == "__main__":
    domains = list(get_all_domains())
    logger.info(f"Processing {len(domains)} domains")

    favicons: List[Tuple[str, str]]
    with Pool(CONCURRENCY) as pool:
        favicons = pool.map(get_favicon, domains)

    processed_favicons: List[Tuple[str, str]]
    with Pool(CONCURRENCY) as pool:
        processed_favicons = pool.map(process_favicons_image, favicons)

    result = json.dumps(dict(processed_favicons), indent=4)
    with open(f"{FAVICON_LOOKUP_FILE}.json", "w") as f:
        f.write(result)

    logger.info("Fetched all the favicons!")

    if not config.NO_UPLOAD:
        upload_file(
            f"{FAVICON_LOOKUP_FILE}.json", PUB_S3_BUCKET, f"{FAVICON_LOOKUP_FILE}.json"
        )
        logger.info(f"{FAVICON_LOOKUP_FILE} is upload to S3")
