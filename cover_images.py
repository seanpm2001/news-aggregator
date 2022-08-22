import requests
from bs4 import BeautifulSoup
import os
import urllib
from PIL import Image

CACHE_FOLDER = './dist/cache'
os.makedirs(CACHE_FOLDER)

def get_html(domain) -> BeautifulSoup:
    try:
        html = requests.get(domain).content.decode('utf-8')
        return BeautifulSoup(html, features='lxml')

    # Failed to download html
    except: return None

def get_manifest_icon(html: BeautifulSoup):
    pass

def get_apple_icon(html: BeautifulSoup):
    pass

def get_open_graph_icon(html: BeautifulSoup):
    pass

def get_filename(url: str):
    return os.path.join(CACHE_FOLDER, urllib.parse.quote_plus(url))

def get_icon(icon_url: str) -> CoverImage:
    filename = get_filename(icon_url)
    if filename.endswith('.svg'):
        # Can't handle SVGs
        return None

    try:
        if not os.path.exists(filename):
            response = requests.get(icon_url, stream=True)
            if not response.ok: return None

            with open(filename, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)

        return Image.open(filename)

    # Failed to download the image, or the thing we downloaded wasn't valid.
    except: return None

def get_background_color(icon: Image):
    pass
