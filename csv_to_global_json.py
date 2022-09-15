import csv
import glob
import hashlib
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import bleach

import config
from utils import ensure_scheme, download_file
from utils import upload_file


def get_favicons_lookup():
    if not config.NO_DOWNLOAD:
        download_file(f'{config.FAVICON_LOOKUP_FILE}.json', config.PUB_S3_BUCKET,
                      f"{config.FAVICON_LOOKUP_FILE}.json")

    if Path(f'{config.FAVICON_LOOKUP_FILE}.json').is_file():
        with open(f'{config.FAVICON_LOOKUP_FILE}.json', 'r') as f:
            favicons_lookup = json.load(f)
            return favicons_lookup
    else:
        return {}


def get_cover_infos_lookup():
    if not config.NO_DOWNLOAD:
        download_file(f'{config.COVER_INFO_LOOKUP_FILE}.json', config.PUB_S3_BUCKET,
                      f"{config.COVER_INFO_LOOKUP_FILE}.json")

    if Path(f'{config.COVER_INFO_LOOKUP_FILE}.json').is_file():
        with open(f'{config.COVER_INFO_LOOKUP_FILE}.json', 'r') as f:
            cover_infos_lookup = json.load(f)
            return cover_infos_lookup
    else:
        return {}


out_path = sys.argv[1]

locales_finder = re.compile(r"sources\.(.*)\.csv")
count = 0
by_url = {}
sources_data = {}
source_files = glob.glob(r'sources.*_*.csv')
for in_path in source_files:
    locales = locales_finder.findall(in_path)
    with open(in_path, 'r') as f:
        for row in csv.reader(f):
            row = [bleach.clean(x, strip=True) for x in row]
            if count < 1:
                count += 1
                continue
            if len(row[2].strip()) == 0:
                # no title = no use
                continue
            feed_url = row[1]
            u = urlparse(feed_url)
            u = u._replace(scheme="https")
            feed_url = urlunparse(u)
            if row[6] == 'On':
                og_images = True
            else:
                og_images = False
            if row[4] == 'Enabled':
                default = True
            else:
                default = False

            if row[7] == '':
                content_type = 'article'
            else:
                content_type = row[7]

            favicons_lookup = get_favicons_lookup()
            cover_infos_lookup = get_cover_infos_lookup()

            domain = ensure_scheme(row[0])
            favicon_url = favicons_lookup.get(domain, "")
            cover_info = cover_infos_lookup.get(domain, {'cover_url': None, 'background_color': None})

            channels = []
            if len(row) >= 11:
                channels = [i.strip() for i in row[10].split(";")]

            rank = None
            if len(row) >= 12:
                rank = int(row[11])

            original_feed = ''
            if len(row) >= 13:
                original_feed = row[12]

            sources_data[
                hashlib.sha256(original_feed.encode('utf-8') if original_feed else feed_url.encode('utf-8')).hexdigest()
            ] = {'enabled': default,
                 'publisher_name': row[2],
                 'category': row[3],
                 'site_url': row[0],
                 'feed_url': row[1],
                 'favicon_url': favicon_url,
                 'cover_url': cover_info['cover_url'],
                 'background_color': cover_info['background_color'],
                 'score': float(row[5] or 0),
                 'destination_domains': row[9].split(';'),
                 'channels': channels,
                 'rank': rank,
                 'locales': locales}
        count = 0

sources_data_as_list = [dict(sources_data[x], publisher_id=x) for x in sources_data]

sources_data_as_list = sorted(sources_data_as_list, key=lambda x: x['publisher_name'])
with open(f"{config.GLOBAL_SOURCES_FILE}.json", 'w') as f:
    f.write(json.dumps(sources_data_as_list))
if not config.NO_UPLOAD:
    upload_file(f"{config.GLOBAL_SOURCES_FILE}.json", config.PUB_S3_BUCKET,
                "{}.json".format(config.GLOBAL_SOURCES_FILE))
    # Temporarily upload also with incorrect filename as a stopgap for
    # https://github.com/brave/brave-browser/issues/20114
    # Can be removed once fixed in the brave-core client for all Desktop users.
    upload_file(f"{config.GLOBAL_SOURCES_FILE}.json", config.PUB_S3_BUCKET,
                "{}json".format(config.GLOBAL_SOURCES_FILE))
