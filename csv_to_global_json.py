import collections.abc
import csv
import glob
import hashlib
import json
import re
from copy import deepcopy
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


def deep_update(d, u):
    """
    Update a nested dictionary or similar mapping.
    Modify ``source`` in place.
    """
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


locales_finder = re.compile(r"sources\.(.*)\.csv")
sources_data = {}

favicons_lookup = get_favicons_lookup()
cover_infos_lookup = get_cover_infos_lookup()

source_files = glob.glob(r'sources.*_*.csv')

for in_path in source_files:
    locale = locales_finder.findall(in_path)[0]
    with open(in_path, 'r') as f:
        for index, row in enumerate(csv.reader(f)):
            row = [bleach.clean(x, strip=True) for x in row]
            if index < 1:
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

            domain = ensure_scheme(row[0])
            favicon_url = favicons_lookup.get(domain, "")
            cover_info = cover_infos_lookup.get(domain, {'cover_url': None, 'background_color': None})

            channels = []
            if len(row) >= 11:
                channels = [i.strip() for i in row[10].split(";")]

            rank = None
            if len(row) >= 12:
                rank = int(row[11] or 1)

            original_feed = ''
            if len(row) >= 13:
                original_feed = row[12] if row[12] else feed_url

            feed_hash = hashlib.sha256(original_feed.encode('utf-8')).hexdigest()

            if sources_data.get(feed_hash):
                if locale not in sources_data.get(feed_hash).get('locales')[0].get("locale")[0]:
                    locales = [{"locale": locale, "rank": rank, 'channels': channels}]
                    update_locales = deepcopy(sources_data.get(feed_hash)['locales'])
                    update_locales.extend(locales)
                    update_locales_dict = {'locales': update_locales}
                    deep_update(sources_data.get(feed_hash), update_locales_dict)
            else:
                locales = [{"locale": locale, "rank": rank, 'channels': channels}]
                sources_data[feed_hash] = {'enabled': default,
                                           'publisher_name': row[2],
                                           'category': row[3],
                                           'site_url': domain,
                                           'feed_url': row[1],
                                           'favicon_url': favicon_url,
                                           'cover_url': cover_info['cover_url'],
                                           'background_color': cover_info['background_color'],
                                           'score': float(row[5] or 0),
                                           'destination_domains': row[9].split(';'),
                                           'locales': locales}

sources_data_as_list = [dict(sources_data[x], publisher_id=x) for x in sources_data]

sources_data_as_list = sorted(sources_data_as_list, key=lambda x: x['publisher_name'])
with open(f"{config.GLOBAL_SOURCES_FILE}.json", 'w') as f:
    f.write(json.dumps(sources_data_as_list))
if not config.NO_UPLOAD:
    upload_file(f"{config.GLOBAL_SOURCES_FILE}.json", config.PUB_S3_BUCKET,
                "{}.json".format(config.GLOBAL_SOURCES_FILE))
