import collections.abc
import csv
import glob
import hashlib
import json
import re
from copy import deepcopy
from urllib.parse import urlparse, urlunparse

import bleach
import structlog

from config import get_config
from lib.utils import get_cover_infos_lookup, get_favicons_lookup, upload_file

config = get_config()

logger = structlog.getLogger(__name__)


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

source_files = glob.glob(r"sources.*_*.csv")

for in_path in source_files:  # noqa: C901
    locale = locales_finder.findall(in_path)[0]
    with open(in_path, "r") as f:
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
            if row[6] == "On":
                og_images = True
            else:
                og_images = False
            if row[4] == "Enabled":
                default = True
            else:
                default = False

            if row[7] == "":
                content_type = "article"
            else:
                content_type = row[7]

            domain = row[0]
            favicon_url = favicons_lookup.get(domain, None)
            cover_info = cover_infos_lookup.get(
                domain, {"cover_url": None, "background_color": None}
            )

            channels = []
            if len(row) >= 11:
                channels = [i.strip() for i in row[10].split(";") if i.strip()]

            rank = None
            try:
                rank = int(row[11])
                if rank == "":
                    rank = None
            except (ValueError, IndexError):
                rank = None

            original_feed = ""
            try:
                original_feed = row[12]
                if original_feed == "":
                    original_feed = feed_url
            except IndexError:
                original_feed = feed_url

            feed_hash = hashlib.sha256(original_feed.encode("utf-8")).hexdigest()

            if sources_data.get(feed_hash):
                if (
                    locale
                    not in sources_data.get(feed_hash)
                    .get("locales")[0]
                    .get("locale")[0]
                ):
                    locales = [{"locale": locale, "rank": rank, "channels": channels}]
                    update_locales = deepcopy(sources_data.get(feed_hash)["locales"])
                    update_locales.extend(locales)
                    update_locales_dict = {"locales": update_locales}
                    deep_update(sources_data.get(feed_hash), update_locales_dict)
            else:
                locales = [{"locale": locale, "rank": rank, "channels": channels}]
                sources_data[feed_hash] = {
                    "enabled": default,
                    "publisher_name": row[2].replace(
                        "&amp;", "&"
                    ),  # workaround limitation in bleach
                    "category": row[3],
                    "site_url": domain,
                    "feed_url": row[1],
                    "favicon_url": favicon_url,
                    "cover_url": cover_info["cover_url"],
                    "background_color": cover_info["background_color"],
                    "score": float(row[5] or 0),
                    "destination_domains": row[9].split(";"),
                    "locales": locales,
                }

sources_data_as_list = [dict(sources_data[x], publisher_id=x) for x in sources_data]

sources_data_as_list = sorted(sources_data_as_list, key=lambda x: x["publisher_name"])

with open(f"{config.output_path / config.global_sources_file}.json", "w") as f:
    f.write(json.dumps(sources_data_as_list))
if not config.no_upload:
    upload_file(
        f"{config.global_sources_file}.json",
        config.pub_s3_bucket,
        "{}.json".format(config.global_sources_file),
    )
