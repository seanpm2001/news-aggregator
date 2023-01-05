# Copyright (c) 2023 The Brave Authors. All rights reserved.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/. */

import hashlib
import html
import json
import logging
import math
import multiprocessing
import shutil
import sys
import time
from datetime import datetime, timedelta
from functools import partial
from io import BytesIO
from queue import Queue
from urllib.parse import quote, urlparse, urlunparse

import bleach
import dateparser
import feedparser
import html2text
import metadata_parser
import pytz
import requests
import requests_cache
import structlog
import unshortenit
from better_profanity import profanity
from bs4 import BeautifulSoup as BS
from pytz import timezone
from requests.exceptions import (
    ConnectTimeout,
    HTTPError,
    InvalidURL,
    ReadTimeout,
    SSLError,
    TooManyRedirects,
)

from config import get_config
from lib.utils import upload_file
from src import image_processor_sandboxed

config = get_config()

TZ = timezone("UTC")
REQUEST_TIMEOUT = 30

im_proc = image_processor_sandboxed.ImageProcessor(config.private_s3_bucket)
unshortener = unshortenit.UnshortenIt(
    default_timeout=5, default_headers={"User-Agent": config.user_agent}
)

logging.getLogger("urllib3").setLevel(logging.ERROR)  # too many un-actionable warnings
logging.getLogger("metadata_parser").setLevel(
    logging.CRITICAL
)  # hide NotParsableFetchError messages

logging.info("Using %s processes for parallel tasks.", config.concurrency)

logger = structlog.getLogger(__name__)

# adding custom bad words for profanity check
custom_badwords = ["vibrators"]
profanity.add_censor_words(custom_badwords)


def get_with_max_size(url, max_bytes):
    response = requests.get(
        url,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": config.user_agent},
        stream=True,
    )
    response.raise_for_status()

    if response.status_code != 200:  # raise for status is not working with 3xx error
        raise HTTPError(f"Http error with status code {response.status_code}")

    if (
        response.headers.get("Content-Length")
        and int(response.headers.get("Content-Length")) > max_bytes
    ):
        raise ValueError("Content-Length too large")
    count = 0
    content = BytesIO()
    for chunk in response.iter_content(4096):
        count += len(chunk)
        content.write(chunk)
        if count > max_bytes:
            raise ValueError("Received more than max_bytes")
    return content.getvalue()


def process_image(item):
    if item["img"] != "":
        try:
            cache_fn = im_proc.cache_image(item["img"])
        except Exception as e:
            cache_fn = None
            logger.error(f"im_proc.cache_image failed [{e}]: {item['img']}")
        if cache_fn:
            if cache_fn.startswith("https"):
                item["padded_img"] = cache_fn
            else:
                item["padded_img"] = (
                    "%s/brave-today/cache/%s" % (config.pcdn_url_base, cache_fn)
                    + ".pad"
                )
        else:
            item["img"] = ""
            item["padded_img"] = ""
    return item


def download_feed(feed):  # noqa: C901
    report = {"size_after_get": None, "size_after_insert": 0}
    max_feed_size = 10000000  # 10M
    try:
        data = get_with_max_size(feed, max_feed_size)
        logger.debug(f"Downloaded feed: {feed}")
    except Exception:
        # Failed to get feed. I will try plain HTTP.
        try:
            u = urlparse(feed)
            u = u._replace(scheme="http")
            feed_url = urlunparse(u)
            data = get_with_max_size(feed_url, max_feed_size)
        except ReadTimeout:
            return None
        except HTTPError as e:
            logger.error(f"Failed to get feed: {feed_url} ({e})")
            return None
        except Exception as e:
            logger.error(f"Failed to get [{e}]: {feed_url}")
            return None
    try:
        feed_cache = feedparser.parse(data)
        report["size_after_get"] = len(feed_cache["items"])
        if report["size_after_get"] == 0:
            return None  # workaround error serialization issue
    except Exception as e:
        logger.error(f"Feed failed to parse [{e}]: {feed}")
        return None
    # bypass serialization issues
    feed_cache = dict(feed_cache)
    if "bozo_exception" in feed_cache:
        del feed_cache["bozo_exception"]
    return {"report": report, "feed_cache": feed_cache, "key": feed}


def fixup_item(item, my_feed):  # noqa: C901
    out_item = {}
    if "category" in my_feed:
        out_item["category"] = my_feed["category"]
    if "updated" in item:
        out_item["publish_time"] = dateparser.parse(item["updated"])
    elif "published" in item:
        out_item["publish_time"] = dateparser.parse(item["published"])
    else:
        return None  # skip (no update field)
    if out_item["publish_time"] is None:
        return None  # skip (no publish time)
    if out_item["publish_time"].tzinfo is None:
        TZ.localize(out_item["publish_time"])
    out_item["publish_time"] = out_item["publish_time"].astimezone(pytz.utc)
    if "link" not in item:
        if "url" in item:
            item["link"] = item["url"]
        else:
            return None  # skip (can't find link)

    # check if the article belongs to allowed domains
    if item.get("link"):
        if not my_feed.get("destination_domains"):
            return None

        if (urlparse(item["link"]).hostname or "") not in my_feed[
            "destination_domains"
        ] and my_feed["destination_domains"] not in (
            urlparse(item["link"]).hostname or ""
        ):
            return None

    # filter the offensive articles
    if profanity.contains_profanity(item.get("title")):
        return None

    try:
        out_item["url"] = unshortener.unshorten(item["link"])
    except (
        requests.exceptions.ConnectionError,
        ConnectTimeout,
        InvalidURL,
        ReadTimeout,
        SSLError,
        TooManyRedirects,
    ):
        return None  # skip (unshortener failed)
    except Exception as e:
        logger.error(f"unshortener failed [{e}]: {item['link']}")
        return None  # skip (unshortener failed)

    # image determination
    if (
        "media_content" in item
        and len(item["media_content"]) > 0
        and "url" in item["media_content"][0]
    ):
        out_item["img"] = item["media_content"][0]["url"]
    elif "media_thumbnail" in item and "url" in item["media_thumbnail"][0]:
        out_item["img"] = item["media_thumbnail"][0]["url"]
    elif (
        "media_content" in item
        and len(item["media_content"]) > 0
        and "url" in item["media_content"][0]
    ):
        out_item["img"] = item["media_content"][0]["url"]
    elif "summary" in item and BS(item["summary"], features="html.parser").find_all(
        "img"
    ):
        result = BS(item["summary"], features="html.parser").find_all("img")
        if "src" in result[0]:
            out_item["img"] = BS(item["summary"], features="html.parser").find_all(
                "img"
            )[0]["src"]
        else:
            out_item["img"] = ""
    elif "urlToImage" in item:
        out_item["img"] = item["urlToImage"]
    elif "image" in item:
        out_item["img"] = item["image"]
    elif (
        "content" in item
        and item["content"]
        and item["content"][0]["type"] == "text/html"
        and BS(item["content"][0]["value"], features="html.parser").find_all("img")
    ):
        r = BS(item["content"][0]["value"], features="html.parser").find_all("img")[0]
        if "img" in r:
            out_item["img"] = BS(
                item["content"][0]["value"], features="html.parser"
            ).find_all("img")[0]["src"]
        else:
            out_item["img"] = ""
    else:
        out_item["img"] = ""
    if "title" not in item:
        # No title. Skip.
        return None

    out_item["title"] = BS(item["title"], features="html.parser").get_text()

    # add some fields
    if "description" in item and item["description"]:
        out_item["description"] = BS(
            item["description"], features="html.parser"
        ).get_text()
    else:
        out_item["description"] = ""
    out_item["content_type"] = my_feed["content_type"]
    if out_item["content_type"] == "audio":
        out_item["enclosures"] = item["enclosures"]
    if out_item["content_type"] == "product":
        out_item["offers_category"] = item["category"]
    out_item["publisher_id"] = my_feed["publisher_id"]
    out_item["publisher_name"] = my_feed["publisher_name"]
    out_item["creative_instance_id"] = my_feed["creative_instance_id"]
    out_item["description"] = out_item["description"][:500]

    # weird hack put in place just for demo
    if "filter_images" in my_feed:
        if my_feed["filter_images"]:
            out_item["img"] = ""

    return out_item


def check_images_in_item(item, feeds):  # noqa: C901
    if item["img"]:
        try:
            parsed = urlparse(item["img"])
            if not parsed.scheme:
                parsed = parsed._replace(scheme="https")
                url = urlunparse(parsed)
            else:
                url = item["img"]
        except Exception as e:
            logger.error(f"Can't parse image [{e}]: {item['img']}")
            item["img"] = ""
        try:
            result = scrape_session.head(url, allow_redirects=True)
            if not result.status_code == 200:
                item["img"] = ""
            else:
                item["img"] = url
        except SSLError:
            item["img"] = ""
        except Exception:
            item["img"] = ""
    if item["img"] == "" or feeds[item["publisher_id"]]["og_images"] is True:
        # if we came out of this without an image, lets try to get it from opengraph
        try:
            page = metadata_parser.MetadataParser(
                url=item["url"],
                requests_session=scrape_session,
                support_malformed=True,
                url_headers={"User-Agent": config.user_agent},
                search_head_only=True,
                strategy=["page", "meta", "og", "dc"],
                requests_timeout=5,
            )
            item["img"] = page.get_metadata_link("image")
        except metadata_parser.NotParsableFetchError as e:
            if e.code and e.code not in (403, 429, 500, 502, 503):
                logger.error(f"Error parsing [{e}]: {item['url']}")
        except (UnicodeDecodeError, metadata_parser.NotParsable) as e:
            logger.error(f"Error parsing: {item['url']} -- {e}")
        if item["img"] is None:
            item["img"] = ""

    if not item["img"] == "":
        parsed_img_url = urlparse(item["img"])

        if len(parsed_img_url.path) >= 4:
            item["img"] = urlunparse(parsed_img_url._replace(scheme="https"))
        else:
            item["img"] = ""

    item["padded_img"] = item["img"]
    return item


expire_after = timedelta(hours=2)
scrape_session = requests_cache.CachedSession(
    expire_after=expire_after, backend="memory", timeout=5
)
scrape_session.cache.remove_expired_responses(datetime.utcnow() - expire_after)
scrape_session.headers.update({"User-Agent": config.user_agent})


class FeedProcessor:
    def __init__(self):
        self.queue = Queue()
        self.h2t = html2text.HTML2Text()
        self.h2t.ignore_links = True
        self.report = {}  # holds reports and stats of all actions
        self.feeds = {}

    def check_images(self, items):
        out_items = []
        logger.info(f"Checking images for {len(items)} items...")
        with multiprocessing.Pool(config.concurrency) as pool:
            for item in pool.imap(
                partial(check_images_in_item, feeds=self.feeds), items
            ):
                out_items.append(item)

        logger.info(f"Caching images for {len(out_items)} items...")
        with multiprocessing.Pool(config.concurrency) as pool:
            result = []
            for item in pool.imap(process_image, out_items):
                result.append(item)
        return result

    def download_feeds(self, my_feeds):
        feed_cache = {}
        logger.info(f"Downloading {len(my_feeds)} feeds...")
        with multiprocessing.Pool(config.concurrency) as pool:
            for result in pool.imap(
                download_feed, [my_feeds[key]["feed_url"] for key in my_feeds]
            ):
                if not result:
                    continue
                self.report["feed_stats"][result["key"]] = result["report"]
                feed_cache[result["key"]] = result["feed_cache"]
                self.feeds[my_feeds[result["key"]]["publisher_id"]] = my_feeds[
                    result["key"]
                ]
        return feed_cache

    def get_rss(self, my_feeds):
        self.feeds = {}
        entries = []
        self.report["feed_stats"] = {}
        feed_cache = self.download_feeds(my_feeds)

        logger.info(
            f"Fixing up and extracting the data for the items in {len(feed_cache)} feeds..."
        )
        for key in feed_cache:
            logger.debug(f"processing: {key}")
            start_time = time.time()
            with multiprocessing.Pool(config.concurrency) as pool:
                for out_item in pool.imap(
                    partial(fixup_item, my_feed=my_feeds[key]),
                    feed_cache[key]["entries"][: my_feeds[key]["max_entries"]],
                ):
                    if out_item:
                        entries.append(out_item)
                    self.report["feed_stats"][key]["size_after_insert"] += 1
            end_time = time.time()
            logger.debug(
                f"processed {key} in {round((end_time - start_time) * 1000)} ms"
            )
        return entries

    def score_entries(self, entries):
        out_entries = []
        variety_by_source = {}
        for entry in entries:
            seconds_ago = (
                datetime.utcnow() - dateparser.parse(entry["publish_time"])
            ).total_seconds()
            recency = math.log(seconds_ago) if seconds_ago > 0 else 0.1
            if entry["publisher_id"] in variety_by_source:
                last_variety = variety_by_source[entry["publisher_id"]]
            else:
                last_variety = 1.0
            variety = last_variety * 2.0
            score = recency * variety
            entry["score"] = score
            out_entries.append(entry)
            variety_by_source[entry["publisher_id"]] = variety
        return out_entries

    def aggregate_rss(self, feeds):
        entries = []
        entries += self.get_rss(feeds)
        sorted_entries = sorted(entries, key=lambda entry: entry["publish_time"])
        sorted_entries.reverse()  # for most recent entries first
        filtered_entries = self.fixup_entries(sorted_entries)
        filtered_entries = self.scrub_html(filtered_entries)
        filtered_entries = self.score_entries(filtered_entries)
        return filtered_entries

    def fixup_entries(self, sorted_entries):
        """This function tends to be used more for fix-ups that require the whole feed like dedupe"""
        url_dedupe = {}
        out = []
        now_utc = datetime.now().replace(tzinfo=pytz.utc)
        for item in sorted_entries:
            # urlencoding url because sometimes downstream things break
            url_hash = hashlib.sha256(item["url"].encode("utf-8")).hexdigest()
            parts = urlparse(item["url"])
            parts = parts._replace(path=quote(parts.path))
            encoded_url = urlunparse(parts)
            if item["content_type"] != "product":
                if item["publish_time"] > now_utc or item["publish_time"] < (
                    now_utc - timedelta(days=60)
                ):
                    if item["content_type"] != "product":
                        continue  # skip (newer than now() or older than 1 month)
            if encoded_url in url_dedupe:
                continue  # skip
            item["publish_time"] = item["publish_time"].strftime("%Y-%m-%d %H:%M:%S")
            if "date_live_from" in item:
                item["date_live_from"] = item["date_live_from"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            if "date_live_to" in item:
                item["date_live_to"] = item["date_live_to"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            item["title"] = html.unescape(item["title"])
            item["url"] = encoded_url
            item["url_hash"] = url_hash
            out.append(item)
            url_dedupe[encoded_url] = True
        out = self.check_images(out)
        return out

    def scrub_html(self, feed):
        """Scrubbing HTML of all entries that will be written to feed"""
        out = []
        for item in feed:
            for key in item:
                if item[key]:
                    item[key] = bleach.clean(item[key], strip=True)
                    item[key] = item[key].replace(
                        "&amp;", "&"
                    )  # workaround limitation in bleach
            out.append(item)
        return out

    def aggregate(self, feeds, out_fn):
        self.feeds = feeds
        with open(out_fn, "w") as f:
            f.write(json.dumps(self.aggregate_rss(feeds)))

    def aggregate_shards(self, feeds):
        by_category = {}
        for item in self.aggregate_rss(feeds):
            if not item["category"] in by_category:
                by_category[item["category"]] = [item]
            else:
                by_category[item["category"]].append(item)
        for key in by_category:
            with open("feed/category/%s.json" % key, "w") as f:
                f.write(json.dumps(by_category[key]))


fp = FeedProcessor()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        category = sys.argv[1]
    else:
        category = "feed"
    with open(f"{config.output_path / category}.json") as f:
        feeds = json.loads(f.read())
        fp.aggregate(feeds, f"{config.output_feed_path / category}.json-tmp")
        shutil.copyfile(
            f"{config.output_feed_path / category}.json-tmp",
            f"{config.output_feed_path / category}.json",
        )

        if not config.no_upload:
            upload_file(
                config.output_feed_path / f"{category}.json",
                config.pub_s3_bucket,
                f"brave-today/{category}{str(config.sources_file).replace('sources', '')}.json",
            )
            # Temporarily upload also with incorrect filename as a stopgap for
            # https://github.com/brave/brave-browser/issues/20114
            # Can be removed once fixed in the brave-core client for all Desktop users.
            upload_file(
                config.output_feed_path / f"{category}.json",
                config.pub_s3_bucket,
                f"brave-today/{category}{str(config.sources_file).replace('sources', '')}json",
            )
    with open(config.output_path / "report.json", "w") as f:
        f.write(json.dumps(fp.report))
