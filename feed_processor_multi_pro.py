import asyncio
import hashlib
import html
import json
import logging
import math
import os
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
from urllib.parse import quote, urlparse, urlunparse

import aiohttp
import bleach
import dateparser
import feedparser
import metadata_parser
import orjson
import pytz
import requests
import unshortenit
import uvloop
from aiohttp import request
from aiomultiprocess import Pool
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
from structlog import get_logger

import config
import image_processor_sandboxed_pro
from config import USER_AGENT
from utils import upload_file

logger = get_logger()
TZ = timezone("UTC")

im_proc = image_processor_sandboxed_pro.ImageProcessor(config.PRIV_S3_BUCKET)
unshortener = unshortenit.UnshortenIt(default_timeout=5)

logging.basicConfig(level=config.LOG_LEVEL)
logging.getLogger("urllib3").setLevel(logging.ERROR)  # too many un-actionable warnings
logging.getLogger("metadata_parser").setLevel(
    logging.CRITICAL
)  # hide NotParsableFetchError messages

# adding custom bad words for profanity check
custom_badwords = ["vibrators"]
profanity.add_censor_words(custom_badwords)

session = None

aiohttp_timeout = aiohttp.ClientTimeout(
    total=30, connect=None, sock_connect=None, sock_read=None
)


async def get_with_max_size(url, max_bytes):
    async with request(
            "GET", url, headers={"User-Agent": USER_AGENT}, timeout=aiohttp_timeout
    ) as response:
        response.raise_for_status()
        if response.status != 200:  # raise for status is not working with 3xx error
            raise HTTPError(f"Http error with status code {response.status}")

        if (
                response.headers.get("Content-Length")
                and int(response.headers.get("Content-Length")) > max_bytes
        ):
            raise ValueError("Content-Length too large")

        return await response.read()


async def download_feed(publisher):
    report = {"size_after_get": None, "size_after_insert": 0}
    max_feed_size = 10000000  # 10M
    try:
        data = await get_with_max_size(publisher, max_feed_size)
    except Exception as e:
        # Failed to get feed. I will try plain HTTP.
        try:
            u = urlparse(publisher)
            u = u._replace(scheme="http")
            feed_url = urlunparse(u)
            data = await get_with_max_size(feed_url, max_feed_size)
        except ReadTimeout:
            logger.error(f"Failed to get [{e}]: {publisher}")
            return None
        except HTTPError as e:
            logger.error(f"Failed to get [{e}]: {publisher}")
            return None
        except Exception as e:
            logger.error(f"Failed to get [{e}]: {publisher}")
            return None
    try:
        feed_cache = feedparser.parse(data)
        report["size_after_get"] = len(feed_cache["items"])
        if report["size_after_get"] == 0:
            logger.error(f"Feed doesn't have any articles: {publisher}")
            return None  # workaround error serialization issue
    except Exception as e:
        logger.error(f"Failed to get [{e}]: {publisher}")
        return None
    # bypass serialization issues
    feed_cache = dict(feed_cache)
    if "bozo_exception" in feed_cache:
        del feed_cache["bozo_exception"]
    return {"report": report, "feed_cache": feed_cache, "key": publisher}


def get_article_img(article):
    # image determination
    img_url = None
    if article.get("image"):
        img_url = article["image"]

    elif article.get("urlToImage"):
        img_url = article["urlToImage"]

    elif article.get("media_content"):
        if len(article["media_content"]) > 0 and all("url" in d for d in article["media_content"]):
            if all("width" in d for d in article["media_content"]):
                img_url = max(article["media_content"], key=lambda item: int(item["width"])).get("url")
            elif all("height" in d for d in article["media_content"]):
                img_url = max(article["media_content"], key=lambda item: int(item["height"])).get("url")
            else:
                img_url = article['media_content'][0].get("url")

    elif article.get("media_thumbnail"):
        if len(article["media_thumbnail"]) > 0 and all("url" in d for d in article["media_thumbnail"]):
            if all("width" in d for d in article["media_thumbnail"]):
                img_url = max(article["media_thumbnail"], key=lambda item: int(item["width"])).get("url")
            elif all("height" in d for d in article["media_thumbnail"]):
                img_url = max(article["media_thumbnail"], key=lambda item: int(item["height"])).get("url")
            else:
                img_url = article['media_thumbnail'][0].get("url")

    elif article.get("summary"):
        image_tags = BS(article["summary"], features="html.parser").find_all("img")
        for img_tag in image_tags:
            if "src" in img_tag:
                # Check resolution of image
                img_url = img_tag.get("src")

    elif article.get("content"):
        image_tags = BS(article["content"][0]["value"], features="html.parser").find_all("img")
        for img_tag in image_tags:
            if "src" in img_tag:
                # Check resolution of image
                img_url = img_tag.get("src")

    return img_url


async def process_articles(article, publisher):
    out_article = defaultdict()

    # Process Title of the article
    if not article.get("title"):
        # No title. Skip.
        return None
    out_article["title"] = BS(article["title"], features="html.parser").get_text()
    out_article["title"] = html.unescape(out_article["title"])

    # Process published time
    if article.get("updated"):
        out_article["publish_time"] = dateparser.parse(article.get("updated"))
    elif article.get("published"):
        out_article["publish_time"] = dateparser.parse(article.get("published"))
    else:
        return None  # skip (no update field)

    if out_article.get("publish_time") is None:
        return None

    if out_article["publish_time"].tzinfo is None:
        TZ.localize(out_article["publish_time"])

    out_article["publish_time"] = out_article["publish_time"].astimezone(pytz.utc)

    now_utc = datetime.now().replace(tzinfo=pytz.utc)
    if publisher["content_type"] != "product":
        if out_article["publish_time"] > now_utc or out_article["publish_time"] < (
                now_utc - timedelta(days=60)
        ):
            return None  # skip (newer than now() or older than 1 month)

    out_article['publish_time'] = out_article['publish_time'].strftime('%Y-%m-%d %H:%M:%S')

    if article.get('date_live_from'):
        out_article['date_live_from'] = article['date_live_from'].strftime('%Y-%m-%d %H:%M:%S')
    if article.get('date_live_to'):
        out_article['date_live_to'] = article['date_live_to'].strftime('%Y-%m-%d %H:%M:%S')

    # Process article URL
    if article.get("link"):
        out_article["link"] = article["link"]
    elif article.get("url"):
        out_article["link"] = article["url"]
    else:
        return None  # skip (can't find link)

    # check if the article belongs to allowed domains
    if out_article.get("link"):
        if not publisher.get("destination_domains"):
            return None

        if (urlparse(out_article["link"]).hostname or "") not in publisher[
            "destination_domains"
        ] and publisher["destination_domains"] not in (
                urlparse(out_article["link"]).hostname or ""
        ):
            return None

    # Filter the offensive articles
    if profanity.contains_profanity(out_article.get("title")):
        return None

    out_article['img'] = get_article_img(article)

    # Add some fields
    out_article["category"] = publisher.get("category")
    if article.get("description"):
        out_article["description"] = BS(article["description"], features="html.parser").get_text()
    else:
        out_article["description"] = ""

    out_article["content_type"] = publisher["content_type"]
    if out_article["content_type"] == "audio":
        out_article["enclosures"] = article["enclosures"]
    if out_article["content_type"] == "product":
        out_article["offers_category"] = article["category"]

    out_article["publisher_id"] = publisher["publisher_id"]
    out_article["publisher_name"] = publisher["publisher_name"]
    out_article["creative_instance_id"] = publisher["creative_instance_id"]

    return out_article

async def unshorten_url(out_article):
    try:
        out_article["url"] = unshortener.unshorten(out_article["link"])
        out_article.pop("link", None)
        url_hash = hashlib.sha256(out_article["url"].encode("utf-8")).hexdigest()
        parts = urlparse(out_article["url"])
        parts = parts._replace(path=quote(parts.path))
        encoded_url = urlunparse(parts)
        out_article["url"] = encoded_url
        out_article["url_hash"] = url_hash
    except (
            requests.exceptions.ConnectionError,
            ConnectTimeout,
            InvalidURL,
            ReadTimeout,
            SSLError,
            TooManyRedirects,
    ) as e:
        logger.error(f"unshortener failed [{out_article['link']}]: {e}")
        return None  # skip (unshortener failed)
    except Exception as e:
        logger.error(f"unshortener failed [{out_article['link']}]: {e}")
        return None  # skip (unshortener failed)

    return out_article


async def check_images_in_item(article, publishers):
    if article["img"]:
        try:
            parsed_img_url = urlparse(article["img"])
            if not parsed_img_url.scheme:
                parsed = parsed_img_url._replace(scheme="https")
                url = urlunparse(parsed)
                article["img"] = url

            if len(parsed_img_url.path) < 4:
                article["img"] = None

        except Exception as e:
            logger.error(f"Can't parse image [{article['img']}]: {e}")
            article["img"] = None

    if article["img"] is None or publishers[article["publisher_id"]]["og_images"] is True:
        # if we came out of this without an image, lets try to get it from opengraph
        try:
            page = metadata_parser.MetadataParser(
                url=article["url"],
                support_malformed=True,
                url_headers={"User-Agent": config.USER_AGENT},
                search_head_only=True,
                strategy=["page", "meta", "og", "dc"],
                requests_timeout=10,
            )
            article["img"] = page.get_metadata_link("image")
        except metadata_parser.NotParsableFetchError as e:
            if e.code and e.code not in (403, 429, 500, 502, 503):
                logger.error(f"Error parsing [{article['url']}]: {e}")
        except (UnicodeDecodeError, metadata_parser.NotParsable) as e:
            logger.error(f"Error parsing: {article['url']} -- {e}")

    article["padded_img"] = article["img"]

    return article


async def process_image(article):
    if article["img"] is not None:
        try:
            cache_fn = await im_proc.cache_image(article["img"])
        except Exception as e:
            cache_fn = None
            logger.error(f"im_proc.cache_image failed [{article['img']}]: {e}")
        if cache_fn:
            if cache_fn.startswith("https"):
                article["padded_img"] = cache_fn
            else:
                article["padded_img"] = (
                        "%s/brave-today/cache/%s" % (config.PCDN_URL_BASE, cache_fn)
                        + ".pad"
                )
        else:
            article["img"] = None
            article["padded_img"] = None
    return article


async def check_images(article, publishers):
    out_items = []
    result = []
    async with Pool(
            config.CONCURRENCY, loop_initializer=uvloop.new_event_loop,
            queuecount=4,
            childconcurrency=80
    ) as pool:
        async for item in pool.map(
                partial(check_images_in_item, publishers=publishers), article
        ):
            out_items.append(item)

    async with Pool(
            config.CONCURRENCY, loop_initializer=uvloop.new_event_loop, childconcurrency=8
    ) as pool:
        async for item in pool.map(process_image, out_items):
            result.append(item)

    return result


async def scrub_html(article):
    """Scrubbing HTML of all entries that will be written to feed."""
    for key in article:
        if article[key]:
            article[key] = bleach.clean(article[key], strip=True)
            article[key] = article[key].replace(
                "&amp;", "&"
            )  # workaround limitation in bleach

    return article


class FeedProcessor:
    def __init__(self, publishers, output_path):
        self.report = defaultdict(dict)  # holds reports and stats of all actions
        self.feeds = defaultdict(dict)
        self.publishers = publishers
        self.output_path = output_path

    if not os.path.isdir("feed"):
        os.mkdir("feed")

    async def download_feeds(self):
        feeds_cache = defaultdict()
        logger.info(f"Downloading feeds... {len(self.publishers)}")

        async with Pool(
                config.CONCURRENCY,
                loop_initializer=uvloop.new_event_loop,
                queuecount=4,
                childconcurrency=80,
        ) as pool:
            async for result in pool.map(
                    download_feed, [self.publishers[key]["url"] for key in self.publishers]
            ):
                if not result:
                    continue
                self.report["feed_stats"][result["key"]] = result["report"]
                feeds_cache[result["key"]] = result["feed_cache"]
                self.feeds[
                    self.publishers[result["key"]]["publisher_id"]
                ] = self.publishers[result["key"]]

        return feeds_cache

    async def get_rss(self):
        entries = []
        feed_cache = await self.download_feeds()

        logger.info(
            f"Fixing up and extracting the data for the items in {len(feed_cache)} feeds..."
        )
        for key in feed_cache:
            async with Pool(
                    config.CONCURRENCY,
                    loop_initializer=uvloop.new_event_loop,
                    childconcurrency=8,
            ) as pool:
                async for out_item in pool.map(
                        partial(process_articles, publisher=self.publishers[key]),
                        feed_cache[key]["entries"][: self.publishers[key]["max_entries"]],
                ):
                    if out_item:
                        entries.append(out_item)
                    self.report["feed_stats"][key]["size_after_insert"] += 1
        return entries

    async def score_entries(self, entries):
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

    async def aggregate_rss(self):
        raw_entries = []
        entries = []
        cleaned_entries = []
        raw_entries += await self.get_rss()

        logger.info(f"Unshorten the URL of {len(raw_entries)} items...")
        async with Pool(
                config.CONCURRENCY,
                loop_initializer=uvloop.new_event_loop,
                queuecount=4,
                childconcurrency=80,
        ) as pool:
            async for out_item in pool.map(unshorten_url, raw_entries):
                if out_item:
                    entries.append(out_item)

        logger.info(f"Sorting for {len(entries)} items...")
        # for most recent entries first
        sorted_entries = sorted(entries, key=lambda entry: entry["publish_time"], reverse=True)

        logger.info(f"Getting images for {len(sorted_entries)} items...")
        fixed_entries = await check_images(sorted_entries, self.feeds)

        logger.info(f"Scrubbing {len(fixed_entries)} items...")
        async with Pool(
                config.CONCURRENCY,
                loop_initializer=uvloop.new_event_loop,
                childconcurrency=8,
        ) as pool:
            async for out_item in pool.map(scrub_html, fixed_entries):
                if out_item:
                    cleaned_entries.append(out_item)

        logger.info(f"Adding Score to {len(cleaned_entries)} items...")
        entries = await self.score_entries(cleaned_entries)

        return entries

    async def aggregate(self):
        with open(self.output_path, "wb") as f:
            feeds = await self.aggregate_rss()
            f.write(orjson.dumps(feeds))

    async def aggregate_shards(self):
        by_category = {}
        feeds = await self.aggregate_rss()
        for item in feeds:
            if not item["category"] in by_category:
                by_category[item["category"]] = [item]
            else:
                by_category[item["category"]].append(item)
        for key in by_category:
            with open("feed/category/%s.json" % (key), "w") as f:
                f.write(json.dumps(by_category[key]))


if __name__ == "__main__":
    logger.info("Using %s processes for parallel tasks.", config.CONCURRENCY)
    if len(sys.argv) > 1:
        category = sys.argv[1]
    else:
        category = "feed"

    with open(f"{category}.json") as f:
        publishers = orjson.loads(f.read())
        output_path = f"feed/{category}.json-tmp"
        fp = FeedProcessor(publishers, output_path)
        asyncio.run(fp.aggregate())
        shutil.copyfile(f"feed/{category}.json-tmp", f"feed/{category}.json")
        if not config.NO_UPLOAD:
            upload_file(
                f"feed/{category}.json",
                config.PUB_S3_BUCKET,
                f"brave-today/{category}{config.SOURCES_FILE.replace('sources', '')}.json",
            )
            # Temporarily upload also with incorrect filename as a stopgap for
            # https://github.com/brave/brave-browser/issues/20114
            # Can be removed once fixed in the brave-core client for all Desktop users.
            upload_file(
                f"feed/{category}.json",
                config.PUB_S3_BUCKET,
                f"brave-today/{category}{config.SOURCES_FILE.replace('sources', '')}json",
            )

    with open("report.json", "wb") as f:
        f.write(orjson.dumps(fp.report))
