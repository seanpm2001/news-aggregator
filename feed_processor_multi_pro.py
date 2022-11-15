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
from urllib.parse import urlparse, urlunparse, quote

import aiohttp
import bleach
import dateparser
import feedparser
import metadata_parser
import orjson
import pytz
import requests
import requests_cache
import unshortenit
import uvloop
from aiohttp import request
from aiomultiprocess import Pool
from better_profanity import profanity
from bs4 import BeautifulSoup as BS
from pytz import timezone
from requests.exceptions import ConnectTimeout, HTTPError, InvalidURL, ReadTimeout, SSLError, TooManyRedirects
from structlog import get_logger

import config
import image_processor_sandboxed_pro
from config import USER_AGENT
from utils import upload_file

logger = get_logger()
TZ = timezone('UTC')

im_proc = image_processor_sandboxed_pro.ImageProcessor(config.PRIV_S3_BUCKET)
unshortener = unshortenit.UnshortenIt(default_timeout=5)

logging.basicConfig(level=config.LOG_LEVEL)
logging.getLogger("urllib3").setLevel(logging.ERROR)  # too many unactionable warnings
logging.getLogger("metadata_parser").setLevel(logging.CRITICAL)  # hide NotParsableFetchError messages
logging.info("Using %s processes for parallel tasks.", config.CONCURRENCY)

# adding custom bad words for profanity check
custom_badwords = ["vibrators"]
profanity.add_censor_words(custom_badwords)

expire_after = timedelta(hours=2)
scrape_session = requests_cache.core.CachedSession(expire_after=expire_after, backend='memory', timeout=5)
scrape_session.cache.remove_old_entries(datetime.utcnow() - expire_after)
scrape_session.headers.update({'User-Agent': USER_AGENT})

session = None

aiohttp_timeout = aiohttp.ClientTimeout(total=30, connect=None,
                                        sock_connect=None, sock_read=None)


async def get_with_max_size(url, max_bytes):
    async with request("GET", url, headers={"User-Agent": USER_AGENT}, timeout=aiohttp_timeout) as response:
        response.raise_for_status()
        if response.status != 200:  # raise for status is not working with 3xx error
            raise HTTPError(f"Http error with status code {response.status}")

        if response.headers.get("Content-Length") and int(response.headers.get("Content-Length")) > max_bytes:
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


async def fixup_item(item, publisher):
    out_item = {}
    if 'category' in publisher:
        out_item['category'] = publisher['category']
    if 'updated' in item:
        out_item['publish_time'] = dateparser.parse(item['updated'])
    elif 'published' in item:
        out_item['publish_time'] = dateparser.parse(item['published'])
    else:
        return None  # skip (no update field)
    if out_item['publish_time'] == None:
        return None  # skip (no publish time)
    if out_item['publish_time'].tzinfo == None:
        TZ.localize(out_item['publish_time'])
    out_item['publish_time'] = out_item['publish_time'].astimezone(pytz.utc)
    if not 'link' in item:
        if 'url' in item:
            item['link'] = item['url']
        else:
            return None  # skip (can't find link)

    # check if the article belongs to allowed domains
    if item.get('link'):
        if not publisher.get('destination_domains'):
            return None

        if (urlparse(item['link']).hostname or '') not in publisher["destination_domains"]:
            return None

    # filter the offensive articles
    if profanity.contains_profanity(item.get("title")):
        return None

    try:
        out_item['url'] = unshortener.unshorten(item['link'])
    except (requests.exceptions.ConnectionError, ConnectTimeout, InvalidURL, ReadTimeout, SSLError, TooManyRedirects):
        return None  # skip (unshortener failed)
    except Exception as e:
        logger.error(f"unshortener failed [{publisher}]: {e}")
        return None  # skip (unshortener failed)

    # image determination
    if 'media_thumbnail' in item and 'url' in item['media_thumbnail'][0]:
        out_item['img'] = item['media_thumbnail'][0]['url']
    elif 'media_content' in item and len(item['media_content']) > 0 and 'url' in item['media_content'][0]:
        out_item['img'] = item['media_content'][0]['url']
    elif 'summary' in item and BS(item['summary'], features="html.parser").find_all('img'):
        result = BS(item['summary'], features="html.parser").find_all('img')
        if 'src' in result[0]:
            out_item['img'] = BS(item['summary'], features="html.parser").find_all('img')[0]['src']
        else:
            out_item['img'] = ""
    elif 'urlToImage' in item:
        out_item['img'] = item['urlToImage']
    elif 'image' in item:
        out_item['img'] = item['image']
    elif 'content' in item and item['content'] and item['content'][0]['type'] == 'text/html' and BS(
            item['content'][0]['value'], features="html.parser").find_all('img'):
        r = BS(item['content'][0]['value'], features="html.parser").find_all('img')[0]
        if 'img' in r:
            out_item['img'] = BS(item['content'][0]['value'], features="html.parser").find_all('img')[0]['src']
        else:
            out_item['img'] = ""
    else:
        out_item['img'] = ""
    if not 'title' in item:
        # No title. Skip.
        return None

    out_item['title'] = BS(item['title'], features="html.parser").get_text()

    # add some fields
    if 'description' in item and item['description']:
        out_item['description'] = BS(item['description'], features="html.parser").get_text()
    else:
        out_item['description'] = ""
    out_item['content_type'] = publisher['content_type']
    if out_item['content_type'] == 'audio':
        out_item['enclosures'] = item['enclosures']
    if out_item['content_type'] == 'product':
        out_item['offers_category'] = item['category']
    out_item['publisher_id'] = publisher['publisher_id']
    out_item['publisher_name'] = publisher['publisher_name']
    out_item['creative_instance_id'] = publisher['creative_instance_id']
    out_item['description'] = out_item['description'][:500]

    # weird hack put in place just for demo
    if 'filter_images' in publisher:
        if publisher['filter_images'] == True:
            out_item['img'] = ""

    return out_item


async def check_images_in_item(item, publishers):
    if item['img']:
        try:
            parsed = urlparse(item['img'])
            if not parsed.scheme:
                parsed = parsed._replace(scheme='https')
                url = urlunparse(parsed)
            else:
                url = item['img']
        except Exception as e:
            logger.error(f"Can't parse image [{item['img']}]: {e}")
            item['img'] = ""
        try:
            result = scrape_session.head(url, allow_redirects=True)
            if not result.status_code == 200:
                item['img'] = ""
            else:
                item['img'] = url
        except SSLError:
            item['img'] = ""
        except:
            item['img'] = ""
    if item['img'] == "" or publishers[item['publisher_id']]['og_images'] == True:
        # if we came out of this without an image, lets try to get it from opengraph
        try:
            page = metadata_parser.MetadataParser(url=item['url'], requests_session=scrape_session,
                                                  support_malformed=True,
                                                  search_head_only=True, strategy=['page', 'meta', 'og', 'dc'],
                                                  requests_timeout=5)
            item['img'] = page.get_metadata_link('image')
        except metadata_parser.NotParsableFetchError as e:
            if e.code and e.code not in (403, 429, 500, 502, 503):
                logger.error(f"Error parsing [{item['url']}]: {e}")
        except (UnicodeDecodeError, metadata_parser.NotParsable) as e:
            logger.error(f"Error parsing: {item['url']} -- {e}")
        if item['img'] == None:
            item['img'] = ""

    if not item["img"] == "":
        parsed_img_url = urlparse(item['img'])

        if len(parsed_img_url.path) >= 4:
            item['img'] = urlunparse(parsed_img_url._replace(scheme='https'))
        else:
            item['img'] = ""

    item['padded_img'] = item["img"]
    return item


async def process_image(item):
    if item['img'] != '':
        try:
            cache_fn = await im_proc.cache_image(item['img'])
        except Exception as e:
            cache_fn = None
            logger.error(f"im_proc.cache_image failed [{item['img']}]: {e}")
        if cache_fn:
            if cache_fn.startswith("https"):
                item['padded_img'] = cache_fn
            else:
                item['padded_img'] = "%s/brave-today/cache/%s" % (config.PCDN_URL_BASE, cache_fn) + ".pad"
        else:
            item['img'] = ""
            item['padded_img'] = ""
    return item


async def check_images(items, publishers):
    out_items = []
    result = []
    async with Pool(config.CONCURRENCY, loop_initializer=uvloop.new_event_loop, childconcurrency=8) as pool:
        async for item in pool.map(partial(check_images_in_item, publishers=publishers), items):
            out_items.append(item)

    async with Pool(config.CONCURRENCY, loop_initializer=uvloop.new_event_loop, childconcurrency=8) as pool:
        async for item in pool.map(process_image, out_items):
            result.append(item)

    return result


async def fixup_entries(entry):
    """This function tends to be used more for fixups that require the whole feed like dedupe."""

    url_dedupe = {}
    now_utc = datetime.now().replace(tzinfo=pytz.utc)

    # urlencoding url because sometimes downstream things break
    url_hash = hashlib.sha256(entry['url'].encode('utf-8')).hexdigest()
    parts = urlparse(entry['url'])
    parts = parts._replace(path=quote(parts.path))
    encoded_url = urlunparse(parts)

    if entry['content_type'] != 'product':
        if entry['publish_time'] > now_utc or entry['publish_time'] < (now_utc - timedelta(days=60)):
            if entry['content_type'] != 'product':
                return None  # skip (newer than now() or older than 1 month)

    if encoded_url in url_dedupe:
        return None  # skip

    entry['publish_time'] = entry['publish_time'].strftime('%Y-%m-%d %H:%M:%S')

    if 'date_live_from' in entry:
        entry['date_live_from'] = entry['date_live_from'].strftime('%Y-%m-%d %H:%M:%S')

    if 'date_live_to' in entry:
        entry['date_live_to'] = entry['date_live_to'].strftime('%Y-%m-%d %H:%M:%S')

    entry['title'] = html.unescape(entry['title'])
    entry['url'] = encoded_url
    entry['url_hash'] = url_hash
    url_dedupe[encoded_url] = True

    return entry


async def scrub_html(item):
    """Scrubbing HTML of all entries that will be written to feed."""

    for key in item:
        if item[key]:
            item[key] = bleach.clean(item[key], strip=True)
            item[key] = item[key].replace('&amp;', '&')  # workaround limitation in bleach

    return item


class FeedProcessor:
    def __init__(self, publishers, output_path):
        self.report = defaultdict(dict)  # holds reports and stats of all actions
        self.feeds = defaultdict(dict)
        self.publishers = publishers
        self.output_path = output_path

    if not os.path.isdir("feed"):
        os.mkdir("feed")

    async def download_feeds(self):
        feeds_cache = {}
        logger.info(f"Downloading feeds... {len(self.publishers)}")

        async with Pool(config.CONCURRENCY, loop_initializer=uvloop.new_event_loop,
                        queuecount=4, childconcurrency=80) as pool:
        # async with Pool(config.CONCURRENCY) as pool:
            async for result in pool.map(download_feed, [self.publishers[key]["url"] for key in self.publishers]):
                if not result:
                    continue
                self.report["feed_stats"][result["key"]] = result["report"]
                feeds_cache[result["key"]] = result["feed_cache"]
                self.feeds[self.publishers[result["key"]]["publisher_id"]] = self.publishers[result["key"]]

        return feeds_cache

    async def get_rss(self):
        entries = []
        feed_cache = await self.download_feeds()

        logger.info(f"Fixing up and extracting the data for the items in {len(feed_cache)} feeds...")
        for key in feed_cache:
            async with Pool(config.CONCURRENCY, loop_initializer=uvloop.new_event_loop, childconcurrency=8) as pool:
                async for out_item in pool.map(partial(fixup_item, publisher=self.publishers[key]),
                                               feed_cache[key]['entries'][:self.publishers[key]['max_entries']]):
                    if out_item:
                        entries.append(out_item)
                    self.report['feed_stats'][key]['size_after_insert'] += 1
        return entries

    async def score_entries(self, entries):
        out_entries = []
        variety_by_source = {}
        for entry in entries:
            seconds_ago = (datetime.utcnow() - dateparser.parse(entry['publish_time'])).total_seconds()
            recency = math.log(seconds_ago) if seconds_ago > 0 else 0.1
            if entry['publisher_id'] in variety_by_source:
                last_variety = variety_by_source[entry['publisher_id']]
            else:
                last_variety = 1.0
            variety = last_variety * 2.0
            score = recency * variety
            entry['score'] = score
            out_entries.append(entry)
            variety_by_source[entry['publisher_id']] = variety
        return out_entries

    async def aggregate_rss(self):
        entries = []
        fixed_entries = []
        cleaned_entries = []
        entries += await self.get_rss()

        logger.info(f"Fixing up {len(entries)} feed articles...")
        async with Pool(config.CONCURRENCY, loop_initializer=uvloop.new_event_loop, childconcurrency=8) as pool:
            async for out_item in pool.map(fixup_entries, entries):
                if out_item:
                    fixed_entries.append(out_item)

        logger.info(f"Getting images for {len(fixed_entries)} items...")
        fixed_entries = await check_images(fixed_entries, self.feeds)

        logger.info(f"Scrubbing {len(fixed_entries)} items...")
        async with Pool(config.CONCURRENCY, loop_initializer=uvloop.new_event_loop, childconcurrency=8) as pool:
            async for out_item in pool.map(scrub_html, fixed_entries):
                if out_item:
                    cleaned_entries.append(out_item)

        logger.info(f"Adding Score to {len(cleaned_entries)} items...")
        scored_entries = await self.score_entries(cleaned_entries)

        logger.info(f"Sorting for {len(scored_entries)} items...")
        sorted_entries = sorted(scored_entries, key=lambda entry: entry["publish_time"])
        sorted_entries.reverse()  # for most recent entries first

        return sorted_entries

    async def aggregate(self):
        with open(self.output_path, 'wb') as f:
            feeds = await self.aggregate_rss()
            f.write(orjson.dumps(feeds))

    async def aggregate_shards(self):
        by_category = {}
        feeds = await self.aggregate_rss()
        for item in feeds:
            if not item['category'] in by_category:
                by_category[item['category']] = [item]
            else:
                by_category[item['category']].append(item)
        for key in by_category:
            with open("feed/category/%s.json" % (key), 'w') as f:
                f.write(json.dumps(by_category[key]))


if __name__ == "__main__":
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
            upload_file(f"feed/{category}.json", config.PUB_S3_BUCKET,
                        f"brave-today/{category}{config.SOURCES_FILE.replace('sources', '')}.json")
            # Temporarily upload also with incorrect filename as a stopgap for
            # https://github.com/brave/brave-browser/issues/20114
            # Can be removed once fixed in the brave-core client for all Desktop users.
            upload_file(f"feed/{category}.json", config.PUB_S3_BUCKET,
                        f"brave-today/{category}{config.SOURCES_FILE.replace('sources', '')}json")

    with open("report.json", 'wb') as f:
        f.write(orjson.dumps(fp.report))
