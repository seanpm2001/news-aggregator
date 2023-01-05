# Copyright (c) 2023 The Brave Authors. All rights reserved.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/. */

import json
import os

import feedparser

from config import get_config
from src import feed_processor_multi

config = get_config()
fp = feed_processor_multi.FeedProcessor()


def test_feed_processor_download():
    result = feed_processor_multi.download_feed("https://brave.com/blog/index.xml")
    assert result


def test_feed_processor_aggregate():
    with open(config.tests_dir / "test.json") as f:
        feeds = json.loads(f.read())
        fp.aggregate(feeds, config.output_feed_path / "test.json")
    assert os.stat(config.output_feed_path / "test.json").st_size != 0

    with open(config.output_feed_path / "test.json") as f:
        data = json.loads(f.read())
    assert data
    assert len(data) != 0


def test_check_images():
    data = [feedparser.parse(config.tests_dir / "test.rss")["items"][0]]
    data[0]["img"] = data[0]["media_content"][0]["url"]
    data[0]["publisher_id"] = ""
    fp = feed_processor_multi.FeedProcessor()
    fp.feeds[""] = {"og_images": False}
    assert fp.check_images(data)


def test_download_feeds():
    with open(config.tests_dir / "test.json") as f:
        data = json.loads(f.read())
    data = {
        "https://brave.com/blog/index.xml": data["https://brave.com/blog/index.xml"]
    }
    fp.report["feed_stats"] = {}
    result = fp.download_feeds(data)
    assert len(result) != 0


def test_get_rss():
    with open(config.tests_dir / "test.json") as f:
        data = json.loads(f.read())
    data = {
        "https://brave.com/blog/index.xml": data["https://brave.com/blog/index.xml"]
    }
    fp.report["feed_stats"] = {}
    result = fp.get_rss(data)
    assert len(result) != 0


def test_fixup_entries():
    with open(config.tests_dir / "test.json") as f:
        data = json.loads(f.read())
    data = {
        "https://brave.com/blog/index.xml": data["https://brave.com/blog/index.xml"]
    }
    fp.report["feed_stats"] = {}
    entries = fp.get_rss(data)
    assert len(entries) != 0

    sorted_entries = sorted(entries, key=lambda entry: entry["publish_time"])
    sorted_entries.reverse()  # for most recent entries first

    filtered_entries = fp.fixup_entries(sorted_entries)
    assert filtered_entries


def test_scrub_html():
    with open(config.tests_dir / "test.json") as f:
        data = json.loads(f.read())
    data = {
        "https://brave.com/blog/index.xml": data["https://brave.com/blog/index.xml"]
    }
    fp.report["feed_stats"] = {}
    entries = fp.get_rss(data)
    assert len(entries) != 0

    sorted_entries = sorted(entries, key=lambda entry: entry["publish_time"])
    sorted_entries.reverse()  # for most recent entries first

    filtered_entries = fp.fixup_entries(sorted_entries)
    filtered_entries = fp.scrub_html(filtered_entries)

    assert filtered_entries


def test_score_entries():
    with open(config.tests_dir / "test.json") as f:
        data = json.loads(f.read())
    data = {
        "https://brave.com/blog/index.xml": data["https://brave.com/blog/index.xml"]
    }
    fp.report["feed_stats"] = {}
    entries = fp.get_rss(data)
    assert len(entries) != 0

    sorted_entries = sorted(entries, key=lambda entry: entry["publish_time"])
    sorted_entries.reverse()  # for most recent entries first

    filtered_entries = fp.fixup_entries(sorted_entries)
    filtered_entries = fp.scrub_html(filtered_entries)
    filtered_entries = fp.score_entries(filtered_entries)

    assert filtered_entries
