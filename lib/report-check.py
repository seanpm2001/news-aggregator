# Copyright (c) 2023 The Brave Authors. All rights reserved.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/. */

import json
import logging
import sys

from config import get_config

config = get_config()


def check_report(report):
    success = True
    for feed in report["feed_stats"]:
        get_size = report["feed_stats"][feed]["size_after_get"]
        insert_size = report["feed_stats"][feed]["size_after_insert"]
        if insert_size > get_size:
            logging.error(
                "Logic error: we inserted %s posts but only downloaded %s.",
                insert_size,
                get_size,
            )
            success = False

        if get_size == 0:
            logging.error("Didn't get any posts from %s.", feed)
            success = False

        if insert_size == 0:
            logging.error("Didn't insert any posts from %s.", feed)
            success = False

    return success


with open(config.output_path / "report.json") as f:
    if not check_report(json.loads(f.read())):
        sys.exit(1)
