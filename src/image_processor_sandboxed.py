# Copyright (c) 2023 The Brave Authors. All rights reserved.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/. */

import hashlib
import os

import boto3
import requests
import structlog
from fake_useragent import UserAgent
from wasmer import Instance, Module, Store, engine
from wasmer_compiler_cranelift import Compiler

from config import get_config
from utils import ObjectNotFound, upload_file

ua = UserAgent(browsers=["edge", "chrome", "firefox", "safari", "opera"])

config = get_config()

logger = structlog.getLogger(__name__)

boto_session = boto3.Session()
s3_client = boto_session.client("s3")
s3_resource = boto3.resource("s3")

wasm_store = Store(engine.JIT(Compiler))
wasm_module = Module(wasm_store, open(config.wasm_thumbnail_path, "rb").read())
instance = Instance(wasm_module)


def resize_and_pad_image(image_bytes, width, height, size, cache_path, quality=80):
    image_length = len(image_bytes)
    input_pointer = instance.exports.allocate(image_length)
    memory = instance.exports.memory.uint8_view(input_pointer)
    memory[0:image_length] = image_bytes

    try:
        output_pointer = instance.exports.resize_and_pad(
            input_pointer, image_length, width, height, size, quality
        )
        instance.exports.deallocate(input_pointer, image_length)

        memory = instance.exports.memory.uint8_view(output_pointer)
        out_bytes = bytes(memory[:size])

        instance.exports.deallocate(output_pointer, size)

        with open(str(cache_path), "wb+") as out_image:
            out_image.write(out_bytes)

        return True
    except RuntimeError:
        logger.info(
            "resize_and_pad() hit a RuntimeError (length=%s, width=%s, height=%s, size=%s): %s.failed",
            image_length,
            width,
            height,
            size,
            cache_path,
        )

        with open(str(cache_path) + ".failed", "wb+") as out_image:
            out_image.write(image_bytes)

        return False


def get_with_max_size(url, max_bytes=1000000):
    is_large = False
    response = requests.get(
        url, timeout=config.request_timeout, headers={"User-Agent": ua.random}
    )
    response.raise_for_status()
    if (
        response.headers.get("Content-Length")
        and int(response.headers.get("Content-Length")) > max_bytes
    ):
        is_large = True

    return response.content, is_large


class ImageProcessor:
    def __init__(
        self, s3_bucket=None, s3_path="brave-today/cache/{}", force_upload=False
    ):
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.force_upload = force_upload

    def cache_image(self, url):  # noqa: C901
        content = None
        cache_path = None
        cache_fn = None

        try:
            content, is_large = get_with_max_size(url)  # 1mb max
            if not is_large and not self.force_upload:
                return url

            cache_fn = f"{hashlib.sha256(url.encode('utf-8')).hexdigest()}.jpg.pad"
            cache_path = config.img_cache_path / cache_fn

            # if we have it don't do it again
            if os.path.isfile(cache_path):
                return cache_fn
            # also check if we have it on s3
            if not config.no_upload:
                try:
                    s3_resource.Object(
                        self.s3_bucket, self.s3_path.format(cache_fn)
                    ).load()
                    exists = True
                except ObjectNotFound:
                    exists = False
                if exists:
                    return cache_fn

        except requests.exceptions.ReadTimeout as e:
            logger.info(f"Image is not already uploaded {url} with {e}")
        except ValueError as e:
            logger.info(f"Image is not already uploaded {url} with {e}")
        except requests.exceptions.SSLError as e:
            logger.info(f"Image is not already uploaded {url} with {e}")
        except requests.exceptions.HTTPError as e:
            logger.info(
                f"Image is not already uploaded [{e.response.status_code}]: {url}"
            )

        if not resize_and_pad_image(content, 1168, 657, 250000, cache_path):
            logger.info(f"Failed to cache image {url}")
            return None

        if self.s3_bucket and not config.no_upload:
            upload_file(
                cache_path,
                self.s3_bucket,
                self.s3_path.format(cache_fn),
            )
        return cache_fn
