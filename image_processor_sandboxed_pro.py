import hashlib
import logging
import os
import pathlib

import aiohttp
import boto3
import botocore
import requests
from aiohttp import request
from requests import HTTPError
from wasmer import engine, Store, Module, Instance
from wasmer_compiler_cranelift import Compiler

import config
from utils import upload_file

boto_session = boto3.Session()
s3_client = boto_session.client('s3')
s3_resource = boto3.resource('s3')

aiohttp_timeout = aiohttp.ClientTimeout(total=30, connect=None,
                                        sock_connect=None, sock_read=None)

wasm_path = 'wasm_thumbnail.wasm'
wasm_store = Store(engine.JIT(Compiler))
wasm_module = Module(wasm_store, open(wasm_path, 'rb').read())


async def resize_and_pad_image(image_bytes, width, height, size, cache_path, quality=80):
    pathlib.Path(os.path.dirname(cache_path)).mkdir(parents=True, exist_ok=True)
    pid = os.fork()
    if pid == 0:
        instance = Instance(wasm_module)

        image_length = len(image_bytes)
        input_pointer = instance.exports.allocate(image_length)
        memory = instance.exports.memory.uint8_view(input_pointer)
        memory[0:image_length] = image_bytes

        try:
            output_pointer = instance.exports.resize_and_pad(input_pointer, image_length, width, height, size, quality)
        except RuntimeError as e:
            logging.warning("resize_and_pad() hit a RuntimeError (length=%s, width=%s, height=%s, size=%s): %s.failed",
                            image_length, width, height, size, cache_path)
            with open("%s.failed" % (cache_path), 'wb+') as out_image:
                out_image.write(image_bytes)

            os._exit(1)

        memory = instance.exports.memory.uint8_view(output_pointer)
        out_bytes = bytes(memory[:size])
        with open("%s.pad" % (cache_path), 'wb+') as out_image:
            out_image.write(out_bytes)

        instance.exports.deallocate(output_pointer, size)

        os._exit(0)

    pid, status = os.waitpid(pid, 0)

    if status == 0:
        return True
    return False


async def get_with_max_size(url, max_bytes=1000000):
    is_large = False
    async with request("GET", url, headers={"User-Agent": config.USER_AGENT}, timeout=aiohttp_timeout) as response:
        response.raise_for_status()
        if response.status != 200:  # raise for status is not working with 3xx error
            raise HTTPError(f"Http error with status code {response.status}")

        if response.headers.get("Content-Length") and int(response.headers.get("Content-Length")) > max_bytes:
            is_large = True

        return await response.read(), is_large


class ImageProcessor:
    def __init__(self, s3_bucket=None, s3_path='brave-today/cache/{}.pad', force_upload=False):
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.force_upload = force_upload

    async def cache_image(self, url):
        try:
            content, is_large = await get_with_max_size(url)  # 5mb max
            if not is_large and not self.force_upload:
                return url

            cache_fn = "%s.jpg" % (hashlib.sha256(url.encode('utf-8')).hexdigest())
            cache_path = "./feed/cache/%s" % cache_fn

            # if we have it don't do it again
            if os.path.isfile(cache_path):
                return cache_fn
            # also check if we have it on s3
            if not config.NO_UPLOAD:
                exists = False
                try:
                    s3_resource.Object(self.s3_bucket, self.s3_path.format(cache_fn)).load()
                    exists = True
                except ValueError as e:
                    exists = False  # make tests work
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        exists = False
                    else:
                        return None  # should retry
                if exists:
                    return cache_fn

        except requests.exceptions.ReadTimeout:
            return None
        except ValueError:
            return None  # skipping (image exceeds maximum size)
        except requests.exceptions.SSLError:
            return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code not in (403, 429, 500, 502, 503):
                logging.error("Failed to get image [%s]: %s", e.response.status_code, url)
            return None

        is_image_resized = await resize_and_pad_image(content, 1168, 657, 250000, cache_path)
        if not is_image_resized:
            logging.error("Failed to cache image %s", url)
            return None

        if self.s3_bucket and not config.NO_UPLOAD:
            upload_file("feed/cache/%s.pad" % cache_fn, self.s3_bucket, self.s3_path.format(cache_fn))
        return cache_fn
