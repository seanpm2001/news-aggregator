import glob
import logging
import mimetypes
import re
from pathlib import Path, PosixPath
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import boto3
import orjson
from botocore.exceptions import ClientError

import config

boto_session = boto3.Session()
s3_client = boto_session.client("s3")

domain_url_fixer = re.compile(r"^https://(www\.)?|^")
subst = "https://www."

config = config.get_config()


class InvalidS3Bucket(Exception):
    pass


def upload_file(file_name: PosixPath, bucket: str, object_name: Optional[str] = None):
    if object_name is None:
        object_name = file_name
    try:
        content_type = mimetypes.guess_type(file_name)[0] or "binary/octet-stream"
        if bucket == config.pub_s3_bucket:
            s3_client.upload_file(
                file_name,
                bucket,
                object_name,
                ExtraArgs={
                    "GrantRead": "id=%s" % config.brave_today_cloudfront_canonical_id,
                    "GrantFullControl": "id=%s" % config.brave_today_canonical_id,
                    "ContentType": content_type,
                },
            )
        elif bucket == config.priv_s3_bucket:
            s3_client.upload_file(
                file_name,
                bucket,
                object_name,
                ExtraArgs={
                    "GrantRead": "id=%s" % config.private_cdn_canonical_id,
                    "GrantFullControl": "id=%s"
                    % config.private_cdn_cloudfront_canonical_id,
                    "ContentType": content_type,
                },
            )
        else:
            raise InvalidS3Bucket("Attempted to upload to unknown S3 bucket.")

    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file(file_name: str, bucket: str, object_name: Optional[str] = None):
    if object_name is None:
        object_name = file_name

    try:
        if bucket == config.pub_s3_bucket:
            s3_client.download_file(bucket, object_name, file_name)
        elif bucket == config.priv_s3_bucket:
            s3_client.download_file(bucket, object_name, file_name)
        else:
            raise InvalidS3Bucket("Attempted to upload to unknown S3 bucket.")

    except ClientError as e:
        logging.error(e)
        return False
    return True


def ensure_scheme(domain):
    """Helper utility for ensuring a domain has a scheme. If none is attached
    this will use the https scheme.

    Note: this will break if domain has a non http(s) scheme.
    example.com ==> https://www.example.com
    https://example.com ==> https://www.example.com
    file://example.com ==> https://file://example.com
    """
    return domain_url_fixer.sub(subst, domain, 1)


def get_all_domains() -> List[str]:
    """Helper utility for getting all domains across all sources"""
    source_files = glob.glob("sources*.csv")
    for source_file in source_files:
        with open(source_file) as f:
            # Skip the first line, with the headers.
            lines = f.readlines()[1:]

            # The domain is the first field on the line
            yield from [line.split(",")[0].strip() for line in lines]


def uri_validator(x):
    """
    'http://www.cwi.nl:80/%7Eguido/Python.html' False
    '/data/Python.html' False
    '532' False
    u'dkakasdkjdjakdjadjfalskdjfalk' False
    'https://stackoverflow.com' True

    :param x: URL
    :return: bool
    """
    try:
        result = urlparse(x)
        return all([result.scheme, result.scheme == "https", result.netloc])
    except Exception:
        return False


def get_favicons_lookup() -> Dict[Any, Any]:
    if not config.no_download:
        download_file(
            f"{config.favicon_lookup_file}.json",
            config.pub_s3_bucket,
            f"{config.favicon_lookup_file}.json",
        )

    if Path(f"{config.favicon_lookup_file}.json").is_file():
        with open(f"{config.favicon_lookup_file}.json", "r") as f:
            favicons_lookup = orjson.loads(f.read())
            return favicons_lookup
    else:
        return {}


def get_cover_infos_lookup() -> Dict[Any, Any]:
    if not config.no_download:
        download_file(
            f"{config.cover_info_lookup_file}.json",
            config.pub_s3_bucket,
            f"{config.cover_info_lookup_file}.json",
        )

    if Path(f"{config.cover_info_lookup_file}.json").is_file():
        with open(f"{config.cover_info_lookup_file}.json", "r") as f:
            cover_infos_lookup = orjson.loads(f.read())
            return cover_infos_lookup
    else:
        return {}
