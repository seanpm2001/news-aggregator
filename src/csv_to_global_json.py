import csv
import re

import structlog
from orjson import orjson
from pydantic import ValidationError

from config import get_config
from lib.utils import get_cover_infos_lookup, get_favicons_lookup, upload_file
from models.publisher import LocaleModel, PublisherGlobal

config = get_config()
logger = structlog.getLogger(__name__)

locales_finder = re.compile(r"sources\.(.*)\.csv")

favicons_lookup = get_favicons_lookup()
cover_infos_lookup = get_cover_infos_lookup()

publisher_include_keys = {
    "enabled": True,
    "publisher_name": True,
    "category": True,
    "site_url": True,
    "feed_url": True,
    "favicon_url": True,
    "cover_info": True,
    "score": True,
    "destination_domains": True,
    "locales": True,
    "publisher_id": True,
}


def get_publisher_by_id(
    publishers: list[PublisherGlobal], publisher_id: str
) -> PublisherGlobal:
    publisher = [
        publisher for publisher in publishers if publisher.publisher_id == publisher_id
    ]
    return publisher[0]


def parse_global_publishers():
    publishers: list[PublisherGlobal] = []
    publishers_ids = []
    source_files = config.sources_dir.glob("sources.*_*.csv")
    for source_file in source_files:
        locale = locales_finder.findall(source_file.name)[0]
        with open(source_file, "r") as publisher_file_pointer:
            publisher_reader = csv.DictReader(publisher_file_pointer)
            for data in publisher_reader:
                try:
                    publisher: PublisherGlobal = PublisherGlobal(**data)

                    if publisher.publisher_id not in publishers_ids:
                        publisher.favicon_url = favicons_lookup.get(
                            publisher.site_url, None
                        )
                        publisher.cover_info = cover_infos_lookup.get(
                            publisher.site_url,
                            {"cover_url": None, "background_color": None},
                        )

                        locale_builder = LocaleModel(**data)
                        locale_builder.locale = locale
                        publisher.locales.append(locale_builder)

                        publishers.append(publisher)

                    if publisher.publisher_id in publishers_ids:
                        existing_publisher = get_publisher_by_id(
                            publishers, publisher.publisher_id
                        )
                        if existing_publisher:
                            locale_builder = LocaleModel(**data)
                            locale_builder.locale = locale
                            existing_publisher.locales.append(locale_builder)

                    publishers_ids.append(publisher.publisher_id)

                except ValidationError as e:
                    logger.info(f"{e} on {data}")

    publishers_data_as_list = [
        x.dict(include=publisher_include_keys) for x in publishers
    ]

    publishers_data_as_list = sorted(
        publishers_data_as_list, key=lambda x: x["publisher_name"]
    )

    with open(f"{config.output_path / config.global_sources_file}", "wb") as f:
        f.write(orjson.dumps(publishers_data_as_list))

    if not config.no_upload:
        upload_file(
            config.output_path / config.global_sources_file,
            config.pub_s3_bucket,
            f"{config.global_sources_file}.json",
        )


if __name__ == "__main__":
    parse_global_publishers()
