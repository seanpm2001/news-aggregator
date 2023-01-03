from __future__ import annotations

import logging
from multiprocessing import cpu_count
from pathlib import Path
from typing import Any, Optional

import structlog
from pydantic import BaseSettings, Field

logger = structlog.getLogger(__name__)
CONFIG: Optional[Configuration] = None


class Configuration(BaseSettings):
    """
    Configuration manager.

    Config values should be added as attributes:

        class Configuration(BaseConfig):
            my_required_config_value: int
            my_optional_config_value: Optional[str]

    Config values will be automatically read from environment
    variables with the same name as the attribute (case-insensitive),
    e.g. the config value my_required_config_value will be read from
    the environment variable MY_REQUIRED_CONFIG_VALUE.

    If a required config value isn't found in the environment, an
    exception will be raised when the Configuration object is
    instantiated.

    For setting dynamic defaults, a Pydantic validator can be used:

        from pydantic import Field, validator

        class Configuration(BaseConfig):
            my_dynamic_config_value: str = Field(None)  # Need to use Field(None) to appease mypy

            @validator("my_dynamic_config_value", pre=True, always=True)
            def get_my_value(cls: Configuration, found_value: Optional[str]) -> str:
                return found_value or call_some_function()
    """

    user_agent: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    )

    output_feed_path: Path = Field(default=Path(__file__).parent / "output/feed")
    output_path: Path = Field(default=Path(__file__).parent / "output")
    wasm_thumbnail_path: Path = Field(
        default=Path(__file__).parent / "wasm_thumbnail.wasm"
    )
    img_cache_path: Path = Field(
        default=Path(__file__).parent / "output/feed" / "cache"
    )

    # Set the number of processes to spawn for all multiprocessing tasks.
    concurrency = max(1, cpu_count())
    thread_pool_size = max(1, cpu_count() * 10)

    # Disable uploads and downloads to S3. Useful when running locally or in CI.
    no_upload: Optional[str] = None
    no_download: Optional[str] = None

    pcdn_url_base: str = Field(default="https://pcdn.brave.software")

    # Canonical ID of the private S3 bucket
    priv_s3_bucket: str = Field(default="brave-private-cdn-development")
    private_cdn_canonical_id: str = ""
    private_cdn_cloudfront_canonical_id: str = ""

    # Canonical ID of the public S3 bucket
    pub_s3_bucket: str = Field(default="brave-today-cdn-development")
    brave_today_canonical_id: str = ""
    brave_today_cloudfront_canonical_id: str = ""

    sources_file: Path = Field(default="sources")
    sources_dir: Path = Field(default=Path(__file__).parent / "sources")
    global_sources_file: Path = Field(default="sources.global.json")
    favicon_lookup_file: Path = Field(default="favicon_lookup.json")
    cover_info_lookup_file: Path = Field(default="cover_info_lookup.json")
    cover_info_cache_dir: Path = Field(default="cover_info_cache")
    tests_dir: Path = Field(default="tests")

    sentry_url: str = ""

    log_level = logging.INFO

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
    )

    if sentry_url:
        import sentry_sdk

        sentry_sdk.init(dsn=sentry_url, traces_sample_rate=0)


def get_config() -> Configuration:
    global CONFIG
    if CONFIG is None:
        CONFIG = Configuration()

        # Creating the tmp dir
        if not CONFIG.img_cache_path.exists():
            CONFIG.img_cache_path.mkdir(parents=True, exist_ok=True)
    return CONFIG


def set_config(key: str, value: Any) -> None:
    """For testing overrides"""
    config = get_config()
    logger.warning("overriding config value", key=key, value=value)
    setattr(config, key, value)
