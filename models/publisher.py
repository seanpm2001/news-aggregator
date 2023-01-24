# Copyright (c) 2023 The Brave Authors. All rights reserved.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://mozilla.org/MPL/2.0/. */

import hashlib
from typing import Any, Dict, List, Optional

import bleach
from pydantic import Field, HttpUrl, root_validator, validator

from models.base import Model


class PublisherBase(Model):
    enabled: bool = Field(alias="Status")
    publisher_name: str = Field(alias="Title")
    category: str = Field(alias="Category")
    site_url: HttpUrl = Field(alias="Domain")
    feed_url: HttpUrl = Field(alias="Feed")
    favicon_url: Optional[HttpUrl] = Field(default=None)
    cover_info: Optional[dict] = Field(
        default={"cover_url": Optional[HttpUrl], "background_color": Optional[str]}
    )
    score: float = Field(default=0, alias="Score")
    destination_domains: list[str] = Field(alias="Destination Domains")
    original_feed: Optional[str] = Field(default=None, alias="Original_Feed")
    og_images: bool = Field(default=None, alias="OG-Images")
    max_entries: int = Field(default=20)
    creative_instance_id: str = Field(default="", alias="Creative Instance ID")
    content_type: str = Field(default="article", alias="Content Type")
    publisher_id: str = ""

    @root_validator(pre=True)
    def bleach_each_value(cls, values: dict) -> Dict[str, Any]:
        for k, v in values.items():
            if isinstance(v, str):
                values[k] = bleach.clean(v, strip=True).replace(
                    "&amp;", "&"
                )  # workaround limitation in bleach

        return values

    @validator("enabled", pre=True, always=True)
    def fix_enabled_format(cls, v: str) -> bool:
        return v == "Enabled"

    @validator("score", pre=True, always=True)
    def fix_score_format(cls, v: str) -> float:
        return v if v else 0

    @validator("og_images", pre=True, always=True)
    def fix_og_images_format(cls, v: str) -> bool:
        return v == "On"

    @validator("publisher_name", pre=True, always=True)
    def validate_publisher_name(cls, v: str) -> str:
        if not v:
            raise ValueError("must contain a value")
        return v

    @validator("destination_domains", pre=True)
    def fix_destination_domains_format(cls, v: str) -> List[str]:
        if not v:
            raise ValueError("must contain a value")
        return v.split(";")

    @validator("publisher_id", pre=True, always=True)
    def add_publisher_id(cls, v: str, values: Dict[str, Any]) -> str:
        return hashlib.sha256(
            values.get("original_feed").encode("utf-8")
            if values.get("original_feed")
            else values.get("feed_url").encode("utf-8")
        ).hexdigest()


class PublisherModel(PublisherBase):
    channels: Optional[list[str]] = Field(default=[], alias="Channels")
    rank: Optional[int] = Field(default=None, alias="Rank")

    @validator("rank", pre=True, always=True)
    def fix_rank_format(cls, v: str) -> Optional[int]:
        return int(v) if v else None

    @validator("channels", pre=True)
    def fix_channels_format(cls, v: str) -> Optional[List[str]]:
        return v.split(";") if v else []


class LocaleModel(Model):
    locale: str = ""
    channels: Optional[list[str]] = Field(default=[], alias="Channels")
    rank: Optional[int] = Field(default=None, alias="Rank")

    @validator("rank", pre=True, always=True)
    def fix_rank_format(cls, v: str) -> Optional[int]:
        return int(v) if v else None

    @validator("channels", pre=True)
    def fix_channels_format(cls, v: str) -> Optional[List[str]]:
        return v.split(";") if v else []


class PublisherGlobal(PublisherBase):
    locales: list[LocaleModel] = []
