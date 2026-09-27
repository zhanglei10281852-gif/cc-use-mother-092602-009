from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ImportRequest(BaseModel):
    source: str = Field(min_length=1, max_length=120)
    batch_key: str = Field(min_length=1, max_length=160)
    format: Literal["csv", "json"]
    content: str = Field(min_length=1, max_length=5_000_000)


class RecomputeRequest(BaseModel):
    rule_version: str | None = Field(default=None, min_length=1, max_length=40)
    actor: str = Field(default="system", min_length=1, max_length=120)
