from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

KNOWN_FLAGS = ("TEMP_ABOVE_THRESHOLD",)


class RuleConfig(BaseModel):
    """聚合与校验规则版本配置。"""

    model_config = ConfigDict(extra="forbid")

    valid_phases: list[str] = Field(default_factory=lambda: ["ramp_up", "soak", "ramp_down", "cooldown"], min_length=1)
    temperature_units: list[str] = Field(default_factory=lambda: ["C", "F", "K"], min_length=1)
    duration_units: list[str] = Field(default_factory=lambda: ["s", "min", "h"], min_length=1)
    min_temperature_c: float = -80.0
    max_temperature_c: float = 400.0
    max_run_seconds: float = Field(default=172800.0, gt=0)
    anomaly_temperature_c: float = 180.0
    max_anomaly_count: int = Field(default=1000, ge=0)
    effective_exclude_flags: list[str] = Field(default_factory=lambda: ["TEMP_ABOVE_THRESHOLD"])

    @model_validator(mode="after")
    def check_bounds(self) -> "RuleConfig":
        if self.min_temperature_c >= self.max_temperature_c:
            raise ValueError("min_temperature_c 必须小于 max_temperature_c")
        unknown = set(self.effective_exclude_flags) - set(KNOWN_FLAGS)
        if unknown:
            raise ValueError(f"未知异常标记: {sorted(unknown)}")
        return self


class RuleSetCreate(BaseModel):
    code: str = Field(min_length=2, max_length=60, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    version: str = Field(min_length=1, max_length=40)
    config: RuleConfig = Field(default_factory=RuleConfig)
    created_by: str = Field(default="system", min_length=1, max_length=80)


class ImportRequest(BaseModel):
    """批次导入请求：JSON 行数组或 CSV 文本，二者由 format 决定。"""

    batch_key: str = Field(min_length=3, max_length=120)
    source: str = Field(min_length=1, max_length=80)
    format: Literal["json", "csv"] = "json"
    rows: list[Any] | None = Field(default=None, max_length=10000)
    content: str | None = Field(default=None, max_length=2_000_000)
    rule_code: str | None = Field(default=None, max_length=60)
    rule_version: str | None = Field(default=None, max_length=40)

    @model_validator(mode="after")
    def check_payload(self) -> "ImportRequest":
        if self.format == "json" and self.rows is None:
            raise ValueError("JSON 导入必须提供 rows")
        if self.format == "csv" and not self.content:
            raise ValueError("CSV 导入必须提供 content")
        return self


class GroupKey(BaseModel):
    payload_id: str = Field(min_length=1, max_length=80)
    cycle_phase: str = Field(min_length=1, max_length=60)
    model_version: str = Field(min_length=1, max_length=60)


class RecomputeRequest(BaseModel):
    rule_code: str | None = Field(default=None, max_length=60)
    rule_version: str | None = Field(default=None, max_length=40)
    groups: list[GroupKey] | None = Field(default=None, max_length=500)
    requested_by: str = Field(default="system", min_length=1, max_length=80)
