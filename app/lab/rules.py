"""实验室读数聚合规则版本注册表。

聚合规则以不可变的 RuleSet 描述并登记在 RULE_SETS 中，每次重算都记录
所使用的规则版本；新增规则只允许追加新版本，不允许修改历史版本，从而
保证历史重算结果可以被原样复现。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Anomaly:
    """单条读数触发的一条异常解释。"""

    rule_code: str
    detail: str
    observed: float | None
    threshold: float | None


@dataclass(frozen=True)
class RuleSet:
    """一个版本的聚合规则参数。"""

    version: str
    temp_min_c: float
    temp_max_c: float
    runtime_cap_seconds: float
    peak_excludes_flagged: bool
    description: str


RULE_SETS: dict[str, RuleSet] = {
    "v1": RuleSet(
        version="v1",
        temp_min_c=-80.0,
        temp_max_c=250.0,
        runtime_cap_seconds=7200.0,
        peak_excludes_flagged=False,
        description="初始规则：温度量程 -80~250°C，单次运行时长上限 7200 秒，峰值包含源标记读数",
    ),
    "v2": RuleSet(
        version="v2",
        temp_min_c=-55.0,
        temp_max_c=175.0,
        runtime_cap_seconds=3600.0,
        peak_excludes_flagged=True,
        description="收紧规则：温度量程 -55~175°C，单次运行时长上限 3600 秒，峰值剔除源标记读数",
    ),
}

DEFAULT_RULE_VERSION = "v2"


def evaluate_reading(reading: dict[str, Any], rules: RuleSet) -> tuple[list[Anomaly], float, bool]:
    """按规则评估一条当前读数。

    返回 (异常列表, 计入有效运行时长的秒数, 是否参与峰值温度统计)。
    温度超量程的读数视为传感器失效：不计入有效时长，也不参与峰值。
    """

    anomalies: list[Anomaly] = []
    temperature = float(reading["temperature_c"])
    runtime = float(reading["runtime_seconds"])
    temp_in_range = rules.temp_min_c <= temperature <= rules.temp_max_c
    if not temp_in_range:
        bound = rules.temp_min_c if temperature < rules.temp_min_c else rules.temp_max_c
        anomalies.append(
            Anomaly(
                "temperature_out_of_range",
                f"温度 {temperature}°C 超出有效量程 [{rules.temp_min_c}, {rules.temp_max_c}]°C，该读数不计入有效时长与峰值",
                temperature,
                bound,
            )
        )
    effective = min(runtime, rules.runtime_cap_seconds) if temp_in_range else 0.0
    if runtime > rules.runtime_cap_seconds:
        anomalies.append(
            Anomaly(
                "runtime_exceeds_cap",
                f"运行时长 {runtime}s 超过单次上限 {rules.runtime_cap_seconds}s，按上限计入有效时长",
                runtime,
                rules.runtime_cap_seconds,
            )
        )
    flagged = int(reading["anomaly_flag"]) == 1
    if flagged:
        anomalies.append(Anomaly("source_flagged", "来源批次将该读数标记为异常", None, None))
    counts_for_peak = temp_in_range and not (flagged and rules.peak_excludes_flagged)
    return anomalies, effective, counts_for_peak


def aggregate_group(readings: list[dict[str, Any]], rules: RuleSet) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """聚合同一 (载荷, 循环阶段, 模型版本) 分组内的当前读数。

    返回 (聚合指标, 异常解释列表)。异常次数按“触发至少一条规则的读数数量”统计。
    """

    peak: float | None = None
    effective = 0.0
    anomaly_readings = 0
    explanations: list[dict[str, Any]] = []
    recorded: list[str] = []
    for reading in readings:
        anomalies, effective_seconds, counts_for_peak = evaluate_reading(reading, rules)
        if anomalies:
            anomaly_readings += 1
        effective += effective_seconds
        if counts_for_peak:
            temperature = float(reading["temperature_c"])
            peak = temperature if peak is None else max(peak, temperature)
        recorded.append(str(reading["recorded_at"]))
        for anomaly in anomalies:
            explanations.append(
                {
                    "reading_id": reading["id"],
                    "payload": reading["payload"],
                    "cycle_phase": reading["cycle_phase"],
                    "model_version": reading["model_version"],
                    "recorded_at": reading["recorded_at"],
                    "rule_code": anomaly.rule_code,
                    "detail": anomaly.detail,
                    "observed_value": anomaly.observed,
                    "threshold_value": anomaly.threshold,
                }
            )
    metrics = {
        "reading_count": len(readings),
        "peak_temperature_c": round(peak, 6) if peak is not None else None,
        "effective_runtime_seconds": round(effective, 6),
        "anomaly_count": anomaly_readings,
        "first_recorded_at": min(recorded),
        "last_recorded_at": max(recorded),
    }
    return metrics, explanations
