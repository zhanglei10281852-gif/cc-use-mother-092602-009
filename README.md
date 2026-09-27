# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。
- 热测试数据管线：按来源批次导入热循环与辐射测试读数（CSV/JSON），逐行校验字段与单位，坏行不阻塞好行；相同读数去重，迟到数据以修订链替换当前值且保留全部历史；按载荷、循环阶段和模型版本分组，用指定规则版本重算峰值温度、有效运行时长与异常次数；原始读数与聚合分表存放，聚合永不覆盖读数；重复批次返回相同摘要，异常解释接口可追溯到具体读数与触发的规则阈值。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

## 热测试数据管线

统一前缀 `/api/telemetry`：

- `POST /imports`：批次导入，`format=json` 传 `rows` 数组，`format=csv` 传 `content` 文本；`batch_key` 幂等，同内容重放返回相同摘要（HTTP 200），同键不同内容返回 409。摘要含接受/拒绝/去重/修订计数、受影响分组和按 `(row_index, error_code)` 稳定排序的错误报告。
- `GET /imports/{batch_key}`：按批次键取回同一份摘要。
- `GET /readings`、`GET /readings/{reading_key}/history`：查询当前原始读数（`include_superseded=true` 含历史）与单条读数的修订链。
- `POST /rules`、`GET /rules`：注册与查看规则版本（有效阶段、单位白名单、物理量程、异常阈值、有效时长排除标记）。
- `POST /recomputes`：按规则版本重算聚合，可传 `groups` 限定范围；记录本次使用的规则版本、受影响分组、读数摘要与起止时间。`GET /recomputes`、`GET /recomputes/{id}` 查看历史。
- `GET /aggregates`：当前聚合（峰值温度、有效运行时长、异常次数），可按分组与规则版本过滤；`GET /aggregates/history` 查看某分组跨重算的变化，用于评估迟到数据对历史结论的影响范围。
- `GET /anomalies/explain`：按分组解释异常构成，列出贡献读数、触发的规则阈值与是否计入有效时长。

读数行字段：`reading_key, payload_id, cycle_phase, model_version, recorded_at, temperature, temperature_unit, run_duration, duration_unit, anomaly_count`。温度支持 C/F/K 自动归一为摄氏度，时长支持 s/min/h 归一为秒；`anomaly_count` 缺省为 0。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  telemetry/       热测试读数批次导入、校验去重、迟到修订、按版本重算与异常解释
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
