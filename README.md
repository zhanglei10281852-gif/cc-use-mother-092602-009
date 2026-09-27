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

## 实验室测试数据管线

`/api/lab` 前缀下提供热循环与辐射测试读数的完整数据管线：批次导入 → 逐行校验与单位换算 → 去重与迟到修订 → 按规则版本重算聚合 → 异常解释查询。

- 批次导入：`POST /api/lab/imports` 接收 `{source, batch_key, format, content}`，`format` 为 `csv` 或 `json`。CSV 需要表头 `payload,cycle_phase,model_version,recorded_at,temperature,temperature_unit,runtime,runtime_unit`（可选 `anomaly_flag,revision,sensor_id`）；JSON 为读数数组或 `{"readings": [...]}`。
- 字段与单位校验：逐行校验必填字段、时间格式、数值范围与单位，温度支持 C/K/F，时长支持 s/min/h，统一换算为摄氏度与秒。坏行只记入错误报告（按行号与错误码稳定排序），不阻塞好行。
- 幂等批次：相同 `(source, batch_key)` 且内容一致的重复导入返回首次保存的相同摘要（状态码 200）；内容不一致返回 409。
- 去重与迟到修订：读数以 `(payload, cycle_phase, model_version, recorded_at, sensor_id)` 为自然键。相同修订号的相同内容为重复行；更高修订号成为新的当前读数（迟到修订），旧行保留且永不修改；更低或冲突的修订号记入错误报告。
- 按版本重算：聚合规则登记在 `app/lab/rules.py`（当前 v1/v2），导入后按当前规则版本自动重算，`POST /api/lab/aggregations/recompute` 可指定版本手动重算。相同规则版本加相同输入的重算是幂等的，直接复用已有运行。每次运行记录规则版本、触发方、受影响分组及每组前后指标（迟到数据对历史结论的影响范围），见 `GET /api/lab/aggregations/runs` 与 `/runs/{id}`。
- 聚合口径：按载荷、循环阶段、模型版本分组，输出峰值温度、有效运行时长（超量程读数不计、单次上限截断）与异常次数（触发至少一条规则的读数数量）。
- 异常解释：`GET /api/lab/anomalies` 返回当前（或指定）运行下每条异常的规则编码、观测值、阈值与来源批次，按分组与时间稳定排序。
- 原始保护：原始读数只插入不更新，聚合结果保存在独立的运行表中，重算永远不会覆盖原始读数；`GET /api/lab/readings` 可查看全部修订及当前标记。

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
python -m app.cli lab-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。`lab-demo` 导入一批含混合单位与坏行的热循环读数，展示导入摘要、聚合结果与异常解释。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  lab/             实验室读数批次导入、去重、迟到修订与按版本聚合
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营、实验室管线与身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
