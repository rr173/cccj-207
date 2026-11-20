# 本地数据质量审计（Audit）策略与报告说明

审计功能对**已确认入仓**的结果事实（`messages` + `results` 两张事实表）按调用者
给定的两个半开时间窗做可复跑的形态/管道质量比较。它只描述数据形态与管道质量，
**不判断检验结果的临床含义**；拒绝或未确认的 `inbox` 记录永远不会被计入事实。

入口：

```bash
python -m hl7recv audit --db PATH --policy POLICY.json \
    [--baseline-start ISO --baseline-end ISO \
     --current-start  ISO --current-end  ISO] \
    [--output REPORT.json]   # 缺省或 '-' 输出到 stdout
```

审计以只读 URI（`mode=ro`）打开数据库，不执行恢复、不写任何表。

## 1. 策略文件（版本化 JSON）

当前唯一支持的版本是 `"policy_version": 1`。任何字段非法、版本未知或规则与指标
类型不匹配都会在访问数据库**之前**报错退出（退出码 4），无效策略不可能产出
“零异常”报告。

```json
{
  "policy_version": 1,
  "name": "可选的人类可读名称，会原样进入报告",
  "selection": {
    "senders": ["APP^FAC", "..."],
    "test_codes": ["GLU", "..."]
  },
  "windows": {
    "baseline": {"start": "...", "end": "..."},
    "current":  {"start": "...", "end": "..."}
  },
  "limits": {
    "max_samples_per_metric": 20,
    "text_length_buckets": [1, 4, 8, 16, 32, 64, 128, 256]
  },
  "metrics": [ { "metric_id": "...", "type": "...", "rules": [] } ]
}
```

### 1.1 选择条件 `selection`

| 字段 | 必填 | 说明 |
|---|---|---|
| `senders` | 是 | 非空、去重的发送方列表，取值为 `MSH-3^MSH-4` 原文 |
| `test_codes` | 否 | 缺省/`null` 表示不过滤；给出时为非空去重的 OBR-4 代码列表 |

### 1.2 时间窗 `windows`

* 可在策略中省略，改用 CLI 的四个 `--*-start/--*-end` 参数（必须四个一起给）。
* 边界均为 **`[start, end)`**，作用在 `messages.received_at` 上。
* 边界文本必须是带显式 UTC 偏移的 ISO-8601（如 `Z`、`+00:00`），朴素时间戳被
  拒绝，以免依赖宿主机时区；解析后统一规范化为毫秒 UTC，规范化结果与调用者
  原文同时进入报告。
* 入仓时间戳本身就是毫秒 UTC，规范化后的字符串比较与时间顺序一致，窗口谓词是
  纯字符串比较：`received_at >= start_canonical AND received_at < end_canonical`。
* 毫秒以下的更细边界按毫秒**截断**（不四舍五入），该口径在报告中可复核。

### 1.3 限制 `limits`

| 字段 | 默认 | 说明 |
|---|---|---|
| `max_samples_per_metric` | 20 | 每个指标证据样例的硬上限，超出只计数不保留 |
| `text_length_buckets` | `[1,4,8,16,32,64,128,256]` | 文本长度直方图上界，严格递增正整数；最后一个分桶之后是开放区间 `(最大上界, inf)` |

## 2. 指标目录与规则

每个指标需要唯一的 `metric_id`；`rules` 中同一规则名只能出现一次，每条规则带
`severity`（`warning` / `failure`）与数值 `parameter`。规则与指标类型不匹配会被
拒绝。

### 2.1 `message_count` / `result_count`（体量）

* 口径：窗内匹配选择条件的已提交消息数 / 其结果行数。零是合法的可计算值。
* 规则：
  * `count_drop_gte`（参数：绝对下降量，基线-当前 ≥ 参数即命中）
  * `current_to_baseline_ratio_below`（参数：比率，当前/基线 < 参数；基线为 0 不命中）
  * `min_sample_size`（当前窗样本低于参数 → 指标 `not_computable`）

### 2.2 `field_nonnull_rate`（字段非空率）

* 必须给 `field`，取值为结果事实列名。必填列：`sender, control_id, test_code,
  test_system, obs_code, obs_system, value_type, value_text, rstatus, obr_index,
  obx_index`；可选列：`patient_authority, patient_id, visit_id, placer_order,
  filler_order, test_name, obs_name, sub_id, units, ref_range, abnormal,
  observed_at, reported_at`。报告标注字段是 required 还是 optional。
* 非空定义：既不是 SQL `NULL` 也不是空串 `''`（空串与缺失在入仓时就严格区分）。
* 规则：`nonnull_rate_below`（当前窗比率 < 参数，参数 ∈ [0,1]）、
  `nonnull_rate_drop_gte`（基线-当前下降 ≥ 参数）、`min_sample_size`。
* 全空列得到比率 0.0 的**可计算**结果；窗口内没有结果行才是 `not_computable`。

### 2.3 `cardinality`（基数）

* 必须给 `dimension`：`sender` / `test_code` / `obx2_type`，即对应列的
  `DISTINCT` 计数；报告附每个取值的行数与首次出现来源坐标。
* 规则：`cardinality_drop_gte`、`cardinality_ratio_below`、`min_sample_size`。

### 2.4 `text_length_distribution`（文本值长度分布）

* 仅对文本类 OBX-2（`ST/TX/FT`）统计 `value_text` 的字符长度；其他类型计入
  `excluded_rows` 并在报告中说明排除原因。
* 输出固定分桶直方图、最小/最大/平均长度，以及落在开放区间的长值有界样例。
* 规则：`fraction_over_length_gte`（当前窗超过最大桶上界的比例 ≥ 参数）、
  `max_length_gte`（当前窗最大长度 ≥ 参数）、`min_sample_size`。
* 窗内没有任何文本类行 → `not_computable`，不会伪装成零异常。

### 2.5 `numeric_quantile_distribution`（可解析数值的分位点）

* 仅对 `NM` 行尝试数值解析：`float()` 解析去空白文本，要求有限数；解析失败
  （如 `N/A`、`pending`、`<0.5`）计入 `unparseable_rows`，**原值不改写、不用于
  医学判断**。非 NM 行计入 `excluded_rows`。
* 分位点方法为 R-7 线性插值（与 numpy/Excel 默认一致），概率可通过 `quantiles`
  配置（默认 `[0.5, 0.9]`，须在 [0,1] 内且不重复）。
* 规则：`quantile_shift_gte`（任一声明分位点当前与基线的绝对偏移 ≥ 参数）、
  `unparseable_fraction_gte`（当前窗不可解析比例 ≥ 参数）、`min_sample_size`。
* 没有 NM 行或全部解析失败 → `not_computable`。

## 3. 状态、退出码与不可计算语义

| 指标状态 | 含义 |
|---|---|
| `ok` | 两窗均可计算且无规则命中 |
| `warning` | 至少一条 warning 规则命中（无 failure） |
| `failure` | 至少一条 failure 规则命中（failure 优先） |
| `not_computable` | 空窗口（形态/样本类指标）、零适用行、全空适用样本或触发 `min_sample_size`；带明确原因，不触发任何阈值规则，`diff` 为 `null`。注意体量类指标（消息数/结果行数）在任何窗口都可计算：空窗口产出合法的数值 0 与正常 `diff`，不会变成 `not_computable` |

CLI 退出码：`0` 无异常；`1` 警戒；`2` 失败；`3` 无法计算（存在 not_computable
且无更高严重级别）；`4` 用法/策略/数据库错误（不产出报告）。stdout 在成功路径
上只有报告 JSON；错误只在 stderr 输出单个 JSON 错误对象。

## 4. 报告结构（确定性 JSON）

顶层字段（键顺序由序列化器固定，`sort_keys=True`、两空格缩进、末尾单个 LF，
禁止 NaN/Infinity）：

* `report_format_version`、`report_status`、`exit_code`
* `policy_summary`：策略名、版本、来源路径、选择条件、限制、指标 ID 列表
* `windows`：两窗的原始边界、规范化边界、`[start, end)` 与时间基准说明
* `query_filters`：事实表/排除表、状态过滤、发送方/代码过滤、JOIN 方式、
  窗口谓词与实际使用的 SQL 字面量
* `snapshot`：WAL 模式、两表最大 rowid、总行数、两表稳定顺序的 SHA-256 指纹
* `metrics[]`：每个指标含 `spec`、`calculation`（口径说明）、两窗值（各自带
  窗口消息数、分母 `eligible_*`、`excluded_rows`、`unparseable_rows`）、
  `diff`（基线值/当前值/差值）、`threshold_hits`（规则、级别、参数、证据）、
  `not_computable_reasons`、`evidence`（有界样例与来源坐标）
* `summary`：各级别指标计数与两窗匹配消息数
* `scope_statement`：范围声明（仅形态/管道质量，排除 inbox，无临床解释）

### 4.1 来源坐标

每条证据样例至少包含 `sender`、`control_id`、`obr_index`、`obx_index`、
`obr_seg`、`obx_seg`，可直接用 `query results --message SENDER:CONTROL_ID`
回溯到参与计算的具体消息与结果行。样例按来源坐标固定排序，数量受
`max_samples_per_metric` 约束；`candidate_count` 与 `truncated` 标明被截断的
真实候选规模。

## 5. 快照一致性

审计打开独立的只读连接并立即开启一个事务，持有到报告组装完成。生产库运行在
WAL 模式：读不阻塞写，事务内的读视图固定在首次读时刻，之后接收服务的提交对
本报告不可见。因此同一报告的所有分子、分母、样例来自同一逻辑视图；报告内嵌
快照指纹，相同快照 + 相同策略 + 相同参数产出**字节相同**的报告。

## 6. 可复跑合成演练

```bash
python -m drills.run_audit_drill --workdir /tmp/drill-run
```

演练在运行时生成覆盖多发送方、多检验代码、空字段、文本/数值、边界时间戳、
低样本、阈值越界、无可计算样本与被拒绝 inbox 记录的合成库，运行产品审计后再
用独立参考程序（`refcheck/audit_reference.py`，不导入任何产品审计代码）逐项
核对。大型压力数据在测试运行时生成，不提交到仓库。
