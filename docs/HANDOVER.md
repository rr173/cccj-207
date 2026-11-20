# 本地交班对账台账（shiftledger）

数据运营交班前，手里有两份东西：派工系统给的**应收清单**，和 `hl7recv`
已经落定的 `messages` / `results`。本台账只核对**数据交付**——不读检验值、
不判断医学含义，全部使用合成资料。它是一个独立的 SQLite 库，**只读**对接
接收端，绝不写入、封锁或改造接收端行为。

## 1. 它解决什么问题

多个仪器沿用相同本地单号、同一项目可能要求分别回传两项，所以“收到了什么”
不等于“应收都齐了”。台账先判定每条回传**归属哪个工单**，再在已定归属的工单
内把观察**销到具体应收项**：

* 一条结果不能抵两项；
* 归属仍有争议的回传不能被当成已完成；
* 自动只做剩余关系里的**唯一一对一**，绝不按输入顺序、到达先后或更小 ID 猜测。

产出四类可继续处理的工作：欠收项、待裁决项、未被采用的回传，以及每一项销账
实际采用的回传证据。

## 2. 应收清单（manifest）格式

一个目录，四个 JSON/JSONL 文件。所有时间必须是**带显式时区**的 ISO-8601
（如 `2026-11-20T08:00:00+08:00` 或 `...Z`），按实际时刻比较，绝不按字符串。

### `manifest.json`

```json
{
  "shift_id": "SHIFT-1",
  "shift_start": "2026-11-20T08:00:00+08:00",
  "cutoff": "2026-11-20T11:00:00+08:00",
  "senders": ["ANALYZER-A"]
}
```

`cutoff` 可省，但结算时必须显式给出（命令行 `--cutoff` 可覆盖），且严格晚于
`shift_start`。

### `work_orders.jsonl`

每行一个工单：`work_order_id`、`patient_domain`、`patient_id`。

### `items.jsonl`

每行一个应收项：

```json
{"item_id":"WO1-X","work_order_id":"WO1","code_system":"LN","code":"X",
 "sub_id":null,"due_at":"2026-11-20T12:00:00+08:00"}
```

* `item_id` 全局稳定且唯一；同一工单允许存在识别条件完全相同的多个应收项，
  它们仍分开对账。
* `sub_id` 为 `null` 是**明确条件而非通配符**：只匹配 sub id 同样为空的观察。

### `mappings.jsonl`

每行一条外部单号映射，时间窗为半开 `[valid_from, valid_to)`：

```json
{"sender":"ANALYZER-A","number_class":"placer","local_number":"L-SHARED",
 "work_order_id":"WO1","valid_from":"...T08:00+08:00",
 "valid_to":"...T13:00+08:00"}
```

一枚本地号可同时指向多个工单，也可在不同时段交接给不同工单（号段切换）。
`number_class` 取 `placer`（申请方单号）或 `filler`（执行方单号）。

### 校验

身份、引用、时间窗在登记时整体校验：naive 时间、重复 ID、引用不存在的工单/
发送方、`valid_to <= valid_from`、截止不晚于班次起点等任一不满足，**整份清单
无效，不建立台账**，失败不留下半成品。

## 3. 归属判定（先定归属，再销项）

事实只取自参与发送方、`received_at` 落在 `[shift_start, cutoff)` 的已落定
`messages` / `results`；不从 inbox、原始消息或审计报告重建。回传组是
`(sender, control_id, obr_index)`，组内再加 `obx_index` 标识观察；**组内所有
观察只能归属同一工单**。判定严格、可区分：

| 组状态 | 含义 |
| --- | --- |
| `assigned` | 唯一候选工单（自动或人工指定） |
| `ambiguous` | 交集+患者过滤后仍剩多个候选，交运营裁决 |
| `no_patient_identity` | 观察行无患者域/ID |
| `no_order_number` | 既无 placer 也无 filler |
| `unregistered_order` | 提供的某个号查无映射，另一号不能兜底 |
| `out_of_window` | 号已登记但在 `received_at` 时刻无有效映射 |
| `mapping_conflict` | 各号有效映射集合交集为空（互相矛盾） |
| `patient_mismatch` | 候选工单的患者域/ID 与组患者严格不符 |

规则要点：对每个**实际提供**的号分别求有效映射再取交集；有一个提供的号查不到
映射，不能用另一个号兜底。映射生效与到期都以消息的 `received_at` 时刻判定。
唯一候选自动定归属；多个候选保留为 `ambiguous`，不因某工单缺项较多而替人选择。

## 4. 销账与观察去向

在已定归属工单内，观察按 `(code_system, code, sub_id)` 精确形成候选边。人工
锁定的观察与应收项先扣除，自动销账只允许剩余关系里的**唯一一对一**：该观察只
对应这一个未占用项，该项也只有这一条已归属候选观察。

还有一种不能提前结单的情况：某回传组尚未选定工单（`ambiguous`），但其观察若
归过来**可能**满足某项——这种 `potential` 边不销账，却会阻止该项自动销账；
已人工锁定的项不受此阻止。

### 应收项状态

| 状态 | 含义 |
| --- | --- |
| `auto_settled` | 唯一一对一自动销账 |
| `lock_settled` | 人工把观察锁给该项而销账 |
| `pending_decision` | 未销，存在候选竞争或待定归属阻断 |
| `pending_receipt` | 无候选，且截止时刻不晚于到期（仍待收） |
| `overdue` | 无候选，且截止时刻晚于到期（逾期未收） |

已销项再按实际到达时刻分 `on_time` / `late`。

### 观察去向（每个入选观察恰有一个）

| 去向 | 含义 |
| --- | --- |
| `adopted` | 已被某应收项采用（自动或锁定） |
| `group_pending` | 所在组归属未决 |
| `unattributable` | 组无法归属（见上表各类原因） |
| `candidate_unassigned` | 有符合的未占用项，但尚未分配 |
| `matching_items_occupied` | 符合的项都已被占用 |
| `no_matching_item` | 在所属工单内没有对应应收项 |

## 5. 人工裁决与版本

决策是**仅追加**的稳定记录，带理由、操作人与所依据的版本号。支持四种动作，
可在同一请求里组合组归属与项锁定：

* `assign_group`：为多候选组选定一个工单（必须仍是合法候选）；
* `withdraw_group`：撤回该选择，组回到 `ambiguous`；
* `lock_observation`：把已归属组的某观察锁给仍满足匹配条件的应收项；
* `withdraw_lock`：撤回锁定。

非法边、同一观察销两项、同一项被锁两次、撤回组归属后遗留无有效归属的项锁，
都会**拒绝整次变更**，不产生半版。请求必须带调用者看到的 `base_version`；
过期请求（他人已发布新版本）返回 `stale_version`，不能盖掉别人的选择。

后续结算承接仍有效的人工决定，但**不会把上一版的自动匹配当作人工决定**。
新到回传可能把原来的自动已销改回待裁决；历史版本与逐项证据永久可查。

## 6. 事实批次与延后截止

每次结算绑定一个不可变**事实批次**（登记时从接收库只读复制，并记录接收库
rowid 上界与两张事实表的 SHA-256 指纹）。对同一批已读事实做裁决时复用该批
输入，不会顺带纳入后来写入的事实。**只有显式延后截止**才读取新批次。

延后截止时若既有人工决定已失效，发布会被拒绝并说明原因；允许在同一请求里
通过 `withdrawals` 撤回失效决定后再结算。每次成功操作交付完整可见的一版，
一次失败不留下半版。

## 7. 命令行与脚本接口

入口（隔离模式友好）：

```bash
python3 -I /workspace/bin/shiftledger <command> [options]
```

| 命令 | 作用 |
| --- | --- |
| `register` | 校验清单、建台账、读事实批次、发布 v1 |
| `decide --request req.json` | 提交裁决请求，发布新版本 |
| `extend --request ext.json` | 延后截止、读新批次、重算 |
| `versions` | 列出所有已发布版本 |
| `overview [--version N]` | 版本头与计数、事实批次指纹 |
| `items [--status ...] [--page N]` | 分页消费应收项 |
| `item --item-id ID` | 单项的全部候选/阻断边与销账证据 |
| `observations [--status ...]` | 分页消费观察去向 |
| `groups [--status ...]` | 分页消费组归属与逐号映射理由 |
| `decisions` | 仅追加的人工决定日志 |

成功输出确定性 JSON 到 stdout；用法/校验错误输出 JSON 到 stderr，退出码
`4`，陈旧版本退出码 `5`。

### 裁决请求示例

```json
{
  "ledger_id": "SHIFT-1",
  "base_version": 1,
  "requested_by": "op-zhang",
  "actions": [
    {"action": "assign_group",
     "group_key": "ANALYZER-A\u001fMSG-SHARED\u001f1",
     "work_order_id": "WO2", "reason": "共享号确认归 WO2"},
    {"action": "lock_observation",
     "obs_key": "ANALYZER-A\u001fMSG-ONE\u001f1\u001f1",
     "item_id": "WO1-X", "reason": "锁定专属回传"}
  ]
}
```

`group_key` 与 `obs_key` 含单元分隔符 `\u001f`，脚本应原样透传。

### Python API

```python
from shiftledger import service, reader

service.register_and_settle(ledger_db, receiver_db, manifest_dir, cutoff="...")
service.submit_decisions(ledger_db, request_dict)
service.extend_cutoff(ledger_db, receiver_db, ledger_id=..., base_version=...,
                      new_cutoff="...", requested_by=..., withdrawals=[...])
for item in reader.iter_items(ledger_db, ledger_id, version, page=200):
    ...  # 逐项/分批消费，不装入全量
```

## 8. 内存与可复跑验证

* 事实读取、清单加载、版本消费全部 keyset 分页；候选/阻断/采纳等边集合放在
  落盘的 SQLite 临时表，Python 侧只有人本规模的人工决定驻留。
* 20 万应收项 / 50 万观察在运行时生成（不提交大数据文件），默认本地进程峰值
  RSS 远低于 256 MiB；台账可逐项或分批消费。
* 独立参考核对器 `refcheck/handover_reference.py` **不导入对账模块**，用普通
  Python 集合独立重算并逐项枚举比对候选、观察去向与欠收项，而非只比汇总数。
* 复跑：`python3 -I tests/run_isolated.py`（在隔离解释器下运行全部测试）。
