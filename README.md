# hl7recv — 可靠的 HL7 v2 MLLP 接收服务（合成数据演练）

本机可运行的接收端：接受 MLLP 帧、按限定 profile 解析 ORU^R01、把检验结果事实
可靠写入 SQLite，并提供消息状态、结果行与来源定位查询。**全部使用合成数据**，
不连接医院或真实仪器，不做临床解释、单位换算或医学判断。

仅使用 Python 标准库（3.11+）。

## 目录结构

```
hl7recv/            接收服务包
  mllp.py           MLLP 增量组帧（VT..FS CR，read 边界无关）
  hl7.py           层次解析（声明的分隔符、转义、重复/组件/子组件、空 vs 缺失）
  profile.py       限定 profile 表（v2.3.1 / v2.5.1 子集）
  builder.py       结构校验 + 事实模型构建（OBR 组、NTE 归属、重复键合并）
  store.py         SQLite 持久化（单事务提交、去重、恢复、有界诊断）
  server.py        TCP 服务（线程每连接、优雅停止、提交后才发 AA）
  ack.py           ACK 构建（AA/AE + 有界 ERR）
  faults.py        确定性故障注入点
  queries.py       查询 API（按消息 / 患者 / 订单，稳定排序）
  cli.py           命令行入口
  audit/           本地数据质量审计（只读、两窗对照、确定性 JSON 报告）
shiftledger/        本地交班对账台账（只读对接接收库，独立 SQLite）
  manifest.py      应收清单流式加载与严格校验（时间按带时区实际时刻比较）
  intake.py        从 messages/results 只读复制不可变事实批次
  engine.py        归属判定 → 候选/阻断边 → 人工锁 → 唯一一对一自动销账 → 发布
  decisions.py     仅追加的人工裁决日志与生效集合、陈旧版本保护
  service.py       登记并结算 / 裁决 / 延后截止重算（每次成功交付完整一版）
  reader.py        只读、分页的版本视图（项、观察去向、组归属、逐项证据）
  cli.py / vocab.py / errors.py / timeutil.py / store.py
peer/               可复建的本地 MLLP 对端（发送方，支持分片/原始流）
refcheck/           独立参考检查程序（不调用产品解析或写库代码）
  handover_reference.py  交班台账独立重算与逐项枚举核对（不导入对账模块）
drills/             运行时生成的可复跑合成审计演练与示例策略
  handover_drill.py     交班三工单故事与 20 万/50 万规模合成资料（运行时生成）
tests/              单元测试、验收套件、进程退出演练、压力/内存检查
docs/PROFILE.md     profile 说明（字段表、可空性、去重与提交语义）
docs/AUDIT.md       审计策略格式、指标口径、报告契约与快照一致性说明
docs/HANDOVER.md    交班台账清单/裁决接口、归属与销账口径、内存与复跑说明
```

## 运行服务

```bash
python3 -m hl7recv serve --db /tmp/hl7.db --host 127.0.0.1 --port 2575
# 打印 LISTENING 127.0.0.1 2575；SIGTERM/SIGINT 优雅停止
```

可选参数：`--max-frame N`（单帧字节上限）、
`--fault-inject point:action:n`（演练用确定性故障注入，见 docs/PROFILE.md 第 8 节）。

## 发送合成消息（本地 MLLP 对端）

```bash
python3 -m peer --port 2575 --file tests_data_msg.hl7 --expect AA
python3 -m peer --port 2575 --file msg.hl7 --chunk 7 --delay 0.05   # 分片到达
python3 -m peer --port 2575 --raw-file stream.bin --expect-acks 2    # 原始字节流
```

## 查询

```bash
python3 -m hl7recv query --db /tmp/hl7.db messages
python3 -m hl7recv query --db /tmp/hl7.db results --message 'LAB1^HOSP:MSG00001'
python3 -m hl7recv query --db /tmp/hl7.db results --patient 'HOSP:P12345'
python3 -m hl7recv query --db /tmp/hl7.db results --order FILLER100
python3 -m hl7recv query --db /tmp/hl7.db diagnostics
```

诊断状态：`acked`（已确认）、`duplicate`（重复重送）、`rejected`（业务拒绝）、
`conflict`（身份冲突）、`ack_lost`（已提交但 ACK 未确认发出）、
`recovered_incomplete`（重启时发现的未完成恢复项）；帧级错误在
`frame_errors` 中以 `(conn_id, offset)` 定位。

## 独立参考检查

参考程序独立解析原始消息（不调用产品代码），逐字段比对数据库：

```bash
python3 -m refcheck.reference --db /tmp/hl7.db msg1.hl7 msg2.hl7
# 全部匹配时退出码 0 并打印 REFERENCE CHECK PASSED
```

## 本地数据质量审计（只读，两窗对照）

针对已确认入仓的结果事实，按版本化 JSON 策略对两个半开窗口
`[start, end)`（基于消息 `received_at`）生成确定性 JSON 比较报告：
接收/结果体量、必填/可选字段非空率、发送方/检验代码/OBX-2 基数、
文本值长度分布与可解析数值分位点；每个异常都带阈值级别、分母/排除/
不可解析计数和可回溯的来源坐标（发送方、控制 ID、OBR/OBX 序号）。
空窗口、全空字段、无效策略或不适用指标显式标记为 `not_computable`，
不会伪装成零异常。详见 `docs/AUDIT.md`。

```bash
python3 -m hl7recv audit --db /tmp/hl7.db --policy drills/audit_policy.example.json
# 退出码：0 无异常 / 1 警戒 / 2 失败 / 3 无法计算 / 4 用法错误
# --output PATH 写文件（缺省输出纯 JSON 到 stdout）；窗口也可由四个
# --baseline-start/--baseline-end/--current-start/--current-end 显式给出
```

审计在 WAL 只读事务内固定单一逻辑快照，接收服务并发提交不会混合进同一
报告；审计不修改接收库，拒绝/未确认的 inbox 记录不计入事实。

可复跑合成演练（运行时生成数据，不提交大数据集）：

```bash
python3 -m drills.run_audit_drill --workdir /tmp/drill-run
# 产品审计 + 独立参考核对（refcheck.audit_reference，不导入产品审计代码）
```

## 测试与演练

```bash
cd /workspace
python3 -m unittest discover -s tests -v
```

- `test_framing.py`：MLLP 组帧（分片、多帧、坏帧、超限、断连残留）
- `test_parser.py`：层次解析、转义、profile 校验、错误定位
- `test_store.py`：单事务提交、去重/冲突、查询、恢复重分类
- `test_acceptance.py`：主验收（两条手写消息逐字段推导 + 全部边界情形）
- `test_recovery.py`：进程退出演练（提交前 / 提交后 ACK 前 / ACK 后 / 解析中 / 收帧后）
- `test_stress.py`：运行时生成的大消息、流式分批内存检查
- `test_cli.py`、`test_refcheck.py`：CLI 表面与独立对照
- `test_audit_policy.py`：审计策略版本化与严格校验（无效策略无法伪装零异常）
- `test_audit_drill.py`：两窗合成数据逐项手工推导、边界时间戳、证据溯源、退出码
- `test_audit_rules.py`：各阈值规则族、窗口语义与不可计算判定
- `test_audit_snapshot.py` / `test_audit_coexist.py`：快照固定、并发写入不混合、接收服务在线共存
- `test_audit_reference.py`：独立参考核对及篡改检出
- `test_audit_stress.py`：运行时大样本、有界内存/样例、稳定排序
- `test_handover_acceptance.py`：交班三工单故事全流程（共享号阻断、人工指源、迟到回传重开、锁后迟到销账）
- `test_handover_attribution.py`：号映射矛盾、号段切换边界时刻、相同项竞争、NULL 子 ID 精确匹配
- `test_handover_decisions.py`：非法边/双锁/孤儿锁整次拒绝、组合裁决、自动匹配不算人工决定
- `test_handover_cli.py`：命令行端到端与陈旧版本退出码 5
- `test_handover_scale.py`：20 万应收/50 万观察分进程峰值 RSS < 256 MiB 与守恒不变量

压力输入（如 2 万条 OBX 的消息）在运行时生成，不提交到仓库。

## 本地交班对账台账（shiftledger）

在不改动接收库的前提下，把派工应收清单与已入仓结果对账。**只核对数据交付**，
不读检验值、不判断医学含义，全部为合成资料。完整口径见 `docs/HANDOVER.md`。

```bash
# 登记并发布 v1（清单目录含 manifest.json + 三个 .jsonl）
python3 -I bin/shiftledger register --ledger-db /tmp/hand.db \
  --receiver-db /tmp/hl7.db --manifest-dir /tmp/manifest
# 查看欠收/待裁决/已销项与每条观察去向（分页，JSON）
python3 -I bin/shiftledger items  --ledger-db /tmp/hand.db --ledger-id SHIFT-1
python3 -I bin/shiftledger groups --ledger-db /tmp/hand.db --ledger-id SHIFT-1
# 提交人工裁决（请求带 base_version，过期返回退出码 5）
python3 -I bin/shiftledger decide --ledger-db /tmp/hand.db --request decision.json
# 显式延后截止才读取新事实批次并重算，历史版本与证据保留
python3 -I bin/shiftledger extend --ledger-db /tmp/hand.db \
  --receiver-db /tmp/hl7.db --request extend.json
```

可复跑合成演练（运行时生成数据，不提交大文件）与独立枚举核对：

```bash
python3 -I tests/run_isolated.py    # 全部测试（含独立参考逐项核对与 RSS 证明）
```
