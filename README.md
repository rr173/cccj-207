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
peer/               可复建的本地 MLLP 对端（发送方，支持分片/原始流）
refcheck/           独立参考检查程序（不调用产品解析或写库代码）
tests/              单元测试、验收套件、进程退出演练、压力/内存检查
docs/PROFILE.md     profile 说明（字段表、可空性、去重与提交语义）
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

压力输入（如 2 万条 OBX 的消息）在运行时生成，不提交到仓库。
