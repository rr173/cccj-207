# 结果订阅与下游副本（Result Subscription & Replica）

分析端与接收端**不共享数据库**：接收端把已提交的检验结果事实按持久提交顺序发布成
一个个确定性的**页文件**（本地文件，间歇拷贝），分析端把页导入自己的 SQLite 副本。
全部使用合成资料；不搭网络服务，不重新解析 HL7；只读已提交的 `messages`/`results`
与同步元数据，**从不**读取 inbox 尝试日志或 ACK 状态来代替事实。

## 1. 位置、上界与提交语义

- **位置（position）**：持久、稠密、无空洞的整数，从 1 开始，按**提交顺序**分配
  （不是业务时间 `received_at`，也不是查询行号）。位置一旦发布绝不重编号。
- 离线升级（见 §5）为历史消息按**有文档的固定顺序**——`messages.rowid` 升序，即
  单写者接收端应用提交的顺序——赋予初始位置。升级后，`Store.commit_message` 在
  **同一个 SQLite 事务**内为新消息分配位置：提交与位置共同生效；回滚同时回滚计数器，
  因此提交了但 ACK 未发出的消息照样能被追上，未提交的尝试不会留下可供消费者跳过的
  空洞。重送（duplicate）与身份冲突（conflict）不产生新 `messages` 行，**不消耗位置**。
- **上界（bound / watermark）**：`sync watermark` 报告当前发布上界（已分配的最大
  位置）。调用者先取上界 W，再从起点分段取到 W；下次从已应用的位置取新上界继续。
  `sync page` 严格使用调用者给定的 `--bound`：绝不自行换成更晚的水位，超出当前
  水位的上界直接报错（退出码 7），未来的提交不会被提前夹进旧页。
- 同步过程中**无需暂停接收端**：取页是有界只读范围扫描，与接收提交并发安全。

## 2. 订阅身份

订阅 = 固定的发送方集合（`--sender` 可重复）。规范化：去空白、拒绝空串、去重、
排序；身份 = `"sub1:" + sha256(规范 JSON 字节)`。同一集合的不同写法得到同一身份；
不同订阅的身份不同，游标与页不可混用。不同发送方即使使用同一控制 ID 也是各自独立
的消息（源库主键即 `(sender, control_id)`）。

## 3. 页格式（`hl7recv-sync/1`）

页 = 单个**规范 JSON** 对象（UTF-8、`sort_keys`、无空白分隔符）；相同逻辑内容永远
得到相同字节，这是"同参数重取字节相同"的基础。

```json
{
  "protocol": "hl7recv-sync/1",
  "source_id": "<持久源身份，升级时生成的 uuid4 hex>",
  "subscription_id": "sub1:<sha256>",
  "bound": 20,            // 取页时使用的发布上界
  "start": 10,            // 起始位置（不含）
  "end": 15,              // 本页覆盖到的位置（含）；扫描区间内最后一条提交
  "budget": 5,            // 每页扫描消息数上限（含不符合订阅的消息）
  "terminal": 0,          // 1 表示 end == bound，已扫到上界
  "messages": [           // 仅命中订阅的消息，按 position 升序
    {"position": 12, "sender": "LAB1^HOSP", "control_id": "MSG00001",
     "received_at": "2026-11-01T08:30:00.123+00:00",
     "results": [
       {"obr_index": 1, "obx_index": 1, "obs_system": "LN", "obs_code": "WBC",
        "sub_id": "1", "value_type": "NM", "value_text": "7.5",
        "units": "10*9/L"}]}
  ],
  "digest": "<sha256(去掉 digest 字段后的规范字节)>"
}
```

- 页只扫描 `(start, bound]` 内的提交；`budget` 限制**扫描**的消息数（含未命中的）。
  一整页发送方都不匹配时 `messages` 为空，但 `end` 仍推进到已覆盖的位置。
  同一条消息（含其全部结果行）绝不跨页。
- 载荷只含：发送方、控制 ID、`received_at`，以及每条结果的 OBR/OBX 坐标
  （`obr_index`/`obx_index`）、观察代码系统与代码、子 ID、值类型、原值、单位。
  **空字符串与 NULL 按事实原样传递**（JSON `""` vs `null`）。不携带原始帧、
  患者标识、备注或任何其他源库内容。
- **完整性**：`digest` 覆盖头部与全部载荷。丢失尾部 → JSON 解析失败；内容损坏 →
  digest 不匹配。该校验针对偶发损坏，不构成密码学信任体系。
- **确定性重取**：相同（源、订阅、start、bound、budget）的重取字节相同——即使
  接收端继续写入、服务重启或执行过 `VACUUM`（位置是普通列数据，不依赖 rowid）。

## 4. 副本（分析端）

```
python3 -m hl7recv replica init --db R --source-id HEX --sender S ...
python3 -m hl7recv replica import --db R --page FILE
python3 -m hl7recv replica position --db R     # 当前游标与身份
python3 -m hl7recv replica facts --db R        # 消费已落地事实（JSON）
```

- `init` 绑定一对（源身份， 订阅身份），游标从 0 开始；重复 init 且身份一致为幂等。
- `import` 规则：
  - **仅当页 start == 当前游标**才应用；页内事实 + 游标推进在**单个事务**内生效，
    读者只能看到导入前或导入后的整页状态，不会看到半页事实或只有进度没有事实。
  - 相邻的非终止页覆盖连续扫描区间（即使一页载荷为空也推进游标）。
  - 重复导入已完整应用的同一页（按 `(start,end)` 找到记录且 digest 相同）→
    无副作用，报告 `"duplicate"`。
  - 覆盖相同进度但字节不同的陈旧文件 → 拒绝（退出码 4），不算成功重放。
  - 断档（start > 游标）或跨骑游标 → 拒绝（退出码 3）；源/订阅错配 → 退出码 2；
    损坏页 → 退出码 1。**任何拒绝都不推进游标、不落地事实。**
  - 两个进程从同一游标提交不同且各自合法的页：写事务串行化，至多一页先推进，
    另一页得到游标不匹配拒绝。
- 副本表：`sub_messages(sender, control_id, received_at, position)`、
  `sub_results(坐标与值八列)`、`applied_pages(start, end, digest)`（重放判定依据）。

## 5. 离线升级与恢复操作

```
python3 -m hl7recv sync upgrade --db RECV [--source-id HEX]
```

- **明确离线执行**（接收服务已停止）。整个升级是**单个 SQLite 事务**：创建
  `sync_positions` 元数据表、按 `messages.rowid` 升序为全部历史消息赋位置、
  生成并记录持久源身份与计数器。不重接收任何消息，不改动任何原事实。
- 升级中途退出（崩溃）→ 事务回滚，**不暴露半套位置**；重跑完整赋位。
  已启用同步的库重跑只做一致性校验并报告 `"already"`，**绝不重编号**已发布的历史。
- 恢复操作：
  - 接收端崩溃恢复沿用既有 inbox 重分类；同步位置随提交事务天然一致，无需额外恢复。
  - 副本导入中断 → 重开库后游标与事实保持一致（整页原子），从游标处重新取页继续。
  - 页文件丢失 → 用相同参数重新取页（字节相同）；副本损坏 → 重新 `init` 并从 0 重放。

## 6. 命令与退出码

```
python3 -m hl7recv sync upgrade   --db RECV [--source-id HEX]
python3 -m hl7recv sync watermark --db RECV --sender S ...
python3 -m hl7recv sync page      --db RECV --sender S ... \
    --start N --bound M --budget K --output FILE
```

退出码：0 成功（import 区分 `"applied"`/`"duplicate"`）；1 页损坏；2 源/订阅错配；
3 游标断档/跨骑；4 陈旧页冲突；5 用法错误；6 源库未启用同步；7 页窗口非法
（含上界超过当前水位）。

## 7. 规模与内存

取页是 `sync_positions` 主键上的有界范围扫描（`seq > :start AND seq <= :bound`，
`LIMIT :budget`），不从订阅起点重扫历史，也不保留跨调用的读事务。页内存占用由
`budget` 决定；20 万消息历史的追赶演练（见 `tests/test_sync_scale.py`）证明默认
配置下单进程峰值 RSS < 256 MiB。大型历史在运行时生成，不提交仓库。
