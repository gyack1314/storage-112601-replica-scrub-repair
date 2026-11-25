# 长程记录副本的在线巡检与条件修复

块为 UTF-8 合成文本，通过所属版本的真实 SHA-256 验证。资料中的坏内容是独立输入，版本引用和节点代次均为合成。只使用 Python 3.11 标准库，所有对象、状态和档案都写在本地目录。

## 一键反复演练

```bash
bash scripts/run_drills.sh
```

该命令会：

1. 运行全部单元测试；
2. 用 `scenario_reader.py` 核对输入契约；
3. 重放 `examples/*.jsonl`，为每个场景生成 `report.json` 和 `report.md`；
4. 运行内置三副本损坏、在线读接管、修复领取/校验、重启恢复和不可恢复原因演练；
5. 写出值班记录 `var/demo/oncall-report.md` 与 `var/demo/oncall-report.json`。

仓库中也保留了一份最近生成的样例：[`records/example-oncall-report.md`](records/example-oncall-report.md)。

也可以单独运行：

```bash
python3 -m unittest discover -v
python3 scenario_runner.py
python3 drill.py demo --report-dir var/demo
python3 drill.py --root var/demo/cluster inspect --scan --format markdown
```

## 作为 Python 存储使用

```python
from replica_store import ReplicaStore

store = ReplicaStore("var/cluster")
store.register_node("a", boot=1)
store.register_node("b", boot=1)
store.register_node("c", boot=1)

# 写三个副本；返回的 rev 是写入代次。
store.write_object(
    "process-archive",
    ["block-zero", "block-one"],
    nodes=["a", "b", "c"],
)

# 普通读会逐块按已提交清单校验；坏副本会被记录并尝试其他副本。
obj = store.read_object("process-archive")

# 注入/巡检损坏后，规划并领取修复。
store.corrupt_replica("c", "process-archive", 1, 0, "bad bytes")
store.scrub_disk("c", "process-archive", 1)
job = store.claim_next(batch_size=10)

store.record_result({
    "id": job["id"], "block": 0, "boot": 1, "rev": 1, "text": "block-zero"
})
# 只有回执仍不算可信；独立校验或重启后的实际哈希扫描才能恢复资格。
store.verify_repair({"id": job["id"], "block": 0, "text": "block-zero"})

print(store.inspect("process-archive")["markdown"])
```

主要 API：

- `register_node(node, boot)`：登记节点启动身份，并按磁盘真实文件扫描。
- `propose_manifest(key, block_digests, rev=..., generation=...)`：提议版本清单。
- `write_object(...)` / `stage_block(...)`：写合成块和副本。
- `commit(key, rev, generation=...)`：只有写入代次一致才发布清单。
- `read_object(key, rev=None)` / `read_block(...)`：按指定已提交版本校验读取。
- `report_scrub(record)` / `scrub_disk(...)`：处理巡检报告或扫描本地文件。
- `plan_repairs(key=None)` / `claim_repairs(...)` / `claim_next(...)`：按优先级和可信来源领取一个或多个块。
- `record_result(...)`：接收目标端回执并执行版本/启动代次围栏检查。
- `verify_repair(...)`：独立校验修复文本或目标磁盘。
- `restart()`：重新扫描实际落盘字节并恢复半途作业。
- `inspect(key=None, scan=False)`：生成值班 JSON 和 Markdown 记录。
- `replay_file(path, root, reset=True)`：重放本文的 JSONL 演练资料。

## 命令行

全局 `--root` 是协调端状态和节点文件根目录，必须放在子命令前：

```bash
python3 drill.py --root var/cluster node a 1
python3 drill.py --root var/cluster write docs \
  --blocks-json '["hello", "world"]' --nodes a,b,c
python3 drill.py --root var/cluster commit docs 1
python3 drill.py --root var/cluster read docs
python3 drill.py --root var/cluster inspect docs --scan --record var/docs-report.json
```

修复流程：

```bash
python3 drill.py --root var/cluster repair plan
python3 drill.py --root var/cluster repair claim \
  --id job-1 --item docs:1:0:a:c
# 可选：由协调端从已校验来源实际条件复制；也可等待目标端 result 回执。
python3 drill.py --root var/cluster repair execute --id job-1 --block 0
python3 drill.py --root var/cluster repair result \
  --id job-1 --block 0 --boot 1 --rev 1 --text hello
python3 drill.py --root var/cluster repair verify \
  --id job-1 --block 0 --from-disk
```

重放单个契约样例：

```bash
python3 drill.py --root var/replay replay \
  examples/repair-races-new-version.jsonl --reset
python3 drill.py --root var/replay replay \
  examples/late-scrub-after-rejoin.jsonl --reset
```

## 核心安全规则

### 写入代次与清单

`version` 只建立版本清单；只有匹配 `key/rev`（或显式 `generation/write_id`）的 `commit` 后，该版本才可读。错误代次的发布会抛出 `GenerationFenceError`，未提交版本不会作为当前版本返回。

### 读路径

- 读指定 `rev` 时只使用该版本清单中的 SHA-256 块身份。
- 每个实际副本都重新哈希；摘要不符的节点会记录为 `corrupt`，不能用于回复。
- 某块没有可信副本时，对象读以 `BlockUnavailable` 失败并列出缺失块和原因。
- 不会用上一版本的同块填补新版本缺口；报告中有 `substituted_previous_revision: false`。
- 块级 API 仍可读取其他完好块；对象级默认不返回拼接不完整的对象。

### 巡检与节点代次

巡检结论绑定：

- 真实对象身份：`key`、`rev`、`manifest_digest`；
- 节点身份：`node`、报告中的 `boot`、当前 `active_boot`；
- 区间：`range: [start, end)`，单块报告等价于 `[block, block+1)`。

旧 `boot` 的巡检只归档为 `stale_boot_report` / `historical_generation`，不会隔离节点以新启动代次重新扫描或写入验证过的真实副本。节点重新加入时会遍历自己的 `nodes/<node>/objects/...`，按实际文件哈希接受访问。

### 修复围栏

领取时固定：

- `key/rev/block/digest`；
- 已实际哈希验证的 `source_node/source_boot`；
- `target_node/expected_target_boot`；
- 领取时当前对象代次 `fence_current_rev`。

目标回执必须证明预定版本、块摘要和节点启动代次一致。若修复领取后新版本已经提交，旧作业结论为：

- `status = "stale"`
- `is_stale = true`
- `reason_code = "new_version_committed"`

过时回执不会写入受围栏保护的当前槽位，也不会把旧版本提升为当前内容。若领取时显式指定修复一个已经被新版本超过的历史版本，作业标记为 `historical`，可安全修复该旧清单版本，但不会改变当前版本。

`result` 只表示目标端报告已写入：状态是 `written_unverified`，检查记录不会把该副本计入可信副本数。随后 `verify` 文本/磁盘哈希一致才变成 `verified`。在线读若亲自从磁盘读到匹配字节，也会产生一次新的实际校验观察。

### 优先级与局部检修

`priority critical|high|normal|low` 会影响 `plan_repairs()` 和 `claim_next()` 的顺序；同一优先级中完全不可读的块优先于欠副本块。

修复锁只针对精确槽位 `node|key|rev|block`。因此：

- block 0 检修不阻塞 block 1；
- 新版本的 `rev/block` 是另一个锁和路径，可以正常写入；
- 对同一已提交版本的同块无围栏改写会得到 `BlockUnderRepair`。

### 重启与不可恢复原因

`restart()` 会重新扫描所有节点的真实文件：

- 目标块哈希正确：半途作业恢复为 `verified`，原因码 `restart_verified_from_actual_disk`；
- 写入不完整但来源仍可信：恢复为 `retry_ready`；
- 来源和目标都不能证明块身份：恢复为 `blocked`，并在对象的 `missing_reasons` 与 `missing_history` 中保留 `all_placement_replicas_corrupt`、`corrupt_and_missing_replicas`、`all_replicas_missing_or_unknown` 等原因。

## 值班记录

`inspect` / `report.md` 包含：

- 每个对象的当前已提交 `rev` 和是否完整可读；
- 每块有效副本数、预定副本数、可信节点、各节点 boot 与状态；
- 哪些块完全没有可信副本，哪些块只是欠副本；
- 缺失原因；
- 最新修复的作业号、有效/过时/未校验结论、原因码、中文说明和各项代次检查；
- 高风险优先的修复领取队列。

## 本地目录

```text
var/cluster/
  coordinator/state.json       # 事务替换的协调状态
  coordinator/journal.jsonl    # 操作档案
  coordinator/store.lock       # 进程间文件锁
  nodes/<node>/objects/<key>/rev-<rev>/block-<block>.bin
var/demo/oncall-report.{json,md}
var/scenario/<name>/report.{json,md}
```

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **node**　node、boot 登记启动事实。
- **version**　key、rev 和 blocks 各块预期摘要。
- **stored**　节点报告 key、rev、block 的当前 text 与 boot。
- **commit**　写入方确认 rev 的清单为当前对象版本。
- **read**　按 key、rev、block 发起读。
- **scrub**　巡检复述 node/boot 对该版本和块实际观测 text。
- **repair**　领取 id，给出 source、target、key、rev、block；也支持 `items` 批量块。
- **result**　修复回报 id 实际写入后对应的 boot/rev 与 text。
- **verify**　修复后独立检验回报 id 和 text。
- **restart**　副本协调端重启。
- **inspect**　查询对象。

## 示例

- `examples/repair-races-new-version.jsonl`：旧修复回执到达时 rev=2 已发布，结论必须是 `new_version_committed` / stale。
- `examples/late-scrub-after-rejoin.jsonl`：b/boot=1 的旧巡检只能归档；b/boot=2 的真实副本可信，r2 修复最终 `repair_verified`。

资料读取器仍可单独运行：

```bash
python3 scenario_reader.py examples
python3 -m unittest discover -v
```
