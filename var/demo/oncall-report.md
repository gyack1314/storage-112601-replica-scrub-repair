# 反复演练总记录

# 副本存储值班记录


## 当前已提交版本
- `cannot-yet-recover`：当前版本 rev=1，暂不能完整读取，优先级=normal
  - 块 0：有效副本 0/2；可信节点 无；状态 [a/boot1:corrupt；b/boot1:corrupt；c/boot1:absent]
    - 缺失原因：`all_placement_replicas_corrupt` 预定副本节点上的所有已知字节摘要均与清单不同
- `half-finished`：当前版本 rev=1，完整可读，优先级=normal
  - 块 0：有效副本 3/3；可信节点 a,b,c；状态 [a/boot1:valid；b/boot1:valid；c/boot1:valid]
- `process-archive`：当前版本 rev=1，完整可读，优先级=high
  - 块 0：有效副本 3/3；可信节点 a,b,c；状态 [a/boot1:valid；b/boot1:valid；c/boot1:valid]
  - 块 1：有效副本 3/3；可信节点 a,b,c；状态 [a/boot1:valid；b/boot1:valid；c/boot1:valid]

## 缺少有效副本的块
- `cannot-yet-recover` rev=1 block=0：`all_placement_replicas_corrupt` 预定副本节点上的所有已知字节摘要均与清单不同

## 最新一次修复
- 作业 `fix-half` / half-finished rev=1 block=0
- 结论：**有效**；状态 `verified`；原因代码 `restart_verified_from_actual_disk`
- 说明：重启后直接哈希实际落盘块，摘要与预定块身份一致
- 来源：a boot=1；目标：b 预定 boot=1
- 代次检查：current_target_boot_match=是，current_version_fence=是，planned_boot_match=是，same_block_identity=是，same_object_version=是，source_verified=是

## 修复领取顺序
- [normal] `cannot-yet-recover` rev=1 block=0 无已校验来源 -> 无目标（no_verified_source）


## 内置演练结论

- 三副本对象损坏 c/block0 后，在线读由 `a` 接管。
- 修复回报后、校验前，c 的状态是 `written_unverified`，不获得可信资格。
- 检修 block0 期间，block1 仍可读取：`synthetic-block-1`。
- 重启恢复结论：verified / restart_verified_from_actual_disk。
- 新版本竞争样例：new_version_committed。
- 节点重加样例：repair_verified。

## 样例记录

- 竞争 JSON：`var/demo/race/report.json`；Markdown：`var/demo/race/report.md`
- 重加 JSON：`var/demo/rejoin/report.json`；Markdown：`var/demo/rejoin/report.md`
