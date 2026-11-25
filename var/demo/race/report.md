# 副本存储值班记录


## 当前已提交版本
- `trace`：当前版本 rev=2，完整可读，优先级=normal
  - 块 0：有效副本 1/2；可信节点 b；状态 [a/boot1:missing；b/boot1:valid]

## 缺少有效副本的块
- 无：当前版本每块都有已校验可信副本

## 最新一次修复
- 作业 `r` / trace rev=1 block=0
- 结论：**过时**；状态 `stale`；原因代码 `new_version_committed`
- 说明：检修期间已有更新写入代次发布；旧字节不得覆盖新版本
- 来源：a boot=1；目标：b 预定 boot=1
- 代次检查：current_target_boot_match=是，current_version_fence=否，planned_boot_match=是，same_block_identity=是，same_object_version=是，source_verified=是

## 修复领取顺序
- [normal] `trace` rev=2 block=0 b -> a（under_replicated）
