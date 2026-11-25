# 副本存储值班记录

查询对象：`proof`

## 当前已提交版本
- `proof`：当前版本 rev=1，完整可读，优先级=normal
  - 块 0：有效副本 2/2；可信节点 a,b；状态 [a/boot1:valid；b/boot2:valid]

## 缺少有效副本的块
- 无：当前版本每块都有已校验可信副本

## 最新一次修复
- 作业 `r2` / proof rev=1 block=0
- 结论：**有效**；状态 `verified`；原因代码 `repair_verified`
- 说明：来源、对象版本、块身份、目标代次和独立摘要校验全部一致
- 来源：b boot=2；目标：a 预定 boot=1
- 代次检查：current_target_boot_match=是，current_version_fence=是，planned_boot_match=是，same_block_identity=是，same_object_version=是，source_verified=是

## 修复领取顺序
- 无待领取项
