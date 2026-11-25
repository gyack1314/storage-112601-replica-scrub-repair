# 长程记录副本的在线巡检与条件修复

块为 UTF-8 合成文本，通过所属版本的真实 SHA-256 验证。资料中的坏内容是独立输入，版本引用和节点代次均为合成。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **node**　node、boot 登记启动事实。
- **version**　key、rev 和 blocks 各块预期摘要。
- **stored**　节点报告 key、rev、block 的当前 text 与 boot。
- **commit**　写入方确认 rev 的清单为当前对象版本。
- **read**　按 key、rev、block 发起读。
- **scrub**　巡检复述 node/boot 对该版本和块实际观测 text。
- **repair**　领取 id，给出 source、target、key、rev、block。
- **result**　修复回报 id 实际写入后对应的 boot/rev 与 text。
- **verify**　修复后独立检验回报 id 和 text。
- **restart**　副本协调端重启。
- **inspect**　查询对象。

## 示例

- repair-races-new-version.jsonl
- late-scrub-after-rejoin.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
