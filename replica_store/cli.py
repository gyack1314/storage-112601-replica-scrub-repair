"""Command line interface for repeatable local replica drills."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .errors import BlockUnavailable, ReplicaStoreError
from .replay import replay_file
from .store import ReplicaStore
from .util import write_atomic, write_json_atomic


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="本地三副本、已提交清单和条件修复演练")
    parser.add_argument("--root", default="var/store", help="本地数据目录")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("node", help="登记节点启动代次并扫描其真实文件")
    p.add_argument("node")
    p.add_argument("boot", type=int)

    p = sub.add_parser("write", help="写入一个或多个合成块到副本节点")
    p.add_argument("key")
    p.add_argument("--rev", type=int)
    p.add_argument("--block", action="append", default=[], help="每传一次增加一个块，值为 UTF-8 合成文本")
    p.add_argument("--blocks-json", help='例如 ["甲", "乙"] 或 {"0":"甲"}')
    p.add_argument("--nodes", default="a,b,c")
    p.add_argument("--generation")
    p.add_argument("--stage-only", action="store_true", help="只提议清单和写副本，不发布确认")

    p = sub.add_parser("commit", help="用相同写入代次发布已提议清单")
    p.add_argument("key")
    p.add_argument("rev", type=int)
    p.add_argument("--generation")

    p = sub.add_parser("read", help="按已提交版本读取并逐块校验")
    p.add_argument("key")
    p.add_argument("--rev", type=int)
    p.add_argument("--block", type=int)
    p.add_argument("--output", type=Path)

    p = sub.add_parser("corrupt", help="注入本地静默损坏（演练用）")
    p.add_argument("node")
    p.add_argument("key")
    p.add_argument("rev", type=int)
    p.add_argument("block", type=int)
    p.add_argument("--text", default="silent-corruption")

    p = sub.add_parser("scrub", help="提交巡检报告 JSON，或扫描真实磁盘")
    p.add_argument("--json", dest="json_text")
    p.add_argument("--file", type=Path)
    p.add_argument("--disk", action="store_true")
    p.add_argument("--node")
    p.add_argument("--key")
    p.add_argument("--rev", type=int)

    repair = sub.add_parser("repair", help="规划、领取、回报、校验修复")
    repair_sub = repair.add_subparsers(dest="repair_command", required=True)
    repair_sub.add_parser("plan").add_argument("--key")
    claim = repair_sub.add_parser("claim")
    claim.add_argument("--id", required=True)
    claim.add_argument("--item", action="append", default=[], help="key:rev:block:source:target")
    claim.add_argument("--items-json")
    claim_next = repair_sub.add_parser("claim-next")
    claim_next.add_argument("--batch-size", type=int, default=3)
    execute = repair_sub.add_parser("execute", help="从已校验来源条件复制到本地目标")
    execute.add_argument("--id", required=True)
    execute.add_argument("--item-id")
    execute.add_argument("--block", type=int)
    result = repair_sub.add_parser("result")
    result.add_argument("--id", required=True)
    result.add_argument("--item-id")
    result.add_argument("--block", type=int)
    result.add_argument("--boot", type=int)
    result.add_argument("--rev", type=int)
    result.add_argument("--text")
    verify = repair_sub.add_parser("verify")
    verify.add_argument("--id", required=True)
    verify.add_argument("--item-id")
    verify.add_argument("--block", type=int)
    verify.add_argument("--text")
    verify.add_argument("--from-disk", action="store_true")

    p = sub.add_parser("priority", help="设置对象抢修优先级")
    p.add_argument("key")
    p.add_argument("level", choices=["critical", "high", "normal", "low"])

    sub.add_parser("restart", help="模拟协调端重启并按磁盘实际状态恢复")

    p = sub.add_parser("inspect", help="查询值班记录")
    p.add_argument("key", nargs="?")
    p.add_argument("--scan", action="store_true", help="先实际哈希扫描所有节点文件")
    p.add_argument("--format", choices=["markdown", "json"], default="markdown")
    p.add_argument("--record", type=Path, help="同时写入记录文件；JSON 会另写 .md")

    p = sub.add_parser("replay", help="反复重放 examples 中的 JSONL 演练")
    p.add_argument("scenario", type=Path)
    p.add_argument("--reset", action="store_true")
    p.add_argument("--format", choices=["summary", "json"], default="summary")

    p = sub.add_parser("demo", help="运行内置三副本静默损坏、抢修和新版本竞争演练")
    p.add_argument("--report-dir", type=Path, default=Path("var/demo"))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    store = ReplicaStore(args.root)
    try:
        return _dispatch(args, store)
    except BlockUnavailable as error:
        print_json({"ok": False, "error": str(error), "report": error.report})
        return 2
    except ReplicaStoreError as error:
        print_json({"ok": False, "error": str(error)})
        return 3
    except (ValueError, KeyError, TypeError) as error:
        print_json({"ok": False, "error": f"{type(error).__name__}: {error}"})
        return 4


def _dispatch(args: argparse.Namespace, store: ReplicaStore) -> int:
    if args.command == "node":
        return print_json(store.register_node(args.node, args.boot))
    if args.command == "write":
        blocks = _parse_write_blocks(args)
        nodes = tuple(args.nodes.split(",")) if args.nodes else store.default_nodes
        result = store.write_object(
            args.key,
            blocks,
            rev=args.rev,
            nodes=nodes,
            generation=args.generation,
            commit=not args.stage_only,
        )
        return print_json(_without_bytes(result))
    if args.command == "commit":
        return print_json(store.commit(args.key, args.rev, args.generation))
    if args.command == "read":
        if args.block is not None:
            result = store.read_block(args.key, args.block, rev=args.rev)
            data = result.pop("data")
        else:
            result = store.read_object(args.key, rev=args.rev)
            data = result.pop("data")
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(data)
            result["output"] = str(args.output)
        return print_json(_without_bytes(result))
    if args.command == "corrupt":
        return print_json(store.corrupt_replica(args.node, args.key, args.rev, args.block, args.text))
    if args.command == "scrub":
        return _scrub(args, store)
    if args.command == "repair":
        return _repair(args, store)
    if args.command == "priority":
        return print_json(store.set_priority(args.key, args.level))
    if args.command == "restart":
        return print_json(store.restart())
    if args.command == "inspect":
        return _inspect(args, store)
    if args.command == "replay":
        root = Path(args.root) / "replays" / args.scenario.stem
        summary = replay_file(args.scenario, root, reset=args.reset)
        report = summary["report"]
        if args.format == "json":
            print_json({k: v for k, v in summary.items() if k != "report"})
        else:
            print(report["markdown"])
            print(f"记录：{summary['report_markdown']}")
        return 0
    if args.command == "demo":
        from .demo import run_demo

        result = run_demo(args.report_dir)
        print(result["markdown"])
        print(f"JSON 记录：{result['json_path']}")
        return 0
    return 64


def _parse_write_blocks(args: argparse.Namespace) -> list[Any] | dict[str, Any]:
    if args.blocks_json:
        value = json.loads(args.blocks_json)
        return value
    if not args.block:
        raise ValueError("至少用 --block 提供一个合成块，或使用 --blocks-json")
    return args.block


def _scrub(args: argparse.Namespace, store: ReplicaStore) -> int:
    if args.disk:
        result = store.scrub_disk(args.node, args.key, args.rev)
        return print_json(result)
    if args.file:
        record = json.loads(args.file.read_text(encoding="utf-8"))
    elif args.json_text:
        record = json.loads(args.json_text)
    else:
        raise ValueError("scrub 需要 --json、--file 或 --disk")
    return print_json(store.report_scrub(record, adopt_observed_bytes=False))


def _repair(args: argparse.Namespace, store: ReplicaStore) -> int:
    if args.repair_command == "plan":
        return print_json(store.plan_repairs(args.key))
    if args.repair_command == "claim":
        if args.items_json:
            items = json.loads(args.items_json)
        else:
            items = [_parse_item(value) for value in args.item]
        return print_json(store.claim_repairs(args.id, items))
    if args.repair_command == "claim-next":
        return print_json(store.claim_next(batch_size=args.batch_size))
    if args.repair_command == "execute":
        return print_json(
            store.execute_repair(args.id, block=args.block, item_id=args.item_id)
        )
    if args.repair_command == "result":
        record = {
            "id": args.id,
            "item_id": args.item_id,
            "block": args.block,
            "boot": args.boot,
            "rev": args.rev,
            "text": args.text,
        }
        record = {k: v for k, v in record.items() if v is not None}
        return print_json(store.record_result(record))
    if args.repair_command == "verify":
        record: dict[str, Any] = {"id": args.id, "item_id": args.item_id, "block": args.block}
        if args.text is not None:
            record["text"] = args.text
        # If neither text nor digest is supplied, ReplicaStore.verify_repair
        # independently hashes the actual target block file.
        record = {k: v for k, v in record.items() if v is not None}
        return print_json(store.verify_repair(record))
    raise ValueError("未知 repair 子命令")


def _parse_item(value: str) -> dict[str, Any]:
    parts = value.split(":")
    if len(parts) != 5:
        raise ValueError("--item 格式应为 key:rev:block:source:target")
    key, rev, block, source, target = parts
    return {"key": key, "rev": int(rev), "block": int(block), "source": source, "target": target}


def _inspect(args: argparse.Namespace, store: ReplicaStore) -> int:
    report = store.inspect(args.key, scan=args.scan)
    markdown = report.pop("markdown")
    if args.record:
        args.record.parent.mkdir(parents=True, exist_ok=True)
        write_json_atomic(args.record, report)
        write_atomic(args.record.with_suffix(".md"), markdown.encode("utf-8"))
    if args.format == "json":
        print_json(report)
    else:
        print(markdown)
    return 0


def print_json(value: Any) -> int:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


def _without_bytes(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_bytes(v) for k, v in value.items() if k != "data"}
    if isinstance(value, list):
        return [_without_bytes(v) for v in value]
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    return value


if __name__ == "__main__":
    sys.exit(main())
