"""结果交付服务：抽取记录按交付格式的分片导出、核验确认与交付回执。

从版本快照读取产物，按指定格式和分片策略导出到目标系统：

- JSONL（历史格式）：每行一条 JSON 记录，历史分片的下载方式保持不变；
- CSV（按列接收）：实体的平铺字段直接成列，关系/向量等多值字段以 JSON
  编码进单元格，列类型与 CSV 方言在交付清单中声明，接收方既能按列装载，
  也能按清单把逗号、引号、换行无歧义读回，并完整恢复原始记录。

交付稳定性约定：

- 分片先写临时文件再原子改名，并发读取永远看不到写了一半的文件；
- 全部分片落盘并回读确认后，才原子写入状态为 confirmed 的交付清单；
- 回执只出自 confirmed 清单：逐分片校验 SHA256 与行数、并重新核对源头
  记录明细（数量与内容摘要）全部通过后才出具，未全部确认的文件不会出现
  在已完成回执中；
- 同一快照的交付/核验/发布用锁串行化，发布与交付并发时回执不会包含
  未确认分片。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService

# 同一快照的交付、核验、回执与发布串行化，保证并发请求不会读到中间态
_DELIVERY_LOCKS: dict[str, threading.Lock] = {}
_DELIVERY_LOCKS_GUARD = threading.Lock()


def _lock_for(key: str) -> threading.Lock:
    """按快照编号取交付锁，串行化同一快照的交付、核验与发布。"""
    with _DELIVERY_LOCKS_GUARD:
        lock = _DELIVERY_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _DELIVERY_LOCKS[key] = lock
        return lock


def _sha256_bytes(data: bytes) -> str:
    """计算字节内容 SHA256 全量摘要，用于分片完整性核验。"""
    return hashlib.sha256(data).hexdigest()


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """先写临时文件并 fsync，再原子改名：并发读者只能看到完整文件。"""
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
    try:
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


class ResultDeliverService(BaseModuleService):
    MODULE = "result_deliver"
    ENTITY = "交付任务"
    OPERATION_TERM = "导出"
    LIST_FIELDS = ["交付编号", "快照编号", "交付格式", "目标系统", "字段范围", "记录条数", "分片大小", "交付时间"]
    REQUIRED_FIELDS = ["交付编号", "快照编号", "交付格式"]
    STATUS_ORDER = ["草稿", "待导出", "导出中", "已交付"]
    ACTION_RULES = {"启动导出": "待导出", "确认交付": "已交付", "标记失败": "已交付"}
    NEGATIVE_ACTIONS = []

    SUPPORTED_FORMATS = {
        "jsonl": {"mime": "application/x-ndjson", "implemented": True,
                  "description": "每行一条JSON记录的文本格式（历史交付格式，保持兼容）"},
        "csv": {"mime": "text/csv", "implemented": True,
                "description": "逗号分隔值表格格式（RFC 4180，多值单元格JSON编码，含可核验清单）"},
        "parquet": {"mime": "application/octet-stream", "implemented": False,
                    "description": "列式存储格式（暂未实现分片导出）"},
        "xlsx": {"mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "implemented": False,
                 "description": "Excel表格格式（暂未实现分片导出）"},
    }

    # CSV 方言声明：写入与读回都以此为准，逗号/引号/换行按 RFC 4180 转义
    CSV_DIALECT = {"delimiter": ",", "quotechar": '"', "doublequote": True,
                   "lineterminator": "\n", "quoting": "minimal"}

    def deliver_from_snapshot(
        self,
        snapshot_id: str,
        fmt: str = "jsonl",
        shard_size: int = 500,
        target: str = "下游知识库",
    ) -> dict[str, Any]:
        """从版本快照导出抽取产物到目标系统，支持 JSONL/CSV 分片分批交付。

        分片原子落盘并逐片回读确认，全部确认后写入交付清单（manifest），
        清单与分片 SHA256 是后续核验、回执与下载的唯一依据。
        """
        fmt = (fmt or "jsonl").strip().lower()
        if fmt not in self.SUPPORTED_FORMATS:
            return {"error": f"不支持的交付格式：{fmt}，可选：{'、'.join(sorted(self.SUPPORTED_FORMATS))}"}
        if not self.SUPPORTED_FORMATS[fmt]["implemented"]:
            return {"error": f"交付格式 {fmt} 暂未实现分片导出，请使用 jsonl 或 csv"}
        if shard_size < 1:
            return {"error": "分片大小必须为正整数"}

        lock = _lock_for(snapshot_id)
        with lock:
            snapshot_data = self._find_snapshot(snapshot_id)
            if not snapshot_data:
                return {"error": f"版本快照 {snapshot_id} 不存在，请先创建版本快照"}
            doc_id = snapshot_data.get("文档编号", "")

            # 收集所有可交付的抽取产物（实体/关系/三元组/向量多来源）
            deliverables = self._collect_deliverables(doc_id)
            if not deliverables:
                return {"error": f"快照 {snapshot_id} 无可交付的抽取产物"}

            # 新一轮交付开始，旧的已完成回执即刻失效，重新核验通过前不对外出具
            self._discard_receipt(snapshot_id)

            deliveries = self._data_dir("deliveries")
            layout = self._build_csv_layout(deliverables) if fmt == "csv" else None
            shards = [deliverables[i:i + shard_size] for i in range(0, len(deliverables), shard_size)]

            shard_entries = []
            output_files = []
            all_confirmed = True
            record_no = 0
            for index, shard in enumerate(shards, start=1):
                name = f"{snapshot_id}_shard_{index:03d}.{fmt}"
                path = deliveries / name
                if fmt == "csv":
                    content, record_no = self._render_csv_shard(shard, layout, record_no)
                    payload = content.encode("utf-8")
                else:
                    payload = "".join(
                        json.dumps(rec, ensure_ascii=False, default=str) + "\n" for rec in shard
                    ).encode("utf-8")
                _atomic_write_bytes(path, payload)
                # 回读确认：分片落盘内容可完整读回且行数一致，才标记 confirmed
                disk = path.read_bytes()
                rows = self._count_shard_rows(fmt, disk)
                confirmed = rows == len(shard)
                all_confirmed = all_confirmed and confirmed
                shard_entries.append({
                    "name": name,
                    "format": fmt,
                    "rows": len(shard),
                    "rows_readback": rows,
                    "bytes": len(disk),
                    "sha256": _sha256_bytes(disk),
                    "status": "confirmed" if confirmed else "failed",
                })
                output_files.append(str(path))

            # 记录明细：分来源计数 + 全量内容摘要，供发布核对与接收方复查
            record_counts: dict[str, int] = {}
            for item in deliverables:
                record_counts[item["type"]] = record_counts.get(item["type"], 0) + 1
            records_digest = _sha256_bytes("\n".join(
                json.dumps(item, ensure_ascii=False, sort_keys=True, default=str) for item in deliverables
            ).encode("utf-8"))

            manifest = {
                "delivery_id": f"DELIVER-{snapshot_id}",
                "snapshot_id": snapshot_id,
                "doc_id": doc_id,
                "target": target,
                "format": self._format_declaration(fmt, layout),
                "shard_size": shard_size,
                "total_records": len(deliverables),
                "record_counts": record_counts,
                "records_digest": records_digest,
                "shards": shard_entries,
                "status": "confirmed" if all_confirmed else "failed",
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            manifest_path = self._manifest_path(snapshot_id)
            _atomic_write_bytes(
                manifest_path,
                json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
            )

            delivery = {
                "交付编号": manifest["delivery_id"],
                "快照编号": snapshot_id,
                "交付格式": fmt,
                "目标系统": target,
                "字段范围": "实体+关系+三元组+向量",
                "记录条数": len(deliverables),
                "分片大小": shard_size,
                "交付时间": manifest["created_at"],
                "清单文件": manifest_path.name,
                "记录摘要": records_digest,
                "状态": manifest["status"],
            }
            self._write_jsonl(deliveries / f"{snapshot_id}_delivery.jsonl", [delivery])

            self._write_checkpoint(f"deliver_{snapshot_id}", {
                "snapshot_id": snapshot_id,
                "format": fmt,
                "target": target,
                "total_records": len(deliverables),
                "shards": len(shards),
                "status": manifest["status"],
                "records_digest": records_digest,
            })

            return {
                "snapshot_id": snapshot_id,
                "delivery_id": manifest["delivery_id"],
                "format": fmt,
                "target": target,
                "total_records": len(deliverables),
                "record_counts": record_counts,
                "records_digest": records_digest,
                "shards": len(shards),
                "status": manifest["status"],
                "manifest": str(manifest_path),
                "output_files": output_files,
            }

    def _find_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """在版本快照产物中查找指定快照记录。"""
        for vf in sorted(self._data_dir("versions").glob("*.jsonl")):
            for snap in self._read_jsonl(vf):
                if snap.get("快照编号") == snapshot_id:
                    return snap
        return None

    def _collect_deliverables(self, doc_id: str) -> list[dict[str, Any]]:
        """收集指定文档的所有可交付抽取产物：实体平铺字段、关系、三元组与含多值的向量。"""
        items = []

        # 收集实体（平铺字段，直接成列）
        entity_files = sorted(self._data_dir("entities").glob(f"{doc_id}*.jsonl"))
        for ef in entity_files:
            for e in self._read_jsonl(ef):
                items.append({"type": "entity", "source": str(ef), "data": e})

        # 收集关系（证据等多值内容原样保留）
        relation_files = sorted(self._data_dir("relations").glob(f"{doc_id}*.jsonl"))
        for rf in relation_files:
            for r in self._read_jsonl(rf):
                items.append({"type": "relation", "source": str(rf), "data": r})

        # 收集图谱三元组
        graph_files = sorted(self._data_dir("graph").glob(f"{doc_id}*.jsonl"))
        for gf in graph_files:
            for t in self._read_jsonl(gf):
                items.append({"type": "triple", "source": str(gf), "data": t})

        # 收集向量（保留完整嵌入向量等多值字段，接收方可恢复完整记录）
        vector_files = sorted(self._data_dir("vectors").glob(f"{doc_id}*.jsonl"))
        for vf in vector_files:
            for v in self._read_jsonl(vf):
                items.append({"type": "vector", "source": str(vf), "data": v})

        return items

    def list_formats(self) -> list[dict[str, Any]]:
        """列出所有支持的交付格式。"""
        return [
            {"format": k, "mime": v["mime"], "description": v["description"], "implemented": v["implemented"]}
            for k, v in self.SUPPORTED_FORMATS.items()
        ]

    # -- CSV 格式转换：列布局、单元格编码与读回 --

    def _build_csv_layout(self, deliverables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """汇总全量记录的字段并推断列类型，同一批交付的所有分片共用同一份列布局。"""
        layout: list[dict[str, Any]] = [
            {"name": "record_type", "role": "record_type", "type": "string"},
            {"name": "record_no", "role": "record_no", "type": "number"},
            {"name": "source_file", "role": "source_file", "type": "string"},
        ]
        data_keys: list[str] = []
        for item in deliverables:
            for key in item["data"].keys():
                if key not in data_keys:
                    data_keys.append(key)
        used_names = {col["name"] for col in layout} | {"present_fields"}
        for key in data_keys:
            values = [item["data"][key] for item in deliverables if key in item["data"]]
            name = key if key not in used_names else f"data_{key}"
            used_names.add(name)
            layout.append({
                "name": name,
                "role": "data",
                "key": key,
                "type": self._infer_column_type(values),
            })
        layout.append({"name": "present_fields", "role": "present_fields", "type": "json"})
        return layout

    @staticmethod
    def _infer_column_type(values: list[Any]) -> str:
        """按整列取值推断列类型：string/number/boolean 直接成列，其余（多值/空值/混合）一律 json。"""
        kinds = set()
        for value in values:
            if value is None:
                kinds.add("null")
            elif isinstance(value, bool):
                kinds.add("boolean")
            elif isinstance(value, (int, float)):
                kinds.add("number")
            elif isinstance(value, str):
                kinds.add("string")
            else:
                kinds.add("complex")
        if kinds == {"string"}:
            return "string"
        if kinds and kinds <= {"number"}:
            return "number"
        if kinds == {"boolean"}:
            return "boolean"
        return "json"

    @staticmethod
    def _encode_cell(value: Any, column_type: str) -> str:
        """把任意取值编码进单元格：多值与空值统一 JSON 编码，保证逗号/引号/换行无歧义读回。"""
        if column_type == "string":
            return value
        if column_type == "number":
            return json.dumps(value)
        if column_type == "boolean":
            return "true" if value else "false"
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

    @staticmethod
    def _decode_cell(cell: str, column_type: str) -> Any:
        """按清单声明的列类型读回单元格，与 _encode_cell 互逆。"""
        if column_type == "string":
            return cell
        if column_type == "boolean":
            return cell.strip().lower() == "true"
        return json.loads(cell)

    def _render_csv_shard(
        self,
        shard: list[dict[str, Any]],
        layout: list[dict[str, Any]],
        record_no: int,
    ) -> tuple[str, int]:
        """按列布局渲染一个 CSV 分片（含表头），返回（文本，下一条记录序号）。"""
        buffer = io.StringIO()
        writer = csv.writer(
            buffer,
            delimiter=self.CSV_DIALECT["delimiter"],
            quotechar=self.CSV_DIALECT["quotechar"],
            doublequote=self.CSV_DIALECT["doublequote"],
            lineterminator=self.CSV_DIALECT["lineterminator"],
        )
        writer.writerow([col["name"] for col in layout])
        for item in shard:
            record_no += 1
            data = item["data"]
            row = []
            for col in layout:
                role = col["role"]
                if role == "record_type":
                    row.append(item["type"])
                elif role == "record_no":
                    row.append(record_no)
                elif role == "source_file":
                    row.append(item["source"])
                elif role == "present_fields":
                    row.append(json.dumps(list(data.keys()), ensure_ascii=False))
                else:
                    key = col["key"]
                    row.append(self._encode_cell(data[key], col["type"]) if key in data else "")
            writer.writerow(row)
        return buffer.getvalue(), record_no

    def _decode_csv_row(
        self,
        header: list[str],
        row: list[str],
        layout: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """把一行 CSV 按列布局还原成 {type, source, data} 完整记录。"""
        cells = dict(zip(header, row))
        present = set(json.loads(cells["present_fields"]))
        data: dict[str, Any] = {}
        for col in layout:
            if col["role"] != "data":
                continue
            key = col["key"]
            if key in present:
                data[key] = self._decode_cell(cells[col["name"]], col["type"])
        return {"type": cells["record_type"], "source": cells["source_file"], "data": data}

    def recover_records(self, snapshot_id: str) -> Any:
        """按交付清单把 CSV 分片逐行读回，恢复成完整记录列表（接收方视角）。"""
        manifest = self.get_manifest(snapshot_id)
        if manifest is None:
            return {"error": f"快照 {snapshot_id} 没有交付清单，请先执行交付"}
        if manifest["format"]["name"] != "csv":
            return {"error": f"交付格式 {manifest['format']['name']} 请按行读取分片恢复记录"}
        layout = manifest["format"]["columns"]
        deliveries = self._data_dir("deliveries")
        recovered: list[tuple[int, dict[str, Any]]] = []
        for entry in manifest["shards"]:
            path = deliveries / entry["name"]
            if not path.exists():
                return {"error": f"分片 {entry['name']} 缺失，无法恢复完整记录"}
            # 按原始字节解码，不做换行符归一，字段内的 \\r\\n 才能无歧义读回
            text = path.read_bytes().decode("utf-8")
            reader = csv.reader(io.StringIO(text), delimiter=",", quotechar='"', doublequote=True)
            rows = list(reader)
            if not rows:
                return {"error": f"分片 {entry['name']} 缺少表头，不符合宣称格式"}
            header = rows[0]
            no_index = header.index("record_no")
            for row in rows[1:]:
                recovered.append((int(row[no_index]), self._decode_csv_row(header, row, layout)))
        recovered.sort(key=lambda pair: pair[0])
        return [item for _, item in recovered]

    # -- 清单、核验与回执 --

    def _manifest_path(self, snapshot_id: str) -> Path:
        return self._data_dir("deliveries") / f"{snapshot_id}_manifest.json"

    def _receipt_path(self, snapshot_id: str) -> Path:
        return self._data_dir("deliveries") / f"{snapshot_id}_receipt.json"

    def _discard_receipt(self, snapshot_id: str) -> None:
        """作废已持久化的回执：重新交付或核验失败时，旧回执不得继续外流。"""
        path = self._receipt_path(snapshot_id)
        if path.exists():
            path.unlink()

    @staticmethod
    def _read_json_file(path: Path) -> dict[str, Any] | None:
        """读取 JSON 文件；原子替换的瞬时窗口内文件可能短暂缺失，按不存在处理。"""
        try:
            return json.loads(path.read_bytes().decode("utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
            return None

    def get_manifest(self, snapshot_id: str) -> dict[str, Any] | None:
        """读取交付清单，不存在、损坏或替换窗口内返回 None。"""
        path = self._manifest_path(snapshot_id)
        if not path.exists():
            return None
        return self._read_json_file(path)

    def get_receipt(self, snapshot_id: str) -> dict[str, Any] | None:
        """读取已完成的交付回执，未核验确认或已作废返回 None。"""
        path = self._receipt_path(snapshot_id)
        if not path.exists():
            return None
        return self._read_json_file(path)

    def _format_declaration(self, fmt: str, layout: list[dict[str, Any]] | None) -> dict[str, Any]:
        """生成清单中的格式声明：方言、编码、列类型，接收方据此无歧义读回。"""
        if fmt == "csv":
            return {
                "name": "csv",
                "mime": self.SUPPORTED_FORMATS["csv"]["mime"],
                "encoding": "utf-8",
                "dialect": dict(self.CSV_DIALECT),
                "header": True,
                "multi_value_encoding": "json",
                "columns": layout,
            }
        return {
            "name": "jsonl",
            "mime": self.SUPPORTED_FORMATS["jsonl"]["mime"],
            "encoding": "utf-8",
            "record_per_line": True,
        }

    def _count_shard_rows(self, fmt: str, payload: bytes) -> int:
        """统计分片数据行数：CSV 按方言解析（字段内换行不误判），JSONL 按非空行。"""
        text = payload.decode("utf-8")
        if fmt == "csv":
            reader = csv.reader(io.StringIO(text), delimiter=",", quotechar='"', doublequote=True)
            return max(len(list(reader)) - 1, 0)
        return sum(1 for line in text.splitlines() if line.strip())

    @staticmethod
    def _read_csv_rows(payload: bytes) -> list[list[str]]:
        """按声明方言解析 CSV 分片全部行（含表头）。"""
        text = payload.decode("utf-8")
        reader = csv.reader(io.StringIO(text), delimiter=",", quotechar='"', doublequote=True)
        return list(reader)

    def verify_delivery(self, snapshot_id: str) -> dict[str, Any]:
        """核验交付分片与记录明细：逐片校验 SHA256/行数/表头，并重新核对源头记录。

        全部确认后出具并持久化可复查回执；任一分片未确认则不出具回执，
        且作废旧回执，保证已完成回执中不会出现未全部确认的文件。
        """
        lock = _lock_for(snapshot_id)
        with lock:
            manifest = self.get_manifest(snapshot_id)
            if manifest is None:
                return {"ok": False, "error": f"快照 {snapshot_id} 没有交付清单，请先执行交付"}
            if manifest.get("status") != "confirmed":
                self._discard_receipt(snapshot_id)
                return {
                    "ok": False,
                    "error": "交付分片未全部确认，不能出具回执",
                    "delivery_id": manifest.get("delivery_id"),
                }

            fmt = manifest["format"]["name"]
            deliveries = self._data_dir("deliveries")
            declared_header = None
            if fmt == "csv":
                declared_header = [col["name"] for col in manifest["format"].get("columns", [])]

            shard_reports = []
            shards_ok = True
            for entry in manifest["shards"]:
                report = {
                    "name": entry["name"],
                    "rows": entry["rows"],
                    "bytes": entry["bytes"],
                    "sha256": entry["sha256"],
                    "status": "confirmed",
                }
                path = deliveries / entry["name"]
                if not path.exists():
                    report["status"] = "missing"
                else:
                    payload = path.read_bytes()
                    if _sha256_bytes(payload) != entry["sha256"]:
                        report["status"] = "checksum_mismatch"
                    elif self._count_shard_rows(fmt, payload) != entry["rows"]:
                        report["status"] = "row_count_mismatch"
                    elif fmt == "csv":
                        rows = self._read_csv_rows(payload)
                        if not rows or rows[0] != declared_header:
                            report["status"] = "format_mismatch"
                        elif any(len(row) != len(declared_header) for row in rows):
                            report["status"] = "format_mismatch"
                if report["status"] != "confirmed":
                    shards_ok = False
                shard_reports.append(report)

            # 记录明细核对：重新收集源头产物，比对数量与内容摘要
            source = self._collect_deliverables(manifest.get("doc_id", ""))
            counts: dict[str, int] = {}
            for item in source:
                counts[item["type"]] = counts.get(item["type"], 0) + 1
            digest = _sha256_bytes("\n".join(
                json.dumps(item, ensure_ascii=False, sort_keys=True, default=str) for item in source
            ).encode("utf-8"))
            records_ok = (
                len(source) == manifest.get("total_records")
                and counts == manifest.get("record_counts")
                and digest == manifest.get("records_digest")
            )

            ok = shards_ok and records_ok
            receipt = {
                "receipt_id": f"RCPT-{manifest['delivery_id']}",
                "delivery_id": manifest["delivery_id"],
                "snapshot_id": snapshot_id,
                "format": fmt,
                "target": manifest.get("target", ""),
                "total_records": manifest["total_records"],
                "record_counts": manifest["record_counts"],
                "records_digest": manifest["records_digest"],
                "shards": shard_reports,
                "checks": {"shards_ok": shards_ok, "records_ok": records_ok},
                "ok": ok,
                "verified_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if ok:
                _atomic_write_bytes(
                    self._receipt_path(snapshot_id),
                    json.dumps(receipt, ensure_ascii=False, indent=2).encode("utf-8"),
                )
            else:
                self._discard_receipt(snapshot_id)
            return {"ok": ok, "receipt": receipt, "error": None if ok else "交付核验未通过"}

    def list_shards(self, snapshot_id: str) -> dict[str, Any]:
        """列出交付分片：有清单按清单返回，无清单回退到历史 JSONL 分片。"""
        manifest = self.get_manifest(snapshot_id)
        if manifest is not None:
            return {
                "delivery_id": manifest["delivery_id"],
                "snapshot_id": snapshot_id,
                "format": manifest["format"]["name"],
                "status": manifest["status"],
                "shards": manifest["shards"],
            }
        deliveries = self._data_dir("deliveries")
        legacy = sorted(deliveries.glob(f"{snapshot_id}_shard_*.jsonl"))
        return {
            "delivery_id": f"DELIVER-{snapshot_id}",
            "snapshot_id": snapshot_id,
            "format": "jsonl",
            "status": "legacy",
            "shards": [
                {"name": p.name, "format": "jsonl", "bytes": p.stat().st_size, "status": "legacy"}
                for p in legacy
            ],
        }

    def shard_download_info(self, snapshot_id: str, shard_name: str) -> dict[str, Any] | None:
        """校验分片可下载性并返回下载信息，未全部确认或内容不符宣称格式返回 None。

        - 清单内分片：必须 confirmed 且落盘内容 SHA256 与清单一致才允许下载；
        - 历史 JSONL 分片：无清单或不在当前清单内的旧分片，逐行校验 JSON
          可解析后按历史格式继续提供下载，保证旧交付方式可用。
        """
        if not shard_name or shard_name != os.path.basename(shard_name) or shard_name.startswith("."):
            return None
        if not shard_name.startswith(f"{snapshot_id}_shard_"):
            return None
        deliveries = self._data_dir("deliveries")
        path = deliveries / shard_name
        manifest = self.get_manifest(snapshot_id)

        if manifest is not None and manifest.get("status") == "confirmed":
            entry = next((s for s in manifest["shards"] if s["name"] == shard_name), None)
            if entry is not None:
                try:
                    payload = path.read_bytes()
                except FileNotFoundError:
                    return None
                if _sha256_bytes(payload) != entry["sha256"]:
                    return None
                fmt = manifest["format"]["name"]
                return {
                    "path": path,
                    "media_type": self.SUPPORTED_FORMATS[fmt]["mime"],
                    "sha256": entry["sha256"],
                    "format": fmt,
                    "legacy": False,
                }
            # 不在当前清单内：可能是上一次 JSONL 交付的历史分片，走兼容回退

        if shard_name.endswith(".jsonl") and path.exists():
            try:
                payload = path.read_bytes()
            except FileNotFoundError:
                return None
            try:
                for line in payload.decode("utf-8").splitlines():
                    if line.strip():
                        json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                return None
            return {
                "path": path,
                "media_type": self.SUPPORTED_FORMATS["jsonl"]["mime"],
                "sha256": _sha256_bytes(payload),
                "format": "jsonl",
                "legacy": True,
            }
        return None
