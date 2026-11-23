"""结果交付 CSV 分片与回执核验测试。

覆盖：多值字段与引号快照的 CSV 往返恢复、宣称格式一致性、并发交付与发布
不出未确认回执、发布前核验拦截、历史 JSONL 下载兼容。
"""
import csv
import hashlib
import io
import json
import os
import sys
import threading
import uuid

import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.services.format_parse import FormatParseService
from app.services.chunk_split import ChunkSplitService
from app.services.entity_recognize import EntityRecognizeService
from app.services.relation_extract import RelationExtractService
from app.services.kg_build import KgBuildService
from app.services.vector_embed import VectorEmbedService
from app.services.doc_version import DocVersionService
from app.services.result_deliver import ResultDeliverService
from app.services.base import DATA_ROOT


def _uid(prefix="ID"):
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def _nasty_entities(doc_id):
    """含逗号、引号、换行、多值、空值与布尔字段的实体记录。"""
    return [
        {
            "entity_id": "ENT-X001",
            "chunk_id": "CHUNK-X001",
            "entity_type": "ORG",
            "entity_text": '逗号,引号"换行\n公司',
            "aliases": ["别名甲,带逗号", '别名"乙"带引号', "别名丙\n带换行", "别名丁\r\n带CRLF"],
            "start_offset": 0,
            "end_offset": 9,
            "confidence": 0.95,
            "model": None,
            "verified": True,
            "model_version": "v3.1.0",
            "recognized_at": 1700000000.123456,
            "备注": '多行\n文本\r\nCRLF,逗号"引号"',
        },
        {
            "entity_id": "ENT-X002",
            "chunk_id": "CHUNK-X002",
            "entity_type": "PERSON",
            "entity_text": '张"三"',
            "aliases": [],
            "start_offset": 3,
            "end_offset": 6,
            "confidence": 0.5,
            "model": "NER-BiLSTM-CRF",
            "verified": False,
            "model_version": "v3.1.0",
            "recognized_at": 1700000001.0,
            "备注": "",
        },
    ]


def _nasty_relations(doc_id):
    return [
        {
            "relation_id": "REL-X001",
            "head_entity": '头实体,"甲"\n',
            "head_type": "ORG",
            "tail_entity": "尾实体乙",
            "tail_type": "LOC",
            "relation_type": "LOCATE",
            "confidence": 0.88,
            "evidence_sentence": '证据，含逗号\n与"引号"的句子',
            "evidence_parts": ["片段1,逗号", '片段2"引号"', "片段3\n换行"],
            "method": "Relation-BERT",
            "model_version": "v2.3.0",
            "extracted_at": 1700000002.5,
        }
    ]


def _nasty_vectors(doc_id):
    return [
        {
            "chunk_id": "CHUNK-X001",
            "doc_id": doc_id,
            "vector_dim": 4,
            "vector": [0.1, -0.25, 0.3333333333333333, 1e-07],
            "model": 'test,"model"\n带引号逗号换行',
            "text_length": 10,
            "index": "vector_index/CHUNK-X001",
            "embedded_at": 1700000003.25,
            "labels": ["标签,甲", '标签"乙"'],
        }
    ]


def _build_snapshot(with_nasty=True, extra_entity_count=0):
    """跑通解析→分块→实体→关系→图谱→向量→快照链路，并可注入特殊记录。"""
    doc_id = _uid("DD")
    parse_svc = FormatParseService()
    task, _ = parse_svc.create_entry({
        "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
    })
    parse_svc.execute_parse(task["id"])
    ChunkSplitService().split_from_document(f"{doc_id}_parsed")
    EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
    RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
    KgBuildService().build_from_relations(f"{doc_id}_parsed")
    VectorEmbedService().embed_from_chunk(f"{doc_id}_parsed")

    svc = ResultDeliverService()
    if with_nasty:
        svc._write_jsonl(DATA_ROOT / "entities" / f"{doc_id}_parsed_entities.jsonl",
                         _nasty_entities(doc_id))
        svc._write_jsonl(DATA_ROOT / "relations" / f"{doc_id}_parsed_relations.jsonl",
                         _nasty_relations(doc_id))
        svc._write_jsonl(DATA_ROOT / "vectors" / f"{doc_id}_parsed_vectors.jsonl",
                         _nasty_vectors(doc_id))
    if extra_entity_count:
        bulk = [
            {
                "entity_id": f"ENT-BULK-{i:06d}",
                "chunk_id": f"CHUNK-BULK-{i % 7}",
                "entity_type": "ORG",
                "entity_text": f"批量实体{i}，含,逗号",
                "start_offset": i,
                "end_offset": i + 3,
                "confidence": 0.8,
                "model": "NER-BiLSTM-CRF",
                "model_version": "v3.1.0",
                "recognized_at": 1700000100.0 + i,
            }
            for i in range(extra_entity_count)
        ]
        svc._write_jsonl(DATA_ROOT / "entities" / f"{doc_id}_parsed_entities.jsonl", bulk)

    snap_result = DocVersionService().create_snapshot(doc_id, "v1.0")
    assert "error" not in snap_result, snap_result.get("error", "")
    return doc_id, snap_result["snapshot"]["快照编号"]


def _deliveries():
    return DATA_ROOT / "deliveries"


class TestCsvDeliveryRoundTrip:
    """含多值字段与引号的快照：CSV 交付后按清单恢复完整记录并对比原记录。"""

    def test_csv_delivery_confirmed_with_manifest(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        result = svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=7)
        assert "error" not in result, result.get("error", "")
        assert result["status"] == "confirmed"
        assert result["shards"] > 1, "小分片尺寸应产生多个分片"

        manifest = svc.get_manifest(snapshot_id)
        assert manifest is not None
        assert manifest["status"] == "confirmed"
        assert manifest["format"]["name"] == "csv"
        assert manifest["total_records"] == result["total_records"]
        assert set(manifest["record_counts"]) >= {"entity", "relation", "vector"}
        assert all(s["status"] == "confirmed" for s in manifest["shards"])

    def test_csv_recovers_complete_records(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        result = svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=7)
        assert result["status"] == "confirmed"

        recovered = svc.recover_records(snapshot_id)
        assert isinstance(recovered, list), recovered
        expected = svc._collect_deliverables(doc_id)
        assert len(recovered) == len(expected)
        # 逐条对比：多值向量、证据列表、空值、布尔、浮点全部精确恢复
        assert recovered == expected

    def test_csv_special_chars_read_back_unambiguously(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        result = svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=500)
        assert result["status"] == "confirmed"
        manifest = svc.get_manifest(snapshot_id)
        header = [c["name"] for c in manifest["format"]["columns"]]

        rows = []
        for entry in manifest["shards"]:
            payload = (_deliveries() / entry["name"]).read_bytes()
            rows.extend(list(csv.reader(io.StringIO(payload.decode("utf-8"))))[1:])

        by_id = {}
        for row in rows:
            cells = dict(zip(header, row))
            if cells["record_type"] == "entity" and cells.get("entity_id", "").startswith("ENT-X"):
                by_id[cells["entity_id"]] = cells

        # 逗号、引号、换行（含 CRLF）在字段内无歧义读回
        assert by_id["ENT-X001"]["entity_text"] == '逗号,引号"换行\n公司'
        assert by_id["ENT-X001"]["备注"] == '多行\n文本\r\nCRLF,逗号"引号"'
        assert by_id["ENT-X002"]["entity_text"] == '张"三"'
        # 多值单元格按 JSON 读回
        assert json.loads(by_id["ENT-X001"]["aliases"]) == [
            "别名甲,带逗号", '别名"乙"带引号', "别名丙\n带换行", "别名丁\r\n带CRLF",
        ]
        # 空值与布尔按声明类型读回
        assert json.loads(by_id["ENT-X001"]["model"]) is None
        assert by_id["ENT-X001"]["verified"] == "true"
        assert by_id["ENT-X002"]["verified"] == "false"

    def test_csv_vector_multi_value_roundtrip(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=500)
        recovered = svc.recover_records(snapshot_id)
        vectors = [r for r in recovered if r["type"] == "vector"]
        nasty = [v for v in vectors if v["data"].get("chunk_id") == "CHUNK-X001"]
        assert len(nasty) == 1
        assert nasty[0]["data"]["vector"] == [0.1, -0.25, 0.3333333333333333, 1e-07]
        assert nasty[0]["data"]["labels"] == ["标签,甲", '标签"乙"']
        # 流水线产出的 1024 维向量也完整恢复
        full = [v for v in vectors if v["data"].get("vector_dim") == 1024]
        assert full and all(len(v["data"]["vector"]) == 1024 for v in full)


class TestDeclaredFormatConformance:
    """每个成功下载的分片都符合清单宣称的格式。"""

    def test_shards_match_declared_format(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        result = svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=5)
        assert result["status"] == "confirmed"
        manifest = svc.get_manifest(snapshot_id)
        declared_cols = [c["name"] for c in manifest["format"]["columns"]]
        dialect = manifest["format"]["dialect"]

        total_rows = 0
        for entry in manifest["shards"]:
            payload = (_deliveries() / entry["name"]).read_bytes()
            # SHA256 与字节数符合清单宣称
            assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
            assert len(payload) == entry["bytes"]
            # 按宣称方言解析：表头一致、每行宽度一致、行数一致
            rows = list(csv.reader(
                io.StringIO(payload.decode("utf-8")),
                delimiter=dialect["delimiter"],
                quotechar=dialect["quotechar"],
                doublequote=dialect["doublequote"],
            ))
            assert rows[0] == declared_cols
            assert all(len(row) == len(declared_cols) for row in rows)
            assert len(rows) - 1 == entry["rows"]
            total_rows += entry["rows"]
        assert total_rows == manifest["total_records"]

    def test_download_info_matches_declared_content(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=9)
        manifest = svc.get_manifest(snapshot_id)
        for entry in manifest["shards"]:
            info = svc.shard_download_info(snapshot_id, entry["name"])
            assert info is not None, f"已确认分片 {entry['name']} 应可下载"
            assert info["media_type"] == "text/csv"
            payload = info["path"].read_bytes()
            assert hashlib.sha256(payload).hexdigest() == info["sha256"] == entry["sha256"]

    def test_tampered_shard_rejected(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=9)
        manifest = svc.get_manifest(snapshot_id)
        victim = _deliveries() / manifest["shards"][0]["name"]
        victim.write_bytes(victim.read_bytes() + b"tampered")
        # 内容与宣称不符：不允许下载，核验不通过
        assert svc.shard_download_info(snapshot_id, victim.name) is None
        assert svc.verify_delivery(snapshot_id)["ok"] is False


class TestVerifyReceiptAndPublish:
    """核验回执与发布核对。"""

    def test_verify_issues_reviewable_receipt(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=6)
        result = svc.verify_delivery(snapshot_id)
        assert result["ok"] is True
        receipt = result["receipt"]
        # 回执可复查：与清单一致的分片、记录明细与摘要
        manifest = svc.get_manifest(snapshot_id)
        assert [s["name"] for s in receipt["shards"]] == [s["name"] for s in manifest["shards"]]
        assert all(s["status"] == "confirmed" for s in receipt["shards"])
        assert receipt["record_counts"] == manifest["record_counts"]
        assert receipt["records_digest"] == manifest["records_digest"]
        assert receipt["checks"] == {"shards_ok": True, "records_ok": True}
        # 回执已持久化，可再次读取复查
        assert svc.get_receipt(snapshot_id) == receipt

    def test_publish_blocked_when_verification_fails(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        version_svc = DocVersionService()
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=8)
        victim = _deliveries() / f"{snapshot_id}_shard_001.csv"
        victim.write_bytes(victim.read_bytes() + b"corrupt")

        published = version_svc.publish_snapshot(snapshot_id)
        assert "error" in published, "分片被篡改时不应发布"
        assert not (_data_versions() / f"{snapshot_id}_published.jsonl").exists()
        # 未全部确认：不出具已完成回执
        assert svc.get_receipt(snapshot_id) is None

        # 修复（重新交付）后发布成功并返回可复查回执
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=8)
        published = version_svc.publish_snapshot(snapshot_id)
        assert published.get("status") == "已发布"
        assert published["delivery_receipt"]["ok"] is True

    def test_publish_with_delivery_returns_receipt(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=10)
        published = DocVersionService().publish_snapshot(snapshot_id)
        assert published.get("status") == "已发布"
        receipt = published.get("delivery_receipt")
        assert receipt is not None and receipt["ok"] is True
        assert all(s["status"] == "confirmed" for s in receipt["shards"])

    def test_publish_without_delivery_keeps_legacy_behavior(self):
        doc_id = _uid("DD")
        snapshot_id = _uid("SNAP")
        svc = ResultDeliverService()
        svc._write_jsonl(DATA_ROOT / "versions" / f"{doc_id}_vv9.jsonl", [{
            "快照编号": snapshot_id, "文档编号": doc_id, "版本号": "v9",
            "变更摘要": "手工登记", "分块数量": 0, "实体数量": 0,
            "生成方式": "手工", "生成时间": "2026-11-23 00:00:00",
        }])
        published = DocVersionService().publish_snapshot(snapshot_id)
        assert published.get("status") == "已发布"
        assert "delivery_receipt" not in published


def _data_versions():
    return DATA_ROOT / "versions"


class TestConcurrentDeliverAndPublish:
    """发布与交付请求并发时，没有全部确认的文件不能出现在已完成回执中。"""

    def test_concurrent_deliver_verify_publish(self):
        doc_id, snapshot_id = _build_snapshot(extra_entity_count=1200)
        svc = ResultDeliverService()
        version_svc = DocVersionService()
        errors = []
        observations = []
        stop = threading.Event()

        def deliver_loop():
            try:
                for _ in range(3):
                    result = svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=50)
                    if "error" in result:
                        errors.append(result["error"])
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

        def observe_loop():
            while not stop.is_set():
                manifest = svc.get_manifest(snapshot_id)
                if manifest and manifest.get("status") == "confirmed":
                    # confirmed 清单中的分片必须已完整落盘
                    for entry in manifest["shards"]:
                        if not (_deliveries() / entry["name"]).exists():
                            observations.append(f"分片未落盘却进入confirmed清单: {entry['name']}")
                receipt = svc.get_receipt(snapshot_id)
                if receipt is not None:
                    # 已完成回执中不允许出现未确认分片
                    if not receipt.get("ok"):
                        observations.append("回执ok不为True")
                    for shard in receipt["shards"]:
                        if shard["status"] != "confirmed":
                            observations.append(f"回执含未确认分片: {shard['name']}")

        def publish_loop():
            while not stop.is_set():
                published = version_svc.publish_snapshot(snapshot_id)
                if "error" not in published:
                    receipt = published.get("delivery_receipt")
                    if receipt is not None and not receipt.get("ok"):
                        observations.append("发布返回的回执未通过核验")

        threads = [
            threading.Thread(target=deliver_loop),
            threading.Thread(target=observe_loop),
            threading.Thread(target=observe_loop),
            threading.Thread(target=publish_loop),
        ]
        for t in threads:
            t.start()
        threads[0].join()
        stop.set()
        for t in threads[1:]:
            t.join()

        assert not errors, f"并发交付出错: {errors}"
        assert not observations, f"并发期间出现未确认文件外流: {observations[:5]}"

        # 收敛后：核验通过、回执与磁盘分片一致
        final = svc.verify_delivery(snapshot_id)
        assert final["ok"] is True
        receipt = svc.get_receipt(snapshot_id)
        assert receipt is not None
        for shard in receipt["shards"]:
            payload = (_deliveries() / shard["name"]).read_bytes()
            assert hashlib.sha256(payload).hexdigest() == shard["sha256"]


class TestJsonlCompatibility:
    """历史数据的 JSONL 下载方式继续可用。"""

    def test_jsonl_delivery_roundtrip(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        result = svc.deliver_from_snapshot(snapshot_id, fmt="jsonl", shard_size=11)
        assert result["status"] == "confirmed"
        manifest = svc.get_manifest(snapshot_id)
        assert manifest["format"]["name"] == "jsonl"

        records = []
        for entry in manifest["shards"]:
            payload = (_deliveries() / entry["name"]).read_bytes().decode("utf-8")
            for line in payload.splitlines():
                if line.strip():
                    records.append(json.loads(line))
        assert records == svc._collect_deliverables(doc_id)

    def test_legacy_jsonl_download_without_manifest(self):
        svc = ResultDeliverService()
        snapshot_id = _uid("SNAP")
        legacy_file = _deliveries() / f"{snapshot_id}_shard_001.jsonl"
        svc._write_jsonl(legacy_file, [
            {"type": "entity", "source": "old_pipeline", "data": {"entity_id": "E1", "entity_text": "旧实体"}},
        ])
        assert svc.get_manifest(snapshot_id) is None
        info = svc.shard_download_info(snapshot_id, legacy_file.name)
        assert info is not None, "无清单的历史 JSONL 分片应继续可下载"
        assert info["legacy"] is True
        assert info["media_type"] == "application/x-ndjson"
        lines = [json.loads(l) for l in info["path"].read_bytes().decode("utf-8").splitlines() if l.strip()]
        assert lines[0]["data"]["entity_text"] == "旧实体"

    def test_old_jsonl_shards_still_downloadable_after_csv_delivery(self):
        doc_id, snapshot_id = _build_snapshot()
        svc = ResultDeliverService()
        # 快照创建时已自动完成一次 JSONL 交付，再覆盖一次 CSV 交付
        result = svc.deliver_from_snapshot(snapshot_id, fmt="csv", shard_size=13)
        assert result["status"] == "confirmed"
        assert svc.get_manifest(snapshot_id)["format"]["name"] == "csv"
        # 旧 JSONL 分片不在新清单内，但按历史方式仍可下载
        info = svc.shard_download_info(snapshot_id, f"{snapshot_id}_shard_001.jsonl")
        assert info is not None and info["legacy"] is True
        payload = info["path"].read_bytes().decode("utf-8")
        for line in payload.splitlines():
            if line.strip():
                json.loads(line)


class TestDeliveryApi:
    """HTTP 层：交付→清单→核验→回执→下载→发布全流程。"""

    @pytest.fixture(autouse=True)
    def _client(self):
        httpx = pytest.importorskip("httpx")
        from fastapi.testclient import TestClient
        from app.main import app
        self.client = TestClient(app)

    def test_full_delivery_flow_over_http(self):
        doc_id, snapshot_id = _build_snapshot()
        client = self.client

        r = client.post("/api/delivery/deliver",
                        json={"snapshot_id": snapshot_id, "format": "csv", "shard_size": 6})
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "confirmed"

        m = client.get(f"/api/delivery/{snapshot_id}/manifest")
        assert m.status_code == 200
        manifest = m.json()
        assert manifest["format"]["dialect"]["delimiter"] == ","

        v = client.post(f"/api/delivery/{snapshot_id}/verify")
        assert v.status_code == 200 and v.json()["ok"] is True

        rc = client.get(f"/api/delivery/{snapshot_id}/receipt")
        assert rc.status_code == 200
        assert all(s["status"] == "confirmed" for s in rc.json()["shards"])

        shard_name = manifest["shards"][0]["name"]
        d = client.get(f"/api/delivery/{snapshot_id}/download/{shard_name}")
        assert d.status_code == 200
        assert d.headers["content-type"].startswith("text/csv")
        assert d.headers["x-checksum-sha256"] == manifest["shards"][0]["sha256"]
        assert hashlib.sha256(d.content).hexdigest() == manifest["shards"][0]["sha256"]
        rows = list(csv.reader(io.StringIO(d.content.decode("utf-8"))))
        assert rows[0] == [c["name"] for c in manifest["format"]["columns"]]

        p = client.post(f"/api/delivery/publish/{snapshot_id}")
        assert p.status_code == 200, p.text
        assert p.json()["delivery_receipt"]["ok"] is True

    def test_http_receipt_and_download_guards(self):
        doc_id, snapshot_id = _build_snapshot()
        client = self.client
        # 未交付：无清单 404，无回执 409
        assert client.get(f"/api/delivery/{_uid('SNAP')}/manifest").status_code == 404
        assert client.get(f"/api/delivery/{snapshot_id}/receipt").status_code == 409
        # 交付后未核验：回执 409
        client.post("/api/delivery/deliver",
                    json={"snapshot_id": snapshot_id, "format": "csv", "shard_size": 500})
        # 核验通过前有旧回执则已被作废
        svc = ResultDeliverService()
        svc._discard_receipt(snapshot_id)
        assert client.get(f"/api/delivery/{snapshot_id}/receipt").status_code == 409
        # 历史 JSONL 分片兼容下载
        legacy = client.get(f"/api/delivery/{snapshot_id}/download/{snapshot_id}_shard_001.jsonl")
        assert legacy.status_code == 200
        assert legacy.headers["x-delivery-format"] == "jsonl"
        for line in legacy.content.decode("utf-8").splitlines():
            if line.strip():
                json.loads(line)
        # 篡改分片后禁止下载
        victim = _deliveries() / f"{snapshot_id}_shard_001.csv"
        victim.write_bytes(victim.read_bytes() + b"x")
        assert client.get(f"/api/delivery/{snapshot_id}/download/{victim.name}").status_code == 409

    def test_formats_and_resource_router_still_work(self):
        client = self.client
        formats = client.get("/api/delivery/formats")
        assert formats.status_code == 200
        names = {item["format"] for item in formats.json()["items"]}
        assert {"jsonl", "csv"} <= names
        # 通用资源路由不受影响
        assert client.get("/api/health").status_code == 200
        assert client.get("/api/result_deliver").status_code == 200
