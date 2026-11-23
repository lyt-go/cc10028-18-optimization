"""结果交付专用接口：分片交付、交付清单、核验回执与分片下载。

与通用资源路由分离：交付分片的格式声明、核验与回执有独立契约，
路由注册在通用资源路由之前，避免被 /api/{module} 动态路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.services.doc_version import DocVersionService
from app.services.result_deliver import ResultDeliverService

router = APIRouter(prefix="/api/delivery", tags=["结果交付"])


class DeliverPayload(BaseModel):
    snapshot_id: str
    format: str = "csv"
    shard_size: int = Field(default=500, ge=1)
    target: str = "下游知识库"


def _deliver_service() -> ResultDeliverService:
    return ResultDeliverService()


@router.get("/formats")
def list_formats() -> dict[str, Any]:
    """列出支持的交付格式（jsonl 历史格式与 csv 可核验分片）。"""
    return {"items": _deliver_service().list_formats()}


@router.post("/deliver")
def deliver(payload: DeliverPayload) -> dict[str, Any]:
    """按格式导出交付分片：原子落盘、逐片确认后生成可核验交付清单。"""
    result = _deliver_service().deliver_from_snapshot(
        payload.snapshot_id,
        fmt=payload.format,
        shard_size=payload.shard_size,
        target=payload.target,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/publish/{snapshot_id}")
def publish_snapshot(snapshot_id: str) -> dict[str, Any]:
    """发布版本快照：先核对交付分片与记录明细，核验通过才发布并返回回执。"""
    result = DocVersionService().publish_snapshot(snapshot_id)
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.get("/{snapshot_id}/manifest")
def get_manifest(snapshot_id: str) -> dict[str, Any]:
    """读取交付清单：宣称的格式方言、列类型、分片 SHA256 与记录明细。"""
    manifest = _deliver_service().get_manifest(snapshot_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"快照 {snapshot_id} 没有交付清单")
    return manifest


@router.get("/{snapshot_id}/shards")
def list_shards(snapshot_id: str) -> dict[str, Any]:
    """列出交付分片；无清单的历史交付回退列出 JSONL 分片。"""
    return _deliver_service().list_shards(snapshot_id)


@router.post("/{snapshot_id}/verify")
def verify_delivery(snapshot_id: str) -> dict[str, Any]:
    """核验交付分片与记录明细，全部确认后出具可复查回执。"""
    result = _deliver_service().verify_delivery(snapshot_id)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result.get("error", "交付核验未通过"))
    return result


@router.get("/{snapshot_id}/receipt")
def get_receipt(snapshot_id: str) -> dict[str, Any]:
    """获取已完成的交付回执；未全部确认时返回 409，不出具回执。"""
    receipt = _deliver_service().get_receipt(snapshot_id)
    if receipt is None:
        raise HTTPException(status_code=409, detail="交付未完成核验确认，暂无已完成回执")
    return receipt


@router.get("/{snapshot_id}/download/{shard_name}")
def download_shard(snapshot_id: str, shard_name: str) -> FileResponse:
    """下载交付分片：清单内分片校验 SHA256 后按宣称格式提供，历史 JSONL 分片兼容下载。"""
    info = _deliver_service().shard_download_info(snapshot_id, shard_name)
    if info is None:
        raise HTTPException(status_code=409, detail="分片不存在、未全部确认或内容与宣称格式不符，不能下载")
    return FileResponse(
        info["path"],
        media_type=info["media_type"],
        filename=shard_name,
        headers={
            "X-Checksum-Sha256": info["sha256"],
            "X-Delivery-Format": info["format"],
        },
    )
