"""业务路由汇总：交付专用路由在前，通用资源路由在后，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import delivery, resource

ROUTERS = [delivery, resource]
