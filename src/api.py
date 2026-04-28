"""
查询REST API - FastAPI提供调用记录查询接口
"""
from fastapi import FastAPI, Query
from typing import Optional
import uvicorn

from src.storage import CallStorage


def create_query_api(storage: CallStorage) -> FastAPI:
    """创建查询API应用"""
    
    app = FastAPI(
        title="LLM Router Query API",
        description="查询LLM调用记录",
        version="1.0.0"
    )
    
    @app.get("/api/calls")
    async def get_calls(
        limit: int = Query(default=100, ge=1, le=1000, description="返回记录数"),
        offset: int = Query(default=0, ge=0, description="偏移量")
    ):
        """获取调用记录列表"""
        calls = await storage.get_calls(limit=limit, offset=offset)
        total = await storage.get_call_count()
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "calls": calls
        }
    
    @app.get("/api/calls/{call_id}")
    async def get_call(call_id: int):
        """获取单条调用记录"""
        calls = await storage.get_calls(limit=1, offset=0)
        # 这里需要优化，应该按ID查询
        for call in calls:
            if call["id"] == call_id:
                return call
        return {"error": "Call not found"}
    
    @app.get("/api/stats")
    async def get_stats():
        """获取统计信息"""
        total = await storage.get_call_count()
        return {
            "total_calls": total
        }
    
    @app.get("/health")
    async def health_check():
        """健康检查"""
        return {"status": "ok"}
    
    return app


def start_query_api(storage: CallStorage, port: int = 38889):
    """启动查询API服务器"""
    app = create_query_api(storage)
    uvicorn.run(app, host="0.0.0.0", port=port)
