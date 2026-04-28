"""
SQLite存储模块 - 异步写入调用记录
"""
import aiosqlite
import json
from pathlib import Path
from typing import Optional


class CallStorage:
    """LLM调用记录存储"""
    
    def __init__(self, db_path: str = "./llm_calls.db"):
        self.db_path = db_path
        self._initialized = False
    
    async def initialize(self):
        """初始化数据库表"""
        if self._initialized:
            return
        
        # 确保数据库目录存在
        db_dir = Path(self.db_path).parent
        if str(db_dir) != "." and not db_dir.exists():
            db_dir.mkdir(parents=True, exist_ok=True)
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS llm_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT NOT NULL UNIQUE,
                    timestamp TEXT NOT NULL,
                    url TEXT NOT NULL,
                    method TEXT,
                    request_headers TEXT,
                    request_body TEXT,
                    response_headers TEXT,
                    response_body TEXT,
                    duration_ms INTEGER,
                    tokens_input INTEGER,
                    tokens_output INTEGER,
                    token_source TEXT,
                    stream_type TEXT,
                    first_token_ms INTEGER,
                    original_model TEXT,
                    overridden_model TEXT
                )
            """)
            await db.commit()
        
        self._initialized = True
    
    async def save_call(
        self,
        call_id: str,
        timestamp: str,
        url: str,
        method: str,
        request_headers: dict,
        request_body: str,
        response_headers: dict,
        response_body: str,
        duration_ms: int,
        tokens_input: int,
        tokens_output: int,
        token_source: str,
        stream_type: str = "non_stream",
        first_token_ms: Optional[int] = None,
        original_model: str = None,
        overridden_model: str = None
    ):
        """保存一次LLM调用记录"""
        await self.initialize()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO llm_calls (
                    call_id, timestamp, url, method,
                    request_headers, request_body,
                    response_headers, response_body,
                    duration_ms, tokens_input, tokens_output, token_source,
                    stream_type, first_token_ms,
                    original_model, overridden_model
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                call_id,
                timestamp,
                url,
                method,
                json.dumps(request_headers, ensure_ascii=False),
                request_body,
                json.dumps(response_headers, ensure_ascii=False),
                response_body,
                duration_ms,
                tokens_input,
                tokens_output,
                token_source,
                stream_type,
                first_token_ms,
                original_model,
                overridden_model
            ))
            await db.commit()
    
    async def get_calls(self, limit: int = 100, offset: int = 0) -> list:
        """查询调用记录"""
        await self.initialize()
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM llm_calls 
                ORDER BY timestamp DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def get_call_count(self) -> int:
        """获取总调用次数"""
        await self.initialize()
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM llm_calls") as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
