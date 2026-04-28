"""
SQLite/PostgreSQL存储模块 - 异步写入调用记录
"""
import json
from pathlib import Path
from typing import Optional

from src.config import PostgreSQLConfig


class CallStorage:
    """LLM调用记录存储"""

    def __init__(self, db_path: str = "./data/llm_calls.db", postgresql: PostgreSQLConfig = None):
        self.db_path = db_path
        self.postgresql = postgresql
        self._initialized = False
        self._use_postgres = postgresql is not None

    async def _get_pg_conn(self):
        """获取PostgreSQL连接"""
        import asyncpg
        return await asyncpg.connect(
            host=self.postgresql.host,
            port=self.postgresql.port,
            user=self.postgresql.user,
            password=self.postgresql.password,
            database=self.postgresql.dbname
        )

    async def initialize(self):
        """初始化数据库表"""
        if self._initialized:
            return

        if self._use_postgres:
            conn = await self._get_pg_conn()
            try:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS llm_calls (
                        id SERIAL PRIMARY KEY,
                        call_id TEXT NOT NULL UNIQUE,
                        timestamp TEXT NOT NULL,
                        url TEXT NOT NULL,
                        method TEXT,
                        request_headers JSON,
                        request_body TEXT,
                        response_headers JSON,
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
            finally:
                await conn.close()
        else:
            import aiosqlite
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

        if self._use_postgres:
            conn = await self._get_pg_conn()
            try:
                await conn.execute("""
                    INSERT INTO llm_calls (
                        call_id, timestamp, url, method,
                        request_headers, request_body,
                        response_headers, response_body,
                        duration_ms, tokens_input, tokens_output, token_source,
                        stream_type, first_token_ms,
                        original_model, overridden_model
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                """,
                    call_id, timestamp, url, method,
                    json.dumps(request_headers, ensure_ascii=False),
                    request_body,
                    json.dumps(response_headers, ensure_ascii=False),
                    response_body,
                    duration_ms, tokens_input, tokens_output, token_source,
                    stream_type, first_token_ms,
                    original_model, overridden_model
                )
            finally:
                await conn.close()
        else:
            import aiosqlite
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
                    call_id, timestamp, url, method,
                    json.dumps(request_headers, ensure_ascii=False),
                    request_body,
                    json.dumps(response_headers, ensure_ascii=False),
                    response_body,
                    duration_ms, tokens_input, tokens_output, token_source,
                    stream_type, first_token_ms,
                    original_model, overridden_model
                ))
                await db.commit()

    async def get_calls(self, limit: int = 100, offset: int = 0) -> list:
        """查询调用记录"""
        await self.initialize()

        if self._use_postgres:
            conn = await self._get_pg_conn()
            try:
                rows = await conn.fetch("""
                    SELECT * FROM llm_calls
                    ORDER BY timestamp DESC
                    LIMIT $1 OFFSET $2
                """, limit, offset)
                return [dict(r) for r in rows]
            finally:
                await conn.close()
        else:
            import aiosqlite
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

        if self._use_postgres:
            conn = await self._get_pg_conn()
            try:
                row = await conn.fetchrow("SELECT COUNT(*) FROM llm_calls")
                return row["count"] if row else 0
            finally:
                await conn.close()
        else:
            import aiosqlite
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM llm_calls") as cursor:
                    row = await cursor.fetchone()
                    return row[0] if row else 0
