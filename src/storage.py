"""
SQLite/PostgreSQL存储模块 - 异步写入调用记录
"""
import json
from pathlib import Path
from typing import Optional
from datetime import date

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

    # ========== 认证相关方法（同步，供 proxy 和 console_api 调用） ==========

    def init_auth_tables(self, is_pg: bool = False, cur=None, conn=None):
        """初始化认证表（同步，启动时调用）"""
        # users 表
        if is_pg:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    name VARCHAR(100) NOT NULL,
                    key VARCHAR(64) UNIQUE NOT NULL,
                    expires_at DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT true
                )
            """)
            # llm_calls 表增加 user_id 和 api_key_id 字段
            try:
                cur.execute("ALTER TABLE llm_calls ADD COLUMN user_id INTEGER")
            except Exception:
                pass  # 字段已存在
            try:
                cur.execute("ALTER TABLE llm_calls ADD COLUMN api_key_id INTEGER")
            except Exception:
                pass
            conn.commit()
        else:
            import sqlite3
            # 用默认连接创建
            db_conn = sqlite3.connect(self.db_path)
            db_cur = db_conn.cursor()
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login_at TIMESTAMP
                )
            """)
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    name TEXT NOT NULL,
                    key TEXT UNIQUE NOT NULL,
                    expires_at DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1
                )
            """)
            try:
                db_cur.execute("ALTER TABLE llm_calls ADD COLUMN user_id INTEGER")
            except Exception:
                pass
            try:
                db_cur.execute("ALTER TABLE llm_calls ADD COLUMN api_key_id INTEGER")
            except Exception:
                pass
            db_conn.commit()
            db_conn.close()

    def create_user(self, email: str, password_hash: str) -> int:
        """创建用户，返回用户ID"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
                       (email, password_hash))
            user_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            return user_id
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("INSERT INTO users (email, password_hash) VALUES (?, ?)",
                       (email, password_hash))
            user_id = cur.lastrowid
            conn.commit()
            conn.close()
            return user_id

    def find_user_by_email(self, email: str) -> Optional[dict]:
        """根据邮箱查找用户"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("SELECT id, email, password_hash, created_at, last_login_at FROM users WHERE email = %s",
                       (email,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return {"id": row[0], "email": row[1], "password_hash": row[2],
                        "created_at": row[3], "last_login_at": row[4]}
            return None
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE email = ?", (email,))
            row = cur.fetchone()
            conn.close()
            if row:
                return dict(row)
            return None

    def update_last_login(self, user_id: int):
        """更新最后登录时间"""
        from datetime import datetime
        now = datetime.now()
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("UPDATE users SET last_login_at = %s WHERE id = %s", (now, user_id))
            conn.commit()
            cur.close()
            conn.close()
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now.isoformat(), user_id))
            conn.commit()
            conn.close()

    def get_user_count(self) -> int:
        """获取用户总数"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            return count
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            count = cur.fetchone()[0]
            conn.close()
            return count

    def create_api_key(self, user_id: int, name: str, key: str, expires_at: date) -> int:
        """创建 API Key，返回 Key ID"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO api_keys (user_id, name, key, expires_at) VALUES (%s, %s, %s, %s) RETURNING id",
                (user_id, name, key, expires_at)
            )
            key_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            return key_id
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO api_keys (user_id, name, key, expires_at) VALUES (?, ?, ?, ?)",
                (user_id, name, key, expires_at.isoformat())
            )
            key_id = cur.lastrowid
            conn.commit()
            conn.close()
            return key_id

    def get_api_keys_by_user(self, user_id: int) -> list:
        """获取用户的 API Key 列表"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name, key, expires_at, created_at, is_active "
                "FROM api_keys WHERE user_id = %s ORDER BY created_at DESC",
                (user_id,)
            )
            rows = cur.fetchall()
            result = []
            for row in rows:
                result.append({
                    "id": row[0], "name": row[1], "key": row[2],
                    "expires_at": row[3].isoformat() if row[3] else None,
                    "created_at": row[4].isoformat() if row[4] else None,
                    "is_active": row[5]
                })
            cur.close()
            conn.close()
            return result
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(
                "SELECT id, name, key, expires_at, created_at, is_active "
                "FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,)
            )
            rows = cur.fetchall()
            conn.close()
            return [dict(r) for r in rows]

    def verify_api_key(self, key: str) -> Optional[dict]:
        """验证 API Key，返回 {id, user_id} 或 None"""
        from datetime import date
        today = date.today().isoformat()
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT id, user_id FROM api_keys "
                "WHERE key = %s AND is_active = true AND expires_at >= %s",
                (key, today)
            )
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return {"id": row[0], "user_id": row[1]}
            return None
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute(
                "SELECT id, user_id FROM api_keys "
                "WHERE key = ? AND is_active = 1 AND expires_at >= ?",
                (key, today)
            )
            row = cur.fetchone()
            conn.close()
            if row:
                return {"id": row[0], "user_id": row[1]}
            return None

    def delete_api_key(self, user_id: int, key_id: int) -> bool:
        """删除 API Key"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM api_keys WHERE id = %s AND user_id = %s",
                (key_id, user_id)
            )
            deleted = cur.rowcount > 0
            conn.commit()
            cur.close()
            conn.close()
            return deleted
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("DELETE FROM api_keys WHERE id = ? AND user_id = ?", (key_id, user_id))
            deleted = cur.rowcount > 0
            conn.commit()
            conn.close()
            return deleted

    def save_call_with_user(
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
        overridden_model: str = None,
        user_id: Optional[int] = None,
        api_key_id: Optional[int] = None
    ):
        """保存调用记录（带用户和 API Key 关联）"""
        if self.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO llm_calls (
                    call_id, timestamp, url, method,
                    request_headers, request_body,
                    response_headers, response_body,
                    duration_ms, tokens_input, tokens_output, token_source,
                    stream_type, first_token_ms,
                    original_model, overridden_model,
                    user_id, api_key_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                call_id, timestamp, url, method,
                json.dumps(request_headers, ensure_ascii=False),
                request_body,
                json.dumps(response_headers, ensure_ascii=False),
                response_body,
                duration_ms, tokens_input, tokens_output, token_source,
                stream_type, first_token_ms,
                original_model, overridden_model,
                user_id, api_key_id
            ))
            conn.commit()
            cur.close()
            conn.close()
        else:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO llm_calls (
                    call_id, timestamp, url, method,
                    request_headers, request_body,
                    response_headers, response_body,
                    duration_ms, tokens_input, tokens_output, token_source,
                    stream_type, first_token_ms,
                    original_model, overridden_model,
                    user_id, api_key_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                call_id, timestamp, url, method,
                json.dumps(request_headers, ensure_ascii=False),
                request_body,
                json.dumps(response_headers, ensure_ascii=False),
                response_body,
                duration_ms, tokens_input, tokens_output, token_source,
                stream_type, first_token_ms,
                original_model, overridden_model,
                user_id, api_key_id
            ))
            conn.commit()
            conn.close()
