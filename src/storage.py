"""
SQLite/PostgreSQL存储模块 - 异步写入调用记录
"""
import json
import threading
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
        self._pg_pool = None
        self._index_ready = False
        self._index_lock = threading.Lock()

        if self._use_postgres:
            import psycopg2.pool
            self._pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                host=self.postgresql.host,
                port=self.postgresql.port,
                user=self.postgresql.user,
                password=self.postgresql.password,
                database=self.postgresql.dbname,
            )

    # ========== PostgreSQL 辅助方法 ==========

    def _pg_conn(self):
        """获取 PostgreSQL 连接，返回 (conn, cur)"""
        if self._pg_pool is not None:
            conn = self._pg_pool.getconn()
        else:
            import psycopg2
            conn = psycopg2.connect(
                host=self.postgresql.host, port=self.postgresql.port,
                user=self.postgresql.user, password=self.postgresql.password,
                database=self.postgresql.dbname
            )
        return conn, conn.cursor()

    def _pg_close(self, conn, cur, commit: bool = False):
        """关闭 PostgreSQL 连接"""
        try:
            if commit:
                conn.commit()
            elif self._pg_pool is not None:
                conn.rollback()
        except Exception:
            pass
        finally:
            try:
                cur.close()
            except Exception:
                pass
            if self._pg_pool is not None:
                self._pg_pool.putconn(conn)
            else:
                conn.close()

    def _ensure_hot_path_indexes(self):
        """为高频查询/写入路径补齐索引（存在即跳过）。"""
        if self._index_ready:
            return

        with self._index_lock:
            if self._index_ready:
                return

            conn, cur = self._pg_conn() if self.postgresql else self._sqlite_conn()
            try:
                if self.postgresql:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_lookup ON api_keys (key, is_active, expires_at)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_call_id_api_key ON llm_calls (call_id, api_key_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_user_ts ON llm_calls (user_id, timestamp DESC)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_models_ts ON llm_calls (original_model, overridden_model, timestamp DESC)")
                else:
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_lookup ON api_keys (key, is_active, expires_at)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_call_id_api_key ON llm_calls (call_id, api_key_id)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_user_ts ON llm_calls (user_id, timestamp DESC)")
                    cur.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_models_ts ON llm_calls (original_model, overridden_model, timestamp DESC)")

                if self.postgresql:
                    conn.commit()
                else:
                    conn.commit()
                self._index_ready = True
            except Exception:
                # 表可能尚未初始化，后续请求会重试补索引。
                try:
                    conn.rollback()
                except Exception:
                    pass
            finally:
                if self.postgresql:
                    self._pg_close(conn, cur)
                else:
                    self._sqlite_close(conn, cur)

    def _sqlite_conn(self, row_factory: bool = False):
        """获取 SQLite 连接，返回 (conn, cur)"""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        if row_factory:
            conn.row_factory = sqlite3.Row
        return conn, conn.cursor()

    def _sqlite_close(self, conn, cur, commit: bool = False):
        """关闭 SQLite 连接"""
        try:
            if commit:
                conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            conn.close()

    def close(self):
        """释放底层连接资源。"""
        if self._pg_pool is not None:
            self._pg_pool.closeall()

    # ========== 异步方法 ==========

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
            conn, cur = self._pg_conn()
            try:
                cur.execute("INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
                           (email, password_hash))
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("INSERT INTO users (email, password_hash) VALUES (?, ?)",
                           (email, password_hash))
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def find_user_by_email(self, email: str) -> Optional[dict]:
        """根据邮箱查找用户"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("SELECT id, email, password_hash, created_at, last_login_at FROM users WHERE email = %s", (email,))
                row = cur.fetchone()
                if row:
                    return {"id": row[0], "email": row[1], "password_hash": row[2],
                            "created_at": row[3], "last_login_at": row[4]}
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute("SELECT * FROM users WHERE email = ?", (email,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def update_last_login(self, user_id: int):
        """更新最后登录时间"""
        from datetime import datetime
        now = datetime.now()
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("UPDATE users SET last_login_at = %s WHERE id = %s", (now, user_id))
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now.isoformat(), user_id))
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def get_user_count(self) -> int:
        """获取用户总数"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("SELECT COUNT(*) FROM users")
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("SELECT COUNT(*) FROM users")
                return cur.fetchone()[0]
            finally:
                self._sqlite_close(conn, cur)

    def create_api_key(self, user_id: int, name: str, key: str, expires_at: date) -> int:
        """创建 API Key，返回 Key ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "INSERT INTO api_keys (user_id, name, key, expires_at) VALUES (%s, %s, %s, %s) RETURNING id",
                    (user_id, name, key, expires_at)
                )
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "INSERT INTO api_keys (user_id, name, key, expires_at) VALUES (?, ?, ?, ?)",
                    (user_id, name, key, expires_at.isoformat())
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def get_api_keys_by_user(self, user_id: int) -> list:
        """获取用户的 API Key 列表"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "SELECT id, name, key, expires_at, created_at, is_active "
                    "FROM api_keys WHERE user_id = %s ORDER BY created_at DESC",
                    (user_id,)
                )
                rows = cur.fetchall()
                return [
                    {"id": r[0], "name": r[1], "key": r[2],
                     "expires_at": r[3].isoformat() if r[3] else None,
                     "created_at": r[4].isoformat() if r[4] else None,
                     "is_active": r[5]}
                    for r in rows
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(
                    "SELECT id, name, key, expires_at, created_at, is_active "
                    "FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
                    (user_id,)
                )
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def verify_api_key(self, key: str) -> Optional[dict]:
        """验证 API Key，返回 {id, user_id} 或 None"""
        from datetime import date
        self._ensure_hot_path_indexes()
        today = date.today().isoformat()
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "SELECT id, user_id FROM api_keys "
                    "WHERE key = %s AND is_active = true AND expires_at >= %s",
                    (key, today)
                )
                row = cur.fetchone()
                return {"id": row[0], "user_id": row[1]} if row else None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "SELECT id, user_id FROM api_keys "
                    "WHERE key = ? AND is_active = 1 AND expires_at >= ?",
                    (key, today)
                )
                row = cur.fetchone()
                return {"id": row[0], "user_id": row[1]} if row else None
            finally:
                self._sqlite_close(conn, cur)

    def delete_api_key(self, user_id: int, key_id: int) -> bool:
        """删除 API Key"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("DELETE FROM api_keys WHERE id = %s AND user_id = %s", (key_id, user_id))
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("DELETE FROM api_keys WHERE id = ? AND user_id = ?", (key_id, user_id))
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def save_call_with_user(
        self,
        call_id: str, timestamp: str, url: str, method: str,
        request_headers: dict, request_body: str,
        response_headers: dict, response_body: str,
        duration_ms: int, tokens_input: int, tokens_output: int, token_source: str,
        stream_type: str = "non_stream", first_token_ms: Optional[int] = None,
        original_model: str = None, overridden_model: str = None,
        user_id: Optional[int] = None, api_key_id: Optional[int] = None,
        previous_response_id: Optional[str] = None, full_context: Optional[str] = None
    ):
        """保存调用记录（带用户和 API Key 关联）"""
        self._ensure_hot_path_indexes()
        args = (
            call_id, timestamp, url, method,
            json.dumps(request_headers, ensure_ascii=False),
            request_body,
            json.dumps(response_headers, ensure_ascii=False),
            response_body,
            duration_ms, tokens_input, tokens_output, token_source,
            stream_type, first_token_ms,
            original_model, overridden_model, user_id, api_key_id,
            previous_response_id, full_context
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("""
                    INSERT INTO llm_calls (
                        call_id, timestamp, url, method,
                        request_headers, request_body,
                        response_headers, response_body,
                        duration_ms, tokens_input, tokens_output, token_source,
                        stream_type, first_token_ms,
                        original_model, overridden_model, user_id, api_key_id,
                        previous_response_id, full_context
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, args)
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("""
                    INSERT INTO llm_calls (
                        call_id, timestamp, url, method,
                        request_headers, request_body,
                        response_headers, response_body,
                        duration_ms, tokens_input, tokens_output, token_source,
                        stream_type, first_token_ms,
                        original_model, overridden_model, user_id, api_key_id,
                        previous_response_id, full_context
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, args)
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def get_call_history(self, call_id: str, api_key_id: int) -> Optional[dict]:
        """查询历史调用记录，按 api_key_id 隔离。

        返回 llm_calls 记录（包含 request_body 和 response_body），
        如果找不到或不属于该 api_key_id，返回 None。
        """
        self._ensure_hot_path_indexes()
        sql = (
            "SELECT request_body, response_body, full_context FROM llm_calls "
            "WHERE call_id = %s AND api_key_id = %s"
        ) if self.postgresql else (
            "SELECT request_body, response_body, full_context FROM llm_calls "
            "WHERE call_id = ? AND api_key_id = ?"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (call_id, api_key_id))
                row = cur.fetchone()
                if row:
                    return {
                        "request_body": row[0],
                        "response_body": row[1],
                        "full_context": row[2]
                    }
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (call_id, api_key_id))
                row = cur.fetchone()
                if row:
                    return dict(row)
                return None
            finally:
                self._sqlite_close(conn, cur)

    # ========== RBAC 相关方法（同步） ==========

    def create_role(self, name: str, description: str = "") -> int:
        """创建角色，返回角色 ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("INSERT INTO roles (name, description) VALUES (%s, %s) RETURNING id", (name, description))
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("INSERT INTO roles (name, description) VALUES (?, ?)", (name, description))
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def find_role_by_name(self, name: str) -> Optional[dict]:
        """按名称查找角色"""
        sql = "SELECT id, name, description FROM roles WHERE name = %s" if self.postgresql else "SELECT * FROM roles WHERE name = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (name,))
                row = cur.fetchone()
                return {"id": row[0], "name": row[1], "description": row[2]} if row else None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (name,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def create_menu(self, code: str, name: str, icon: str = "", sort_order: int = 0) -> int:
        """创建菜单，返回菜单 ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "INSERT INTO menus (code, name, icon, sort_order) VALUES (%s, %s, %s, %s) RETURNING id",
                    (code, name, icon, sort_order)
                )
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "INSERT INTO menus (code, name, icon, sort_order) VALUES (?, ?, ?, ?)",
                    (code, name, icon, sort_order)
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def find_menu_by_code(self, code: str) -> Optional[dict]:
        """按标识查找菜单"""
        sql = "SELECT id, code, name, icon, sort_order FROM menus WHERE code = %s" if self.postgresql else "SELECT * FROM menus WHERE code = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (code,))
                row = cur.fetchone()
                return {"id": row[0], "code": row[1], "name": row[2], "icon": row[3], "sort_order": row[4]} if row else None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (code,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def assign_menu_to_role(self, role_id: int, menu_id: int):
        """为角色分配菜单权限"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "INSERT INTO role_menus (role_id, menu_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (role_id, menu_id)
                )
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO role_menus (role_id, menu_id) VALUES (?, ?)",
                    (role_id, menu_id)
                )
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def get_user_role(self, user_id: int) -> Optional[dict]:
        """获取用户的角色信息"""
        sql = ("SELECT r.id, r.name, r.description FROM roles r "
               "JOIN users u ON u.role_id = r.id WHERE u.id = %s") if self.postgresql else \
              ("SELECT r.id, r.name, r.description FROM roles r "
               "JOIN users u ON u.role_id = r.id WHERE u.id = ?")
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (user_id,))
                row = cur.fetchone()
                return {"id": row[0], "name": row[1], "description": row[2]} if row else None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (user_id,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def get_user_menus(self, user_id: int) -> list:
        """获取用户有权访问的菜单列表，按 sort_order 排序"""
        sql = ("SELECT m.id, m.code, m.name, m.icon, m.sort_order FROM menus m "
               "JOIN role_menus rm ON rm.menu_id = m.id "
               "JOIN users u ON u.role_id = rm.role_id "
               "WHERE u.id = %s ORDER BY m.sort_order") if self.postgresql else \
              ("SELECT m.id, m.code, m.name, m.icon, m.sort_order FROM menus m "
               "JOIN role_menus rm ON rm.menu_id = m.id "
               "JOIN users u ON u.role_id = rm.role_id "
               "WHERE u.id = ? ORDER BY m.sort_order")
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (user_id,))
                rows = cur.fetchall()
                return [
                    {"id": r[0], "code": r[1], "name": r[2], "icon": r[3], "sort_order": r[4]}
                    for r in rows
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (user_id,))
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def set_user_role(self, user_id: int, role_id: int):
        """设置用户的角色"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("UPDATE users SET role_id = %s WHERE id = %s", (role_id, user_id))
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("UPDATE users SET role_id = ? WHERE id = ?", (role_id, user_id))
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def get_all_menus(self) -> list:
        """获取所有菜单"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("SELECT id, code, name, icon, sort_order FROM menus ORDER BY sort_order")
                rows = cur.fetchall()
                return [
                    {"id": r[0], "code": r[1], "name": r[2], "icon": r[3], "sort_order": r[4]}
                    for r in rows
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute("SELECT * FROM menus ORDER BY sort_order")
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    # ========== 上游管理 CRUD（同步） ==========

    def get_all_upstreams(self) -> list:
        """获取所有上游列表"""
        sql = "SELECT id, name, target_base_url, api_key, is_active, description, use_claude_features, use_roo_features, health_status, consecutive_failures, created_at, updated_at FROM upstreams ORDER BY name"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [
                    {
                        "id": r[0], "name": r[1], "target_base_url": r[2],
                        "api_key": r[3], "is_active": r[4], "description": r[5],
                        "use_claude_features": r[6], "use_roo_features": r[7],
                        "health_status": r[8] or 'healthy',
                        "consecutive_failures": r[9] or 0,
                        "created_at": r[10].isoformat() if r[10] else None,
                        "updated_at": r[11].isoformat() if r[11] else None,
                    }
                    for r in cur.fetchall()
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def get_upstream(self, upstream_id: int) -> Optional[dict]:
        """按 ID 获取上游"""
        sql = "SELECT id, name, target_base_url, api_key, is_active, description, use_claude_features, use_roo_features, health_status, consecutive_failures, created_at, updated_at FROM upstreams WHERE id = %s" if self.postgresql else \
              "SELECT id, name, target_base_url, api_key, is_active, description, use_claude_features, use_roo_features, health_status, consecutive_failures, created_at, updated_at FROM upstreams WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (upstream_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "id": row[0], "name": row[1], "target_base_url": row[2],
                        "api_key": row[3], "is_active": row[4], "description": row[5],
                        "use_claude_features": row[6], "use_roo_features": row[7],
                        "health_status": row[8] or 'healthy',
                        "consecutive_failures": row[9] or 0,
                        "created_at": row[10].isoformat() if row[10] else None,
                        "updated_at": row[11].isoformat() if row[11] else None,
                    }
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (upstream_id,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def get_upstream_by_name(self, name: str) -> Optional[dict]:
        """按名称获取上游"""
        sql = "SELECT id, name, target_base_url, api_key, is_active, description FROM upstreams WHERE name = %s" if self.postgresql else \
              "SELECT * FROM upstreams WHERE name = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (name,))
                row = cur.fetchone()
                if row:
                    return {"id": row[0], "name": row[1], "target_base_url": row[2],
                            "api_key": row[3], "is_active": row[4], "description": row[5]}
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (name,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def get_upstream_by_url(self, target_base_url: str) -> Optional[dict]:
        """按 URL 获取上游（用于迁移场景）"""
        sql = "SELECT id, name, target_base_url, api_key, is_active, description FROM upstreams WHERE target_base_url = %s" if self.postgresql else \
              "SELECT * FROM upstreams WHERE target_base_url = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (target_base_url,))
                row = cur.fetchone()
                if row:
                    return {"id": row[0], "name": row[1], "target_base_url": row[2],
                            "api_key": row[3], "is_active": row[4], "description": row[5]}
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (target_base_url,))
                row = cur.fetchone()
                return dict(row) if row else None
            finally:
                self._sqlite_close(conn, cur)

    def create_upstream(self, name: str, target_base_url: str, api_key: str = "",
                        description: str = "", is_active: bool = True,
                        use_claude_features: bool = False, use_roo_features: bool = False) -> int:
        """创建上游，返回 ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "INSERT INTO upstreams (name, target_base_url, api_key, description, is_active, use_claude_features, use_roo_features) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    (name, target_base_url, api_key, description, is_active, use_claude_features, use_roo_features)
                )
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "INSERT INTO upstreams (name, target_base_url, api_key, description, is_active, use_claude_features, use_roo_features) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (name, target_base_url, api_key, description, int(is_active), int(use_claude_features), int(use_roo_features))
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def update_upstream(self, upstream_id: int, name: str = None, target_base_url: str = None,
                        api_key: str = None, description: str = None, is_active: bool = None,
                        use_claude_features: bool = None, use_roo_features: bool = None,
                        health_status: str = None, consecutive_failures: int = None) -> bool:
        """更新上游"""
        fields, params = [], []
        if name is not None:
            fields.append("name = %s" if self.postgresql else "name = ?")
            params.append(name)
        if target_base_url is not None:
            fields.append("target_base_url = %s" if self.postgresql else "target_base_url = ?")
            params.append(target_base_url)
        if api_key is not None:
            fields.append("api_key = %s" if self.postgresql else "api_key = ?")
            params.append(api_key)
        if description is not None:
            fields.append("description = %s" if self.postgresql else "description = ?")
            params.append(description)
        if is_active is not None:
            fields.append("is_active = %s" if self.postgresql else "is_active = ?")
            params.append(is_active)
        if use_claude_features is not None:
            fields.append("use_claude_features = %s" if self.postgresql else "use_claude_features = ?")
            params.append(use_claude_features)
        if use_roo_features is not None:
            fields.append("use_roo_features = %s" if self.postgresql else "use_roo_features = ?")
            params.append(use_roo_features)
        if health_status is not None:
            fields.append("health_status = %s" if self.postgresql else "health_status = ?")
            params.append(health_status)
        if consecutive_failures is not None:
            fields.append("consecutive_failures = %s" if self.postgresql else "consecutive_failures = ?")
            params.append(consecutive_failures)
        if not fields:
            return False
        fields.append("updated_at = CURRENT_TIMESTAMP" if self.postgresql else "updated_at = datetime('now')")
        params.append(upstream_id)
        sql = f"UPDATE upstreams SET {', '.join(fields)} WHERE id = %s" if self.postgresql else \
              f"UPDATE upstreams SET {', '.join(fields)} WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def delete_upstream(self, upstream_id: int) -> bool:
        """删除上游"""
        sql = "DELETE FROM upstreams WHERE id = %s" if self.postgresql else "DELETE FROM upstreams WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (upstream_id,))
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, (upstream_id,))
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def link_model_to_upstream(self, model_id: int, upstream_id: int):
        """将模型配置关联到上游"""
        sql = "UPDATE model_configs SET upstream_id = %s WHERE id = %s" if self.postgresql else \
              "UPDATE model_configs SET upstream_id = ? WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (upstream_id, model_id))
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, (upstream_id, model_id))
            finally:
                self._sqlite_close(conn, cur, commit=True)

    # ========== 模型配置 CRUD（同步） ==========

    def get_all_model_configs(self) -> list:
        """获取所有模型配置（JOIN upstreams 获取上游信息）"""
        def _parse_row(r, is_pg):
            if is_pg:
                d = {
                    "id": r[0], "model_key": r[1], "upstream_id": r[2],
                    "target_base_url": r[3], "api_key": r[4], "model_overrides": r[5],
                    "forward_model": r[6],
                    "is_active": r[7], "is_default": r[8],
                    "use_multi_upstream": bool(r[9]) if r[9] is not None else False,
                    "protocol_converter": r[10] or None,
                    "created_at": r[11].isoformat() if r[11] else None,
                    "updated_at": r[12].isoformat() if r[12] else None,
                }
                if len(r) > 13 and r[13] is not None:
                    d["upstream_name"] = r[13]
                if len(r) > 14 and r[14] is not None:
                    d["use_claude_features"] = bool(r[14])
                if len(r) > 15 and r[15] is not None:
                    d["use_roo_features"] = bool(r[15])
                return d
            else:
                d = dict(r)
                d["is_active"] = bool(d["is_active"])
                d["is_default"] = bool(d["is_default"])
                d["use_multi_upstream"] = bool(d.get("use_multi_upstream", False))
                if "use_claude_features" in d:
                    d["use_claude_features"] = bool(d["use_claude_features"])
                if "use_roo_features" in d:
                    d["use_roo_features"] = bool(d["use_roo_features"])
                if d.get("protocol_converter") == "":
                    d["protocol_converter"] = None
                if d.get("created_at") and not isinstance(d["created_at"], str):
                    d["created_at"] = d["created_at"].isoformat()
                if d.get("updated_at") and not isinstance(d["updated_at"], str):
                    d["updated_at"] = d["updated_at"].isoformat()
                return d

        sql = (
            "SELECT mc.id, mc.model_key, mc.upstream_id, "
            "u.target_base_url, u.api_key, "
            "mc.model_overrides, mc.forward_model, mc.is_active, mc.is_default, mc.use_multi_upstream, mc.protocol_converter, mc.created_at, mc.updated_at, "
            "u.name as upstream_name, u.use_claude_features, u.use_roo_features "
            "FROM model_configs mc LEFT JOIN upstreams u ON mc.upstream_id = u.id "
            "ORDER BY mc.model_key"
        ) if self.postgresql else (
            "SELECT mc.id, mc.model_key, mc.upstream_id, "
            "u.target_base_url, u.api_key, "
            "mc.model_overrides, mc.forward_model, mc.is_active, mc.is_default, mc.use_multi_upstream, mc.protocol_converter, mc.created_at, mc.updated_at, "
            "u.name as upstream_name, u.use_claude_features, u.use_roo_features FROM model_configs mc "
            "LEFT JOIN upstreams u ON mc.upstream_id = u.id ORDER BY mc.model_key"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [_parse_row(r, True) for r in cur.fetchall()]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql)
                return [_parse_row(r, False) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def get_model_config(self, model_key: str) -> Optional[dict]:
        """按 model_key 获取单个配置"""
        sql = (
            "SELECT mc.id, mc.model_key, mc.upstream_id, "
            "u.target_base_url, u.api_key, "
            "mc.model_overrides, mc.forward_model, mc.is_active, mc.is_default, mc.use_multi_upstream, mc.protocol_converter, mc.created_at, mc.updated_at, "
            "u.name as upstream_name "
            "FROM model_configs mc LEFT JOIN upstreams u ON mc.upstream_id = u.id "
            "WHERE mc.model_key = %s"
        ) if self.postgresql else (
            "SELECT mc.id, mc.model_key, mc.upstream_id, "
            "u.target_base_url, u.api_key, "
            "mc.model_overrides, mc.forward_model, mc.is_active, mc.is_default, mc.use_multi_upstream, mc.protocol_converter, mc.created_at, mc.updated_at, "
            "u.name as upstream_name FROM model_configs mc "
            "LEFT JOIN upstreams u ON mc.upstream_id = u.id WHERE mc.model_key = ?"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (model_key,))
                row = cur.fetchone()
                if row:
                    d = {
                        "id": row[0], "model_key": row[1], "upstream_id": row[2],
                        "target_base_url": row[3], "api_key": row[4], "model_overrides": row[5],
                        "forward_model": row[6],
                        "is_active": row[7], "is_default": row[8], "use_multi_upstream": bool(row[9]) if row[9] is not None else False,
                        "protocol_converter": row[10] or None,
                        "created_at": row[11].isoformat() if row[11] else None,
                        "updated_at": row[12].isoformat() if row[12] else None,
                    }
                    if row[13] is not None:
                        d["upstream_name"] = row[13]
                    return d
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (model_key,))
                row = cur.fetchone()
                if row:
                    d = dict(row)
                    d["is_active"] = bool(d["is_active"])
                    d["is_default"] = bool(d["is_default"])
                    d["use_multi_upstream"] = bool(d.get("use_multi_upstream", False))
                    if d.get("protocol_converter") == "":
                        d["protocol_converter"] = None
                    return d
                return None
            finally:
                self._sqlite_close(conn, cur)

    def get_model_config_by_id(self, config_id: int) -> Optional[dict]:
        """按 ID 获取模型配置"""
        sql = (
            "SELECT mc.id, mc.model_key, mc.upstream_id, "
            "u.target_base_url, u.api_key, "
            "mc.model_overrides, mc.forward_model, mc.is_active, mc.is_default, mc.use_multi_upstream, mc.protocol_converter, mc.created_at, mc.updated_at, "
            "u.name as upstream_name "
            "FROM model_configs mc LEFT JOIN upstreams u ON mc.upstream_id = u.id "
            "WHERE mc.id = %s"
        ) if self.postgresql else (
            "SELECT mc.id, mc.model_key, mc.upstream_id, "
            "u.target_base_url, u.api_key, "
            "mc.model_overrides, mc.forward_model, mc.is_active, mc.is_default, mc.use_multi_upstream, mc.protocol_converter, mc.created_at, mc.updated_at, "
            "u.name as upstream_name FROM model_configs mc "
            "LEFT JOIN upstreams u ON mc.upstream_id = u.id WHERE mc.id = ?"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (config_id,))
                row = cur.fetchone()
                if row:
                    d = {
                        "id": row[0], "model_key": row[1], "upstream_id": row[2],
                        "target_base_url": row[3], "api_key": row[4], "model_overrides": row[5],
                        "forward_model": row[6],
                        "is_active": row[7], "is_default": row[8], "use_multi_upstream": bool(row[9]) if row[9] is not None else False,
                        "protocol_converter": row[10] or None,
                        "created_at": row[11].isoformat() if row[11] else None,
                        "updated_at": row[12].isoformat() if row[12] else None,
                    }
                    if row[13] is not None:
                        d["upstream_name"] = row[13]
                    return d
                return None
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (config_id,))
                row = cur.fetchone()
                if row:
                    d = dict(row)
                    d["is_active"] = bool(d["is_active"])
                    d["is_default"] = bool(d["is_default"])
                    d["use_multi_upstream"] = bool(d.get("use_multi_upstream", False))
                    if d.get("protocol_converter") == "":
                        d["protocol_converter"] = None
                    return d
                return None
            finally:
                self._sqlite_close(conn, cur)

    def create_model_config(
        self, model_key: str, target_base_url: str = "", api_key: str = "",
        model_overrides: str = "{}", forward_model: str = "", is_active: bool = True, is_default: bool = False,
        upstream_id: int = None, use_multi_upstream: bool = False, protocol_converter: str = None
    ) -> int:
        """创建模型配置，返回 ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = false")
                cur.execute(
                    "INSERT INTO model_configs (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, is_active, is_default, use_multi_upstream, protocol_converter) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, is_active, is_default, use_multi_upstream, protocol_converter)
                )
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = 0")
                cur.execute(
                    "INSERT INTO model_configs (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, is_active, is_default, use_multi_upstream, protocol_converter) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, int(is_active), int(is_default), int(use_multi_upstream), protocol_converter)
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def create_model_config_with_routes(
        self, model_key: str, target_base_url: str = "", api_key: str = "",
        model_overrides: str = "{}", forward_model: str = "", is_active: bool = True, is_default: bool = False,
        upstream_id: int = None, use_multi_upstream: bool = False, protocol_converter: str = None, routes: list = None
    ) -> int:
        """创建模型配置，并在同一事务内写入多上游路由。"""
        routes = routes or []
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = false")
                cur.execute(
                    "INSERT INTO model_configs (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, is_active, is_default, use_multi_upstream, protocol_converter) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, is_active, is_default, use_multi_upstream, protocol_converter)
                )
                config_id = cur.fetchone()[0]
                if use_multi_upstream:
                    self._replace_model_routes_in_cursor(cur, config_id, routes)
                conn.commit()
                return config_id
            except Exception:
                conn.rollback()
                raise
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = 0")
                cur.execute(
                    "INSERT INTO model_configs (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, is_active, is_default, use_multi_upstream, protocol_converter) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (model_key, upstream_id, target_base_url, api_key, model_overrides, forward_model, int(is_active), int(is_default), int(use_multi_upstream), protocol_converter)
                )
                config_id = cur.lastrowid
                if use_multi_upstream:
                    self._replace_model_routes_in_cursor(cur, config_id, routes)
                conn.commit()
                return config_id
            except Exception:
                conn.rollback()
                raise
            finally:
                self._sqlite_close(conn, cur)

    def update_model_config(
        self, config_id: int, model_key: str = None, target_base_url: str = None,
        api_key: str = None, model_overrides: str = None, forward_model: str = None, is_active: bool = None,
        is_default: bool = None, upstream_id: int = None, use_multi_upstream: bool = None,
        protocol_converter: str = None
    ) -> bool:
        """更新模型配置"""
        fields, params = [], []
        for fmt_pg, fmt_sqlite, v in [
            ("model_key = %s", "model_key = ?", model_key),
            ("upstream_id = %s", "upstream_id = ?", upstream_id),
            ("target_base_url = %s", "target_base_url = ?", target_base_url),
            ("api_key = %s", "api_key = ?", api_key),
            ("model_overrides = %s", "model_overrides = ?", model_overrides),
            ("forward_model = %s", "forward_model = ?", forward_model),
            ("is_active = %s", "is_active = ?", is_active),
            ("use_multi_upstream = %s", "use_multi_upstream = ?", use_multi_upstream),
            ("protocol_converter = %s", "protocol_converter = ?", protocol_converter),
        ]:
            if v is not None:
                fields.append(fmt_pg if self.postgresql else fmt_sqlite)
                params.append(v)
        if is_default is not None:
            if is_default:
                if self.postgresql:
                    conn, cur = self._pg_conn()
                    try:
                        cur.execute("UPDATE model_configs SET is_default = false")
                    finally:
                        self._pg_close(conn, cur, commit=True)
                else:
                    conn, cur = self._sqlite_conn()
                    try:
                        cur.execute("UPDATE model_configs SET is_default = 0")
                    finally:
                        self._sqlite_close(conn, cur, commit=True)
            fields.append("is_default = %s" if self.postgresql else "is_default = ?")
            params.append(is_default)
        fields.append("updated_at = CURRENT_TIMESTAMP" if self.postgresql else "updated_at = datetime('now')")
        params.append(config_id)
        sql = f"UPDATE model_configs SET {', '.join(fields)} WHERE id = %s" if self.postgresql else \
              f"UPDATE model_configs SET {', '.join(fields)} WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def update_model_config_with_routes(
        self, config_id: int, model_key: str = None, target_base_url: str = None,
        api_key: str = None, model_overrides: str = None, forward_model: str = None, is_active: bool = None,
        is_default: bool = None, upstream_id: int = None, use_multi_upstream: bool = None,
        protocol_converter: str = None, routes: list = None
    ) -> bool:
        """更新模型配置，并在同一事务内替换多上游路由。"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = false")
                fields = [
                    "model_key = %s",
                    "upstream_id = %s",
                    "target_base_url = %s",
                    "api_key = %s",
                    "forward_model = %s",
                    "is_active = %s",
                    "is_default = %s",
                    "use_multi_upstream = %s",
                    "protocol_converter = %s",
                    "updated_at = CURRENT_TIMESTAMP",
                ]
                params = [model_key, upstream_id, target_base_url, api_key, forward_model, is_active, is_default, use_multi_upstream, protocol_converter, config_id]
                cur.execute(f"UPDATE model_configs SET {', '.join(fields)} WHERE id = %s", params)
                if cur.rowcount == 0:
                    conn.rollback()
                    return False
                if routes is not None or not use_multi_upstream:
                    self._replace_model_routes_in_cursor(cur, config_id, routes if use_multi_upstream else [])
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = 0")
                fields = [
                    "model_key = ?",
                    "upstream_id = ?",
                    "target_base_url = ?",
                    "api_key = ?",
                    "forward_model = ?",
                    "is_active = ?",
                    "is_default = ?",
                    "use_multi_upstream = ?",
                    "protocol_converter = ?",
                    "updated_at = datetime('now')",
                ]
                params = [
                    model_key, upstream_id, target_base_url, api_key, forward_model,
                    int(is_active), int(is_default), int(use_multi_upstream), protocol_converter, config_id
                ]
                cur.execute(f"UPDATE model_configs SET {', '.join(fields)} WHERE id = ?", params)
                if cur.rowcount == 0:
                    conn.rollback()
                    return False
                if routes is not None or not use_multi_upstream:
                    self._replace_model_routes_in_cursor(cur, config_id, routes if use_multi_upstream else [])
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise
            finally:
                self._sqlite_close(conn, cur)

    def delete_model_config(self, config_id: int) -> bool:
        """删除模型配置"""
        sql = "DELETE FROM model_configs WHERE id = %s" if self.postgresql else "DELETE FROM model_configs WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (config_id,))
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, (config_id,))
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    # ========== 用户管理 CRUD（同步） ==========

    def get_all_users(self) -> list:
        """获取所有用户列表"""
        sql = ("SELECT u.id, u.email, u.created_at, u.last_login_at, r.id as role_id, r.name as role_name "
               "FROM users u LEFT JOIN roles r ON u.role_id = r.id ORDER BY u.created_at DESC")
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [
                    {"id": r[0], "email": r[1],
                     "created_at": r[2].isoformat() if r[2] else None,
                     "last_login_at": r[3].isoformat() if r[3] else None,
                     "role_id": r[4], "role_name": r[5]}
                    for r in cur.fetchall()
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def delete_user(self, user_id: int) -> bool:
        """删除用户"""
        sql = "DELETE FROM users WHERE id = %s" if self.postgresql else "DELETE FROM users WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (user_id,))
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, (user_id,))
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    # ========== 角色管理 CRUD（同步） ==========

    def get_role_menus(self, role_id: int) -> list:
        """获取角色已关联的菜单 ID 列表"""
        sql = "SELECT menu_id FROM role_menus WHERE role_id = %s" if self.postgresql else "SELECT menu_id FROM role_menus WHERE role_id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (role_id,))
                return [r[0] for r in cur.fetchall()]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, (role_id,))
                return [r[0] for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def update_role_menus(self, role_id: int, menu_ids: list):
        """更新角色的菜单（先清空再批量插入）"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("DELETE FROM role_menus WHERE role_id = %s", (role_id,))
                for menu_id in menu_ids:
                    cur.execute(
                        "INSERT INTO role_menus (role_id, menu_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (role_id, menu_id)
                    )
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("DELETE FROM role_menus WHERE role_id = ?", (role_id,))
                for menu_id in menu_ids:
                    cur.execute(
                        "INSERT OR IGNORE INTO role_menus (role_id, menu_id) VALUES (?, ?)",
                        (role_id, menu_id)
                    )
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def get_all_roles(self) -> list:
        """获取所有角色列表"""
        sql = "SELECT id, name, description, created_at FROM roles ORDER BY id" if self.postgresql else "SELECT * FROM roles ORDER BY id"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [
                    {"id": r[0], "name": r[1], "description": r[2],
                     "created_at": r[3].isoformat() if r[3] else None}
                    for r in cur.fetchall()
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def update_role(self, role_id: int, name: str = None, description: str = None) -> bool:
        """更新角色"""
        fields, params = [], []
        if name is not None:
            fields.append("name = %s" if self.postgresql else "name = ?")
            params.append(name)
        if description is not None:
            fields.append("description = %s" if self.postgresql else "description = ?")
            params.append(description)
        if not fields:
            return False
        params.append(role_id)
        sql = f"UPDATE roles SET {', '.join(fields)} WHERE id = %s" if self.postgresql else \
              f"UPDATE roles SET {', '.join(fields)} WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def delete_role(self, role_id: int) -> bool:
        """删除角色（同时删除 role_menus 关联）"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute("DELETE FROM role_menus WHERE role_id = %s", (role_id,))
                cur.execute("DELETE FROM roles WHERE id = %s", (role_id,))
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute("DELETE FROM role_menus WHERE role_id = ?", (role_id,))
                cur.execute("DELETE FROM roles WHERE id = ?", (role_id,))
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    # ========== 模型路由管理 CRUD（多上游） ==========

    def _replace_model_routes_in_cursor(self, cur, model_config_id: int, routes: list):
        """在当前事务中替换模型路由。调用方负责提交或回滚事务。"""
        delete_sql = "DELETE FROM model_upstream_routes WHERE model_config_id = %s" if self.postgresql else \
                     "DELETE FROM model_upstream_routes WHERE model_config_id = ?"
        insert_sql = (
            "INSERT INTO model_upstream_routes (model_config_id, upstream_id, forward_model, protocol_converter, sort_order) "
            "VALUES (%s, %s, %s, %s, %s)"
        ) if self.postgresql else (
            "INSERT INTO model_upstream_routes (model_config_id, upstream_id, forward_model, protocol_converter, sort_order) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        cur.execute(delete_sql, (model_config_id,))
        for route in routes or []:
            cur.execute(
                insert_sql,
                (
                    model_config_id,
                    route["upstream_id"],
                    route.get("forward_model", "") or "",
                    route.get("protocol_converter") or None,
                    route.get("sort_order", 0),
                )
            )

    def get_model_routes(self, model_config_id: int) -> list:
        """获取某个模型配置的所有路由（按 sort_order 排序）"""
        sql = (
            "SELECT mur.id, mur.model_config_id, mur.upstream_id, mur.forward_model, mur.protocol_converter, "
            "mur.sort_order, mur.is_active, mur.created_at, mur.updated_at, "
            "u.name as upstream_name, u.target_base_url, u.api_key, u.use_claude_features, u.use_roo_features, "
            "u.health_status "
            "FROM model_upstream_routes mur "
            "JOIN upstreams u ON mur.upstream_id = u.id "
            "WHERE mur.model_config_id = %s ORDER BY mur.sort_order"
        ) if self.postgresql else (
            "SELECT mur.id, mur.model_config_id, mur.upstream_id, mur.forward_model, mur.protocol_converter, "
            "mur.sort_order, mur.is_active, mur.created_at, mur.updated_at, "
            "u.name as upstream_name, u.target_base_url, u.api_key, u.use_claude_features, u.use_roo_features, "
            "u.health_status "
            "FROM model_upstream_routes mur "
            "JOIN upstreams u ON mur.upstream_id = u.id "
            "WHERE mur.model_config_id = ? ORDER BY mur.sort_order"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (model_config_id,))
                return [
                    {
                        "id": r[0], "model_config_id": r[1], "upstream_id": r[2],
                        "forward_model": r[3] or "", "protocol_converter": r[4] or None,
                        "sort_order": r[5],
                        "is_active": r[6],
                        "created_at": r[7].isoformat() if r[7] else None,
                        "updated_at": r[8].isoformat() if r[8] else None,
                        "upstream_name": r[9], "target_base_url": r[10],
                        "api_key": r[11] or "",
                        "use_claude_features": bool(r[12]) if r[12] is not None else False,
                        "use_roo_features": bool(r[13]) if r[13] is not None else False,
                        "health_status": r[14] or 'healthy',
                    }
                    for r in cur.fetchall()
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (model_config_id,))
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def get_all_model_routes(self) -> list:
        """获取所有多上游路由（用于 proxy 缓存加载）"""
        sql = (
            "SELECT mur.model_config_id, mur.upstream_id, mur.forward_model, mur.protocol_converter, mur.sort_order, "
            "u.target_base_url, u.api_key, u.use_claude_features, u.use_roo_features, "
            "u.health_status, mc.model_key "
            "FROM model_upstream_routes mur "
            "JOIN upstreams u ON mur.upstream_id = u.id "
            "JOIN model_configs mc ON mur.model_config_id = mc.id "
            "WHERE mur.is_active = true AND mc.is_active = true AND mc.use_multi_upstream = true "
            "ORDER BY mc.model_key, mur.sort_order"
        ) if self.postgresql else (
            "SELECT mur.model_config_id, mur.upstream_id, mur.forward_model, mur.protocol_converter, mur.sort_order, "
            "u.target_base_url, u.api_key, u.use_claude_features, u.use_roo_features, "
            "u.health_status, mc.model_key "
            "FROM model_upstream_routes mur "
            "JOIN upstreams u ON mur.upstream_id = u.id "
            "JOIN model_configs mc ON mur.model_config_id = mc.id "
            "WHERE mur.is_active = 1 AND mc.is_active = 1 AND mc.use_multi_upstream = 1 "
            "ORDER BY mc.model_key, mur.sort_order"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [
                    {
                        "model_config_id": r[0], "upstream_id": r[1],
                        "forward_model": r[2] or "", "protocol_converter": r[3] or None,
                        "sort_order": r[4],
                        "target_base_url": r[5], "api_key": r[6] or "",
                        "use_claude_features": bool(r[7]) if r[7] is not None else False,
                        "use_roo_features": bool(r[8]) if r[8] is not None else False,
                        "health_status": r[9] or 'healthy',
                        "model_key": r[10],
                    }
                    for r in cur.fetchall()
                ]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def create_model_route(self, model_config_id: int, upstream_id: int,
                           forward_model: str = "", protocol_converter: str = None, sort_order: int = 0) -> int:
        """创建模型路由"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "INSERT INTO model_upstream_routes (model_config_id, upstream_id, forward_model, protocol_converter, sort_order) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (model_config_id, upstream_id, forward_model or "", protocol_converter, sort_order)
                )
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "INSERT INTO model_upstream_routes (model_config_id, upstream_id, forward_model, protocol_converter, sort_order) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (model_config_id, upstream_id, forward_model or "", protocol_converter, sort_order)
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def update_model_route(self, route_id: int, upstream_id: int = None,
                           forward_model: str = None, protocol_converter: str = None,
                           sort_order: int = None, is_active: bool = None) -> bool:
        """更新模型路由"""
        fields, params = [], []
        if upstream_id is not None:
            fields.append("upstream_id = %s" if self.postgresql else "upstream_id = ?")
            params.append(upstream_id)
        if forward_model is not None:
            fields.append("forward_model = %s" if self.postgresql else "forward_model = ?")
            params.append(forward_model)
        if protocol_converter is not None:
            fields.append("protocol_converter = %s" if self.postgresql else "protocol_converter = ?")
            params.append(protocol_converter)
        if sort_order is not None:
            fields.append("sort_order = %s" if self.postgresql else "sort_order = ?")
            params.append(sort_order)
        if is_active is not None:
            fields.append("is_active = %s" if self.postgresql else "is_active = ?")
            params.append(is_active)
        if not fields:
            return False
        fields.append("updated_at = CURRENT_TIMESTAMP" if self.postgresql else "updated_at = datetime('now')")
        params.append(route_id)
        sql = f"UPDATE model_upstream_routes SET {', '.join(fields)} WHERE id = %s" if self.postgresql else \
              f"UPDATE model_upstream_routes SET {', '.join(fields)} WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, params)
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def delete_model_route(self, route_id: int) -> bool:
        """删除模型路由"""
        sql = "DELETE FROM model_upstream_routes WHERE id = %s" if self.postgresql else "DELETE FROM model_upstream_routes WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (route_id,))
                return cur.rowcount > 0
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(sql, (route_id,))
                return cur.rowcount > 0
            finally:
                self._sqlite_close(conn, cur, commit=True)

    # ========== 上游健康检查方法 ==========

    def get_unhealthy_upstreams(self) -> list:
        """获取所有被标记为 unhealthy 的上游"""
        sql = "SELECT id, name, target_base_url FROM upstreams WHERE health_status = 'unhealthy'"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [{"id": r[0], "name": r[1], "target_base_url": r[2]} for r in cur.fetchall()]
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
            finally:
                self._sqlite_close(conn, cur)

    def get_random_model_for_upstream(self, upstream_id: int) -> Optional[dict]:
        """随机获取上游关联的一个模型（优先多上游路由，否则 model_configs），用于健康检查"""
        # 先从多上游路由找
        sql = (
            "SELECT mur.forward_model, mc.model_key, u.api_key, u.use_claude_features, u.use_roo_features "
            "FROM model_upstream_routes mur "
            "JOIN model_configs mc ON mur.model_config_id = mc.id "
            "JOIN upstreams u ON mur.upstream_id = u.id "
            "WHERE mur.upstream_id = %s AND mur.is_active = true AND mc.is_active = true "
            "ORDER BY RANDOM() LIMIT 1"
        ) if self.postgresql else (
            "SELECT mur.forward_model, mc.model_key, u.api_key, u.use_claude_features, u.use_roo_features "
            "FROM model_upstream_routes mur "
            "JOIN model_configs mc ON mur.model_config_id = mc.id "
            "JOIN upstreams u ON mur.upstream_id = u.id "
            "WHERE mur.upstream_id = ? AND mur.is_active = 1 AND mc.is_active = 1 "
            "ORDER BY RANDOM() LIMIT 1"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (upstream_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "forward_model": row[0] or "",
                        "model_key": row[1],
                        "api_key": row[2] or "",
                        "use_claude_features": bool(row[3]) if row[3] is not None else False,
                        "use_roo_features": bool(row[4]) if row[4] is not None else False,
                    }
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql, (upstream_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "forward_model": row[0] or "",
                        "model_key": row[1],
                        "api_key": row[2] or "",
                        "use_claude_features": bool(row[3]) if row[3] is not None else False,
                        "use_roo_features": bool(row[4]) if row[4] is not None else False,
                    }
            finally:
                self._sqlite_close(conn, cur)

        # 回退：从 model_configs 单上游模式找
        sql2 = (
            "SELECT mc.model_key, mc.forward_model, u.api_key, u.use_claude_features, u.use_roo_features "
            "FROM model_configs mc "
            "JOIN upstreams u ON mc.upstream_id = u.id "
            "WHERE mc.upstream_id = %s AND mc.is_active = true AND mc.use_multi_upstream = false "
            "ORDER BY RANDOM() LIMIT 1"
        ) if self.postgresql else (
            "SELECT mc.model_key, mc.forward_model, u.api_key, u.use_claude_features, u.use_roo_features "
            "FROM model_configs mc "
            "JOIN upstreams u ON mc.upstream_id = u.id "
            "WHERE mc.upstream_id = ? AND mc.is_active = 1 AND mc.use_multi_upstream = 0 "
            "ORDER BY RANDOM() LIMIT 1"
        )
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql2, (upstream_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "model_key": row[0],
                        "forward_model": row[1] or "",
                        "api_key": row[2] or "",
                        "use_claude_features": bool(row[3]) if row[3] is not None else False,
                        "use_roo_features": bool(row[4]) if row[4] is not None else False,
                    }
            finally:
                self._pg_close(conn, cur)
        else:
            conn, cur = self._sqlite_conn(row_factory=True)
            try:
                cur.execute(sql2, (upstream_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "model_key": row[0],
                        "forward_model": row[1] or "",
                        "api_key": row[2] or "",
                        "use_claude_features": bool(row[3]) if row[3] is not None else False,
                        "use_roo_features": bool(row[4]) if row[4] is not None else False,
                    }
            finally:
                self._sqlite_close(conn, cur)

        return None

    def reset_upstream_health(self, upstream_id: int):
        """将上游标记为 healthy，重置连续失败计数"""
        self.update_upstream(upstream_id, health_status='healthy', consecutive_failures=0 if self.postgresql else 0)

    def increment_upstream_failures(self, upstream_id: int, max_failures: int = 3):
        """递增上游连续失败计数，达阈值时标记为 unhealthy"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "UPDATE upstreams SET consecutive_failures = consecutive_failures + 1, "
                    "updated_at = CURRENT_TIMESTAMP WHERE id = %s RETURNING consecutive_failures",
                    (upstream_id,)
                )
                row = cur.fetchone()
                new_count = row[0] if row else 0
                if new_count >= max_failures:
                    cur.execute(
                        "UPDATE upstreams SET health_status = 'unhealthy', consecutive_failures = 0, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = %s", (upstream_id,)
                    )
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "UPDATE upstreams SET consecutive_failures = consecutive_failures + 1, "
                    "updated_at = datetime('now') WHERE id = ?",
                    (upstream_id,)
                )
                conn.commit()
                cur.execute("SELECT consecutive_failures FROM upstreams WHERE id = ?", (upstream_id,))
                row = cur.fetchone()
                new_count = row[0] if row else 0
                if new_count >= max_failures:
                    cur.execute(
                        "UPDATE upstreams SET health_status = 'unhealthy', consecutive_failures = 0, "
                        "updated_at = datetime('now') WHERE id = ?", (upstream_id,)
                    )
            finally:
                self._sqlite_close(conn, cur, commit=True)
