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

    # ========== PostgreSQL 辅助方法 ==========

    def _pg_conn(self):
        """获取 PostgreSQL 连接，返回 (conn, cur)"""
        import psycopg2
        conn = psycopg2.connect(
            host=self.postgresql.host, port=self.postgresql.port,
            user=self.postgresql.user, password=self.postgresql.password,
            database=self.postgresql.dbname
        )
        return conn, conn.cursor()

    def _pg_close(self, conn, cur, commit: bool = False):
        """关闭 PostgreSQL 连接"""
        if commit:
            conn.commit()
        cur.close()
        conn.close()

    def _sqlite_conn(self, row_factory: bool = False):
        """获取 SQLite 连接，返回 (conn, cur)"""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        if row_factory:
            conn.row_factory = sqlite3.Row
        return conn, conn.cursor()

    def _sqlite_close(self, conn, cur, commit: bool = False):
        """关闭 SQLite 连接"""
        if commit:
            conn.commit()
        conn.close()

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
        user_id: Optional[int] = None, api_key_id: Optional[int] = None
    ):
        """保存调用记录（带用户和 API Key 关联）"""
        args = (
            call_id, timestamp, url, method,
            json.dumps(request_headers, ensure_ascii=False),
            request_body,
            json.dumps(response_headers, ensure_ascii=False),
            response_body,
            duration_ms, tokens_input, tokens_output, token_source,
            stream_type, first_token_ms,
            original_model, overridden_model, user_id, api_key_id
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
                        original_model, overridden_model, user_id, api_key_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        original_model, overridden_model, user_id, api_key_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, args)
            finally:
                self._sqlite_close(conn, cur, commit=True)

    # ========== RBAC 相关方法（同步） ==========

    def init_rbac_tables(self, is_pg: bool = False, cur=None, conn=None):
        """初始化 RBAC 表（roles, menus, role_menus, upstreams），并给 users 表加 role_id 字段"""
        if is_pg:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS roles (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL,
                    description VARCHAR(200),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS menus (
                    id SERIAL PRIMARY KEY,
                    code VARCHAR(50) UNIQUE NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    icon VARCHAR(20),
                    sort_order INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS role_menus (
                    role_id INTEGER NOT NULL REFERENCES roles(id),
                    menu_id INTEGER NOT NULL REFERENCES menus(id),
                    PRIMARY KEY (role_id, menu_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS upstreams (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) UNIQUE NOT NULL,
                    target_base_url VARCHAR(500) NOT NULL,
                    api_key VARCHAR(500),
                    is_active BOOLEAN DEFAULT true,
                    description VARCHAR(200),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_configs (
                    id SERIAL PRIMARY KEY,
                    model_key VARCHAR(100) UNIQUE NOT NULL,
                    upstream_id INTEGER REFERENCES upstreams(id),
                    target_base_url VARCHAR(500),
                    api_key VARCHAR(500),
                    model_overrides TEXT,
                    is_active BOOLEAN DEFAULT true,
                    is_default BOOLEAN DEFAULT false,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 对已存在的表添加缺失列（幂等）
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'model_configs' AND column_name = 'upstream_id'
                    ) THEN
                        ALTER TABLE model_configs ADD COLUMN upstream_id INTEGER REFERENCES upstreams(id);
                    END IF;
                END $$;
            """)
            # 兼容旧表：追加缺失列（幂等，用 DO block 避免回滚前面 CREATE TABLE）
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'model_configs' AND column_name = 'is_default'
                    ) THEN
                        ALTER TABLE model_configs ADD COLUMN is_default BOOLEAN DEFAULT false;
                    END IF;
                END $$;
            """)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'users' AND column_name = 'role_id'
                    ) THEN
                        ALTER TABLE users ADD COLUMN role_id INTEGER;
                    END IF;
                END $$;
            """)
            # 最后统一提交
            conn.commit()
        else:
            import sqlite3
            db_conn = sqlite3.connect(self.db_path)
            db_cur = db_conn.cursor()
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS roles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS menus (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    icon TEXT,
                    sort_order INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS role_menus (
                    role_id INTEGER NOT NULL REFERENCES roles(id),
                    menu_id INTEGER NOT NULL REFERENCES menus(id),
                    PRIMARY KEY (role_id, menu_id)
                )
            """)
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS upstreams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    target_base_url TEXT NOT NULL,
                    api_key TEXT,
                    is_active INTEGER DEFAULT 1,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db_cur.execute("""
                CREATE TABLE IF NOT EXISTS model_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_key TEXT UNIQUE NOT NULL,
                    upstream_id INTEGER REFERENCES upstreams(id),
                    target_base_url TEXT,
                    api_key TEXT,
                    model_overrides TEXT,
                    is_active INTEGER DEFAULT 1,
                    is_default INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # 对已存在的 SQLite 表添加 upstream_id 列（幂等，忽略已存在错误）
            try:
                db_cur.execute("ALTER TABLE model_configs ADD COLUMN upstream_id INTEGER REFERENCES upstreams(id)")
                conn.commit()
            except Exception:
                pass  # 列已存在
            try:
                db_cur.execute("ALTER TABLE users ADD COLUMN role_id INTEGER")
            except Exception:
                pass
            db_conn.commit()
            db_conn.close()

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
        sql = "SELECT id, name, target_base_url, api_key, is_active, description, created_at, updated_at FROM upstreams ORDER BY name"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql)
                return [
                    {
                        "id": r[0], "name": r[1], "target_base_url": r[2],
                        "api_key": r[3], "is_active": r[4], "description": r[5],
                        "created_at": r[6].isoformat() if r[6] else None,
                        "updated_at": r[7].isoformat() if r[7] else None,
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
        sql = "SELECT id, name, target_base_url, api_key, is_active, description, created_at, updated_at FROM upstreams WHERE id = %s" if self.postgresql else \
              "SELECT * FROM upstreams WHERE id = ?"
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(sql, (upstream_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "id": row[0], "name": row[1], "target_base_url": row[2],
                        "api_key": row[3], "is_active": row[4], "description": row[5],
                        "created_at": row[6].isoformat() if row[6] else None,
                        "updated_at": row[7].isoformat() if row[7] else None,
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

    def create_upstream(self, name: str, target_base_url: str, api_key: str = "",
                        description: str = "", is_active: bool = True) -> int:
        """创建上游，返回 ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                cur.execute(
                    "INSERT INTO upstreams (name, target_base_url, api_key, description, is_active) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (name, target_base_url, api_key, description, is_active)
                )
                return cur.fetchone()[0]
            finally:
                self._pg_close(conn, cur, commit=True)
        else:
            conn, cur = self._sqlite_conn()
            try:
                cur.execute(
                    "INSERT INTO upstreams (name, target_base_url, api_key, description, is_active) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (name, target_base_url, api_key, description, int(is_active))
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def update_upstream(self, upstream_id: int, name: str = None, target_base_url: str = None,
                        api_key: str = None, description: str = None, is_active: bool = None) -> bool:
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
                    "is_active": r[6], "is_default": r[7],
                    "created_at": r[8].isoformat() if r[8] else None,
                    "updated_at": r[9].isoformat() if r[9] else None,
                }
                # upstream 字段（LEFT JOIN 可能为 None）
                if len(r) > 10 and r[10] is not None:
                    d["upstream_name"] = r[10]
                return d
            else:
                d = dict(r)
                d["is_active"] = bool(d["is_active"])
                d["is_default"] = bool(d["is_default"])
                return d

        sql = (
            "SELECT mc.id, mc.model_key, mc.upstream_id, mc.target_base_url, mc.api_key, "
            "mc.model_overrides, mc.is_active, mc.is_default, mc.created_at, mc.updated_at, "
            "u.name as upstream_name "
            "FROM model_configs mc LEFT JOIN upstreams u ON mc.upstream_id = u.id "
            "ORDER BY mc.model_key"
        ) if self.postgresql else (
            "SELECT mc.*, u.name as upstream_name FROM model_configs mc "
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
            "SELECT mc.id, mc.model_key, mc.upstream_id, mc.target_base_url, mc.api_key, "
            "mc.model_overrides, mc.is_active, mc.is_default, mc.created_at, mc.updated_at, "
            "u.name as upstream_name "
            "FROM model_configs mc LEFT JOIN upstreams u ON mc.upstream_id = u.id "
            "WHERE mc.model_key = %s"
        ) if self.postgresql else (
            "SELECT mc.*, u.name as upstream_name FROM model_configs mc "
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
                        "is_active": row[6], "is_default": row[7],
                        "created_at": row[8].isoformat() if row[8] else None,
                        "updated_at": row[9].isoformat() if row[9] else None,
                    }
                    if row[10] is not None:
                        d["upstream_name"] = row[10]
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
                    return d
                return None
            finally:
                self._sqlite_close(conn, cur)

    def get_model_config_by_id(self, config_id: int) -> Optional[dict]:
        """按 ID 获取模型配置"""
        sql = (
            "SELECT mc.id, mc.model_key, mc.upstream_id, mc.target_base_url, mc.api_key, "
            "mc.model_overrides, mc.is_active, mc.is_default, mc.created_at, mc.updated_at, "
            "u.name as upstream_name "
            "FROM model_configs mc LEFT JOIN upstreams u ON mc.upstream_id = u.id "
            "WHERE mc.id = %s"
        ) if self.postgresql else (
            "SELECT mc.*, u.name as upstream_name FROM model_configs mc "
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
                        "is_active": row[6], "is_default": row[7],
                        "created_at": row[8].isoformat() if row[8] else None,
                        "updated_at": row[9].isoformat() if row[9] else None,
                    }
                    if row[10] is not None:
                        d["upstream_name"] = row[10]
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
                    return d
                return None
            finally:
                self._sqlite_close(conn, cur)

    def create_model_config(
        self, model_key: str, target_base_url: str = "", api_key: str = "",
        model_overrides: str = "{}", is_active: bool = True, is_default: bool = False,
        upstream_id: int = None
    ) -> int:
        """创建模型配置，返回 ID"""
        if self.postgresql:
            conn, cur = self._pg_conn()
            try:
                if is_default:
                    cur.execute("UPDATE model_configs SET is_default = false")
                cur.execute(
                    "INSERT INTO model_configs (model_key, upstream_id, target_base_url, api_key, model_overrides, is_active, is_default) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    (model_key, upstream_id, target_base_url, api_key, model_overrides, is_active, is_default)
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
                    "INSERT INTO model_configs (model_key, upstream_id, target_base_url, api_key, model_overrides, is_active, is_default) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (model_key, upstream_id, target_base_url, api_key, model_overrides, int(is_active), int(is_default))
                )
                return cur.lastrowid
            finally:
                self._sqlite_close(conn, cur, commit=True)

    def update_model_config(
        self, config_id: int, model_key: str = None, target_base_url: str = None,
        api_key: str = None, model_overrides: str = None, is_active: bool = None,
        is_default: bool = None, upstream_id: int = None
    ) -> bool:
        """更新模型配置"""
        fields, params = [], []
        for fmt_pg, fmt_sqlite, v in [
            ("model_key = %s", "model_key = ?", model_key),
            ("upstream_id = %s", "upstream_id = ?", upstream_id),
            ("target_base_url = %s", "target_base_url = ?", target_base_url),
            ("api_key = %s", "api_key = ?", api_key),
            ("model_overrides = %s", "model_overrides = ?", model_overrides),
            ("is_active = %s", "is_active = ?", is_active),
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
