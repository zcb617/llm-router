"""
数据库初始化脚本 — 创建当前完整 schema + 种子数据

用法:
    python scripts/init_database.py                 # 使用默认 config.yaml
    python scripts/init_database.py config.yaml     # 指定配置文件

执行后会:
  1. 创建 schema_version 表
  2. 检查当前版本，如果是空库 → 执行基础初始化
  3. 补齐当前分支新增字段：
     - llm_calls.previous_response_id
     - llm_calls.full_context
     - llm_calls.final_responses_body
     - llm_calls.call_status
     - model_configs.protocol_converter
     - model_upstream_routes.protocol_converter
     - upstreams.auth_mode
     - upstreams.oauth_key
     - upstreams.oauth_host
     - llm_calls.outbound_diagnostics
     - llm_calls.is_internal_relay
     - codex_prompt_cache_affinity
     - PostgreSQL pg_trgm 扩展及模型包含搜索 GIN 索引
     - 调用日志分页过滤与排序索引
  4. 基础内容：
     - 基础表: llm_calls, users, api_keys, model_configs
     - RBAC 表: roles, menus, role_menus, upstreams
     - 预置菜单、角色、关联关系
     - 默认 admin 账号 (admin / admin)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

CURRENT_VERSION = "1.1.8"


def get_pg_conn(config):
    import psycopg2
    pg = config.database.postgresql
    return psycopg2.connect(
        host=pg.host, port=pg.port,
        user=pg.user, password=pg.password,
        database=pg.dbname
    )


def init_schema_version_pg(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY DEFAULT 1,
            version VARCHAR(20) NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CHECK (id = 1)
        )
    """)
    cur.execute("SELECT version FROM schema_version WHERE id = 1")
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def init_schema_version_sqlite(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("SELECT version FROM schema_version WHERE id = 1")
    row = cur.fetchone()
    return row[0] if row else None


def set_version_pg(conn, version):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO schema_version (id, version) VALUES (1, %s) "
        "ON CONFLICT (id) DO UPDATE SET version = %s, applied_at = CURRENT_TIMESTAMP",
        (version, version)
    )
    conn.commit()
    cur.close()


def set_version_sqlite(conn, version):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO schema_version (id, version) VALUES (1, ?)", (version,))
    conn.commit()


# ===== v1.0.0 Schema =====

PG_TABLES = [
    ("llm_calls", """
        CREATE TABLE IF NOT EXISTS llm_calls (
            id SERIAL PRIMARY KEY,
            call_id TEXT NOT NULL UNIQUE,
            timestamp TEXT NOT NULL,
            url TEXT NOT NULL,
            is_internal_relay INTEGER NOT NULL DEFAULT 0,
            method TEXT,
            request_headers JSON,
            request_body TEXT,
            response_headers JSON,
            response_body TEXT,
            final_responses_body TEXT,
            call_status TEXT,
            duration_ms INTEGER,
            tokens_input INTEGER,
            tokens_output INTEGER,
            cached_hit_tokens INTEGER,
            cache_miss_tokens INTEGER,
            tokens_per_second REAL,
            token_source TEXT,
            stream_type TEXT,
            first_token_ms INTEGER,
            original_model TEXT,
            overridden_model TEXT,
            user_id INTEGER,
            api_key_id INTEGER,
            previous_response_id TEXT,
            full_context TEXT,
            outbound_diagnostics JSON
        )
    """),
    ("codex_prompt_cache_affinity", """
        CREATE TABLE IF NOT EXISTS codex_prompt_cache_affinity (
            account_id TEXT NOT NULL DEFAULT '',
            prompt_cache_key TEXT NOT NULL,
            session_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            last_used_at DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (account_id, prompt_cache_key)
        )
    """),
    ("users", """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role_id INTEGER REFERENCES roles(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login_at TIMESTAMP
        )
    """),
    ("api_keys", """
        CREATE TABLE IF NOT EXISTS api_keys (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            name VARCHAR(100) NOT NULL,
            key VARCHAR(64) UNIQUE NOT NULL,
            expires_at DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT true
        )
    """),
    ("roles", """
        CREATE TABLE IF NOT EXISTS roles (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL,
            description VARCHAR(200),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("menus", """
        CREATE TABLE IF NOT EXISTS menus (
            id SERIAL PRIMARY KEY,
            code VARCHAR(50) UNIQUE NOT NULL,
            name VARCHAR(50) NOT NULL,
            icon VARCHAR(20),
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("role_menus", """
        CREATE TABLE IF NOT EXISTS role_menus (
            role_id INTEGER NOT NULL REFERENCES roles(id),
            menu_id INTEGER NOT NULL REFERENCES menus(id),
            PRIMARY KEY (role_id, menu_id)
        )
    """),
    ("upstreams", """
        CREATE TABLE IF NOT EXISTS upstreams (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            target_base_url VARCHAR(500) NOT NULL,
            api_key VARCHAR(500),
            auth_mode VARCHAR(32) DEFAULT 'api_key',
            oauth_key VARCHAR(100) DEFAULT 'oauth/kimi-code',
            oauth_host VARCHAR(200) DEFAULT 'https://auth.kimi.com',
            is_active BOOLEAN DEFAULT true,
            description VARCHAR(200),
            use_claude_features BOOLEAN DEFAULT false,
            use_roo_features BOOLEAN DEFAULT false,
            health_status VARCHAR(20) DEFAULT 'healthy',
            consecutive_failures INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("model_configs", """
        CREATE TABLE IF NOT EXISTS model_configs (
            id SERIAL PRIMARY KEY,
            model_key VARCHAR(100) UNIQUE NOT NULL,
            upstream_id INTEGER REFERENCES upstreams(id),
            target_base_url VARCHAR(500),
            api_key VARCHAR(500),
            model_overrides TEXT,
            forward_model VARCHAR(200),
            is_active BOOLEAN DEFAULT true,
            is_default BOOLEAN DEFAULT false,
            use_multi_upstream BOOLEAN DEFAULT false,
            protocol_converter VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("model_upstream_routes", """
        CREATE TABLE IF NOT EXISTS model_upstream_routes (
            id SERIAL PRIMARY KEY,
            model_config_id INTEGER NOT NULL REFERENCES model_configs(id) ON DELETE CASCADE,
            upstream_id INTEGER NOT NULL REFERENCES upstreams(id),
            forward_model VARCHAR(200),
            protocol_converter VARCHAR(50),
            sort_order INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
]

SQLITE_TABLES = [
    ("llm_calls", """
        CREATE TABLE IF NOT EXISTS llm_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id TEXT NOT NULL UNIQUE,
            timestamp TEXT NOT NULL,
            url TEXT NOT NULL,
            is_internal_relay INTEGER NOT NULL DEFAULT 0,
            method TEXT,
            request_headers TEXT,
            request_body TEXT,
            response_headers TEXT,
            response_body TEXT,
            final_responses_body TEXT,
            call_status TEXT,
            duration_ms INTEGER,
            tokens_input INTEGER,
            tokens_output INTEGER,
            cached_hit_tokens INTEGER,
            cache_miss_tokens INTEGER,
            tokens_per_second REAL,
            token_source TEXT,
            stream_type TEXT,
            first_token_ms INTEGER,
            original_model TEXT,
            overridden_model TEXT,
            user_id INTEGER,
            api_key_id INTEGER,
            previous_response_id TEXT,
            full_context TEXT,
            outbound_diagnostics TEXT
        )
    """),
    ("codex_prompt_cache_affinity", """
        CREATE TABLE IF NOT EXISTS codex_prompt_cache_affinity (
            account_id TEXT NOT NULL DEFAULT '',
            prompt_cache_key TEXT NOT NULL,
            session_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            last_used_at REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (account_id, prompt_cache_key)
        )
    """),
    ("users", """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role_id INTEGER REFERENCES roles(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login_at TIMESTAMP
        )
    """),
    ("api_keys", """
        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            name TEXT NOT NULL,
            key TEXT UNIQUE NOT NULL,
            expires_at TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active INTEGER DEFAULT 1
        )
    """),
    ("roles", """
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("menus", """
        CREATE TABLE IF NOT EXISTS menus (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            icon TEXT,
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("role_menus", """
        CREATE TABLE IF NOT EXISTS role_menus (
            role_id INTEGER NOT NULL REFERENCES roles(id),
            menu_id INTEGER NOT NULL REFERENCES menus(id),
            PRIMARY KEY (role_id, menu_id)
        )
    """),
    ("upstreams", """
        CREATE TABLE IF NOT EXISTS upstreams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            target_base_url TEXT NOT NULL,
            api_key TEXT,
            auth_mode TEXT DEFAULT 'api_key',
            oauth_key TEXT DEFAULT 'oauth/kimi-code',
            oauth_host TEXT DEFAULT 'https://auth.kimi.com',
            is_active INTEGER DEFAULT 1,
            description TEXT,
            use_claude_features INTEGER DEFAULT 0,
            use_roo_features INTEGER DEFAULT 0,
            health_status TEXT DEFAULT 'healthy',
            consecutive_failures INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("model_configs", """
        CREATE TABLE IF NOT EXISTS model_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_key TEXT UNIQUE NOT NULL,
            upstream_id INTEGER REFERENCES upstreams(id),
            target_base_url TEXT,
            api_key TEXT,
            model_overrides TEXT,
            forward_model TEXT,
            is_active INTEGER DEFAULT 1,
            is_default INTEGER DEFAULT 0,
            use_multi_upstream INTEGER DEFAULT 0,
            protocol_converter TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
    ("model_upstream_routes", """
        CREATE TABLE IF NOT EXISTS model_upstream_routes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_config_id INTEGER NOT NULL REFERENCES model_configs(id) ON DELETE CASCADE,
            upstream_id INTEGER NOT NULL REFERENCES upstreams(id),
            forward_model TEXT,
            protocol_converter TEXT,
            sort_order INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
]

SEED_DATA = {
    "menus": [
        {"code": "usage_stats", "name": "用量统计", "icon": "📊", "sort_order": 0},
        {"code": "subscription_quotas", "name": "订阅额度", "icon": "💳", "sort_order": 1},
        {"code": "logs", "name": "调用日志", "icon": "📋", "sort_order": 2},
        {"code": "keys", "name": "密钥管理", "icon": "🔑", "sort_order": 3},
        {"code": "upstreams", "name": "上游管理", "icon": "🔗", "sort_order": 4},
        {"code": "models", "name": "模型配置", "icon": "⚙️", "sort_order": 5},
        {"code": "users", "name": "用户管理", "icon": "", "sort_order": 6},
        {"code": "roles", "name": "角色管理", "icon": "🛡️", "sort_order": 7},
    ],
    "roles": [
        {"name": "admin", "description": "管理员，拥有所有菜单权限"},
        {"name": "viewer", "description": "普通用户，仅查看日志和密钥"},
    ],
    "role_menus": {
        "admin": ["usage_stats", "subscription_quotas", "logs", "keys", "upstreams", "models", "users", "roles"],
        "viewer": ["usage_stats", "logs", "keys"],
    },
}


def run_v100_pg(config):
    """执行 v1.0.0 初始化 (PostgreSQL)"""
    from src.auth import hash_password
    conn = get_pg_conn(config)
    cur = conn.cursor()

    # 1. 建表（先建 roles 等被引用的表）
    table_order = ["roles", "menus", "upstreams", "llm_calls", "codex_prompt_cache_affinity", "users", "api_keys", "role_menus", "model_configs", "model_upstream_routes"]
    table_sql = {name: sql for name, sql in PG_TABLES}
    for name in table_order:
        print(f"    [v1.0.0] 创建表: {name}")
        cur.execute(table_sql[name])
    conn.commit()

    # 2. 插入预置菜单
    for m in SEED_DATA["menus"]:
        cur.execute("SELECT id FROM menus WHERE code = %s", (m["code"],))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO menus (code, name, icon, sort_order) VALUES (%s, %s, %s, %s)",
                (m["code"], m["name"], m["icon"], m["sort_order"])
            )
    conn.commit()
    print(f"    [v1.0.0] 插入 {len(SEED_DATA['menus'])} 个预置菜单")

    # 3. 插入预置角色
    for r in SEED_DATA["roles"]:
        cur.execute("SELECT id FROM roles WHERE name = %s", (r["name"],))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO roles (name, description) VALUES (%s, %s)",
                (r["name"], r["description"])
            )
    conn.commit()
    print(f"    [v1.0.0] 插入 {len(SEED_DATA['roles'])} 个预置角色")

    # 4. 关联角色-菜单
    cur.execute("SELECT id, code FROM menus")
    menu_map = {row[1]: row[0] for row in cur.fetchall()}
    for role_name, menu_codes in SEED_DATA["role_menus"].items():
        cur.execute("SELECT id FROM roles WHERE name = %s", (role_name,))
        role_id = cur.fetchone()[0]
        for code in menu_codes:
            cur.execute(
                "INSERT INTO role_menus (role_id, menu_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (role_id, menu_map[code])
            )
    conn.commit()
    print(f"    [v1.0.0] 关联角色-菜单权限")

    # 5. 创建默认管理员
    cur.execute("SELECT id FROM users WHERE email = %s", ("admin",))
    if cur.fetchone() is None:
        cur.execute("SELECT id FROM roles WHERE name = %s", ("admin",))
        admin_role_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO users (email, password_hash, role_id) VALUES (%s, %s, %s)",
            ("admin", hash_password("admin"), admin_role_id)
        )
        conn.commit()
        print("    [v1.0.0] 默认管理员已创建: admin / admin (角色: admin)")
    else:
        print("    [v1.0.0] admin 用户已存在，跳过")

    cur.close()
    conn.close()


def run_v100_sqlite(config):
    """执行 v1.0.0 初始化 (SQLite)"""
    import sqlite3
    from src.auth import hash_password
    conn = sqlite3.connect(config.database.path)
    cur = conn.cursor()

    # 1. 建表
    table_order = ["roles", "menus", "upstreams", "llm_calls", "codex_prompt_cache_affinity", "users", "api_keys", "role_menus", "model_configs", "model_upstream_routes"]
    table_sql = {name: sql for name, sql in SQLITE_TABLES}
    for name in table_order:
        print(f"    [v1.0.0] 创建表: {name}")
        cur.execute(table_sql[name])
    conn.commit()

    # 2. 插入预置菜单
    for m in SEED_DATA["menus"]:
        cur.execute("SELECT id FROM menus WHERE code = ?", (m["code"],))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO menus (code, name, icon, sort_order) VALUES (?, ?, ?, ?)",
                (m["code"], m["name"], m["icon"], m["sort_order"])
            )
    conn.commit()
    print(f"    [v1.0.0] 插入 {len(SEED_DATA['menus'])} 个预置菜单")

    # 3. 插入预置角色
    for r in SEED_DATA["roles"]:
        cur.execute("SELECT id FROM roles WHERE name = ?", (r["name"],))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO roles (name, description) VALUES (?, ?)",
                (r["name"], r["description"])
            )
    conn.commit()
    print(f"    [v1.0.0] 插入 {len(SEED_DATA['roles'])} 个预置角色")

    # 4. 关联角色-菜单
    cur.execute("SELECT id, code FROM menus")
    menu_map = {row[1]: row[0] for row in cur.fetchall()}
    for role_name, menu_codes in SEED_DATA["role_menus"].items():
        cur.execute("SELECT id FROM roles WHERE name = ?", (role_name,))
        role_id = cur.fetchone()[0]
        for code in menu_codes:
            try:
                cur.execute(
                    "INSERT INTO role_menus (role_id, menu_id) VALUES (?, ?)",
                    (role_id, menu_map[code])
                )
            except sqlite3.IntegrityError:
                pass  # 已存在
    conn.commit()
    print(f"    [v1.0.0] 关联角色-菜单权限")

    # 5. 创建默认管理员
    cur.execute("SELECT id FROM users WHERE email = ?", ("admin",))
    if cur.fetchone() is None:
        cur.execute("SELECT id FROM roles WHERE name = ?", ("admin",))
        admin_role_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO users (email, password_hash, role_id) VALUES (?, ?, ?)",
            ("admin", hash_password("admin"), admin_role_id)
        )
        conn.commit()
        print("    [v1.0.0] 默认管理员已创建: admin / admin (角色: admin)")
    else:
        print("    [v1.0.0] admin 用户已存在，跳过")

    cur.close()
    conn.close()


# ===== v1.1.0 Schema Migration =====

def _pg_add_column(conn, table, column, col_type):
    """PostgreSQL 安全添加列（忽略已存在）"""
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}")
        conn.commit()
        print(f"    [v1.1.0] {table}.{column} 已确保存在")
    finally:
        cur.close()


def _sqlite_add_column(conn, table, column, col_type):
    """SQLite 安全添加列（忽略已存在）"""
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cur.fetchall()}
        if column in existing_columns:
            print(f"    [v1.1.0] {table}.{column} 已存在，跳过")
            return
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        conn.commit()
        print(f"    [v1.1.0] {table}.{column} 已添加")
    finally:
        cur.close()


def run_v110_pg(conn):
    """v1.0.0 -> v1.1.0 升级 (PostgreSQL)"""
    _pg_add_column(conn, "llm_calls", "previous_response_id", "TEXT")
    _pg_add_column(conn, "llm_calls", "full_context", "TEXT")
    _pg_add_column(conn, "llm_calls", "final_responses_body", "TEXT")
    _pg_add_column(conn, "llm_calls", "call_status", "TEXT")
    _pg_add_column(conn, "llm_calls", "cached_hit_tokens", "INTEGER")
    _pg_add_column(conn, "llm_calls", "cache_miss_tokens", "INTEGER")
    _pg_add_column(conn, "llm_calls", "tokens_per_second", "REAL")
    _pg_add_column(conn, "model_configs", "protocol_converter", "VARCHAR(50)")
    _pg_add_column(conn, "model_upstream_routes", "protocol_converter", "VARCHAR(50)")


def run_v110_sqlite(conn):
    """v1.0.0 -> v1.1.0 升级 (SQLite)"""
    _sqlite_add_column(conn, "llm_calls", "previous_response_id", "TEXT")
    _sqlite_add_column(conn, "llm_calls", "full_context", "TEXT")
    _sqlite_add_column(conn, "llm_calls", "final_responses_body", "TEXT")
    _sqlite_add_column(conn, "llm_calls", "call_status", "TEXT")
    _sqlite_add_column(conn, "llm_calls", "cached_hit_tokens", "INTEGER")
    _sqlite_add_column(conn, "llm_calls", "cache_miss_tokens", "INTEGER")
    _sqlite_add_column(conn, "llm_calls", "tokens_per_second", "REAL")
    _sqlite_add_column(conn, "model_configs", "protocol_converter", "TEXT")
    _sqlite_add_column(conn, "model_upstream_routes", "protocol_converter", "TEXT")



def run_v111_pg(conn):
    """v1.1.0 -> v1.1.1 升级 (PostgreSQL): 添加用量统计菜单和权限"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM menus WHERE code = %s", ("usage_stats",))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO menus (code, name, icon, sort_order) VALUES (%s, %s, %s, %s)",
                ("usage_stats", "用量统计", "📊", 0)
            )
            print("    [v1.1.1] 插入菜单: usage_stats (用量统计)")
        else:
            print("    [v1.1.1] 菜单 usage_stats 已存在，跳过")

        cur.execute("SELECT id, code FROM menus")
        menu_map = {row[1]: row[0] for row in cur.fetchall()}

        cur.execute("SELECT id, name FROM roles")
        role_map = {row[1]: row[0] for row in cur.fetchall()}

        for role_name, menu_codes in [("admin", ["usage_stats"]), ("viewer", ["usage_stats"])]:
            role_id = role_map.get(role_name)
            if role_id is None:
                continue
            for code in menu_codes:
                cur.execute(
                    "INSERT INTO role_menus (role_id, menu_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (role_id, menu_map[code])
                )
        conn.commit()
        print("    [v1.1.1] 角色菜单权限已更新")
    finally:
        cur.close()


def run_v111_sqlite(conn):
    """v1.1.0 -> v1.1.1 升级 (SQLite): 添加用量统计菜单和权限"""
    import sqlite3
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM menus WHERE code = ?", ("usage_stats",))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO menus (code, name, icon, sort_order) VALUES (?, ?, ?, ?)",
                ("usage_stats", "用量统计", "📊", 0)
            )
            print("    [v1.1.1] 插入菜单: usage_stats (用量统计)")
        else:
            print("    [v1.1.1] 菜单 usage_stats 已存在，跳过")

        cur.execute("SELECT id, code FROM menus")
        menu_map = {row[1]: row[0] for row in cur.fetchall()}

        cur.execute("SELECT id, name FROM roles")
        role_map = {row[1]: row[0] for row in cur.fetchall()}

        for role_name, menu_codes in [("admin", ["usage_stats"]), ("viewer", ["usage_stats"])]:
            role_id = role_map.get(role_name)
            if role_id is None:
                continue
            for code in menu_codes:
                try:
                    cur.execute(
                        "INSERT INTO role_menus (role_id, menu_id) VALUES (?, ?)",
                        (role_id, menu_map[code])
                    )
                except sqlite3.IntegrityError:
                    pass
        conn.commit()
        print("    [v1.1.1] 角色菜单权限已更新")
    finally:
        cur.close()


def run_v112_pg(conn):
    """v1.1.1 -> v1.1.2 升级 (PostgreSQL): 上游新增 kimi-cli auth 字段"""
    _pg_add_column(conn, "upstreams", "auth_mode", "VARCHAR(32) DEFAULT 'api_key'")
    _pg_add_column(conn, "upstreams", "oauth_key", "VARCHAR(100) DEFAULT 'oauth/kimi-code'")
    _pg_add_column(conn, "upstreams", "oauth_host", "VARCHAR(200) DEFAULT 'https://auth.kimi.com'")


def run_v112_sqlite(conn):
    """v1.1.1 -> v1.1.2 升级 (SQLite): 上游新增 kimi-cli auth 字段"""
    _sqlite_add_column(conn, "upstreams", "auth_mode", "TEXT DEFAULT 'api_key'")
    _sqlite_add_column(conn, "upstreams", "oauth_key", "TEXT DEFAULT 'oauth/kimi-code'")
    _sqlite_add_column(conn, "upstreams", "oauth_host", "TEXT DEFAULT 'https://auth.kimi.com'")


def run_v113_pg(conn):
    """v1.1.2 -> v1.1.3 升级 (PostgreSQL): Codex 出站诊断信息"""
    _pg_add_column(conn, "llm_calls", "outbound_diagnostics", "JSON")


def run_v113_sqlite(conn):
    """v1.1.2 -> v1.1.3 升级 (SQLite): Codex 出站诊断信息"""
    _sqlite_add_column(conn, "llm_calls", "outbound_diagnostics", "TEXT")


def run_v114_pg(conn):
    """v1.1.3 -> v1.1.4：添加订阅额度菜单并授权管理员。"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM menus WHERE code = %s", ("subscription_quotas",))
        if cur.fetchone() is None:
            cur.execute("UPDATE menus SET sort_order = sort_order + 1 WHERE sort_order >= %s", (1,))
        cur.execute(
            "INSERT INTO menus (code, name, icon, sort_order) VALUES (%s, %s, %s, %s) ON CONFLICT (code) DO NOTHING",
            ("subscription_quotas", "订阅额度", "💳", 1),
        )
        cur.execute(
            "INSERT INTO role_menus (role_id, menu_id) "
            "SELECT r.id, m.id FROM roles r, menus m "
            "WHERE r.name = %s AND m.code = %s ON CONFLICT DO NOTHING",
            ("admin", "subscription_quotas"),
        )
        conn.commit()
    finally:
        cur.close()


def run_v114_sqlite(conn):
    """v1.1.3 -> v1.1.4：添加订阅额度菜单并授权管理员。"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM menus WHERE code = ?", ("subscription_quotas",))
        if cur.fetchone() is None:
            cur.execute("UPDATE menus SET sort_order = sort_order + 1 WHERE sort_order >= ?", (1,))
        cur.execute(
            "INSERT OR IGNORE INTO menus (code, name, icon, sort_order) VALUES (?, ?, ?, ?)",
            ("subscription_quotas", "订阅额度", "💳", 1),
        )
        cur.execute(
            "INSERT OR IGNORE INTO role_menus (role_id, menu_id) "
            "SELECT r.id, m.id FROM roles r, menus m WHERE r.name = ? AND m.code = ?",
            ("admin", "subscription_quotas"),
        )
        conn.commit()
    finally:
        cur.close()


def run_v115_pg(conn):
    """v1.1.4 -> v1.1.5：持久化 Codex prompt cache 亲和关系。"""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS codex_prompt_cache_affinity (
                account_id TEXT NOT NULL DEFAULT '',
                prompt_cache_key TEXT NOT NULL,
                session_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                last_used_at DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account_id, prompt_cache_key)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_codex_prompt_cache_affinity_last_used
            ON codex_prompt_cache_affinity (last_used_at)
        """)
        conn.commit()
    finally:
        cur.close()


def run_v115_sqlite(conn):
    """v1.1.4 -> v1.1.5：持久化 Codex prompt cache 亲和关系。"""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS codex_prompt_cache_affinity (
                account_id TEXT NOT NULL DEFAULT '',
                prompt_cache_key TEXT NOT NULL,
                session_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                last_used_at REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account_id, prompt_cache_key)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_codex_prompt_cache_affinity_last_used
            ON codex_prompt_cache_affinity (last_used_at)
        """)
        conn.commit()
    finally:
        cur.close()


def run_v116_pg(conn):
    """v1.1.5 -> v1.1.6：添加内部中继标志并回填历史记录。"""
    _pg_add_column(conn, "llm_calls", "is_internal_relay", "INTEGER NOT NULL DEFAULT 0")
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE llm_calls
            SET is_internal_relay = 1
            WHERE is_internal_relay = 0
              AND (
                  url LIKE 'http://127.0.0.1:%/stream'
                  OR url LIKE 'http://localhost:%/stream'
              )
            """
        )
        conn.commit()
    finally:
        cur.close()


def run_v116_sqlite(conn):
    """v1.1.5 -> v1.1.6：添加内部中继标志并回填历史记录。"""
    _sqlite_add_column(conn, "llm_calls", "is_internal_relay", "INTEGER NOT NULL DEFAULT 0")
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE llm_calls
            SET is_internal_relay = 1
            WHERE is_internal_relay = 0
              AND (
                  url LIKE 'http://127.0.0.1:%/stream'
                  OR url LIKE 'http://localhost:%/stream'
              )
            """
        )
        conn.commit()
    finally:
        cur.close()


def run_v117_pg(conn):
    """v1.1.6 -> v1.1.7：启用 pg_trgm 并为模型包含搜索创建 GIN 索引。"""
    cur = conn.cursor()
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_calls_original_model_trgm
            ON llm_calls USING GIN (original_model gin_trgm_ops)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_calls_overridden_model_trgm
            ON llm_calls USING GIN (overridden_model gin_trgm_ops)
        """)
        conn.commit()
    finally:
        cur.close()


def run_v117_sqlite(conn):
    """v1.1.6 -> v1.1.7：SQLite 不支持 PostgreSQL pg_trgm，跳过模型包含搜索索引。"""
    print("[v1.1.7] SQLite 不支持 PostgreSQL pg_trgm/GIN 索引，跳过模型包含搜索索引。")


def run_v118_pg(conn):
    """v1.1.7 -> v1.1.8：为调用日志分页过滤与排序创建部分索引。"""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_calls_visible_ts_id
            ON llm_calls (timestamp DESC, id DESC)
            WHERE is_internal_relay = 0
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_calls_user_visible_ts_id
            ON llm_calls (user_id, timestamp DESC, id DESC)
            WHERE is_internal_relay = 0
        """)
        conn.commit()
    finally:
        cur.close()


def run_v118_sqlite(conn):
    """v1.1.7 -> v1.1.8：为调用日志分页过滤与排序创建 SQLite 部分索引。"""
    print("[v1.1.8] 创建调用日志分页过滤与排序索引。")
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_calls_visible_ts_id
            ON llm_calls (timestamp DESC, id DESC)
            WHERE is_internal_relay = 0
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_calls_user_visible_ts_id
            ON llm_calls (user_id, timestamp DESC, id DESC)
            WHERE is_internal_relay = 0
        """)
        conn.commit()
    finally:
        cur.close()


def main():
    print("=" * 60)
    print("LLM Router — Database Initialization")
    print("=" * 60)

    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    from src.config import load_config
    config = load_config(config_path)

    is_pg = config.database.postgresql is not None
    db_label = f"PostgreSQL ({config.database.postgresql.dbname})" if is_pg else f"SQLite ({config.database.path})"
    print(f"\nDatabase: {db_label}")
    print(f"Config:   {config_path}")
    print(f"Target:   v{CURRENT_VERSION}")

    # 连接数据库
    if is_pg:
        conn = get_pg_conn(config)
        version = init_schema_version_pg(conn)
    else:
        import sqlite3
        conn = sqlite3.connect(config.database.path)
        version = init_schema_version_sqlite(conn)

    print(f"Current version: {version or '(empty)'}")

    if version is None:
        print(f"\n[v{CURRENT_VERSION}] 空库，执行完整初始化...")
    elif version == CURRENT_VERSION:
        print(f"\n[v{CURRENT_VERSION}] 数据库版本已是最新，检查当前分支新增字段...")
    elif version in ("1.0.0", "1.1.0", "1.1.1", "1.1.2", "1.1.3", "1.1.4", "1.1.5", "1.1.6", "1.1.7"):
        print(f"\n[v{CURRENT_VERSION}] 版本 {version} -> {CURRENT_VERSION}，执行升级...")
    else:
        conn.close()
        raise RuntimeError(f"不支持的数据库版本: {version}")

    # 执行迁移
    if version is None:
        if is_pg:
            run_v100_pg(config)
        else:
            run_v100_sqlite(config)

    # v1.0.0 -> v1.1.0
    if version in (None, "1.0.0", CURRENT_VERSION):
        print(f"\n[v1.1.0] 检查/补齐当前分支新增字段...")
        if is_pg:
            run_v110_pg(conn)
        else:
            run_v110_sqlite(conn)


    # v1.1.0 -> v1.1.1
    if version in (None, "1.0.0", "1.1.0", CURRENT_VERSION):
        print(f"\n[v1.1.1] 检查/补齐用量统计菜单和权限...")
        if is_pg:
            run_v111_pg(conn)
        else:
            run_v111_sqlite(conn)
    # v1.1.1 -> v1.1.2
    if version in (None, "1.0.0", "1.1.0", "1.1.1", CURRENT_VERSION):
        print(f"\n[v1.1.2] 检查/补齐上游 kimi-cli auth 字段...")
        if is_pg:
            run_v112_pg(conn)
        else:
            run_v112_sqlite(conn)
    # v1.1.2 -> v1.1.3
    if version in (None, "1.0.0", "1.1.0", "1.1.1", "1.1.2", CURRENT_VERSION):
        print(f"\n[v1.1.3] 检查/补齐 Codex 出站诊断字段...")
        if is_pg:
            run_v113_pg(conn)
        else:
            run_v113_sqlite(conn)

    if version in (None, "1.0.0", "1.1.0", "1.1.1", "1.1.2", "1.1.3", CURRENT_VERSION):
        print("\n[v1.1.4] 检查/补齐订阅额度菜单和权限...")
        if is_pg:
            run_v114_pg(conn)
        else:
            run_v114_sqlite(conn)

    if version in (None, "1.0.0", "1.1.0", "1.1.1", "1.1.2", "1.1.3", "1.1.4", CURRENT_VERSION):
        print("\n[v1.1.5] 检查/补齐 Codex prompt cache 亲和关系表...")
        if is_pg:
            run_v115_pg(conn)
        else:
            run_v115_sqlite(conn)

    if version in (None, "1.0.0", "1.1.0", "1.1.1", "1.1.2", "1.1.3", "1.1.4", "1.1.5", CURRENT_VERSION):
        print("\n[v1.1.6] 检查/补齐内部中继标志字段...")
        if is_pg:
            run_v116_pg(conn)
        else:
            run_v116_sqlite(conn)
    if version in (None, "1.0.0", "1.1.0", "1.1.1", "1.1.2", "1.1.3", "1.1.4", "1.1.5", "1.1.6", CURRENT_VERSION):
        print("\n[v1.1.7] 检查/补齐 PostgreSQL pg_trgm 模型包含搜索索引...")
        if is_pg:
            run_v117_pg(conn)
        else:
            run_v117_sqlite(conn)
    if version in (None, "1.0.0", "1.1.0", "1.1.1", "1.1.2", "1.1.3", "1.1.4", "1.1.5", "1.1.6", "1.1.7", CURRENT_VERSION):
        print("\n[v1.1.8] 检查/补齐调用日志分页过滤与排序索引...")
        if is_pg:
            run_v118_pg(conn)
        else:
            run_v118_sqlite(conn)
    # 写入版本
    if is_pg:
        set_version_pg(conn, CURRENT_VERSION)
    else:
        set_version_sqlite(conn, CURRENT_VERSION)

    print(f"\n[v{CURRENT_VERSION}] 初始化完成！数据库版本已标记为 {CURRENT_VERSION}")
    conn.close()
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
