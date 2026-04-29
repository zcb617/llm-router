"""
数据库初始化脚本 — 创建 1.0.0 完整 schema + 种子数据

用法:
    python scripts/init_database.py                 # 使用默认 config.yaml
    python scripts/init_database.py config.yaml     # 指定配置文件

执行后会:
  1. 创建 schema_version 表
  2. 检查当前版本，如果是空库 → 执行 v1.0.0 初始化
  3. v1.0.0 内容：
     - 基础表: llm_calls, users, api_keys, model_configs
     - RBAC 表: roles, menus, role_menus, upstreams
     - 预置菜单、角色、关联关系
     - 默认 admin 账号 (admin / admin)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

CURRENT_VERSION = "1.0.0"


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
            overridden_model TEXT,
            user_id INTEGER,
            api_key_id INTEGER
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
            is_active BOOLEAN DEFAULT true,
            description VARCHAR(200),
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
            overridden_model TEXT,
            user_id INTEGER,
            api_key_id INTEGER
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
            is_active INTEGER DEFAULT 1,
            description TEXT,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """),
]

SEED_DATA = {
    "menus": [
        {"code": "logs", "name": "调用日志", "icon": "📋", "sort_order": 1},
        {"code": "keys", "name": "密钥管理", "icon": "🔑", "sort_order": 2},
        {"code": "upstreams", "name": "上游管理", "icon": "🔗", "sort_order": 3},
        {"code": "models", "name": "模型配置", "icon": "⚙️", "sort_order": 4},
        {"code": "users", "name": "用户管理", "icon": "", "sort_order": 5},
        {"code": "roles", "name": "角色管理", "icon": "🛡️", "sort_order": 6},
    ],
    "roles": [
        {"name": "admin", "description": "管理员，拥有所有菜单权限"},
        {"name": "viewer", "description": "普通用户，仅查看日志和密钥"},
    ],
    "role_menus": {
        "admin": ["logs", "keys", "upstreams", "models", "users", "roles"],
        "viewer": ["logs", "keys"],
    },
}


def run_v100_pg(config):
    """执行 v1.0.0 初始化 (PostgreSQL)"""
    from src.auth import hash_password
    conn = get_pg_conn(config)
    cur = conn.cursor()

    # 1. 建表（先建 roles 等被引用的表）
    table_order = ["roles", "menus", "upstreams", "llm_calls", "users", "api_keys", "role_menus", "model_configs"]
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
    table_order = ["roles", "menus", "upstreams", "llm_calls", "users", "api_keys", "role_menus", "model_configs"]
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

    if version == CURRENT_VERSION:
        print(f"\n[v{CURRENT_VERSION}] 数据库已是最新版本，无需操作。")
        conn.close()
        return

    if version is None:
        print(f"\n[v{CURRENT_VERSION}] 空库，执行完整初始化...")
    else:
        print(f"\n[v{CURRENT_VERSION}] 版本 {version} -> {CURRENT_VERSION}，执行升级...")

    # 执行 v1.0.0
    if is_pg:
        run_v100_pg(config)
    else:
        run_v100_sqlite(config)

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
