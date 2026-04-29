"""
RBAC 权限管理 — 角色、菜单、用户关联初始化
"""

# 预置菜单定义
PRESET_MENUS = [
    {"code": "logs", "name": "调用日志", "icon": "📋", "sort_order": 1},
    {"code": "keys", "name": "密钥管理", "icon": "🔑", "sort_order": 2},
    {"code": "upstreams", "name": "上游管理", "icon": "🔗", "sort_order": 3},
    {"code": "models", "name": "模型配置", "icon": "⚙️", "sort_order": 4},
    {"code": "users", "name": "用户管理", "icon": "👤", "sort_order": 5},
    {"code": "roles", "name": "角色管理", "icon": "🛡️", "sort_order": 6},
]

# 预置角色及其菜单权限
PRESET_ROLES = [
    {
        "name": "admin",
        "description": "管理员，拥有所有菜单权限",
        "menus": ["logs", "keys", "upstreams", "models", "users", "roles"],
    },
    {
        "name": "viewer",
        "description": "普通用户，仅查看日志和密钥",
        "menus": ["logs", "keys"],
    },
]


def init_rbac(storage):
    """
    初始化 RBAC 系统：
    1. 创建 RBAC 相关表（roles, menus, role_menus）
    2. 给 users 表加 role_id 字段
    3. 插入预置菜单
    4. 插入预置角色并关联菜单
    5. 如果用户数为 0，创建 admin/admin 用户并设为 admin 角色
    """
    from src.auth import hash_password

    is_pg = storage.postgresql is not None

    # 1. 建表
    if is_pg:
        import psycopg2
        conn = psycopg2.connect(
            host=storage.postgresql.host, port=storage.postgresql.port,
            user=storage.postgresql.user, password=storage.postgresql.password,
            database=storage.postgresql.dbname
        )
        cur = conn.cursor()
        storage.init_rbac_tables(is_pg=True, cur=cur, conn=conn)
        conn.commit()
        cur.close()
        conn.close()
    else:
        storage.init_rbac_tables(is_pg=False)

    # 2. 插入预置菜单
    for menu_def in PRESET_MENUS:
        existing = storage.find_menu_by_code(menu_def["code"])
        if existing is None:
            storage.create_menu(
                code=menu_def["code"],
                name=menu_def["name"],
                icon=menu_def["icon"],
                sort_order=menu_def["sort_order"],
            )

    # 3. 插入预置角色并关联菜单
    for role_def in PRESET_ROLES:
        existing = storage.find_role_by_name(role_def["name"])
        if existing is None:
            role_id = storage.create_role(
                name=role_def["name"],
                description=role_def["description"],
            )
        else:
            role_id = existing["id"]

        # 关联菜单
        for menu_code in role_def["menus"]:
            menu = storage.find_menu_by_code(menu_code)
            if menu:
                storage.assign_menu_to_role(role_id, menu["id"])

    # 4. 如果没有用户，创建默认管理员
    if storage.get_user_count() == 0:
        uid = storage.create_user("admin", hash_password("admin"))
        admin_role = storage.find_role_by_name("admin")
        if admin_role:
            storage.set_user_role(uid, admin_role["id"])
        print("  [RBAC] 默认管理员已创建: admin / admin (角色: admin)")
    else:
        # 已存在用户，检查 admin 用户是否已关联角色
        if storage.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=storage.postgresql.host, port=storage.postgresql.port,
                user=storage.postgresql.user, password=storage.postgresql.password,
                database=storage.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("SELECT id, role_id FROM users WHERE email = %s", ("admin",))
        else:
            import sqlite3
            conn = sqlite3.connect(storage.db_path)
            cur = conn.cursor()
            cur.execute("SELECT id, role_id FROM users WHERE email = ?", ("admin",))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            user_id, role_id = row[0], row[1]
            if role_id is None:
                admin_role = storage.find_role_by_name("admin")
                if admin_role:
                    storage.set_user_role(user_id, admin_role["id"])
                    print("  [RBAC] 已为 admin 用户关联管理员角色")
            else:
                print("  [RBAC] admin 用户已关联角色")
        print("  [RBAC] RBAC 表已就绪")
