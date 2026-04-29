"""
LLM Router 启动脚本
启动代理服务器
"""
import sys
import asyncio
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from src.config import load_config
from src.proxy import create_addon


def init_rbac_and_data(storage):
    """初始化 RBAC 系统（表、角色、菜单、默认管理员）+ 数据迁移"""
    try:
        from src.rbac import init_rbac
        init_rbac(storage)

        # 数据迁移：将已有 model_configs 的 url+key 提取到 upstreams 表
        migrate_upstreams(storage)
    except Exception as e:
        print(f"  [RBAC] 初始化警告: {e}")


def migrate_upstreams(storage):
    """将已有 model_configs 的 target_base_url+api_key 迁移到 upstreams 表"""
    try:
        # 直接查 model_configs 表，不依赖 upstreams JOIN（避免表不存在时报错）
        if storage.postgresql:
            import psycopg2
            conn = psycopg2.connect(
                host=storage.postgresql.host, port=storage.postgresql.port,
                user=storage.postgresql.user, password=storage.postgresql.password,
                database=storage.postgresql.dbname
            )
            cur = conn.cursor()
            cur.execute("SELECT id, model_key, target_base_url, api_key FROM model_configs WHERE target_base_url IS NOT NULL AND target_base_url != ''")
            rows = cur.fetchall()
            configs = [{"id": r[0], "model_key": r[1], "target_base_url": r[2], "api_key": r[3]} for r in rows]
            cur.close()
            conn.close()
        else:
            import sqlite3
            conn = sqlite3.connect(storage.db_path)
            cur = conn.cursor()
            cur.execute("SELECT id, model_key, target_base_url, api_key FROM model_configs WHERE target_base_url IS NOT NULL AND target_base_url != ''")
            configs = [{"id": r[0], "model_key": r[1], "target_base_url": r[2], "api_key": r[3]} for r in cur.fetchall()]
            cur.close()
            conn.close()

        if not configs:
            print("  [迁移] 无需迁移（没有需要迁移的模型配置）")
            return

        url_key_map = {}  # (url, key) -> upstream_id
        migrated_count = 0

        for cfg in configs:
            target_url = cfg["target_base_url"]
            api_key = cfg.get("api_key", "") or ""

            key = (target_url, api_key)
            if key in url_key_map:
                upstream_id = url_key_map[key]
            else:
                # 先检查是否已有相同 url 的 upstream
                existing = storage.get_upstream_by_url(target_url)
                if existing:
                    upstream_id = existing["id"]
                else:
                    name = f"upstream_{len(url_key_map) + 1}"
                    upstream_id = storage.create_upstream(
                        name=name, target_base_url=target_url,
                        api_key=api_key, description="自动从模型配置迁移"
                    )
                    print(f"  [迁移] 创建上游: {name} ({target_url})")
                url_key_map[key] = upstream_id

            # 关联 model_config 到 upstream
            storage.link_model_to_upstream(cfg["id"], upstream_id)
            migrated_count += 1

        if migrated_count > 0:
            print(f"  [迁移] 完成！共迁移 {migrated_count} 个模型配置，创建 {len(url_key_map)} 个上游")
        else:
            print("  [迁移] 无需迁移（所有模型配置已关联上游）")
    except Exception as e:
        import traceback
        print(f"  [迁移] 失败: {e}")
        traceback.print_exc()


async def run_proxy(config, storage):
    """运行代理服务器"""
    from mitmproxy.options import Options
    from mitmproxy.tools.dump import DumpMaster

    options = Options()
    options.set(f"listen_port={config.proxy.listen_port}", f"mode=regular")

    master = DumpMaster(options)

    addon = create_addon(config, storage)
    master.addons.add(addon)

    try:
        await master.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
        master.shutdown()


def main():
    """主函数"""
    print("=" * 60)
    print("LLM Router - Local Proxy for LLM Calls")
    print("=" * 60)

    # 加载配置
    config_path = "config.yaml"
    if len(sys.argv) > 1:
        config_path = sys.argv[1]

    print(f"\nLoading config: {config_path}")
    config = load_config(config_path)

    print(f"Proxy port: {config.proxy.listen_port}")
    print(f"Default model: {config.proxy.default_model or '(none)'}")
    print(f"Database: {config.database.path}")
    print(f"Query API: http://localhost:{config.proxy.listen_port}/api/calls")

    # 初始化 RBAC 系统
    print("\nInitializing RBAC system...")
    from src.storage import CallStorage
    storage = CallStorage(
        config.database.path,
        config.database.postgresql
    )
    init_rbac_and_data(storage)

    # 使用mitmproxy Python API启动代理
    print(f"\nStarting proxy on port {config.proxy.listen_port}...")
    print("Press Ctrl+C to stop\n")

    asyncio.run(run_proxy(config, storage))


if __name__ == "__main__":
    main()
