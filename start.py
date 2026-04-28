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


def init_auth(storage):
    """初始化认证表并创建默认管理员"""
    try:
        # 判断是否使用 PostgreSQL
        is_pg = storage.postgresql is not None
        if is_pg:
            import psycopg2
            conn = psycopg2.connect(
                host=storage.postgresql.host, port=storage.postgresql.port,
                user=storage.postgresql.user, password=storage.postgresql.password,
                database=storage.postgresql.dbname
            )
            cur = conn.cursor()
            storage.init_auth_tables(is_pg=True, cur=cur, conn=conn)
            cur.close()
            conn.close()
        else:
            storage.init_auth_tables(is_pg=False)

        # 检查是否已有用户
        if storage.get_user_count() == 0:
            from src.auth import hash_password
            storage.create_user("admin", hash_password("admin"))
            print("  [认证] 默认管理员已创建: admin / admin")
        else:
            print("  [认证] 认证表已就绪")
    except Exception as e:
        print(f"  [认证] 初始化警告: {e}")


async def run_proxy(config):
    """运行代理服务器"""
    from mitmproxy.options import Options
    from mitmproxy.tools.dump import DumpMaster

    options = Options()
    options.set(f"listen_port={config.proxy.listen_port}", f"mode=regular")

    master = DumpMaster(options)

    addon = create_addon(config)
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
    print(f"Model mappings: {len(config.proxy.model_mappings)}")
    for key, mapping in config.proxy.model_mappings.items():
        api_key_display = f"{mapping.api_key[:8]}..." if mapping.api_key else "(none)"
        print(f"  {key} => {mapping.target_base_url}  [key: {api_key_display}]")
        if mapping.model_overrides:
            for src, dst in mapping.model_overrides.items():
                print(f"    model: {src} -> {dst}")
    print(f"Database: {config.database.path}")
    print(f"Query API: http://localhost:{config.proxy.listen_port}/api/calls")

    # 初始化认证系统
    print("\nInitializing auth system...")
    from src.storage import CallStorage
    storage = CallStorage(
        config.database.path,
        config.database.postgresql
    )
    init_auth(storage)

    # 使用mitmproxy Python API启动代理
    print(f"\nStarting proxy on port {config.proxy.listen_port}...")
    print("Press Ctrl+C to stop\n")

    asyncio.run(run_proxy(config))


if __name__ == "__main__":
    main()
