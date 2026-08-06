"""
LLM Router 启动脚本
启动代理服务器

前置条件：首次运行前需执行数据库初始化
    python scripts/init_database.py [config.yaml]
"""
import sys
import asyncio
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

# 配置日志输出到文件和控制台
log_dir = Path(__file__).parent / "log"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / "llm_router.log"

handler_file = RotatingFileHandler(
    log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
handler_file.setLevel(logging.INFO)
handler_file.setFormatter(logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
))

handler_console = logging.StreamHandler()
handler_console.setLevel(logging.INFO)
handler_console.setFormatter(logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
))

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler_file, handler_console],
)

from src.config import load_config
from src.proxy import create_addon


async def run_proxy(config, storage):
    """运行代理服务器"""
    from mitmproxy.options import Options
    from mitmproxy.tools.dump import DumpMaster
    from src.codex_app_server import CodexBridgeServer

    options = Options()
    options.set(f"listen_port={config.proxy.listen_port}", f"mode=regular")

    master = DumpMaster(options)
    codex_bridge = CodexBridgeServer(storage)
    codex_bridge_url = await codex_bridge.start()

    addon = create_addon(
        config,
        storage,
        codex_bridge_url=codex_bridge_url,
        codex_bridge_token=codex_bridge.bridge_token,
    )
    master.addons.add(addon)

    try:
        await master.run()
    except KeyboardInterrupt:
        print("\nShutting down...")
        master.shutdown()
    finally:
        await codex_bridge.stop()


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

    # 初始化数据库连接
    print("\nConnecting to database...")
    from src.storage import CallStorage
    storage = CallStorage(
        config.database.path,
        config.database.postgresql
    )

    # 启动代理
    print(f"\nStarting proxy on port {config.proxy.listen_port}...")
    print("Press Ctrl+C to stop\n")

    asyncio.run(run_proxy(config, storage))


if __name__ == "__main__":
    main()
