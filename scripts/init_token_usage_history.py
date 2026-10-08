"""
初始化历史 Token 使用汇总。

用法:
    python3 scripts/init_token_usage_history.py                 # 使用默认 config.yaml
    python3 scripts/init_token_usage_history.py config.yaml     # 指定配置文件

脚本只读取今天以前的日志日期，并复用每日定时任务使用的
CallStorage.import_token_usage_history() 逐日导入；已有汇总且数据相等时跳过，来源非空且不一致时重建该日汇总。
"""
import sys
from datetime import date
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))


def get_usage_days(storage, today: str) -> list[str]:
    """读取日志表中早于今天的日期。"""
    conn, cur = storage._pg_conn() if storage.postgresql else storage._sqlite_conn()
    try:
        if storage.postgresql:
            cur.execute(
                "SELECT DISTINCT SUBSTR(timestamp, 1, 10) AS usage_day "
                "FROM llm_calls "
                "WHERE SUBSTR(timestamp, 1, 10) < %s "
                "ORDER BY usage_day",
                (today,),
            )
        else:
            cur.execute(
                "SELECT DISTINCT SUBSTR(timestamp, 1, 10) AS usage_day "
                "FROM llm_calls "
                "WHERE SUBSTR(timestamp, 1, 10) < ? "
                "ORDER BY usage_day",
                (today,),
            )
        return [row[0] for row in cur.fetchall()]
    finally:
        if storage.postgresql:
            storage._pg_close(conn, cur)
        else:
            storage._sqlite_close(conn, cur)


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"

    from src.config import load_config
    from src.storage import CallStorage

    config = load_config(config_path)
    storage = CallStorage(config.database.path, config.database.postgresql)
    today = date.today().isoformat()

    try:
        usage_days = get_usage_days(storage, today)
        print(f"发现 {len(usage_days)} 个历史日期，今天 {today} 不处理。")

        total_rows = 0
        for usage_day in usage_days:
            inserted_rows = storage.import_token_usage_history(usage_day)
            total_rows += inserted_rows
            print(f"{usage_day}: 插入 {inserted_rows} 条汇总记录")

        print(f"历史 Token 使用初始化完成，共插入 {total_rows} 条汇总记录。")
    finally:
        storage.close()


if __name__ == "__main__":
    main()
