from datetime import datetime
from logger_config import get_logger
from run_query import run_query
from apscheduler.schedulers.blocking import BlockingScheduler

logger = get_logger("task.log")

# 来个备注提交下试试

query = """
SELECT *
FROM ml_ods.matchserver_match_end
WHERE logymd = '2026-01-20'
limit 1000
"""


def main():
    """
    主查询函数 - 执行 Presto 查询
    """
    logger.info("=" * 50)
    logger.info(f"执行定时任务 - 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)
    
    try:
        logger.info(f"使用 Presto 查询数据...")
        
        df_presto = run_query(query, connection_type="presto")
        
        logger.info("✅ Presto 查询成功！")
        
        logger.info(f"返回行数: {len(df_presto)}")
        
        logger.info(f"\n{df_presto}")
        
        
    except Exception as e:
        logger.error(f"❌ Presto 查询失败: {type(e).__name__}: {e}")


def start_scheduler():
    """
    启动定时调度器 - 每两分钟执行一次
    """
    scheduler = BlockingScheduler()
    
    # 添加定时任务：每两分钟执行一次 main 函数
    scheduler.add_job(main, 'interval', minutes=2)
    
    logger.info("=" * 60)
    logger.info("定时调度器已启动")
    logger.info("任务: 每两分钟执行一次查询")
    logger.info("=" * 60)
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("调度器已停止")


# 执行查询
if __name__ == "__main__":
    # 立即执行一次
    logger.info("首次运行 - 立即执行查询...")
    main()
    
    # 启动定时调度器
    logger.info("启动定时调度器...")
    start_scheduler()

