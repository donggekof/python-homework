"""
统一查询接口模块
提供 Presto 和 Spark 两种数据库查询方式的统一入口
"""
import configparser
import pandas as pd
from TCLIService.ttypes import TOperationState
from pyhive import presto, hive
from config import get_db_config


def run_presto_query(sql: str, connection_name: str = "presto") -> pd.DataFrame:
    """
    执行 Presto SQL 并返回 DataFrame。
    
    Args:
        sql: SQL查询语句
        connection_name: 连接名称，默认为 "presto"
    
    Returns:
        pd.DataFrame: 查询结果
    """
    cfg = get_db_config(connection_name)
    if not cfg:
        raise ValueError(f"Unknown DB config: {connection_name}")

    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]

    if not host or not user:
        raise ValueError("DB config must include host and user")

    conn = presto.connect(
        host=host,
        username=user,
        port=port,
        catalog=cfg.get("catalog", "hive"),
        schema=cfg.get("schema", "ml_ods")
    )

    try:
        df = pd.read_sql(sql, conn, coerce_float=False)
    finally:
        conn.close()

    return df


def run_spark_query(sql: str, engine_type: str = 'JDBC', incremental_collect: bool = False, status_echo: bool = True) -> pd.DataFrame:
    """
    执行 Spark SQL 并返回 DataFrame。
    
    Args:
        sql: SQL查询语句
        engine_type: 引擎类型，可选 'JDBC' 或 'SPARK_SQL'，默认为 'JDBC'
        incremental_collect: 是否增量收集，默认为 False
        status_echo: 是否打印执行日志，默认为 True
    
    Returns:
        pd.DataFrame: 查询结果
    """
    try:
        assert engine_type in ['JDBC', 'SPARK_SQL']
    except AssertionError:
        print("Invalid engine type. Please choose from the following options:")
        print("- JDBC")
        print("- SPARK_SQL")
        raise
    
    config = configparser.ConfigParser()
    config.read('src/util/odbc.ini')
    configuration = {
        'kyuubi.engine.type': engine_type,
        'spark.yarn.queue': 'online',
        'spark.sql.shuffle.partitions': '1000',
        'spark.dynamicAllocation.maxExecutors': '60'
    }
    if incremental_collect:
        configuration['kyuubi.operation.incremental.collect'] = incremental_collect # type: ignore
    
    db_config = get_db_config("spark")
    cursor = hive.connect(
        host=db_config['host'], port=db_config['port'], # type: ignore
        username=db_config['username'], # type: ignore
        auth='LDAP',
        configuration=configuration,
        password=db_config['password']).cursor() # type: ignore

    cursor.execute(sql, async_=True)

    status = cursor.poll().operationState
    while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
        logs = cursor.fetch_logs()
        if status_echo:
            for message in logs:
                print(message)
            # If needed, an asynchronous query can be cancelled at any time with:
            # cursor.cancel()

        status = cursor.poll().operationState

    col_name = []
    for i in cursor.description: # type: ignore
        col_name.append(i[0])

    data_pd = pd.DataFrame(cursor.fetchall(), columns=col_name)
    return data_pd


def run_query(sql: str, connection_type: str = "spark", **kwargs) -> pd.DataFrame:
    """
    统一查询接口，支持 Presto 和 Spark 两种连接方式。
    
    Args:
        sql: SQL查询语句（字符串）
        connection_type: 连接方式，可选 "presto" 或 "spark"，默认为 "spark"
        **kwargs: 传递给具体查询函数的其他参数
            - 对于 Presto: connection_name
            - 对于 Spark: engine_type, incremental_collect, status_echo
    
    Returns:
        pd.DataFrame: 查询结果
    
    Raises:
        ValueError: 当 connection_type 不是 "presto" 或 "spark" 时
    
    Examples:
        # 使用 Spark 查询（默认）
        df = run_query("SELECT * FROM table WHERE id > 10")
        
        # 使用 Presto 查询
        df = run_query("SELECT * FROM table WHERE id > 10", connection_type="presto")
        
        # 使用 Spark 查询，指定引擎类型
        df = run_query("SELECT * FROM table", connection_type="spark", engine_type="SPARK_SQL")
    """
    connection_type = connection_type.lower()
    
    if connection_type == "presto":
        return run_presto_query(sql, **kwargs)
    elif connection_type == "spark":
        return run_spark_query(sql, **kwargs)
    else:
        raise ValueError(f"Invalid connection_type: {connection_type}. Must be 'presto' or 'spark'.")


if __name__ == "__main__":
    print("=" * 50)
    print("测试 run_query 统一查询接口")
    print("=" * 50)
    
    # 示例 SQL 语句
    query_sql = """
    select time, zoneid, roleid, channel, level from ml_ods.gameserver_login 
    where logymd='2025-01-20'
    order by time
    limit 5
    """
    
    # 测试 Spark 查询（默认）
    print("\n测试 1: 使用 Spark 查询（默认）")
    try:
        df_spark = run_query(query_sql)
        print("✅ Spark 查询成功！")
        print(df_spark)
    except Exception as e:
        print(f"❌ Spark 查询失败: {e}")
    
    # 测试 Spark 查询（显式指定）
    print("\n测试 2: 使用 Spark 查询（显式指定 connection_type='spark'）")
    try:
        df_spark2 = run_query(query_sql, connection_type="spark")
        print("✅ Spark 查询成功！")
        print(df_spark2)
    except Exception as e:
        print(f"❌ Spark 查询失败: {e}")
    
    # 测试 Presto 查询
    print("\n测试 3: 使用 Presto 查询")
    try:
        presto_sql = """
        SELECT * FROM ml_ods.matchserver_match_end
        WHERE logymd = '2026-03-17'
        LIMIT 5
        """
        df_presto = run_query(presto_sql, connection_type="presto")
        print("✅ Presto 查询成功！")
        print(df_presto)
    except Exception as e:
        print(f"❌ Presto 查询失败: {e}")
    
    # 测试无效的连接方式
    print("\n测试 4: 测试无效的连接方式")
    try:
        df_invalid = run_query(query_sql, connection_type="invalid")
    except ValueError as e:
        print(f"✅ 正确捕获错误: {e}")
