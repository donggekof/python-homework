import pymysql
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from config import db_config

# 来个备注提交下试试

# 数据库连接信息
host = db_config['host']
port = db_config['port']
user = db_config['user']
password = db_config['password']
database = db_config['database']

# 连接数据库
try:
    conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
except pymysql.err.OperationalError as e:
    print(f"数据库连接失败: {e}")
    exit(1)

# 查询并合并数据，计算每个英雄的总场次、胜场数、胜率
query = """
SELECT h.hero_id,
       h.hero_name,
       COUNT(br.is_win) AS total_games,
       SUM(CASE WHEN br.is_win = 1 THEN 1 ELSE 0 END) AS wins
FROM hero h
JOIN battle_record br ON h.hero_id = br.hero_id
GROUP BY h.hero_id, h.hero_name
HAVING COUNT(br.is_win) >= 30
ORDER BY (SUM(CASE WHEN br.is_win = 1 THEN 1 ELSE 0 END) / COUNT(br.is_win)) DESC
"""

# 执行查询
df = pd.read_sql(query, engine)

# 计算胜率（百分比，保留一位小数）
df['win_rate'] = (df['wins'] / df['total_games'] * 100).round(1)

# 新增两列
df['analyst'] = '<徐国栋>'
df['run_time'] = datetime.now()

# 调整列名和数据类型以匹配analysis_log表
df = df.rename(columns={'wins': 'win_games'})
df['win_rate'] = df['win_rate'] / 100  # 转为小数

# 导出为Excel文件
try:
    df.to_excel('hero_winrate.xlsx', index=False)
    print("Excel文件已导出: hero_winrate.xlsx")
except PermissionError as e:
    print(f"导出Excel失败: {e}")

# 计算统计摘要
total_heroes = len(df)
avg_winrate = (df['win_rate'].mean() * 100).round(1) if total_heroes > 0 else 0
top_hero = df.iloc[0]['hero_name'] if total_heroes > 0 else '无'

# 在终端打印统计摘要
print(f"总英雄数: {total_heroes}")
print(f"平均胜率: {avg_winrate}%")
print(f"胜率最高的英雄: {top_hero}")

# 将结果写入analysis_log表
df_to_insert = df[['hero_id', 'hero_name', 'total_games', 'win_games', 'win_rate', 'analyst', 'run_time']]
df_to_insert.to_sql('analysis_log', engine, if_exists='append', index=False)

# 查询analysis_log表中所有记录
query_log = "SELECT * FROM analysis_log"
df_log = pd.read_sql(query_log, engine)
print("analysis_log表中的所有记录:")
print(df_log)

# 将df_log存入个人查询记录.xlsx
df_log.to_excel('个人查询记录.xlsx', index=False)
print("个人查询记录.xlsx 已导出")
