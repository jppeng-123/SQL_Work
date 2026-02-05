import akshare as ak
import pyodbc
from datetime import datetime

# ==============================
# 1. 读取申万历史行业分类数据
# ==============================
df = ak.stock_industry_clf_hist_sw()

# ==============================
# 2. 连接 SQL Server
# ==============================
conn_str = "DSN=Trading;UID=sa;PWD=123456"
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()

# ==============================
# 3. 删除旧表并重建 stock_sector，load_time 不用 default，直接写入
# ==============================
create_sql = """
IF OBJECT_ID('dbo.stock_sector', 'U') IS NOT NULL
    DROP TABLE dbo.stock_sector;

CREATE TABLE dbo.stock_sector (
    symbol         VARCHAR(20)    NOT NULL,
    start_date     DATE           NULL,
    industry_code  VARCHAR(50)    NULL,
    update_time    DATETIME       NULL,
    load_time      DATETIME       NOT NULL
);
"""
cursor.execute(create_sql)
cnxn.commit()

# ==============================
# 4. 批量插入 DataFrame，同时把 Python 端的 datetime.now() 作为 load_time
# ==============================
insert_sql = """
INSERT INTO dbo.stock_sector
    (symbol, start_date, industry_code, update_time, load_time)
VALUES
    (?,       ?,          ?,             ?,            ?);
"""

for idx, row in df.iterrows():
    cursor.execute(
        insert_sql,
        row['symbol'],
        row['start_date'],
        row['industry_code'],
        row['update_time'],
        datetime.now()          # 由 Python 决定写入时间
    )

cnxn.commit()
cursor.close()
cnxn.close()

print("数据已插入并由 Python 写入 load_time。")
