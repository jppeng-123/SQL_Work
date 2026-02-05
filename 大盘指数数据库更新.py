import akshare as ak
import pyodbc
import pandas as pd
from datetime import datetime

# Step 1: Input start date
while True:
    start_date = input("请输入起始日期 (YYYY-MM-DD): ").strip()
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        break
    except ValueError:
        print("⚠️ 日期格式错误，请重新输入。")

today_str = datetime.today().strftime("%Y-%m-%d")

# Step 2: Fetch PB data for 4 indices
index_map = {
    "上证": "SH",
    "深证": "SZ",
    "创业板": "CYB",
    "科创版": "STAR"
}

df_list = []

for cn_name, en_code in index_map.items():
    df = ak.stock_market_pb_lg(symbol=cn_name)
    df.columns = ['trade_date', 'index_value', 'pb', 'pb_weighted', 'pb_median']
    df['market'] = en_code
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df[df['trade_date'] >= start_date]  # filter by start date
    df_list.append(df)

# Step 3: Combine and process
df_all = pd.concat(df_list, ignore_index=True)
df_all['trade_date'] = pd.to_datetime(df_all['trade_date'])

# Step 4: Connect to SQL Server
conn = pyodbc.connect('DSN=Trading;UID=sa;PWD=123456')
cursor = conn.cursor()

# Step 5: Create/update temp table [market_index_update]
create_temp_table_sql = """
IF OBJECT_ID('market_index_update', 'U') IS NOT NULL
    DROP TABLE market_index_update;

CREATE TABLE market_index_update (
    trade_date     DATE           NOT NULL,
    market         VARCHAR(10)    NOT NULL,
    index_value    DECIMAL(10, 2) NULL,
    pb             DECIMAL(10, 4) NULL,
    pb_weighted    DECIMAL(10, 4) NULL,
    pb_median      DECIMAL(10, 4) NULL,
    update_time    DATETIME       NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (trade_date, market)
)
"""
cursor.execute(create_temp_table_sql)
conn.commit()

# Step 6: Insert into temp table
insert_temp_sql = """
INSERT INTO market_index_update (trade_date, market, index_value, pb, pb_weighted, pb_median, update_time)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

now = datetime.now()
data = [
    (
        row.trade_date.date(),
        row.market,
        row.index_value,
        row.pb,
        row.pb_weighted,
        row.pb_median,
        now
    )
    for _, row in df_all.iterrows()
]

cursor.executemany(insert_temp_sql, data)
conn.commit()

print(f"✅ Inserted {len(data)} rows into [market_index_update].")

# Step 7: Ask to merge
while True:
    choice = input("是否将 [market_index_update] 合并到 [market_index]？(y/n): ").lower()
    if choice == 'y':
        merge_sql = """
        MERGE market_index AS target
        USING market_index_update AS source
        ON target.trade_date = source.trade_date AND target.market = source.market
        WHEN MATCHED THEN
            UPDATE SET 
                index_value = source.index_value,
                pb = source.pb,
                pb_weighted = source.pb_weighted,
                pb_median = source.pb_median,
                update_time = source.update_time
        WHEN NOT MATCHED THEN
            INSERT (trade_date, market, index_value, pb, pb_weighted, pb_median, update_time)
            VALUES (source.trade_date, source.market, source.index_value, source.pb, source.pb_weighted, source.pb_median, source.update_time);
        """
        cursor.execute(merge_sql)
        conn.commit()
        print("✅ 合并完成，数据已更新到 [market_index]，记得删除表 market_index_update")
        break
    elif choice == 'n':
        print("❗ 合并操作已取消。")
        break
    else:
        print("⚠️ 请输入 y 或 n")

# Step 8: Cleanup
cursor.close()
conn.close()

