import akshare as ak
import pyodbc
import pandas as pd
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed

# ====================== 配置区 ======================
conn_str     = 'DSN=Trading;UID=sa;PWD=123456'
update_table = "stock_info_update"
target_table = "stock_a_daily"

sse_prefixes  = {"600", "601", "603", "605", "688", "689"}
szse_prefixes = {"000", "001", "002", "003", "300", "301"}

def add_prefix(code: str) -> str:
    for p in sse_prefixes:
        if code.startswith(p):
            return "sh" + code
    for p in szse_prefixes:
        if code.startswith(p):
            return "sz" + code
    return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def fetch_spot_data() -> pd.DataFrame:
    # 强制使用后复权接口
    return ak.stock_zh_a_spot_em()

def main():
    # —— 1. 日期确认 —— 
    input_date = input("请输入今天的日期 (YYYY-MM-DD): ").strip()
    try:
        input_date_obj = datetime.strptime(input_date, "%Y-%m-%d").date()
    except ValueError:
        print("❌ 日期格式不正确，程序退出。")
        return

    today = datetime.now().date()
    if input_date_obj != today:
        print(f"❌ 输入日期 {input_date_obj} 与系统日期 {today} 不符，请使用分红扩股数据库补加。")
        return

    # —— 2. 拉取数据并插入到 stock_info_update —— 
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # 创建更新表（如果不存在），去掉 outstanding_share
        ddl = f"""
IF OBJECT_ID(N'dbo.{update_table}', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.{update_table} (
    symbol       VARCHAR(10)   NOT NULL,
    trade_date   DATE          NOT NULL,
    [open]       DECIMAL(9,4)  NULL,
    high         DECIMAL(9,4)  NULL,
    low          DECIMAL(9,4)  NULL,
    [close]      DECIMAL(9,4)  NULL,
    volume       DECIMAL(15,2) NULL,
    amount       DECIMAL(20,2) NULL,
    turnover     DECIMAL(9,6)  NULL,
    update_time  DATETIME      NOT NULL,
    CONSTRAINT PK_{update_table} PRIMARY KEY (symbol, trade_date)
  );
END
"""
        cursor.execute(ddl)
        conn.commit()

        # 拉取并重命名字段
        spot_df = fetch_spot_data().rename(columns={
            '代码':   'symbol_code',
            '今开':   'open',
            '最高':   'high',
            '最低':   'low',
            '最新价': 'close',
            '成交量': 'volume',   # 单位: 手
            '成交额': 'amount',
            '换手率': 'turnover'
        })[[
            'symbol_code','open','high','low','close',
            'volume','amount','turnover'
        ]]

        # 加前缀、过滤
        spot_df['symbol'] = spot_df['symbol_code'].astype(str).map(add_prefix)
        spot_df = spot_df[spot_df['symbol'].notna()]

        # 把 volume（手）转换为 股，并保留 NaN（映射为 None）
        spot_df['volume'] = spot_df['volume'].astype(float) * 100
        spot_df['volume'] = spot_df['volume'].apply(
            lambda x: int(round(x)) if pd.notna(x) else None
        )

        # 数值清洗 & 四舍五入
        for col in ['open','high','low','close']:
            spot_df[col] = pd.to_numeric(spot_df[col], errors='coerce').round(4)
        spot_df['amount']   = pd.to_numeric(spot_df['amount'], errors='coerce').round(2)
        spot_df['turnover'] = pd.to_numeric(spot_df['turnover'], errors='coerce').round(6)

        # 丢弃关键价/量缺失行
        spot_df.dropna(subset=[
            'open','high','low','close','amount','turnover'
        ], inplace=True)

        # 添加日期字段
        now_ts = datetime.now()
        spot_df['trade_date']  = today
        spot_df['update_time'] = now_ts

        # 准备插入（不含 outstanding_share）
        df_upd = spot_df[[
            'symbol','trade_date','open','high','low','close',
            'volume','amount','turnover','update_time'
        ]]
        records = [tuple(row) for row in df_upd.itertuples(index=False)]
        insert_sql = f"""
INSERT INTO dbo.{update_table}
  (symbol, trade_date, [open], high, low, [close],
   volume, amount, turnover, update_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""
        cursor.executemany(insert_sql, records)
        conn.commit()
        print(f"✅ 已插入 {len(records)} 条当日行情到 [{update_table}]")

        # —— 3. 合并到 stock_a_daily 的询问 —— 
        while True:
            choice = input(f"是否将新的更新数据合并到表 [{target_table}]？(y/n): ").strip().lower()
            if choice == 'y':
                merge_sql = f"""
MERGE dbo.{target_table} AS target
USING dbo.{update_table} AS source
  ON target.symbol = source.symbol
 AND target.trade_date = source.trade_date
WHEN MATCHED THEN
  UPDATE SET
    [open]      = source.[open],
    high        = source.high,
    low         = source.low,
    [close]     = source.[close],
    volume      = source.volume,
    amount      = source.amount,
    turnover    = source.turnover,
    update_time = source.update_time
WHEN NOT MATCHED BY TARGET THEN
  INSERT (symbol, trade_date, [open], high, low, [close],
          volume, amount, turnover, update_time)
  VALUES (source.symbol, source.trade_date, source.[open], source.high,
          source.low, source.[close], source.volume, source.amount,
          source.turnover, source.update_time);
"""
                cursor.execute(merge_sql)
                conn.commit()
                print(f"✅ 已将更新数据合并到 [{target_table}]，记得删除 [{update_table}]。")
                break
            elif choice == 'n':
                print("ℹ️ 已取消合并操作。")
                break
            else:
                print("⚠️ 输入无效，请输入 'y' 或 'n'。")

if __name__ == "__main__":
    main()
