import akshare as ak
import pyodbc
import pandas as pd
import logging
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_fixed

def get_start_date():
    """交互式读取起始日期（YYYY-MM-DD）并校验"""
    while True:
        s = input('请输入起始日期 (YYYY-MM-DD)：').strip()
        try:
            datetime.strptime(s, '%Y-%m-%d')
            return s
        except ValueError:
            print('日期格式错误，请重新输入。')

# -------- 配置区 --------
DB_DSN           = 'DSN=Trading;UID=sa;PWD=123456'
TABLE_NAME       = 'dividend_info_update'   # 更新表
MAIN_TABLE       = 'stock_dividend'         # 主表
MAX_WORKERS      = 6                        # 并发线程数
BATCH_SIZE       = 6000                      # 每批处理股票数
REQUEST_INTERVAL = (0.3, 0.6)               # 每次请求后随机休眠区间（秒）
START_DATE       = get_start_date()         # 用户输入
END_DATE         = datetime.now().strftime("%Y-%m-%d")

# -------- 日志配置 --------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dividend_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_stock_symbols():
    """获取所有沪深A股（含科创/创业板）的代码前缀版本"""
    try:
        df = ak.stock_info_a_code_name()
        df['symbol'] = df['code'].astype(str).apply(
            lambda x: f"sh{x}" if x.startswith(('6','9')) else f"sz{x}" if x.startswith(('0','3')) else None
        )
        codes = df['symbol'].dropna().unique().tolist()
        logging.info(f"获取到沪深A股共 {len(codes)} 只")
        return codes
    except Exception as e:
        logging.error(f"获取股票列表失败：{e}")
        return []

def init_database(conn):
    """创建更新表 dividend_info_update（如果不存在）"""
    sql = f"""
IF OBJECT_ID(N'dbo.{TABLE_NAME}', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.{TABLE_NAME} (
        symbol       VARCHAR(10)   NOT NULL,
        trade_date   DATE          NOT NULL,
        dividend     DECIMAL(9,4)  NULL,
        bonus        DECIMAL(9,4)  NULL,
        split        DECIMAL(9,4)  NULL,
        stock_bonus  DECIMAL(9,4)  NULL,
        update_time  DATETIME      NOT NULL,
        PRIMARY KEY (symbol, trade_date)
    );
    CREATE INDEX idx_trade_date ON dbo.{TABLE_NAME}(trade_date);
END
"""
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logging.info(f"表 [{TABLE_NAME}] 已验证或创建完成")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_dividend_data(symbol: str) -> pd.DataFrame:
    """抓取单只股票的除权除息（现金分红/送股/转增）明细"""
    code = symbol[2:]
    logging.info(f"抓取 {symbol} 分红/送转数据 ({START_DATE} ~ {END_DATE})")
    df = ak.stock_fhps_detail_em(symbol=code)
    if df.empty:
        logging.warning(f"{symbol} 无分红/送转记录")
        return None

    df = df.rename(columns={
        '除权除息日':           'trade_date',
        '现金分红-现金分红比例': 'cash_pct',
        '送转股份-送股比例':     'bonus_pct',
        '送转股份-转股比例':     'split_pct'
    })
    df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
    df = df.dropna(subset=['trade_date'])
    df = df[(df['trade_date'] >= pd.to_datetime(START_DATE)) &
            (df['trade_date'] <= pd.to_datetime(END_DATE))]
    if df.empty:
        return None

    df['dividend']    = df['cash_pct'].fillna(0)  / 10.0
    df['bonus']       = df['bonus_pct'].fillna(0) / 10.0
    df['split']       = df['split_pct'].fillna(0) / 10.0
    df['stock_bonus'] = df['bonus'] + df['split']
    df['symbol']      = symbol
    df['update_time'] = datetime.now()

    return df[['symbol','trade_date','dividend','bonus','split','stock_bonus','update_time']]

def save_dividend_data(conn, rows: list):
    """批量 MERGE 写入 dividend_info_update"""
    if not rows:
        return
    sql = f"""
MERGE INTO dbo.{TABLE_NAME} AS target
USING (VALUES (?, ?, ?, ?, ?, ?, ?))
    AS src(symbol,trade_date,dividend,bonus,split,stock_bonus,update_time)
ON target.symbol = src.symbol AND target.trade_date = src.trade_date
WHEN MATCHED THEN
  UPDATE SET
    dividend     = src.dividend,
    bonus        = src.bonus,
    split        = src.split,
    stock_bonus  = src.stock_bonus,
    update_time  = src.update_time
WHEN NOT MATCHED THEN
  INSERT (symbol,trade_date,dividend,bonus,split,stock_bonus,update_time)
  VALUES (src.symbol,src.trade_date,src.dividend,src.bonus,src.split,src.stock_bonus,src.update_time);
"""
    with conn.cursor() as cur:
        cur.fast_executemany = True
        cur.executemany(sql, rows)
    conn.commit()
    logging.info(f"已写入/更新 {len(rows)} 条到 [{TABLE_NAME}]")

def main():
    symbols = get_stock_symbols()
    if not symbols:
        return

    conn = pyodbc.connect(DB_DSN)
    init_database(conn)

    total = len(symbols)
    for i in range(0, total, BATCH_SIZE):
        batch = symbols[i:i+BATCH_SIZE]
        rows = []

        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = {exe.submit(fetch_dividend_data, sym): sym for sym in batch}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    df = fut.result()
                    if df is not None:
                        rows.extend([
                            (
                                r.symbol,
                                r.trade_date.date(),
                                float(r.dividend),
                                float(r.bonus),
                                float(r.split),
                                float(r.stock_bonus),
                                r.update_time
                            )
                            for r in df.itertuples()
                        ])
                except Exception as e:
                    logging.error(f"[{sym}] 处理失败: {e}")
                time.sleep(random.uniform(*REQUEST_INTERVAL))

        if rows:
            save_dividend_data(conn, rows)
            print(f"✅ 本批共 {len(rows)} 条已写入 [{TABLE_NAME}]")

    # —— 全部批次完成后，询问是否合并到主表 ——
    while True:
        ans = input(
            f"是否将更新表 [{TABLE_NAME}] 中的数据合并到主表 [{MAIN_TABLE}]？(y/n): "
        ).strip().lower()
        if ans == 'y':
            merge_sql = f"""
MERGE INTO dbo.{MAIN_TABLE} AS target
USING dbo.{TABLE_NAME} AS source
  ON target.symbol = source.symbol
 AND target.trade_date = source.trade_date
WHEN MATCHED THEN
  UPDATE SET
    dividend     = source.dividend,
    bonus        = source.bonus,
    split        = source.split,
    stock_bonus  = source.stock_bonus,
    update_time  = source.update_time
WHEN NOT MATCHED BY TARGET THEN
  INSERT (symbol,trade_date,dividend,bonus,split,stock_bonus,update_time)
  VALUES (source.symbol,source.trade_date,
          source.dividend,source.bonus,
          source.split,source.stock_bonus,
          source.update_time);
"""
            with conn.cursor() as cur:
                cur.execute(merge_sql)
            conn.commit()
            print(f"✅ 已将更新合并到主表 [{MAIN_TABLE}]，记得删除表 dividend_info_update。")
            break
        elif ans == 'n':
            print("ℹ️ 已取消合并到主表。")
            break
        else:
            print("⚠️ 请输入 'y' 或 'n'。")

    conn.close()
    logging.info("全部处理完毕。")

if __name__ == "__main__":
    main()

import akshare as ak

stock_fhps_em_df = ak.stock_fhps_em(date="20090630")
print(stock_fhps_em_df)