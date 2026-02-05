import akshare as ak
import pyodbc
import pandas as pd
import logging
import time
import random
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_fixed



# --------------------- 配置项 ---------------------
DB_DSN       = 'DSN=Trading;UID=sa;PWD=123456'
TABLE_NAME   = 'stock_dividend'
MAX_WORKERS  = 6                 # 并发线程数
BATCH_SIZE   = 300               # 每批处理股票数
REQUEST_INTERVAL = (0.3, 0.6)    # 请求随机间隔（秒）
START_DATE   = "2010-01-01"
END_DATE     = datetime.now().strftime("%Y-%m-%d")

# --------------------- 日志配置 ---------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_dividend.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --------------------- 工具函数 ---------------------
def get_stock_symbols():
    """获取沪深A股代码（含科创板/创业板）"""
    try:
        df = ak.stock_info_a_code_name()
        df['symbol'] = df['code'].astype(str).apply(
            lambda x: f"sh{x}" if x.startswith(('6','9')) else f"sz{x}" if x.startswith(('0','3')) else None
        )
        codes = df['symbol'].dropna().unique().tolist()
        logging.info(f"获取到沪深A股共 {len(codes)} 只")
        return codes
    except Exception as e:
        logging.error(f"获取股票代码失败: {e}")
        return []

def init_database(conn):
    """初始化数据库表结构（如果不存在则创建）"""
    sql = f"""
    IF OBJECT_ID(N'dbo.{TABLE_NAME}', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.{TABLE_NAME} (
            symbol       VARCHAR(10)   NOT NULL,
            trade_date   DATE          NOT NULL,
            dividend     DECIMAL(9,4)  NULL,   -- 派息（元/股）
            bonus        DECIMAL(9,4)  NULL,   -- 送股比例（股/股）
            split        DECIMAL(9,4)  NULL,   -- 转增比例（股/股）
            stock_bonus  DECIMAL(9,4)  NULL,   -- 合并送转 = bonus + split
            update_time  DATETIME      NOT NULL,
            PRIMARY KEY (symbol, trade_date)
        );
        CREATE INDEX idx_trade_date ON dbo.{TABLE_NAME}(trade_date);
    END
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logging.info("数据库表结构已验证或创建完成")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_dividend_data(symbol: str) -> pd.DataFrame:
    """
    获取单只股票送转预案及分红数据，自动重试 3 次。
    返回 DataFrame 或 None。
    """
    code = symbol[2:]
    logging.info(f"开始抓取 {symbol} 分红送转预案数据")
    df = ak.stock_fhps_detail_em(symbol=code)
    if df.empty:
        logging.warning(f"{symbol} 无任何记录")
        return None

    # 重命名并筛选字段
    df = df.rename(columns={
        '除权除息日':            'trade_date',
        '现金分红-现金分红比例':  'cash_pct',
        '送转股份-送股比例':      'bonus_pct',
        '送转股份-转股比例':      'split_pct'
    })
    df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
    df = df.dropna(subset=['trade_date'])
    df = df[(df['trade_date'] >= pd.to_datetime(START_DATE)) & (df['trade_date'] <= pd.to_datetime(END_DATE))]
    if df.empty:
        return None

    # 计算每股派息/送股/转增及合并送转
    df['dividend']    = df['cash_pct'].fillna(0)   / 10.0
    df['bonus']       = df['bonus_pct'].fillna(0)  / 10.0
    df['split']       = df['split_pct'].fillna(0)  / 10.0
    df['stock_bonus'] = df['bonus'] + df['split']

    # 添加元数据
    df['symbol']      = symbol
    df['update_time'] = datetime.now()

    return df[['symbol','trade_date','dividend','bonus','split','stock_bonus','update_time']]

def save_dividend_data(conn, rows: list):
    """使用 MERGE 语句批量更新/插入"""
    if not rows:
        return
    sql = f"""
    MERGE INTO dbo.{TABLE_NAME} AS target
    USING (VALUES (?, ?, ?, ?, ?, ?, ?))
        AS src (symbol, trade_date, dividend, bonus, split, stock_bonus, update_time)
    ON target.symbol = src.symbol AND target.trade_date = src.trade_date
    WHEN MATCHED THEN
        UPDATE SET
            dividend     = src.dividend,
            bonus        = src.bonus,
            split        = src.split,
            stock_bonus  = src.stock_bonus,
            update_time  = src.update_time
    WHEN NOT MATCHED THEN
        INSERT (symbol, trade_date, dividend, bonus, split, stock_bonus, update_time)
        VALUES (src.symbol, src.trade_date, src.dividend, src.bonus, src.split, src.stock_bonus, src.update_time);
    """
    with conn.cursor() as cur:
        cur.fast_executemany = True
        cur.executemany(sql, rows)
    conn.commit()
    logging.info(f"写入/更新 {len(rows)} 条记录")

# --------------------- 主流程 ---------------------
def main():
    prog_file = "dividend_progress.txt"
    # 手动重置断点：如果存在则删除
    if os.path.exists(prog_file):
        os.remove(prog_file)
        logging.info("手动重置断点：已删除 progress 文件")

    # 读取进度
    start_idx = 0
    if os.path.exists(prog_file):
        with open(prog_file, 'r') as f:
            start_idx = int(f.read())
        logging.info(f"从第 {start_idx} 只股票继续")

    symbols = get_stock_symbols()[start_idx:]
    if not symbols:
        return

    conn = pyodbc.connect(DB_DSN)
    init_database(conn)

    total = len(symbols)
    for batch_i in range(0, total, BATCH_SIZE):
        batch_syms = symbols[batch_i: batch_i + BATCH_SIZE]
        all_rows = []
        with ThreadPoolExecutor(MAX_WORKERS) as exe:
            futures = {exe.submit(fetch_dividend_data, s): s for s in batch_syms}
            for fut in as_completed(futures):
                s = futures[fut]
                try:
                    df = fut.result()
                    if df is not None:
                        rows = [
                            (r.symbol, r.trade_date.date(),
                             float(r.dividend), float(r.bonus), float(r.split),
                             float(r.stock_bonus), r.update_time)
                            for r in df.itertuples()
                        ]
                        all_rows.extend(rows)
                except Exception as e:
                    logging.error(f"[{s}] 重试后仍失败: {e}")
                time.sleep(random.uniform(*REQUEST_INTERVAL))

        if all_rows:
            save_dividend_data(conn, all_rows)

        # 更新进度
        next_idx = start_idx + batch_i + len(batch_syms)
        with open(prog_file, 'w') as f:
            f.write(str(next_idx))
        logging.info(f"已处理 {next_idx}/{total} 支股票")

    if os.path.exists(prog_file):
        os.remove(prog_file)
    conn.close()
    logging.info("全部分红送转数据处理完毕")

if __name__ == "__main__":
    sample = fetch_dividend_data("sh600000")
    if sample is not None:
        logging.info(f"测试数据：\n{sample.head()}")
    main()
    
