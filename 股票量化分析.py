import akshare as ak
import pyodbc
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# --------------------------- 日志配置 ---------------------------
# 日频日志（输出到stock_daily.log）
daily_logger = logging.getLogger('daily')
daily_logger.setLevel(logging.INFO)
daily_handler = logging.FileHandler('stock_daily.log')
daily_console = logging.StreamHandler()
daily_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
daily_handler.setFormatter(daily_formatter)
daily_console.setFormatter(daily_formatter)
daily_logger.addHandler(daily_handler)
daily_logger.addHandler(daily_console)

# 分红日志（输出到stock_dividend.log）
dividend_logger = logging.getLogger('dividend')
dividend_logger.setLevel(logging.INFO)
dividend_handler = logging.FileHandler('stock_dividend.log')
dividend_console = logging.StreamHandler()
dividend_handler.setFormatter(daily_formatter)
dividend_console.setFormatter(daily_formatter)
dividend_logger.addHandler(dividend_handler)
dividend_logger.addHandler(dividend_console)

# --------------------------- 公共配置 ---------------------------
DSN_CONNECTION = 'DSN=Trading;UID=sa;PWD=123456'
START_DATE = "2010-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# --------------------------- 日频数据模块 ---------------------------
def get_daily_stock_codes():
    """获取日频数据用的沪深A股代码（带市场前缀）"""
    try:
        df = ak.stock_info_a_code_name()
        df['symbol'] = df['code'].apply(
            lambda x: f"sh{x}" if x.startswith(('6', '9')) else f"sz{x}"
        )
        return df['symbol'].tolist()
    except Exception as e:
        daily_logger.error(f"获取日频股票代码失败: {str(e)[:200]}")
        return []

def create_daily_table(conn):
    """创建日频数据表"""
    cursor = conn.cursor()
    create_sql = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stock_daily')
    CREATE TABLE stock_daily (
        symbol VARCHAR(10) NOT NULL,
        trade_date DATE NOT NULL,
        [open] DECIMAL(9,4),
        high DECIMAL(9,4),
        low DECIMAL(9,4),
        [close] DECIMAL(9,4),
        volume DECIMAL(15,0),
        amount DECIMAL(15,2),
        update_time DATETIME NOT NULL,
        PRIMARY KEY (symbol, trade_date)
    )
    """
    try:
        cursor.execute(create_sql)
        conn.commit()
        daily_logger.info("日频数据表已创建/已存在")
    except Exception as e:
        daily_logger.error(f"创建日频表失败: {str(e)[:200]}")

def fetch_stock_data(symbol):
    """获取单只股票日频历史数据"""
    try:
        code = symbol[2:]
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=START_DATE.replace("-", ""),
            end_date=END_DATE.replace("-", ""),
            adjust=""
        )
        
        if df.empty:
            daily_logger.warning(f"{symbol} 无有效日频数据")
            return None

        df = df.rename(columns={
            '日期': 'trade_date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount'
        })
        
        df['symbol'] = symbol
        df['update_time'] = datetime.now()
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df[
            (df['trade_date'] >= pd.to_datetime(START_DATE)) &
            (df['trade_date'] <= pd.to_datetime(END_DATE))
        ]
        
        return df[['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'update_time']]
    
    except Exception as e:
        daily_logger.error(f"获取{symbol}日频数据失败: {str(e)[:200]}")
        return None

def batch_insert_daily(conn, data):
    """批量插入日频数据"""
    if not data:
        return
    
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany("""
            INSERT INTO stock_daily 
            (symbol, trade_date, [open], high, low, [close], volume, amount, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        daily_logger.info(f"成功插入 {len(data)} 条日频记录")
    except pyodbc.IntegrityError:
        daily_logger.warning("检测到日频重复数据，已跳过")
    except Exception as e:
        daily_logger.error(f"日频插入失败: {str(e)[:200]}")
        conn.rollback()

def main_daily():
    daily_logger.info("开始处理日频数据...")
    symbols = get_daily_stock_codes()
    if not symbols:
        daily_logger.error("未获取到日频股票代码，程序终止")
        return
    
    conn = None
    try:
        conn = pyodbc.connect(DSN_CONNECTION)
        daily_logger.info("成功连接数据库（日频）")
        create_daily_table(conn)

        BATCH_SIZE = 500
        for i in range(0, len(symbols), BATCH_SIZE):
            batch_symbols = symbols[i:i+BATCH_SIZE]
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_stock_data, sym): sym for sym in batch_symbols}
                for future in as_completed(futures):
                    sym = futures[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            data = [
                                (
                                    row['symbol'],
                                    row['trade_date'].date(),
                                    float(row['open']),
                                    float(row['high']),
                                    float(row['low']),
                                    float(row['close']),
                                    int(row['volume']),
                                    float(row['amount']),
                                    row['update_time']
                                ) for _, row in df.iterrows()
                            ]
                            batch_insert_daily(conn, data)
                    except Exception as e:
                        daily_logger.error(f"处理{sym}日频数据时异常: {str(e)[:200]}")
                    time.sleep(1)
            daily_logger.info(f"日频数据已完成批次 {i//BATCH_SIZE + 1}/{(len(symbols)//BATCH_SIZE)+1}")
    except Exception as e:
        daily_logger.error(f"日频主流程异常: {str(e)[:200]}")
    finally:
        if conn:
            conn.close()
            daily_logger.info("日频数据库连接已关闭")

# --------------------------- 分红数据模块 ---------------------------
def get_dividend_stock_codes():
    """获取分红数据用的沪深A股代码（带市场前缀）"""
    try:
        df = ak.stock_info_a_code_name()
        df['symbol'] = df['code'].apply(
            lambda x: 'sh'+x if x.startswith('6') else 'sz'+x if x.startswith(('0','3')) else None
        )
        return df['symbol'].dropna().tolist()
    except Exception as e:
        dividend_logger.error(f"获取分红股票代码失败: {e}")
        return []

def create_dividend_table(conn):
    """创建分红送转表"""
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'stock_dividend')
    CREATE TABLE stock_dividend (
        symbol       VARCHAR(10)   NOT NULL,
        trade_date   DATE          NOT NULL,
        dividend     DECIMAL(9,4)  NULL,
        stock_bonus  DECIMAL(9,4)  NULL,
        update_time  DATETIME      NOT NULL,
        PRIMARY KEY(symbol, trade_date)
    )
    """)
    conn.commit()
    dividend_logger.info("分红送转表已创建/已存在")

def fetch_dividend_data(symbol):
    """获取某只股票的分红送转数据"""
    code = symbol[2:]
    dividend_logger.info(f"→ 获取 {symbol} 分红数据…")
    try:
        df = ak.stock_dividend_cninfo(symbol=code)
        if df.empty:
            dividend_logger.warning(f"{symbol} 无分红记录")
            return None

        required = {'除权日','派息比例','送股比例','转增比例'}
        if not required.issubset(df.columns):
            dividend_logger.warning(f"{symbol} 缺少字段 {required - set(df.columns)}，跳过")
            return None

        df = df.rename(columns={
            '除权日':   'trade_date',
            '派息比例': 'cash_pct',
            '送股比例': 'bonus_pct',
            '转增比例': 'split_pct'
        })
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
        df = df[
            (df['trade_date'] >= pd.to_datetime(START_DATE)) &
            (df['trade_date'] <= pd.to_datetime(END_DATE))
        ].dropna(subset=['trade_date'])

        df['dividend']    = df['cash_pct'].fillna(0.0) / 10.0
        df['stock_bonus'] = (df['bonus_pct'].fillna(0.0) + df['split_pct'].fillna(0.0)) / 10.0

        out = df[['trade_date','dividend','stock_bonus']].copy()
        out['symbol']      = symbol
        out['update_time'] = datetime.now()
        return out

    except Exception as e:
        dividend_logger.error(f"获取 {symbol} 分红数据失败: {e}")
        return None

def batch_insert_dividend(conn, records):
    """批量插入分红送转数据"""
    if not records:
        return
    cursor = conn.cursor()
    cursor.fast_executemany = True
    params = [
        (
            rec['symbol'],
            rec['trade_date'].date(),
            float(rec['dividend']),
            float(rec['stock_bonus']),
            rec['update_time']
        )
        for rec in records
    ]
    try:
        cursor.executemany("""
            INSERT INTO stock_dividend
            (symbol, trade_date, dividend, stock_bonus, update_time)
            VALUES (?, ?, ?, ?, ?)
        """, params)
        conn.commit()
        dividend_logger.info(f"成功插入 {len(params)} 条分红记录")
    except pyodbc.IntegrityError:
        dividend_logger.warning("检测到分红重复记录，已跳过")
        conn.rollback()
    except Exception as e:
        dividend_logger.error(f"分红插入失败: {e}")
        conn.rollback()

def main_dividend():
    dividend_logger.info("开始处理分红数据...")
    symbols = get_dividend_stock_codes()
    if not symbols:
        dividend_logger.error("未获取到分红股票代码，程序终止")
        return

    conn = None
    try:
        conn = pyodbc.connect(DSN_CONNECTION)
        dividend_logger.info("成功连接数据库（分红）")
        create_dividend_table(conn)

        BATCH_SIZE = 200
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i+BATCH_SIZE]
            records = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(fetch_dividend_data, sym): sym for sym in batch}
                for fut in as_completed(futures):
                    sym = futures[fut]
                    try:
                        df = fut.result()
                        if df is not None and not df.empty:
                            records.extend(df.to_dict('records'))
                    except Exception as e:
                        dividend_logger.error(f"{sym} 分红处理异常: {e}")
                    time.sleep(0.2)
            batch_insert_dividend(conn, records)
            dividend_logger.info(f"分红数据已完成批次 {i//BATCH_SIZE+1}/{(len(symbols)+BATCH_SIZE-1)//BATCH_SIZE}")
    except Exception as e:
        dividend_logger.error(f"分红主流程异常: {e}")
    finally:
        if conn:
            conn.close()
            dividend_logger.info("分红数据库连接已关闭")

# --------------------------- 入口函数 ---------------------------
if __name__ == "__main__":
    # 测试日频数据
    test_daily_df = fetch_stock_data("sh600000")
    print("日频测试数据样例：")
    print(test_daily_df.head() if test_daily_df is not None else "无日频数据")

    # 测试分红数据
    test_dividend_df = fetch_dividend_data("sh600000")
    print("分红测试数据样例：")
    print(test_dividend_df.head() if test_dividend_df is not None else "无分红数据")

    # 执行主任务（可根据需要选择执行日频或分红，或同时执行）
    main_daily()
    main_dividend()