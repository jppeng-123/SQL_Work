import akshare as ak 
import pyodbc 
import pandas as pd
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_fixed
from json import JSONDecodeError

# ====================== 配置区 ======================
conn_str     = 'DSN=Trading;UID=sa;PWD=123456'  # ODBC DSN
target_table = "stock_a_daily"                  # 主表
temp_table   = "stock_info_add"                 # 临时历史数据表
batch_size   = 1000                             # 批量插入批次大小

# ====================== 重试函数定义 ======================
@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def fetch_daily(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    return ak.stock_zh_a_daily(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date
    )

def validate_date(date_str: str) -> datetime.date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("日期格式错误，请使用YYYY-MM-DD格式")

def get_trading_symbols():
    """获取带交易所前缀的有效股票代码"""
    try:
        stock_df = ak.stock_info_a_code_name()
    except JSONDecodeError:
        time.sleep(0.5)
        stock_df = ak.stock_info_a_code_name()
    
    sse_prefixes  = {"600","601","603","605","688","689"}
    szse_prefixes = {"000","001","002","003","300","301"}
    
    symbols = []
    for code in stock_df['code'].astype(str):
        if any(code.startswith(p) for p in sse_prefixes):
            symbols.append(f"sh{code}")
        elif any(code.startswith(p) for p in szse_prefixes):
            symbols.append(f"sz{code}")
    return symbols

def create_table(conn, table_name):
    """创建数据表（去掉 outstanding_share，只保留 turnover）"""
    ddl = f"""
    IF OBJECT_ID(N'dbo.{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.{table_name} (
            symbol      VARCHAR(10)   NOT NULL,
            trade_date  DATE          NOT NULL,
            [open]      DECIMAL(9,4)  NULL,
            high        DECIMAL(9,4)  NULL,
            low         DECIMAL(9,4)  NULL,
            [close]     DECIMAL(9,4)  NULL,
            volume      DECIMAL(15,2) NULL,
            amount      DECIMAL(20,2) NULL,
            turnover    DECIMAL(9,6)  NULL,
            update_time DATETIME      NOT NULL,
            CONSTRAINT PK_{table_name} PRIMARY KEY (symbol, trade_date)
        );
    END
    """
    conn.execute(ddl)
    conn.commit()

def main():
    # 日期范围输入验证
    start_date = input("请输入开始日期 (YYYY-MM-DD): ").strip()
    end_date   = input("请输入结束日期 (YYYY-MM-DD): ").strip()
    
    try:
        start_dt = validate_date(start_date)
        end_dt   = validate_date(end_date)
        if start_dt > end_dt:
            print("❌ 开始日期不能晚于结束日期")
            return
    except ValueError as e:
        print(f"❌ {str(e)}")
        return

    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True

            # 创建临时表
            create_table(cursor, temp_table)
            print(f"✔️ 临时表 [{temp_table}] 准备就绪")

            # 获取股票代码
            symbols = get_trading_symbols()
            print(f"✔️ 获取到 {len(symbols)} 个有效股票代码")

            # 下载并处理历史数据
            failed_symbols = []
            for idx, symbol in enumerate(symbols, 1):
                try:
                    df = fetch_daily(symbol, start_date, end_date)
                    if df.empty:
                        print(f"⚠️ [{idx}/{len(symbols)}] {symbol} 无数据")
                        continue

                    # 数据清洗
                    df = df.reset_index().rename(columns={'date':'trade_date'})
                    df['trade_date']  = pd.to_datetime(df['trade_date']).dt.date
                    df['symbol']      = symbol
                    df['update_time'] = datetime.now()

                    # 分批次插入
                    for i in range(0, len(df), batch_size):
                        batch = df.iloc[i:i+batch_size]
                        records = [
                            (
                                row.symbol,
                                row.trade_date,
                                row.open,
                                row.high,
                                row.low,
                                row.close,
                                row.volume,
                                row.amount,
                                row.turnover,
                                row.update_time
                            )
                            for row in batch.itertuples(index=False)
                        ]
                        cursor.executemany(
                            f"INSERT INTO {temp_table} "
                            "(symbol, trade_date, [open], high, low, [close], "
                            "volume, amount, turnover, update_time) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            records
                        )
                        conn.commit()
                    
                    print(f"✅ [{idx}/{len(symbols)}] {symbol} 插入 {len(df)} 条")
                    time.sleep(0.1)

                except Exception as e:
                    print(f"❌ [{idx}/{len(symbols)}] {symbol} 失败: {str(e)}")
                    failed_symbols.append(symbol)
                    conn.rollback()

            # 合并确认
            while True:
                choice = input(f"是否将 {temp_table} 数据合并到 {target_table}？(y/n): ").lower()
                if choice == 'y':
                    merge_sql = f"""
                    MERGE {target_table} AS target
                    USING {temp_table} AS source
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
                    WHEN NOT MATCHED THEN
                        INSERT (symbol, trade_date, [open], high, low, [close], 
                                volume, amount, turnover, update_time)
                        VALUES (
                            source.symbol, source.trade_date, source.[open],
                            source.high, source.low, source.[close],
                            source.volume, source.amount, source.turnover,
                            source.update_time
                        );
                    """
                    cursor.execute(merge_sql)
                    conn.commit()
                    print(f"✅ 数据已合并到 {target_table}, 记得检查并删除 [{temp_table}]")
                    break

                elif choice == 'n':
                    print("ℹ️ 已取消合并操作")
                    break

                else:
                    print("⚠️ 请输入 y 或 n")

            if failed_symbols:
                print("\n以下股票数据获取失败：")
                print(", ".join(failed_symbols))

    except pyodbc.Error as e:
        print(f"❌ 数据库错误: {str(e)}")

    finally:
        print("▶️ 脚本执行完毕")

if __name__ == "__main__":
    main()
