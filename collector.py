import akshare as ak
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time

class DataManager:
    def __init__(self, db_path="stock_data.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_records (
                    date TEXT, code TEXT, name TEXT, 
                    close REAL, pct_chg REAL, amount REAL, 
                    turnover_rate REAL, amplitude REAL, vol_ratio REAL,
                    PRIMARY KEY (date, code)
                )
            ''')

    def fetch_data(self, symbol: str, name: str, start_date: str, end_date: str):
        try:
            # 1. 识别接口类型
            if symbol.startswith(('sh0', 'sz3')): # 大盘指数
                df = ak.stock_zh_index_daily_em(symbol=symbol)
                df = df.rename(columns={'date': '日期', 'close': '收盘', 'amount': '成交额'})
                df['涨跌幅'] = df['收盘'].pct_change() * 100
                df['换手率'] = 0.0 # 指数通常无换手率
                df['振幅'] = 0.0
            
            elif symbol.startswith(('5', '1')): # ETF (含跨境、黄金、沪深300)
                df = ak.fund_etf_hist_em(symbol=symbol, period="daily", 
                                         start_date=start_date, end_date=end_date, adjust="qfq")
                # 统一列名映射
                mapping = {'日期': '日期', '收盘': '收盘', '涨跌幅': '涨跌幅', 
                           '成交额': '成交额', '换手率': '换手率', '振幅': '振幅'}
                df = df.rename(columns=mapping)
            
            else: # 普通个股
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                        start_date=start_date, end_date=end_date, adjust="qfq")
                df = df.rename(columns={'日期': '日期', '收盘': '收盘', '涨跌幅': '涨跌幅', 
                                       '成交额': '成交额', '换手率': '换手率', '振幅': '振幅'})

            if df.empty: return pd.DataFrame()

            # 2. 统一清洗逻辑
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            # 过滤请求范围（部分接口会返回全量数据）
            df = df[(df['日期'] >= pd.to_datetime(start_date).strftime('%Y-%m-%d')) & 
                    (df['日期'] <= pd.to_datetime(end_date).strftime('%Y-%m-%d'))]
            
            # 计算量比 (当日成交额/5日均值)
            df['vol_ratio'] = (df['成交额'] / df['成交额'].rolling(window=5).mean()).round(2)
            df['vol_ratio'] = df['vol_ratio'].fillna(1.0)
            
            df['code'] = symbol
            df['name'] = name
            
            # 选择最终入库字段
            res = df.rename(columns={
                '日期': 'date', '收盘': 'close', '涨跌幅': 'pct_chg', 
                '成交额': 'amount', '换手率': 'turnover_rate', '振幅': 'amplitude'
            })
            return res[['date', 'code', 'name', 'close', 'pct_chg', 'amount', 'turnover_rate', 'amplitude', 'vol_ratio']]

        except Exception as e:
            print(f"❌ 采集 {name}({symbol}) 异常: {e}")
            return pd.DataFrame()

    def sync_data(self, target_list):
        with sqlite3.connect(self.db_path) as conn:
            for code, name in target_list.items():
                # 检查断点：根据名称查找该标的最后一条记录日期
                res = conn.execute("SELECT MAX(date) FROM daily_records WHERE name = ?", (name,)).fetchone()
                last_date = res[0]
                
                if last_date:
                    start_dt = (datetime.strptime(last_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y%m%d')
                else:
                    start_dt = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
                
                end_dt = datetime.now().strftime('%Y%m%d')
                if start_dt > end_dt:
                    print(f"✅ {name} 已经是最新")
                    continue
                
                print(f"🚀 采集 {name} [{start_dt} -> {end_dt}]")
                data = self.fetch_data(code, name, start_dt, end_dt)
                
                if not data.empty:
                    data.to_sql('daily_records', conn, if_exists='append', index=False)
                    print(f"   已存入 {len(data)} 条")
                time.sleep(0.5) # 避免请求过快

if __name__ == "__main__":
    # 配置监控列表
    monitored_targets = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "159919": "沪深300ETF",
        "513770": "港股互联网",
        "513100": "纳指ETF",
        "513500": "标普500ETF",
        "518880": "黄金ETF",
        "513880": "日经225"
    }
    dm = DataManager()
    dm.sync_data(monitored_targets)