import sqlite3, config
import alpaca_trade_api as tradeapi

from alpaca_trade_api.rest import TimeFrame
from datetime import datetime, timedelta

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol, name FROM stock
""")

rows = cursor.fetchall()

symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)

chunk_size = 200
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]

    bar_sets = api.get_bars_iter(symbol_chunk, TimeFrame.Day, "2023-04-01", yesterday, adjustment='raw')
    for bar in bar_sets:
        symbol = bar.S
        stock_id = stock_dict[bar.S]
        print("processing symbol: " + symbol)
        try:
            cursor.execute("""
                INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))
        except Exception as e:
            print(e)
            print("Could not add stock price for ", symbol)
            
connection.commit()



