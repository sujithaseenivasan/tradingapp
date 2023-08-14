import sqlite3
import config
import alpaca_trade_api as tradeapi
import datetime as date
import time
from alpha_vantage.timeseries import TimeSeries
import smtplib, ssl

ssl._create_default_https_context = ssl._create_unverified_context
context = ssl.create_default_context()

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id from strategy where name = 'opening_range_breakout'
""")

strategy_id = cursor.fetchone()['id']

cursor.execute("""
    SELECT symbol, name FROM stock
    join stock_strategy on stock_strategy.stock_id = stock.id
    where stock_strategy.strategy_id = ?    
""", (strategy_id,))

stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

current_date = date.datetime.today().strftime('%Y-%m-%d')
start_minute_bar = f"{current_date} 09:30:00"
end_minute_bar = f"{current_date} 09:45:00"

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.BASE_URL)
orders = api.list_orders(status=all, limit=500, after=f"{current_date}T13:30:00Z")
#orders = api.list_orders()
existing_order_symbols = [order.symbol for order in orders]

messages = []

for symbol in symbols:
    ts = TimeSeries(key=config.ALPHA_VANTAGE_API_KEY, output_format='pandas')
    minute_bars, meta_data = ts.get_intraday(symbol=symbol,interval='1min', outputsize='full')
    minute_bars.columns = ['open', 'high', 'low', 'close', 'volume']
    
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]
    
    opening_range_low = opening_range_bars['low'].min()
    opening_range_high = opening_range_bars['high'].max()
    opening_range = opening_range_high - opening_range_low

    after_opening_range_mask = minute_bars.index >= end_minute_bar
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]

    after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars['close'] > opening_range_high]

    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:
            existing_order_symbols.append(symbol)
            # selects first minute bar after the close price is greater than the opening range high
            limit_price = round(after_opening_range_breakout.iloc[0]['close'], 2)
            
            messages.append(f"placing order for {symbol} at {limit_price}, closed above {opening_range_high}\n\nat {after_opening_range_breakout.iloc[0].name}\n\n")

            print(f"placing order for {symbol} at {limit_price}, closed above {opening_range_high} at {after_opening_range_breakout.iloc[0].name}")
            api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='100',
                time_in_force='day',
                order_class='bracket',
                limit_price=limit_price,
                take_profit=dict(
                    limit_price=limit_price + opening_range,
                ),
                stop_loss=dict(
                    stop_price=limit_price - opening_range,
                )
            )
        else:
            print(f"Already an order for {symbol}, skipping")


    if len(symbols) > 5:
        time.sleep(13)

print(messages)

with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
    server.login(user=config.EMAIL_ADDRESS, password=config.EMAIL_PASSWORD)
    email_message = f"Subject: Trade Notifications for {current_date}\n\n"
    email_message += "\n".join(messages)
    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, email_message)
    server.sendmail(config.EMAIL_ADDRESS, config.SMS_EMAIL_ADDRESS, email_message)