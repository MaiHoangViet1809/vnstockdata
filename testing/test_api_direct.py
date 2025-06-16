from rest_api_interface import StockService
from datetime import datetime
from utils.debug import print_table

STOCK_SYMBOL = "VN30"
PAYLOAD = {'timeFrame': 'ONE_MINUTE', 'symbols': ['VN30'], 'from': 1748710800, 'to': 1748793600}

print(f"{datetime.fromtimestamp(1748710800)=}")
print(f"{datetime.fromtimestamp(1748793600)=}")

df_output = StockService.get_candle(symbol=STOCK_SYMBOL, dt_from=datetime(2025, 6, 16), dt_to=datetime(2025, 6, 16, 23))

if df_output.size > 0:
    print_table(df_output)
