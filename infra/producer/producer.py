import time
import json
import requests
from kafka import KafkaProducer
API_kEY = "d60ffj1r01qto1rcubkgd60ffj1r01qto1rcubl0"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def fetch_stock_data(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_kEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {symbol}: {response.status_code}")
        return None


while True:
    for symbol in SYMBOLS:
        data = fetch_stock_data(symbol)
        if data:
            message = {
                "symbol": symbol,
                "data": data,
                "timestamp": int(time.time())
            }
            producer.send('stock_quotes', value=message)
            print(f"Sent data for {symbol}: {message}")
    time.sleep(6)  # Fetch data every 60 seconds
