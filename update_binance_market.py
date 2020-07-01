import os
import sys
import ccxt
import json
import pprint

exchange_id = 'binance'
exchange = getattr(ccxt, exchange_id)({
                    'rateLimit': 3000,
                    'enableRateLimit': True
                 })

#print(str(exchange) +  " market pairs will be updated.")

markets = exchange.load_markets()
market_pairs = list(markets.keys())
print("No. of market pairs = ", len(market_pairs))

gekko_market_pairs = []
gekko_currencies = []
gekko_assets = []
for market_pair in market_pairs:
    market_symbol = market_pair.split("/")[1] 
    asset_symbol = market_pair.split("/")[0] 
    gekko_currencies.append(market_symbol)
    gekko_assets.append(asset_symbol)

gekko_currencies = list(set(gekko_currencies))
gekko_assets = list(set(gekko_assets))

#print(gekko_currencies)
#print(gekko_assets)

dict = {}
dict["assets"] = gekko_assets
dict["currencies"] = gekko_currencies
dict["markets"] = []

market_pairs = list(set(market_pairs))
for market_pair in market_pairs:
    market_symbol = market_pair.split("/")[1] 
    asset_symbol = market_pair.split("/")[0] 

    market = {}
    pair = []
    pair.append(market_symbol)
    pair.append(asset_symbol)

    minimal_order = {}
    minimal_order["amount"] = 0.01
    minimal_order["price"] = 1e-8
    minimal_order["order"] = 0.001

    market["pair"] = pair
    market["minimal_order"] = minimal_order

    dict["markets"].append(market)

jp = json.dumps(dict, indent = 4, sort_keys=True)
#print(jp)
