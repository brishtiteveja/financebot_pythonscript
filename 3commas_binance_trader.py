#Paragon bot
#https://github.com/jsdev63/paragon_trade_bot/blob/a8e45872b831ebdf70421a92912ed9650f313aae/ThreeCommas.py

from py3cw.request import Py3CW

from pprint import pprint

key = "32dfe9ef79814f48ae66c3108eee2750d7f253183afd4c209286078d29af3e0a"
secret = "348178c5a195860016ec90e67bd3fff121d57c1c7589295077bd27ce9c47da35f55e720385053b7015b6413ba5f6c28fef59855f0eafc8130dc159bf4e4a764ab6c2b7f2bd5c352a17803fe2af89e4e2e188a29a869f67506d22fbf2b8c01b8c8595527d"

print("Connecting to 3commas")
p3cw = Py3CW(key=key, secret=secret)
print("Connected")

# get accounts
error, data = p3cw.request(
    entity='accounts',
    action='',
    payload = {
    }
)

accountsData = data
#pprint(accountsData)
print("\n\n***********************************************\n\n")


# get exchange market pairs 
error, data = p3cw.request(
    entity='accounts',
    action='market_pairs',
    payload = {
	"pretty_display_type": "Binance"
    }
)

marketPairs = data
#pprint(data)


# get bots
error, data = p3cw.request(
    entity='bots',
    action='',
    payload = {
    }
)

botsData = data
nbots = len(botsData)
print("Number of bots = ", nbots)

pprint(data)


# With payload data
# Destruct response to error and data
# and check first if we have an error, otherwise check the data
'''
error, data  = p3cw.request(
    entity='smart_trades', 
    action='create_smart_trade', 
    payload={
        "account_id": 123456
    }
)
'''

# With action_id replaced in URL
# Destruct response to error and data
# and check first if we have an error, otherwise check the data
'''
error, data = p3cw.request(
    entity='smart_trades', 
    action='pie_chart_data',
    action_id='123456'
)
'''



