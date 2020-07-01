import asyncio
import os
import sys
import datetime
import time
from matplotlib import pyplot as plt
import numpy as np

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async_support as ccxt  # noqa: E402

exchange = None

# set up plotting
fig, ax = plt.subplots(1, 2, figsize=(15, 5))
l1 = ax[0].plot([], [], marker='.', color='green')
ax[0].set_xlabel("Bid order (USD)")
ax[0].set_ylabel("Number of orders")

l2 = ax[1].plot([], [], marker='.', color='red')
ax[1].set_xlabel("Ask order (USD)")
ax[1].set_ylabel("Number of orders")

fig.canvas.draw()
fig.suptitle('Oder Book (Left: Bid order, Right: Ask order)')
plt.show(block=False)

async def poll():
    global exchange
    exchange = ccxt.coinbasepro()
    while True:
        try:
            yield await exchange.fetch_order_book('BTC/USD')
            await asyncio.sleep(exchange.rateLimit / 1000)
            await update_line(ax[0], [], [])
            await update_line(ax[1], [], [])
        except Exception as e:
            print("Exception Occurred")
            time.sleep(5)
    exchange.close()


async def main():
    async for orderbook in poll():
        global x, y, xa, ya, xb, yb
        nbids = len(orderbook['bids'])
        nasks = len(orderbook['asks'])
        i = 0
        now = exchange.seconds()
        t = datetime.datetime.utcfromtimestamp(now).strftime("%Y-%m-%d %H:%M-%S")
        print("Start Time = ", t)

        print("Bids = ", nbids, ", Asks = ", nasks)

        xb = None
        yb = None
        xa = None
        ya = None
        while True:

            if i < nbids and i < nasks: 
                b = orderbook['bids'][i]
                a = orderbook['asks'][i]

                x1 = np.array(b[0])
                y1 = np.array(b[1])

                xb = np.append(xb, x1)
                yb = np.append(yb, y1)

                x2 = np.array(a[0]) 
                y2 = np.array(a[1])
       
                xa = np.append(xa, x2)
                ya = np.append(ya, y2)

                s = "{0:3}: Bid: [{1:15}, {2:15}], Ask: [{3:15}, {4:15}]".format(i, b[0], b[1], a[0], a[1])
                print(s)
               
                i += 1
            else:
                j = i
                while j < nbids: 
                    print(orderbook['bids'][i], "Empty asks")
                    j += 1
                while j < nasks: 
                    print("Empty bids", orderbook['asks'][i])
                    j += 1

                break
        print("\n \n")
        print("--------------------------------------------")
        now = exchange.seconds()
        t = datetime.datetime.utcfromtimestamp(now).strftime("%Y-%m-%d %H:%M-%S")
        print("End Time = ", t)


        # get bid and ask data
        xb = xb[xb != np.array(None)]
        yb = yb[yb != np.array(None)]
        xa = xa[xa != np.array(None)]
        ya = ya[ya != np.array(None)]

        try:
           l = l1[0]
           l.set_data(xb, yb)
           ax[0].relim()
           ax[0].autoscale_view(True, True, True)
        
           l = l2[0]
           l.set_data(xa, ya)
           ax[1].relim()
           ax[1].autoscale_view(True, True, True)

           fig.canvas.draw()
           plt.pause(0.005)
        except Exception as e:
           print(e)
           plt.close('all')

asyncio.get_event_loop().run_until_complete(main())
