from hftbacktest import BUY, SELL
from hftbacktest.marketdepth import MarketDepth, INVALID_MAX, INVALID_MIN


depth = MarketDepth(0.1, 0.1)

depth.update_bid_depth(1.2, 0.5, 1, None)
depth.update_bid_depth(1.1, 0.4, 1, None)
depth.update_bid_depth(0.9, 0.4, 1, None)
depth.update_bid_depth(0.7, 0.4, 1, None)
depth.update_bid_depth(0.6, 0.4, 1, None)
depth.update_ask_depth(2.1, 0.4, 1, None)
depth.update_ask_depth(2.2, 0.5, 1, None)
depth.update_ask_depth(2.8, 0.5, 1, None)
depth.update_ask_depth(3.0, 0.5, 1, None)
depth.update_ask_depth(3.1, 0.5, 1, None)
#*
print(depth.best_ask_tick)
print(depth.high_ask_tick)
print(depth.best_bid_tick)
print(depth.low_bid_tick)

#*
print(depth.bid_depth)
depth.clear_depth(BUY, 0.9)
print(depth.bid_depth)
print(depth.best_ask_tick)
print(depth.best_bid_tick)

#* depth.clear_depth(BUY, 0.6)

print(depth.ask_depth)
depth.clear_depth(SELL, 2.8)
print(depth.ask_depth)
print(depth.best_ask_tick)
print(depth.best_bid_tick)

# depth.clear_depth(SELL, 3.1)
