# -*- coding:utf-8 -*-

"""
Market Server.

Market Server will get market data from Exchange via Websocket or REST as soon as possible, then packet market data into
MarketEvent and publish into EventCenter.

Author: 0x1be20
Date:   2022/10/13
"""

from itertools import product
import json
import sys

from quant import const
from quant.quant import quant
from quant.config import config
from quant.tasks import SingleTask
from aiohttp import web

import aiohttp_cors
import nest_asyncio
nest_asyncio.apply()

markets = []

def connect_market(platform,symbols,channels,orderbook_length=20):
    if platform == const.OKEX or platform == const.OKEX_MARGIN:
        from markets.okex import OKEx as Market
    elif platform == const.OKEX_FUTURE or platform == const.OKEX_SWAP:
        from markets.okex_ftu import OKExFuture as Market
    elif platform == const.BINANCE:
        from markets.binance import Binance as Market
    elif platform == const.BINANCE_FUTURE:
        from markets.binance_future import BinanceFuture as Market
    elif platform == const.DERIBIT:
        from markets.deribit import Deribit as Market
    elif platform == const.BITMEX:
        from markets.bitmex import Bitmex as Market
    elif platform == const.HUOBI:
        from markets.huobi import Huobi as Market
    elif platform == const.COINSUPER:
        from markets.coinsuper import CoinsuperMarket as Market
    elif platform == const.COINSUPER_PRE:
        from markets.coinsuper_pre import CoinsuperPreMarket as Market
    elif platform == const.KRAKEN:
        from markets.kraken import KrakenMarket as Market
    elif platform == const.GATE:
        from markets.gate import GateMarket as Market
    elif platform == const.GEMINI:
        from markets.gemini import GeminiMarket as Market
    elif platform == const.COINBASE_PRO:
        from markets.coinbase import CoinbaseMarket as Market
    elif platform == const.KUCOIN:
        from markets.kucoin import KucoinMarket as Market
    elif platform == const.HUOBI_FUTURE:
        from markets.huobi_future import HuobiFutureMarket as Market
    else:
        from quant.utils import logger
        logger.error("platform error! platform:", platform)
    cc = {
        "symbols":symbols,
        "channels":channels,
        "platform":platform,
        "orderbook_length":orderbook_length
    }
    global markets
    markets.append(Market(**cc))
    
def setup_manage_server(port=9092):
    routes = web.RouteTableDef()
    app = web.Application()

    def check_market(platform,symbol,channel):
        for market in markets:
            if platform==market.platform:
                if market.symbol_to_channel(symbol,channel) in market.channels:
                    return market
        return None

    @routes.get("/get_markets")
    async def get_markets(request:web.Request):
        channels = []
        global markets
        for market in markets:
            channels.append("{}:{}".format(market.platform,"/".join(market.channels)))
        return web.json_response(channels)

    @routes.post("/add_market")
    async def add_market(request:web.Request):
        data = await request.json()
        for (symbol,channel) in product(data["symbols"],data['channels']):
            if check_market(data['platform'],symbol,channel):
                return web.json_response(json.dumps({"mesg":"channel already exist","symbol":symbol,'channel':channel}))
        connect_market(data['platform'],data['symbols'],data['channels'])
        return web.json_response(json.dumps({"mesg":"ok","data":data}))

    @routes.post("/del_market")
    async def del_market(request:web.Request):
        data = await request.json()
        symbols = data['symbols']
        platform = data['platform']
        channels = data['channels']

        closed = []

        for (symbol,channel) in product(symbols,channels):
            market = check_market(platform,symbol,channel)
            if market:
                closed.append(market.symbol_to_channel(symbol,channel))
                await market.close_market([symbol],[channel])

        return web.json_response(json.dumps({"mesg":"ok","data":closed}))

    app.add_routes(routes)

    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*"
            )
        })

    for route in list(app.router.routes()):
        cors.add(route)
    web.run_app(app,port=port)

def main():
    config_file = sys.argv[1]  # config file, e.g. config.json.
    quant.initialize(config_file)
    SingleTask.call_later(setup_manage_server,5,getattr(config,"port",9092))
    quant.start()


if __name__ == "__main__":
    main()
