# -*— coding:utf-8 -*-

"""
Binance Market Server.
https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md

Author: HuangTao
Date:   2018/07/04
Email:  huangtao@ifclover.com
"""

from itertools import product
import json
import random
from time import time
from quant import const
from quant.utils import tools
from quant.utils import logger
from quant.utils.web import Websocket
from quant.order import ORDER_ACTION_BUY, ORDER_ACTION_SELL
from quant.event import EventTrade, EventKline, EventOrderbook

from quant.tasks import SingleTask


class BinanceFuture:
    """ BinanceFuture Market Server.

    Attributes:
        kwargs:
            platform: Exchange platform name, must be `binance`.
            wss: Exchange Websocket host address, default is `wss://stream.binance.com:9443`.
            symbols: Symbol list.
            channels: Channel list, only `orderbook` / `trade` / `kline` to be enabled.
            orderbook_length: The length of orderbook's data to be published via OrderbookEvent, default is 10.
    """

    def __init__(self, **kwargs):
        self._platform = kwargs["platform"]
        self._wss = kwargs.get("wss", "wss://fstream.binance.com:443")
        self._symbols = list(set(kwargs.get("symbols")))
        self._channels = kwargs.get("channels")
        self._orderbook_length = kwargs.get("orderbook_length", 20)

        self._left_channels = set(self._to_channels(self._symbols,self._channels))

        self._c_to_s = {}
        self._tickers = {}

        url = self._make_url()
        self._ws = Websocket(url, process_callback=self.process)
        self._ws.initialize()

    def _make_url(self):
        """Generate request url.
        """
        cc = []
        for ch in self._channels:
            if ch == "kline":
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "kline_1m")
                    cc.append(c)
            elif ch == "orderbook":
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "depth20@100ms")
                    cc.append(c)
            elif ch in ["trade", "aggTrade"]:
                for symbol in self._symbols:
                    c = self._symbol_to_channel(symbol, "aggTrade")
                    cc.append(c)
            else:
                logger.error("channel error! channel:", ch, caller=self)
        url = self._wss + "/stream?streams=" + "/".join(cc)
        return url

    @property
    def platform(self):
        return self._platform
    @property
    def symbols(self):
        return self._symbols

    @property
    def channels(self):
        return list(self._left_channels)

    async def process(self, msg):
        """Process message that received from Websocket connection.

        Args:
            msg: Message received from Websocket connection.
        """
        # logger.debug("msg:", msg, caller=self)
        # print(msg)
        if not isinstance(msg, dict):
            return

        channel = msg.get("stream")
        if channel not in self._c_to_s:
            logger.warn("unkown channel, msg:", msg, caller=self)
            return

        symbol = self._c_to_s[channel]
        data = msg.get("data")
        e = data.get("e")

        if e == "kline":
            await self.process_kline(symbol, data)
        elif e == 'depthUpdate':
            await self.process_orderbook(symbol, data)
        elif e == 'aggTrade':
            await self.process_trade(symbol, data)

    async def process_kline(self, symbol, data):
        """Process kline data and publish KlineEvent."""
        kline = {
            "platform": self._platform,
            "symbol": symbol,
            "open": float(data.get("k").get("o")),
            "high": float(data.get("k").get("h")),
            "low": float(data.get("k").get("l")),
            "close": float(data.get("k").get("c")),
            "volume": float(data.get("k").get("q")),
            "timestamp": float(data.get("k").get("t")),
            "kline_type": const.MARKET_TYPE_KLINE,
            "_eventtime": time()
        }
        EventKline(**kline).publish()
        logger.info("symbol:", symbol, "kline:", kline, caller=self)

    async def process_orderbook(self, symbol, data):
        """Process orderbook data and publish OrderbookEvent."""
        bids = []
        asks = []
        for bid in data.get("b")[:self._orderbook_length]:
            bids.append([float(bid[0]),float(bid[1])])
        for ask in data.get("a")[:self._orderbook_length]:
            asks.append([float(ask[0]),float(ask[1])])
        orderbook = {
            "platform": self._platform,
            "symbol": symbol,
            "asks": asks,
            "bids": bids,
            "timestamp": tools.get_cur_timestamp_ms(),
            "_eventtime": time()
        }
        EventOrderbook(**orderbook).publish()
        logger.info("symbol:", symbol, "orderbook:", orderbook, caller=self)

    async def process_trade(self, symbol, data):
        """Process trade data and publish TradeEvent."""
        trade = {
            "platform": self._platform,
            "symbol": symbol,
            "action":  ORDER_ACTION_SELL if data["m"] else ORDER_ACTION_BUY,
            "price": float(data.get("p")),
            "quantity": float(data.get("q")),
            "timestamp": float(data.get("T")),
            "_eventtime": time()
        }
        EventTrade(**trade).publish()
        logger.info("symbol:", symbol, "trade:", trade, caller=self)

    def _symbol_to_channel(self, symbol, channel_type="ticker"):
        channel = self.symbol_to_channel(symbol,channel_type)
        self._c_to_s[channel] = symbol
        return channel

    def symbol_to_channel(self,symbol,channel_type):
        channel = "{x}@{y}".format(x=symbol.replace("/", "").lower(), y=channel_type)
        return channel

    async def close_market(self,symbols=[],channels=[]):
        close_channels = set(self._to_channels(symbols,channels))
        self._left_channels = self._left_channels-close_channels
        logger.info("close channels:",list(close_channels),caller=self)
        if len(self._left_channels)==0:
            self._ws.close()
        await self._ws.send(json.dumps({
            "method":"UNSUBSCRIBE",
            "params":list(
                map(
                    lambda v:self._symbol_to_channel(v[0],self._to_standard_channel(v[1])),
                    list(map(lambda v:v.split("@"),close_channels))
                )),
            "id":int(random.random()*10000)
        }))

    def _to_standard_channel(self,ch):
        if ch == "kline":
            return "kline_1m"
        elif ch == "orderbook":
            return "depth20@100ms"
        elif ch in ["trade", "aggTrade"]:
            return "aggTrade"
        else:
            return ch

    def _to_channels(self,symbols=[],channels=[]):
        return list(map(lambda v:self.symbol_to_channel(v[0],v[1]),product(symbols,channels)))