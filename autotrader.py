# Zu beachten (aus Readme.md):
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Memory limit: 2GB
# Total disk usage limit: 100MB (including the log file)
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet

import asyncio
import itertools
from dataclasses import dataclass
from typing import List, Dict

import numpy as np

from ready_trader_go import BaseAutoTrader, Lifespan, Side

MAKER_FEE = -0.0001
TAKER_FEE = 0.0002
FEE = TAKER_FEE + MAKER_FEE


@dataclass
class OrderBook:
    instrument: int
    sequence_number: int
    ask_prices: List[int]
    ask_volumes: List[int]
    bid_prices: List[int]
    bid_volumes: List[int]

    def as_dict(self) -> Dict:
        return vars(self)

    def get_asset_price(self) -> float:
        # TODO: Define Asset Price
        return self.ask_prices[0] if self.ask_prices[0] != 0 else None

class AutoTrader(BaseAutoTrader):

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        # Letzte Order Books der beiden Instrumente
        self.last_order_book_0: OrderBook = OrderBook(-1, -1, [], [], [], [])
        self.last_order_book_1: OrderBook = OrderBook(-2, -2, [], [], [], [])
        # Liste aller Asset Preise
        self.asset_prices_0: List[float] = []
        self.asset_prices_1: List[float] = []
        # Liste aller Spreads
        self.spreads: List[float] = []
        # Position in Instruments
        self.position_0: int = 0
        self.position_1: int = 0
        # Aktive ETF Orders
        self.bids = set()
        self.asks = set()
        # Iterator um eine unique Order ID zu erzeugen
        self.id_iter = itertools.count(start=0, step=1)

    def best_order_price(self, order_book: OrderBook, side: Side, amount: int):
        # TODO: Order Volume betrachten
        if side.BUY:
            return order_book.ask_prices[0]
        elif side.SELL:
            return order_book.bid_prices[0]

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        order_book = OrderBook(instrument, sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)

        # Order Book speichern und Assetpreis der Liste hinzufügen
        if order_book.instrument == 0:
            self.last_order_book_0 = order_book
            self.asset_prices_0.append(order_book.get_asset_price())
        elif order_book.instrument == 1:
            self.last_order_book_1 = order_book
            self.asset_prices_1.append(order_book.get_asset_price())

        # Wir warten bis beide Order Books updated wurden
        if self.last_order_book_0.sequence_number != self.last_order_book_1.sequence_number:
            return

        # Wir brechen außerdem ab, wenn eins der Orderbooks keine Preise enthält (z.B. am Anfang der Runde)
        if self.asset_prices_0[-1] is None or self.asset_prices_1[-1] is None:
            # Beide Einträge der Listen löschen, um inkonsistente Zustände zu vermeiden
            del self.asset_prices_0[-1], self.asset_prices_1[-1]
            return

        # aktuelle Assetpreise
        price_0: float = self.last_order_book_0.get_asset_price()
        price_1: float = self.last_order_book_1.get_asset_price()

        # Array aller Assetpreise
        # asset_price_array_0 = np.array(self.asset_prices_0)
        # asset_price_array_1 = np.array(self.asset_prices_1)

        # Spread zwischen Assets berechnen
        spread = price_0 - price_1

        # Spreads der Spread-Liste hinzfügen
        self.spreads.append(spread)

        # Spread zwischen 0 und 1 normalisieren
        try:
            spread_norm = (spread - min(self.spreads)) / (max(self.spreads) - min(self.spreads))
        except ZeroDivisionError:
            spread_norm = 0

        self.logger.info(
            {"Prices 0": self.asset_prices_0[-3:], "Prices 1": self.asset_prices_1[-3:], "spread": spread,
             "spread_norm": spread_norm})

        # Zu besitztende Instrumentenmenge mit Spread berechnen
        target_amount: int = round(100 * spread_norm)

        # Sell 0 (Future), Buy 1 (ETF)
        # spread = price_0 - price_1
        if spread > 0:

            # Falls wir zu viel Future besitzen (target negativ weil short)
            if -target_amount < self.position_0:
                # Future verkaufen
                volume = target_amount+self.position_0 # stimmt safe
                price = self.best_order_price(self.last_order_book_0, Side.SELL, volume)
                self.send_hedge_order(next(self.id_iter), Side.SELL, price, volume)

            # Falls wir zu wenig Future besitzen
            elif -target_amount > self.position_0:
                # Future kaufen
                volume = -target_amount-self.position_0 # stimmt safe
                price = self.best_order_price(self.last_order_book_0, Side.BUY, volume)
                self.send_hedge_order(next(self.id_iter), Side.BUY, price, volume)

            # Falls wir zu viel ETF besitzen
            if target_amount < self.position_1:
                # ETF verkaufen
                volume = -target_amount+self.position_0
                price = self.best_order_price(self.last_order_book_1, Side.SELL, volume)
                self.send_insert_order(next(self.id_iter), Side.SELL, price, volume, Lifespan.IMMEDIATE_OR_CANCEL)

            # Falls zu wenig ETF
            elif target_amount > self.position_1:
                # ETF kaufen
                volume = target_amount-self.position_0
                price = self.best_order_price(self.last_order_book_1, Side.BUY, volume)
                self.send_insert_order(next(self.id_iter), Side.BUY, price, volume, Lifespan.IMMEDIATE_OR_CANCEL)


        # Buy 0 (Future), Sell 1 (ETF)
        # spread = price_0 - price_1
        elif spread < 0:

            # Falls wir zu viel Future besitzen (target positiv weil long)
            if target_amount < self.position_0:
                # Future verkaufen
                volume = self.position_0-target_amount
                price = self.best_order_price(self.last_order_book_0, Side.SELL, volume)
                self.send_hedge_order(next(self.id_iter), Side.SELL, price, volume)

            elif target_amount > self.position_0:
                # Future kaufen
                volume = target_amount-self.position_0
                price = self.best_order_price(self.last_order_book_0, Side.BUY, volume)
                self.send_hedge_order(next(self.id_iter), Side.BUY, price, volume)

            # Falls wir zu wenig Short im ETF sind
            if -target_amount < self.position_1:
                # ETF verkaufen
                volume = target_amount+self.position_0
                price = self.best_order_price(self.last_order_book_1, Side.SELL, volume)
                self.send_insert_order(next(self.id_iter), Side.SELL, price, volume, Lifespan.IMMEDIATE_OR_CANCEL)

            # Falls zu stark short Position ETF
            elif -target_amount > self.position_1:
                # ETF kaufen
                volume = -self.position_0-target_amount
                price = self.best_order_price(self.last_order_book_1, Side.BUY, volume)
                self.send_insert_order(next(self.id_iter), Side.BUY, price, volume, Lifespan.IMMEDIATE_OR_CANCEL)


    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        self.logger.warning(f"Error with order {client_order_id}: {error_message.decode()}")

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info(f"Hedge Order {client_order_id} filled; volume filled {volume}; at price {price}; Future position {self.position_0}")
        if client_order_id in self.bids:
            self.position_0 += volume
        elif client_order_id in self.asks:
            self.position_0 -= volume

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info(f"Order {client_order_id} filled; volume filled {volume}; at price {price}; ETF position {self.position_1}")
        if client_order_id in self.bids:
            self.position_1 += volume
        elif client_order_id in self.asks:
            self.position_1 -= volume


    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        self.logger.info(f"Order {client_order_id} status update; volume filled {fill_volume}; volume remaining {remaining_volume}; fees {fees}; Future position {self.position_0}; ETF position {self.position_1}")

    # def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
    #                            ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
    #     self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
    #                      sequence_number)
