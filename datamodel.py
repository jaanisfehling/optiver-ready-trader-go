# Keep in mind (from the Readme):
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet
import itertools
import math
from dataclasses import dataclass
from itertools import count
from typing import List, Dict

import numpy as np
from numpy import ndarray

from ready_trader_go import Lifespan, Side

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
        return min(self.ask_prices)


@dataclass
class Order:
    client_order_id: int
    side: Side
    price: int
    volume: int
    lifespan: Lifespan


class TradingState:
    # Letzte Order Books der beiden Instrumente
    last_order_book_0: OrderBook
    last_order_book_1: OrderBook
    # Liste aller Asset Preise
    asset_prices_0: List[float]
    asset_prices_1: List[float]
    # Iterator um unique Order ID zu erzeugen
    order_id_iterator: count[int]
    # Zuletzt benutzte Order ID jedes Instruments
    last_order_id_0: int
    last_order_id_1: int
    # Z-Scores der letzten n Order Book Updates
    z_scores: list[float]

    def __init__(self):
        self.asset_prices_0 = []
        self.asset_prices_1 = []
        self.order_id_iterator = itertools.count(start=1, step=1)
        self.last_order_id_1 = 0
        self.last_order_id_1 = 0
        self.z_scores = []

    def update_orders(self, ratio: float, updated_instrument: int, order_book_0: OrderBook, order_book_1: OrderBook) -> \
            Dict[str, List[Order or int]]:
        result_orders: Dict[str, List[Order or int]] = dict(send=list())

        # Asset 0 goes up
        if ratio:
            # Cancel both orders
            result_orders["cancel"] = [self.last_order_id_0, self.last_order_id_1]

            # Create new Orders
            # Sell Asset 0
            self.last_order_id_0 = next(self.order_id_iterator)
            best_current_bid_0 = min(order_book_0.bid_prices)
            result_orders["send"].append(
                Order(self.last_order_id_0, Side.BUY, best_current_bid_0, 10, Lifespan.IMMEDIATE_OR_CANCEL))

            # Buy Asset 1 at best current ask price
            self.last_order_id_1 = next(self.order_id_iterator)
            best_current_ask_1 = min(order_book_0.ask_prices)
            result_orders["send"].append(
                Order(self.last_order_id_1, Side.BUY, best_current_ask_1, 10, Lifespan.IMMEDIATE_OR_CANCEL))

        # Asset 1 goes up
        elif True:
            # Cancel both orders
            result_orders["cancel"] = [self.last_order_id_0, self.last_order_id_1]

            # Create new Orders
            # Sell Asset 1
            self.last_order_id_1 = next(self.order_id_iterator)
            best_current_bid_1 = min(order_book_1.bid_prices)
            result_orders["send"].append(
                Order(self.last_order_id_1, Side.BUY, best_current_bid_1, 10, Lifespan.IMMEDIATE_OR_CANCEL))

            # Buy Asset 0 at best current ask price
            self.last_order_id_0 = next(self.order_id_iterator)
            best_current_ask_0 = min(order_book_0.ask_prices)
            result_orders["send"].append(
                Order(self.last_order_id_0, Side.BUY, best_current_ask_0, 10, Lifespan.IMMEDIATE_OR_CANCEL))

    def update_order_book(self, order_book: OrderBook) -> Dict[str, List[Order or int]]:

        if order_book.instrument == 0:
            # Order Book speichern
            self.last_order_book_0 = order_book
            # Asset Preis der Liste hinzufügen
            self.asset_prices_0.append(order_book.get_asset_price())
            # Falls das andere Instrument noch keine Updates erhalten hat, werden wir nichts tun
            if not self.asset_prices_1:
                return {}
            # Letzten Asset Preis des anderen Instruments kopieren und der anderen Liste hinzufügen,
            # um keine Abweichungen in der Array-Division zu erhalten
            self.asset_prices_1.append(self.asset_prices_1[-1])
        # Das gleiche im Falle des anderen Instruments
        elif order_book.instrument == 1:
            self.last_order_book_1 = order_book
            self.asset_prices_1.append(order_book.get_asset_price())
            # Falls das andere Instrument noch keine Updates erhalten hat, werden wir nichts tun
            if not self.asset_prices_1:
                return {}
            self.asset_prices_0.append(self.asset_prices_0[-1])

        # aktuelle Assetpreise
        price_0: float = self.asset_prices_0[-1]
        price_1: float = self.asset_prices_1[-1]

        # Letzte n Assetpreise
        n = 50
        asset_price_array_0 = np.array(self.asset_prices_0)
        asset_price_array_1 = np.array(self.asset_prices_1)

        # Logarithmus der Assetpreise
        log_prices_0: ndarray = np.log(asset_price_array_0)
        log_prices_1: ndarray = np.log(asset_price_array_1)

        # Berechne für jeden Log-Asset-Preis den Quotienten
        weight_array = log_prices_0 / log_prices_1

        # Nehme den Mittelwert der Quotienten
        weight_avg = np.average(weight_array)

        # Standartabweichung
        weight_std = np.std(weight_array)

        # Log Spread Funktion
        spread = math.log(price_0) - weight_avg * math.log(price_1)

        # Z-Score
        z_score = spread / weight_std

        # Z-Score Array updaten
        self.z_scores.append(z_score)

        return self.update_orders(z_score, self.order_books_0[-1], self.order_books_1[-1])
