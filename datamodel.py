# Keep in mind (from the Readme):
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet
from dataclasses import dataclass
from itertools import count
from typing import List, Dict

import numpy as np
import pandas as pd

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


class Order:
    client_order_id: int
    side: Side
    price: int
    volume: int
    lifespan: Lifespan


class BaseState:
    bids: set
    asks: set
    order_ids: count[int]

    def update_orders(self, k: float, updated_instrument: int, order_book_0: OrderBook, order_book_1: OrderBook) -> \
    List[Order]:
        result_orders: List[Order] = list()

        # Falls updated_instrument gestiegen ist, ist R positiv
        if R >= 0:
            best_current_ask = min(order_book_0.ask_prices)
            #

        return result_orders


class LongAShortB(BaseState):
    pass


class ShortALongB(BaseState):
    pass


class Context:
    state: BaseState
    order_books_0: List[OrderBook]
    order_books_1: List[OrderBook]

    def __init__(self, initial_state: BaseState):
        self.state = initial_state
        self.order_books_0 = []
        self.order_books_1 = []

    def update_order_book(self, order_book: OrderBook) -> List[Order]:
        if order_book.instrument == 0:
            self.order_books_0.append(order_book)
        elif order_book.instrument == 1:
            self.order_books_1.append(order_book)

        # Erwartungswerte
        n = 50
        asset_prices_0 = [o.get_asset_price() for o in self.order_books_0[-n:]]
        array_0 = np.array(asset_prices_0)
        E_0: float = float(np.mean(array_0))

        asset_prices_1 = [o.get_asset_price() for o in self.order_books_1[-n:]]
        array_1 = np.array(asset_prices_1)
        E_1: float = float(np.mean(array_1))

        array_01 = np.multiply(array_0, array_1)
        E_01: float = float(np.mean(array_01))

        # aktuelle Assetpreise
        A_0: float = self.order_books_0[-1].get_asset_price()
        A_1: float = self.order_books_1[-1].get_asset_price()

        # Korrelationskoeffizient
        Cov: float = E_01 - (E_0 * E_1)
        k: float = Cov / 1  # TODO: Formel..

        return self.state.update_orders(k, order_book.instrument, self.order_books_0[-1], self.order_books_1[-1])
