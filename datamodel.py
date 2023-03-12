# Keep in mind (from the Readme):
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet
from dataclasses import dataclass
from itertools import count
from typing import List, Dict

import numpy as np

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


class BaseState:
    bids: set
    asks: set
    order_id_iterator: count[int]
    last_order_id_0: int
    last_order_id_1: int

    def update_orders(self, k: float, updated_instrument: int, order_book_0: OrderBook, order_book_1: OrderBook) -> \
            Dict[str, List[Order or int]]:
        result_orders: Dict[str, List[Order or int]] = dict(send=list())

        # Asset 0 goes up
        if True:
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

    def update_order_book(self, order_book: OrderBook) -> Dict[str, List[Order or int]]:
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
        Price_0: float = self.order_books_0[-1].get_asset_price()
        Price_1: float = self.order_books_1[-1].get_asset_price()

        # Korrelationskoeffizient
        Cov: float = E_01 - (E_0 * E_1)
        k: float = Cov / 1  # TODO: Formel..

        return self.state.update_orders(k, order_book.instrument, self.order_books_0[-1], self.order_books_1[-1])
