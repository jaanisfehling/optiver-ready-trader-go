# Keep in mind (from the Readme):
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet
from dataclasses import dataclass
from typing import List, Dict


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
        return self.ask_prices[0]


class BaseState:
    pass


class IdleState(BaseState):
    pass


class LongAShortB(BaseState):
    pass


class ShortALongB(BaseState):
    pass


class Context:
    state: BaseState = IdleState
    order_books_0: List[OrderBook]
    order_books_1: List[OrderBook]

    def __init__(self, initial_state: BaseState):
        self.state = initial_state
        self.order_books_0 = []
        self.order_books_1 = []

    def update_order_book(self, order_book: OrderBook):
        if order_book.instrument == 0:
            self.order_books_0.append(order_book)
        elif order_book.instrument == 1:
            self.order_books_1.append(order_book)

        # Evaluate the order book update
        # The Estimated correlation, should be calculated using a pandas df
        E: float = 1.0
        # The actual correlation using the most recent order books
        Q: float = self.order_books_0[-1].get_asset_price() / self.order_books_1[-1].get_asset_price()
        # Magic Formula
        R: float = E - Q
        self.state.take_action(R)
