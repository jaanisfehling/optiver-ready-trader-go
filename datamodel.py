from typing import List


class OrderBook:
    def __init__(self,
                 instrument: int,
                 sequence_number: int,
                 ask_prices: List[int],
                 ask_volumes: List[int],
                 bid_prices: List[int],
                 bid_volumes: List[int]
                 ) -> None:
        self.instrument = instrument
        self.sequence_number = sequence_number
        self.ask_prices = ask_prices
        self.ask_volumes = ask_volumes
        self.bid_prices = bid_prices
        self.bid_volumes = bid_volumes

class BaseState:
    def update(self, order_book: OrderBook):


    def evaluate(self):



class IdleState(BaseState):
    pass


class LongAShortB(BaseState):



class ShortALongB(BaseState):



