# From Readme.md:
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Memory limit: 2GB
# Total disk usage limit: 100MB (including the log file)
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet

import asyncio
import itertools
from dataclasses import dataclass
from typing import List
from ready_trader_go import BaseAutoTrader, Lifespan, Side


@dataclass
class OrderBook:
    instrument: int
    sequence_number: int
    ask_prices: List[int]
    ask_volumes: List[int]
    bid_prices: List[int]
    bid_volumes: List[int]

class AutoTrader(BaseAutoTrader):

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        # Letzte Order Books der beiden Instrumente
        self.last_order_book_0: OrderBook = OrderBook(-1, -1, [], [], [], [])
        self.last_order_book_1: OrderBook = OrderBook(-2, -2, [], [], [], [])
        # Anzahl an Lots die wir besitzen
        self.position_0: int = 0
        self.position_1: int = 0
        # Aktive ETF Orders
        self.bids_1 = set()
        self.asks_1 = set()
        # Aktive Future Orders
        self.bids_0 = set()
        self.asks_0 = set()
        # Iterator um eine unique Order ID zu erzeugen
        self.id_iter = itertools.count(start=0, step=1)


    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        order_book = OrderBook(instrument, sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
        self.logger.info(f"ORDER BOOK {instrument}; BIDS: {bid_prices}; BID VOLS: {bid_volumes}; ASKS: {ask_prices}; ASK VOLS: {ask_volumes}")

        # Order Book speichern
        if order_book.instrument == 0:
            self.last_order_book_0 = order_book
        elif order_book.instrument == 1:
            self.last_order_book_1 = order_book

        # Wir warten bis beide Order Books updated wurden
        if self.last_order_book_0.sequence_number != self.last_order_book_1.sequence_number:
            return

        # Wir brechen ab, wenn eins der Orderbooks keine Preise enthält (z.B. am Anfang der Runde)
        if self.last_order_book_0.ask_prices[0] == 0 or self.last_order_book_1.ask_prices[0] == 0:
            return

        etf_asks = self.last_order_book_1.ask_prices
        etf_bids = self.last_order_book_1.bid_prices
        etf_ask_vols = list(self.last_order_book_1.ask_volumes)
        etf_bid_vols = list(self.last_order_book_1.bid_volumes)
        future_asks = list(self.last_order_book_0.ask_prices)
        future_bids = list(self.last_order_book_0.bid_prices)
        future_ask_vols = list(self.last_order_book_0.ask_volumes)
        future_bid_vols = list(self.last_order_book_0.bid_volumes)

        sent_volume: int = 0
        for i in range(len(etf_asks)):
            for j in range(len(etf_asks)):

                # Long ETF and Short Future
                # Falls es profitabel ist
                if etf_asks[i] + 0 < future_bids[j] - 0 and etf_asks[i] != 0 and future_bids[j] != 0:

                    # Mögliches profitables Volume
                    volume: int = min(etf_ask_vols[i], future_bid_vols[j])

                    # Falls wir bereits max. Volumen erreicht haben (100 Stück)
                    tradable_vol: int = 100 - (self.position_1 + sent_volume)

                    # Order ID zu laufenden Bids hinzufügen
                    id = next(self.id_iter)
                    self.bids_1.add(id)

                    if volume >= tradable_vol:
                        self.send_insert_order(id, Side.BUY, etf_asks[i], tradable_vol, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER; side {Side.BUY}; price {etf_asks[i]}; volume: {tradable_vol}")
                        return

                    # Sonst: Order nach profitablem Volumen ausführen
                    else:
                        sent_volume += volume
                        future_bid_vols[j] -= volume
                        etf_ask_vols[j] -= volume
                        self.send_insert_order(id, Side.BUY, etf_asks[i], volume, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER; side {Side.BUY}; price {etf_asks[i]}; volume: {volume}")

                # Nun: Short ETF und Long Future
                elif etf_bids[i] - 0 > future_asks[j] + 0 and etf_bids[i] != 0 and future_asks[j] != 0:

                    # Mögliches profitables Volume
                    volume = min(etf_bid_vols[i], future_ask_vols[j])

                    # Falls wir bereits max. Volumen erreicht haben (100 Stück)
                    tradable_vol = 100 - (abs(self.position_1) + sent_volume)

                    # Order ID zu laufenden Asks hinzufügen
                    id = next(self.id_iter)
                    self.asks_1.add(id)

                    if volume >= tradable_vol:
                        self.send_insert_order(id, Side.SELL, etf_bids[i], tradable_vol, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER; side {Side.SELL}; price {etf_bids[i]}; volume: {tradable_vol}")
                        return

                    # Sonst: Order nach profitablem Volumen ausführen
                    else:
                        sent_volume += volume
                        future_ask_vols[j] -= volume
                        etf_bid_vols[j] -= volume
                        self.send_insert_order(id, Side.SELL, etf_bids[i], volume, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER; side {Side.SELL}; price {etf_bids[i]}; volume: {volume}")




    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        self.logger.warning(f"Error with order {client_order_id}: {error_message.decode()}")


    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        if client_order_id in self.bids_0:
            self.position_0 += volume
        elif client_order_id in self.asks_0:
            self.position_0 -= volume
        self.logger.info(f"Future Order {client_order_id} filled; volume filled {volume}; at price {price}; Future position {self.position_0}")


    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:

        # Falls wir ETF gekauft haben, müssen wir Future verkaufen
        if client_order_id in self.bids_1:
            self.position_1 += volume

            # Future verkaufen
            for i in range(5):

                remaining_volume = self.last_order_book_0.bid_volumes[i]

                id = next(self.id_iter)
                self.asks_0.add(id)

                # Falls genügend Volume vorhanden ist
                if volume <= remaining_volume:
                    self.send_hedge_order(id, Side.SELL, self.last_order_book_0.bid_prices[i], volume)
                    self.logger.info(
                        f"SEND FUTURE ORDER; side {Side.SELL}; price {self.last_order_book_0.bid_prices[i]}; volume: {volume}")
                    return

                # Falls nur ein Teil der benötigten Volume vorhanden ist
                elif volume > remaining_volume:
                    volume -= remaining_volume
                    self.send_hedge_order(id, Side.SELL, self.last_order_book_0.bid_prices[i], volume)
                    self.logger.info(
                        f"SEND FUTURE ORDER; side {Side.SELL}; price {self.last_order_book_0.bid_prices[i]}; volume: {volume}")

        # Falls wir ETF verkauft haben
        elif client_order_id in self.asks_1:
            self.position_1 -= volume

            # Future kaufen
            for i in range(5):

                id = next(self.id_iter)
                self.bids_0.add(id)

                remaining_volume = self.last_order_book_0.ask_volumes[i]

                # Falls genügend Volume vorhanden ist
                if volume <= remaining_volume:
                    self.send_hedge_order(id, Side.BUY, self.last_order_book_0.ask_prices[i], volume)
                    self.logger.info(f"SEND FUTURE ORDER; side {Side.BUY}; price {self.last_order_book_0.ask_prices[i]}; volume: {volume}")
                    return

                # Falls nur ein Teil der benötigten Volume vorhanden ist
                elif volume > remaining_volume:
                    volume -= remaining_volume
                    self.send_hedge_order(id, Side.BUY, self.last_order_book_0.ask_prices[i], volume)
                    self.logger.info(f"SEND FUTURE ORDER; side {Side.BUY}; price {self.last_order_book_0.ask_prices[i]}; volume: {volume}")


    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        # self.logger.info(f"Order {client_order_id} status update; volume filled {fill_volume}; volume remaining {remaining_volume}; fees {fees}; Future position {self.position_0}; ETF position {self.position_1}")
        if remaining_volume == 0:
            # Könnte entweder ein Bid oder Ask sein, die IDs sind sowieso unique
            self.bids_1.discard(client_order_id)
            self.asks_1.discard(client_order_id)


    # def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
    #                            ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
    #     self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
    #                      sequence_number)
