# From Readme.md:
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Memory limit: 2GB
# Total disk usage limit: 100MB (including the log file)
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet

import asyncio
import itertools
from typing import List
from ready_trader_go import BaseAutoTrader, Lifespan, Side


class AutoTrader(BaseAutoTrader):

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.etf_asks: List[int] = []
        self.etf_bids: List[int] = []
        self.etf_ask_vols: List[int] = []
        self.etf_bid_vols: List[int] = []
        self.future_asks: List[int] = []
        self.future_bids: List[int] = []
        self.future_ask_vols: List[int] = []
        self.future_bid_vols: List[int] = []
        self.last_sequence_number: int = 0
        # Anzahl an Lots die wir besitzen
        self.position_0: int = 0
        self.position_1: int = 0
        # Aktive ETF Orders
        self.bids_1 = set()
        self.asks_1 = set()
        # Aktive Future Orders
        self.bids_0 = set()
        self.asks_0 = set()
        # Traded Volumes für jede Order ID
        self.etf_order_id_to_volume_map = dict()
        self.future_order_id_to_volume_map = dict()
        # Iterator um eine unique Order ID zu erzeugen
        self.id_iter = itertools.count(start=0, step=1)
        # Anzahl der Versuche eine Order erneut zu senden
        self.retries_0 = 0
        self.retries_1 = 0


    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info(f"{sequence_number} ORDER BOOK {instrument}; BIDS: {bid_prices}; BID VOLS: {bid_volumes}; ASKS: {ask_prices}; ASK VOLS: {ask_volumes}")

        # Order Book speichern
        if instrument == 0:
            self.future_asks: List[int] = list(ask_prices)
            self.future_bids: List[int] = list(bid_prices)
            self.future_ask_vols: List[int] = list(ask_volumes)
            self.future_bid_vols: List[int] = list(bid_volumes)
        elif instrument == 1:
            self.etf_asks: List[int] = list(ask_prices)
            self.etf_bids: List[int] = list(bid_prices)
            self.etf_ask_vols: List[int] = list(ask_volumes)
            self.etf_bid_vols: List[int] = list(bid_volumes)

        # Wir warten bis beide Order Books updated wurden
        if self.last_sequence_number != sequence_number:
            self.last_sequence_number = sequence_number
            return

        # Wir brechen ab, wenn eins der Orderbooks keine Preise enthält (z.B. am Anfang der Runde)
        if self.etf_asks[0] == 0 or self.future_asks[0] == 0:
            return

        sent_volume: int = 0
        for i in range(5):
            for j in range(5):

                # Long ETF and Short Future
                # Falls es profitabel ist inkl. gebührenfaktor
                if self.etf_asks[i]*1.0002 < self.future_bids[j] and self.etf_asks[i] != 0 and self.future_bids[j] != 0:

                    # Mögliches profitables Volume
                    volume: int = min(self.etf_ask_vols[i], self.future_bid_vols[j])

                    # Das noch handelbare Volume, bis wir das Limit erreicht haben (100 Stück)
                    tradable_vol: int = 100 - (self.position_1 + sent_volume)

                    if tradable_vol == 0:
                        return

                    # Order ID zu laufenden Bids hinzufügen
                    id1 = next(self.id_iter)
                    self.bids_1.add(id1)
                    id0 = next(self.id_iter)
                    self.asks_0.add(id0)

                    # Falls genügend Volume vorhanden ist, traden wir soviel wir können und brechen dann ab
                    if volume >= tradable_vol:
                        self.etf_order_id_to_volume_map[id1] = tradable_vol
                        self.logger.info(f"SEND ETF ORDER {id1}; side {Side.BUY}; price {self.etf_asks[i]}; volume: {tradable_vol}")
                        self.send_insert_order(id1, Side.BUY, self.etf_asks[i], tradable_vol, Lifespan.IMMEDIATE_OR_CANCEL)

                        # Hedge Orders erstellen
                        # self.future_order_id_to_volume_map[id0] = tradable_vol
                        # self.logger.info(f"SEND FUTURE ORDER {id0}; side {Side.SELL}; price {self.future_bids[j]}; volume: {tradable_vol}")
                        # self.send_hedge_order(id0, Side.SELL, self.future_bids[j], tradable_vol)
                        return

                    # Sonst: Order nach maximal viel profitablem Volumen ausführen
                    else:
                        sent_volume += volume
                        self.etf_ask_vols[i] -= volume
                        self.future_bid_vols[j] -= volume
                        self.etf_order_id_to_volume_map[id1] = volume
                        self.send_insert_order(id1, Side.BUY, self.etf_asks[i], volume, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER {id1}; side {Side.BUY}; price {self.etf_asks[i]}; volume: {volume}")

                        # self.future_order_id_to_volume_map[id0] = volume
                        # self.logger.info(f"SEND FUTURE ORDER {id0}; side {Side.SELL}; price {self.future_bids[j]}; volume: {volume}")
                        # self.send_hedge_order(id0, Side.SELL, self.future_bids[j], volume)

                        if self.etf_ask_vols[i] == 0:
                            break

                # Nun: Short ETF und Long Future
                elif self.etf_bids[i] > self.future_asks[j]*1.0002 and self.etf_bids[i] != 0 and self.future_asks[j] != 0:

                    # Mögliches profitables Volume
                    volume = min(self.etf_bid_vols[i], self.future_ask_vols[j])

                    # Das noch handelbare Volume, bis wir das Limit erreicht haben (-100 Stück)
                    tradable_vol = 100+self.position_1-sent_volume

                    if tradable_vol == 0:
                        return

                    # Order ID zu laufenden Asks hinzufügen
                    id1 = next(self.id_iter)
                    self.asks_1.add(id1)
                    id0 = next(self.id_iter)
                    self.bids_0.add(id0)

                    if volume >= tradable_vol:
                        self.etf_order_id_to_volume_map[id1] = tradable_vol
                        self.send_insert_order(id1, Side.SELL, self.etf_bids[i], tradable_vol, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER {id1}; side {Side.SELL}; price {self.etf_bids[i]}; volume: {tradable_vol}")

                        # self.future_order_id_to_volume_map[id0] = tradable_vol
                        # self.logger.info(f"SEND FUTURE ORDER {id0}; side {Side.BUY}; price {self.future_asks[j]}; volume: {tradable_vol}")
                        # self.send_hedge_order(id0, Side.BUY, self.future_asks[j], tradable_vol)
                        return

                    # Sonst: Order nach profitablem Volumen ausführen
                    else:
                        sent_volume += volume
                        self.etf_bid_vols[i] -= volume
                        self.future_ask_vols[j] -= volume
                        self.etf_order_id_to_volume_map[id1] = volume
                        self.send_insert_order(id1, Side.SELL, self.etf_bids[i], volume, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER {id1}; side {Side.SELL}; price {self.etf_bids[i]}; volume: {volume}")

                        # self.future_order_id_to_volume_map[id0] = volume
                        # self.logger.info(f"SEND FUTURE ORDER {id0}; side {Side.BUY}; price {self.future_asks[j]}; volume: {volume}")
                        # self.send_hedge_order(id0, Side.BUY, self.future_asks[j], volume)

                        if self.etf_bid_vols[i] == 0:
                            break




    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        self.logger.warning(f"Error with order {client_order_id}: {error_message.decode()}")


    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        volume_to_hedge = volume

        # Falls wir ETF gekauft haben
        if client_order_id in self.bids_1:
            self.position_1 += volume
            self.logger.info(f"ETF Order {client_order_id} filled; volume filled {volume}; at price {price}; ETF position {self.position_1}")

            # Future verkaufen
            for i in range(5):

                available_volume = self.future_bid_vols[i]
                id = next(self.id_iter)
                self.asks_0.add(id)

                # Falls genügend Volume vorhanden ist
                if volume_to_hedge <= available_volume:
                    self.future_order_id_to_volume_map[id] = volume_to_hedge
                    self.logger.info(f"SEND FUTURE ORDER {id}; side {Side.SELL}; price {self.future_bids[i]}; volume: {volume_to_hedge}")
                    self.send_hedge_order(id, Side.SELL, self.future_bids[i], volume_to_hedge)
                    break

                # Falls nur ein Teil der benötigten Volume vorhanden ist
                else:
                    volume_to_hedge -= available_volume
                    self.future_order_id_to_volume_map[id] = available_volume
                    self.logger.info(f"SEND FUTURE ORDER {id}; side {Side.SELL}; price {self.future_bids[i]}; volume: {available_volume}")
                    self.send_hedge_order(id, Side.SELL, self.future_bids[i], available_volume)


        # Falls wir ETF verkauft haben
        elif client_order_id in self.asks_1:
            self.position_1 -= volume
            self.logger.info(f"ETF Order {client_order_id} filled; volume filled {volume}; at price {price}; ETF position {self.position_1}")

            # Future kaufen
            for i in range(5):

                available_volume = self.future_ask_vols[i]
                id = next(self.id_iter)
                self.bids_0.add(id)

                # Falls genügend Volume vorhanden ist
                if volume_to_hedge <= available_volume:
                    self.future_order_id_to_volume_map[id] = volume_to_hedge
                    self.logger.info(f"SEND FUTURE ORDER {id}; side {Side.BUY}; price {self.future_asks[i]}; volume: {volume_to_hedge}")
                    self.send_hedge_order(id, Side.BUY, self.future_asks[i], volume_to_hedge)
                    break

                # Falls nur ein Teil der benötigten Volume vorhanden ist
                else:
                    volume_to_hedge -= available_volume
                    self.future_order_id_to_volume_map[id] = available_volume
                    self.logger.info(f"SEND FUTURE ORDER {id}; side {Side.BUY}; price {self.future_asks[i]}; volume: {available_volume}")
                    self.send_hedge_order(id, Side.BUY, self.future_asks[i], available_volume)


        # Falls ETF Order nicht komplett gefillt wurde
        if self.etf_order_id_to_volume_map[client_order_id] > volume:

            missed_volume = self.etf_order_id_to_volume_map[client_order_id] - volume
            id = next(self.id_iter)

            # Falls die ETF Order ein Buy war
            if client_order_id in self.bids_1:
                # ETF nachkaufen
                self.bids_1.add(id)
                for i in range(self.retries_1, 5):
                    if self.etf_ask_vols[i] > missed_volume:
                        self.etf_order_id_to_volume_map[id] = missed_volume
                        self.send_insert_order(id, Side.BUY, self.etf_asks[i], missed_volume, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER {id}; side {Side.BUY}; price {self.etf_asks[i]}; volume: {missed_volume}")

            # Falls die ETF Order ein Sell war
            elif client_order_id in self.asks_1:
                # ETF verkaufen
                self.asks_1.add(id)
                for i in range(self.retries_1, 5):
                    if self.etf_bid_vols[i] > missed_volume:
                        self.etf_order_id_to_volume_map[id] = missed_volume
                        self.send_insert_order(id, Side.SELL, self.etf_bids[i], missed_volume, Lifespan.IMMEDIATE_OR_CANCEL)
                        self.logger.info(f"SEND ETF ORDER {id}; side {Side.SELL}; price {self.etf_bids[i]}; volume: {missed_volume}")

            self.retries_1 = min(self.retries_1 + 1, 4)
        else:
            self.retries_1 = 0


    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        if client_order_id in self.bids_0:
            self.position_0 += volume
        elif client_order_id in self.asks_0:
            self.position_0 -= volume
        self.logger.info(f"Future Order {client_order_id} filled; volume filled {volume}; at price {price}; Future position {self.position_0}")

        # Falls die Future Order nicht komplett gefillt wurde
        if self.future_order_id_to_volume_map[client_order_id] > volume:

            missed_volume = self.future_order_id_to_volume_map[client_order_id] - volume
            id = next(self.id_iter)

            # Falls die Future Order ein Buy war
            if client_order_id in self.bids_0:
                # Future nachkaufen
                self.bids_0.add(id)
                for i in range(self.retries_0, 5):
                    if self.future_ask_vols[i] > missed_volume:
                        self.future_order_id_to_volume_map[id] = missed_volume
                        self.logger.info(f"SEND FUTURE ORDER {id}; side {Side.BUY}; price {self.future_asks[i]}; volume: {missed_volume}")
                        self.send_hedge_order(id, Side.BUY, self.future_asks[i], missed_volume)
                        self.retries_0 = min(self.retries_0 + 1, 4)
                        return

            # Falls die Future Order ein Sell war
            elif client_order_id in self.asks_0:
                # Future verkaufen
                self.asks_0.add(id)
                for i in range(self.retries_0, 5):
                    if self.future_bid_vols[i] > missed_volume:
                        self.future_order_id_to_volume_map[id] = missed_volume
                        self.logger.info(f"SEND FUTURE ORDER {id}; side {Side.SELL}; price {self.future_bids[i]}; volume: {missed_volume}")
                        self.send_hedge_order(id, Side.SELL, self.future_bids[i], missed_volume)
                        self.retries_0 = min(self.retries_0 + 1, 4)
                        return

        else:
            self.retries_0 = 0
