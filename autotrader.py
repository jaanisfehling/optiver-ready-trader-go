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

    def get_asset_price(self) -> float or None:
        """
        We calculate the Asset Price based on the best 800 lots of both ask and bid prices.
        We add up the best 800 available lots of both ask and bid, and then divide by 800 * 2.
        800 comes from 8 contenders, that possibly want to buy 100 lots each.
        This makes sure when we trade, there will be lots left that are probably (!) worth it.
        """
        covered_ask_volume = 0
        covered_bid_volume = 0
        price = 0

        for i in range(len(self.ask_prices)):
            # Solange wir noch Volume auffüllen müssen
            if covered_ask_volume < 800:
                # Falls mehr Volume da ist, als wir noch brauchen
                if self.ask_volumes[i] > 800-covered_ask_volume:
                    volume = 800-covered_ask_volume
                # Ansonsten gesamt verfügbares Volume benutzen
                else:
                    volume = self.ask_volumes[i]
                price += volume * self.ask_prices[i]
                covered_ask_volume += volume

            if covered_bid_volume < 800:
                # Falls mehr Volume da ist, als wir noch brauchen
                if self.bid_volumes[i] > 800-covered_bid_volume:
                    volume = 800-covered_bid_volume
                # Ansonsten gesamt verfügbares Volume benutzen
                else:
                    volume = self.bid_volumes[i]
                price += volume * self.bid_prices[i]
                covered_bid_volume += volume

        # Wir teilen durch das tatsächlich gefundene Volume
        # Kann einen Unterschied bei iliquiden Orderbooks machen
        try:
            return price / (covered_ask_volume + covered_bid_volume)
        except ZeroDivisionError:
            return None


class AutoTrader(BaseAutoTrader):

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        # Letzte Order Books der beiden Instrumente
        self.last_order_book_0: OrderBook = OrderBook(-1, -1, [], [], [], [])
        self.last_order_book_1: OrderBook = OrderBook(-2, -2, [], [], [], [])
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

    def send_orders(self, instrument: int, order_book: OrderBook, side: Side, volume: int):
        if side == Side.BUY:
            prices = order_book.ask_prices
            volumes = order_book.ask_volumes
        elif side == Side.SELL:
            prices = order_book.bid_prices
            volumes = order_book.bid_volumes
        for i in range(len(prices)):
            if volume > 0:
                id = next(self.id_iter)
                if instrument == 1:
                    if volume <= volumes[i]:
                        vol = volume
                        volume = 0
                    elif volume > volumes[i]:
                        vol = volumes[i]
                        volume -= vol
                    self.send_insert_order(id, side, prices[i], vol, Lifespan.IMMEDIATE_OR_CANCEL)
                    self.logger.info(f"ETF {side}; price {prices[i]}; volume: {vol}")
                    if side == Side.SELL:
                        self.asks.add(id)
                    elif side == Side.BUY:
                        self.bids.add(id)
                elif instrument == 0:
                    if volume <= volumes[i]:
                        vol = volume
                        volume = 0
                    elif volume > volumes[i]:
                        vol = volumes[i]
                        volume -= vol
                    self.send_hedge_order(id, side, prices[i], vol)
                    self.logger.info(f"FUTURE {side}; price {prices[i]}; volume: {vol}")
                    if side == Side.SELL:
                        self.asks.add(id)
                    elif side == Side.BUY:
                        self.bids.add(id)
            else:
                break

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

        # aktuelle Assetpreise berechnen
        price_0: float = self.last_order_book_0.get_asset_price()
        price_1: float = self.last_order_book_1.get_asset_price()

        # Wir brechen ab, wenn eins der Orderbooks keine Preise enthält (z.B. am Anfang der Runde)
        if price_0 is None or price_1 is None:
            return

        # Spread zwischen Assets berechnen
        spread = price_0 - price_1

        # Spread der Spreads-Liste hinzfügen
        self.spreads.append(spread)

        # Spread zwischen 0 und 1 normalisieren
        try:
            spread_norm = (spread - min(self.spreads)) / (max(self.spreads) - min(self.spreads))
        except ZeroDivisionError:
            # Falls ein normalisierter Spread nicht existiert (Anfang der Runde), handeln wir nicht
            spread_norm = 0

        self.logger.info(
            {"Price 0": price_0, "Price 1": price_1, "spread": spread, "spread_norm": spread_norm})

        # Zu besitztende Instrumentenmenge mit Spread berechnen
        target_amount: int = round(100 * spread_norm)

        # Sell 0 (Future), Buy 1 (ETF)
        # spread = price_0 - price_1
        if spread > 0:

            # Falls wir zu viel ETF besitzen
            if target_amount < self.position_1:
                # ETF verkaufen
                self.send_orders(1, self.last_order_book_1, Side.SELL, -target_amount+self.position_1)

            # Falls zu wenig ETF
            elif target_amount > self.position_1:
                # ETF kaufen
                self.send_orders(1, self.last_order_book_1, Side.BUY, target_amount-self.position_1)


        # Buy 0 (Future), Sell 1 (ETF)
        # spread = price_0 - price_1
        elif spread < 0:

            # Falls wir zu wenig Short im ETF sind
            if -target_amount < self.position_1:
                # ETF verkaufen
                self.send_orders(1, self.last_order_book_1, Side.SELL, target_amount+self.position_1)

            # Falls zu stark short Position ETF
            elif -target_amount > self.position_1:
                # ETF kaufen
                self.send_orders(1, self.last_order_book_1, Side.BUY, -self.position_1-target_amount)


    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        self.logger.warning(f"Error with order {client_order_id}: {error_message.decode()}")

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info(f"Future Order {client_order_id} filled; volume filled {volume}; at price {price}; Future position {self.position_0}")
        if client_order_id in self.bids:
            self.position_0 += volume
        elif client_order_id in self.asks:
            self.position_0 -= volume

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:

        # Falls wir ETF gekauft haben, müssen wir Future verkaufen
        if client_order_id in self.bids:
            self.position_1 += volume

            # Wir hedgen unsere gesamte ETF Position
            target_amount = -self.position_1

            # Future verkaufen
            self.send_orders(0, self.last_order_book_0, Side.SELL, self.position_0-target_amount)


        # Falls wir ETF verkauft haben
        elif client_order_id in self.asks:
            self.position_1 -= volume

            # Wir hedgen unsere gesamte ETF Position
            target_amount = -self.position_1

            # Future kaufen
            self.send_orders(0, self.last_order_book_0, Side.BUY, target_amount-self.position_0)

        self.logger.info(f"ETF Order {client_order_id} filled; volume filled {volume}; at price {price}; ETF position {self.position_1}")


    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        self.logger.info(f"Order {client_order_id} status update; volume filled {fill_volume}; volume remaining {remaining_volume}; fees {fees}; Future position {self.position_0}; ETF position {self.position_1}")
        if remaining_volume == 0:
            # Könnte entweder ein Bid oder Ask sein, die IDs sind sowieso unique
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)


    # def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
    #                            ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
    #     self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
    #                      sequence_number)
