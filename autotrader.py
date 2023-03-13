# Zu beachten (aus Readme.md):
# Available libraries: numpy 1.24.2; pandas 1.5.3; scipy 1.10.1
# Memory limit: 2GB
# Total disk usage limit: 100MB (including the log file)
# Maximum number of autotraders per match: 8
# Autotraders may not create sub-processes but may have multiple threads
# Autotraders may not access the internet

import asyncio
import itertools
import math
from dataclasses import dataclass
from typing import List, Dict

import numpy as np

from ready_trader_go import BaseAutoTrader, Lifespan, Side

MAKER_FEE = -0.0001
TAKER_FEE = 0.0002
FEE = TAKER_FEE + MAKER_FEE
ACTIVE_VOLUME_LIMIT = 200


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


@dataclass
class Order:
    id: int
    side: Side
    price: int
    volume: int
    lifespan: Lifespan


class Trader:
    # Letzte Order Books der beiden Instrumente
    last_order_book_0: OrderBook
    last_order_book_1: OrderBook
    # Liste aller Asset Preise
    asset_prices_0: List[float]
    asset_prices_1: List[float]
    # Zuletzt benutzte Order ID jedes Instruments
    order_id_0: int
    order_id_1: int

    # Z-Scores der letzten t Order Book Updates
    # z_scores: list[float]

    def __init__(self):
        self.asset_prices_0 = []
        self.asset_prices_1 = []
        # Iterator um eine unique Order ID zu erzeugen
        self.order_id_iterator = itertools.count(start=1, step=1)
        self.order_id_0 = 0
        self.order_id_1 = 0
        # self.z_scores = []

    def update_orders(self, order_book: OrderBook) -> Dict[str, List[int or Order]]:

        if order_book.instrument == 0:
            # Order Book speichern
            self.last_order_book_0 = order_book
            # Asset Preis der Liste hinzufügen
            if order_book.get_asset_price() is not None:
                self.asset_prices_0.append(order_book.get_asset_price())
            else:
                return {"cancel": [], "send": []}
            # Falls das andere Instrument noch keine Updates erhalten hat, werden wir nichts tun
            if not self.asset_prices_1:
                return {"cancel": [], "send": []}
            # Letzten Asset Preis des anderen Instruments kopieren und der anderen Liste hinzufügen,
            # um keine Abweichungen in der Array-Division zu erhalten
            self.asset_prices_1.append(self.asset_prices_1[-1])
        # Das Gleiche im Falle des anderen Instruments
        elif order_book.instrument == 1:
            self.last_order_book_1 = order_book
            if order_book.get_asset_price() is not None:
                self.asset_prices_1.append(order_book.get_asset_price())
            else:
                return {"cancel": [], "send": []}
            if not self.asset_prices_0:
                return {"cancel": [], "send": []}
            self.asset_prices_0.append(self.asset_prices_0[-1])

        # aktuelle Assetpreise
        price_0: float = self.asset_prices_0[-1]
        price_1: float = self.asset_prices_1[-1]

        # Letzte t Assetpreise
        t = 50
        # Falls einer der Listen weniger als t Assetpreise beinhaltet
        if len(self.asset_prices_0) < t or len(self.asset_prices_1) < t:
            # Wählen wir t nach der Länge der kleineren Liste
            t = min(len(self.asset_prices_0), len(self.asset_prices_1))
        asset_price_array_0 = np.array(self.asset_prices_0[-t:])
        asset_price_array_1 = np.array(self.asset_prices_1[-t:])

        # Logarithmus der Assetpreise
        log_prices_0 = np.log(asset_price_array_0)
        log_prices_1 = np.log(asset_price_array_1)

        # Berechne für jeden Log-Asset-Preis den Quotienten (Hedge Ratio)
        ratio_array = log_prices_0 / log_prices_1

        # Nehme den Mittelwert der Quotienten (Average Hedge Ratio)
        ratio_avg = np.mean(ratio_array)

        # Standardabweichung des Hedge Ratio Arrays
        ratio_std = np.std(ratio_array)
        # Falls die Standardabweichung 0 ergibt (typischerweise, wenn die Werte anfangs alle gleich sind)
        # brechen wir ab
        if ratio_std == 0:
            return {"cancel": [], "send": []}

        # Log Spread Funktion
        spread = math.log(price_0) - ratio_avg * math.log(price_1)

        # Z-Score
        z_score = spread / ratio_std

        # Z-Score Array updaten
        # self.z_scores.append(z_score)

        # Finales Order Dictionary
        result_orders: Dict[str, List[int or Order]] = {"cancel": [], "send": []}

        # Zu kaufende Instrumentenmenge mit Hedge Ratio berechnen
        amount_1 = round(ACTIVE_VOLUME_LIMIT / (ratio_avg + 1))
        amount_0 = round(amount_1 * ratio_avg)

        # Falls Z-Score den Upper Threshold (2-sigma) überschreitet
        # Sell 0, Buy 1
        if z_score > 2 * ratio_std:
            # Alte Orders Canceln
            result_orders["cancel"] = [self.order_id_0, self.order_id_1]

            # Asset 0 verkaufen
            # Nächste freie Order ID nehmen
            self.order_id_0 = next(self.order_id_iterator)
            # Aktuell nehmen wir einfach den höchsten Bid Preis (ohne Rücksicht auf Volume)
            best_current_bid_0 = self.last_order_book_0.bid_prices[0]
            # Order erstellen
            result_orders["send"].append(
                Order(self.order_id_0, Side.SELL, best_current_bid_0, amount_0, Lifespan.LIMIT_ORDER))

            # Asset 1 kaufen
            # Nächste freie Order ID nehmen
            self.order_id_1 = next(self.order_id_iterator)
            # Aktuell nehmen wir einfach den niedrigsten Ask Preis (ohne Rücksicht auf Volume)
            best_current_ask_1 = self.last_order_book_1.ask_prices[0]
            result_orders["send"].append(
                Order(self.order_id_1, Side.BUY, best_current_ask_1, amount_1, Lifespan.LIMIT_ORDER))

        # Falls Z-Score den Lower Threshold (2-sigma) unterschreitet
        # Buy 0, Sell 1
        elif z_score < 2 * ratio_std:
            # Alte Orders Canceln
            result_orders["cancel"] = [self.order_id_0, self.order_id_1]

            # Asset 0 kaufen
            # Nächste freie Order ID nehmen
            self.order_id_0 = next(self.order_id_iterator)
            # Aktuell nehmen wir einfach den niedrigsten Ask Preis (ohne Rücksicht auf Volume)
            best_current_ask_0 = self.last_order_book_0.ask_prices[0]
            # Order erstellen
            result_orders["send"].append(
                Order(self.order_id_0, Side.BUY, best_current_ask_0, amount_0, Lifespan.LIMIT_ORDER))

            # Asset 1 verkaufen
            # Nächste freie Order ID nehmen
            self.order_id_1 = next(self.order_id_iterator)
            # Aktuell nehmen wir einfach den höchsten Bid Preis (ohne Rücksicht auf Volume)
            best_current_bid_1 = self.last_order_book_1.bid_prices[0]
            result_orders["send"].append(
                Order(self.order_id_1, Side.SELL, best_current_bid_1, amount_1, Lifespan.LIMIT_ORDER))

        return result_orders


class AutoTrader(BaseAutoTrader):

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        self.trader = Trader()

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        order_book = OrderBook(instrument, sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
        self.logger.info(order_book)
        new_orders = self.trader.update_orders(order_book)
        self.logger.info(new_orders)
        for order_id in new_orders["cancel"]:
            self.send_cancel_order(order_id)
        for order in new_orders["send"]:
            self.send_insert_order(order.id, order.side, order.price, order.volume, order.lifespan)

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id, price,
                         volume)

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)
