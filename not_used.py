def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
    super().__init__(loop, team_name, secret)
    # Letzte Order Books der beiden Instrumente
    self.last_order_book_0: OrderBook = OrderBook(-1, -1, [], [], [], [])
    self.last_order_book_1: OrderBook = OrderBook(-2, -2, [], [], [], [])
    # Liste aller Asset Preise
    self.asset_prices_0: List[float] = []
    self.asset_prices_1: List[float] = []
    # Iterator um eine unique Order ID zu erzeugen
    self.order_id_iterator = itertools.count(start=1, step=1)
    # Zuletzt benutzte Order ID jedes Instruments
    self.order_id_0: int = 0
    self.order_id_1: int = 0
    # True, Falls ein Hedge Order aktiv ist
    self.is_hedge_active = False
    # Z-Scores der letzten t Order Book Updates
    # z_scores: list[float] = []


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
    price_0: float = self.asset_prices_0[-1]
    price_1: float = self.asset_prices_1[-1]

    # Letzte t Assetpreise
    t = 50
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
        return None

    # Log Spread Funktion
    spread = math.log(price_0) - ratio_avg * math.log(price_1)

    # Z-Score
    z_score = spread / ratio_std

    # Z-Score Array updaten
    # self.z_scores.append(z_score)

    self.logger.info({"Prices 0": self.asset_prices_0[-3:], "Prices 1": self.asset_prices_1[-3:],
                      "Standardabweichung": ratio_std,
                      "Z-Score": z_score,
                      "Spread": spread,
                      "Durschnittsquotient geglättet": ratio_avg})

    # Zu kaufende Instrumentenmenge mit Hedge Ratio berechnen
    amount_1 = round(ACTIVE_VOLUME_LIMIT / (ratio_avg + 1))
    amount_0 = round(amount_1 * ratio_avg)

    # Falls Z-Score den Upper Threshold (2-sigma) überschreitet
    # Sell 0, Buy 1
    if z_score > 2 * ratio_std:
        # Alte ETF Orders Canceln
        self.send_cancel_order(self.order_id_1)

        # Asset 0 verkaufen
        # Nächste freie Order ID nehmen
        self.order_id_0 = next(self.order_id_iterator)
        # Aktuell nehmen wir einfach den höchsten Bid Preis (ohne Rücksicht auf Volume)
        best_current_bid_0 = self.last_order_book_0.bid_prices[0]
        # Order erstellen
        if not self.is_hedge_active:
            self.send_hedge_order(self.order_id_0, Side.SELL, best_current_bid_0, amount_0)
            self.is_hedge_active = True

        # Asset 1 kaufen
        # Nächste freie Order ID nehmen
        self.order_id_1 = next(self.order_id_iterator)
        # Aktuell nehmen wir einfach den niedrigsten Ask Preis (ohne Rücksicht auf Volume)
        best_current_ask_1 = self.last_order_book_1.ask_prices[0]
        self.send_insert_order(self.order_id_1, Side.BUY, best_current_ask_1, amount_1,
                               Lifespan.IMMEDIATE_OR_CANCEL)

    # Falls Z-Score den Lower Threshold (2-sigma) unterschreitet
    # Buy 0, Sell 1
    elif z_score < 1.5 * ratio_std:
        # Alte ETF Orders Canceln
        self.send_cancel_order(self.order_id_1)

        # Asset 0 kaufen
        # Nächste freie Order ID nehmen
        self.order_id_0 = next(self.order_id_iterator)
        # Aktuell nehmen wir einfach den niedrigsten Ask Preis (ohne Rücksicht auf Volume)
        best_current_ask_0 = self.last_order_book_0.ask_prices[0]
        # Order erstellen
        if not self.is_hedge_active:
            self.send_hedge_order(self.order_id_0, Side.BUY, best_current_ask_0, amount_0)
            self.is_hedge_active = True

        # Asset 1 verkaufen
        # Nächste freie Order ID nehmen
        self.order_id_1 = next(self.order_id_iterator)
        # Aktuell nehmen wir einfach den höchsten Bid Preis (ohne Rücksicht auf Volume)
        best_current_bid_1 = self.last_order_book_1.bid_prices[0]
        self.send_insert_order(self.order_id_1, Side.SELL, best_current_bid_1, amount_1,
                               Lifespan.IMMEDIATE_OR_CANCEL)