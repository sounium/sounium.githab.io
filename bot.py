from datetime import datetime
from enum import Enum
import json
import os
import threading
import time
import ccxt
import pandas as pd
import talib
import logging
import requests
import websocket

symbol = "BTC/JPY"
product_code = "FX_BTC_JPY"
leverage = 1.9

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
DISCORD_USER_ID = os.getenv("DISCORD_USER_ID")


def fetch_balance():
    """残高を取得"""
    return float(base.private_get_getcollateral()["collateral"])


def fetch_closed_orders():
    """全てのポジションを取得"""
    return base.private_get_getpositions(params={"product_code": product_code})


def fetch_open_orders():
    """未約定注文を取得"""
    return base.fetch_open_orders("BTC/JPY:JPY", params={"product_code": product_code})


def calc_amount(price):
    """
    ロット数を計算
    balance * leverage / price
    """
    balance = fetch_balance()
    return "{:.3f}".format(balance * leverage / price)


class Position(Enum):
    """ポジション状態"""
    NONE = 0
    BUY = 1
    SELL = 2


def create_position(side: str, price: float = None, amount: float = 0.0, params={}):
    """注文"""
    params["product_code"] = product_code
    order_type = "limit" if price else "market"
    base.create_order(symbol, order_type, side, amount, price, params)


def notify(text):
    """Discordに通知"""
    requests.post(
        DISCORD_WEBHOOK_URL,
        json={"content": text},
    )


prices = []
timestamp = None
last = 0


def on_message(ws, message):
    """
    歩み値を処理
    
    歩み値を元にローソク足を計算する
    """
    global timestamp
    global ohlcv
    global prices
    global last

    data = json.loads(message)
    for message in data["params"]["message"]:
        new_timestamp = datetime.fromisoformat(
            message["exec_date"].split(".")[0] + "+00:00"
        )
        price = message["price"]

        last = price

        prices.append(price)

        if not timestamp:
            timestamp = new_timestamp
            prices = [price]
            continue

        if (new_timestamp - timestamp).seconds > 60:
            timestamp = new_timestamp
            ohlcv.append([max(prices), min(prices), prices[-1]])
            if len(ohlcv) > 1000:
                ohlcv.pop(0)
            prices = []
            return


def cancel_open_orders():
    """全ての未約定注文をキャンセル"""
    open_orders = fetch_open_orders()
    if len(open_orders) > 0:
        for order in open_orders:
            base.cancel_order(order["id"], symbol, {"product_code": product_code})
        while True:
            if len(fetch_open_orders()) == 0:
                break
            time.sleep(1)


def cancel_closed_orders():
    """全てのポジションを解消"""
    buy = 0.0
    sell = 0.0

    for order in fetch_closed_orders():
        size = float(order["size"])
        if order["side"] == "BUY":
            sell += size
        else:
            buy += size

    if buy >= 0.01:
        create_position("buy", amount="{:.8f}".format(buy))
    if sell >= 0.01:
        create_position("sell", amount="{:.8f}".format(sell))

    if buy >= 0.01 or sell >= 0.01:
        while True:
            if len(fetch_closed_orders()) == 0:
                break
            time.sleep(1)


def cancel_all_orders():
    """全ての未約定注文及びポジションを解消"""
    cancel_open_orders()
    cancel_closed_orders()


def on_open(ws):
    """WebSocketが疎通したら歩み値の購読を開始"""
    ws.send(
        json.dumps(
            {
                "method": "subscribe",
                "params": {"channel": "lightning_executions_FX_BTC_JPY"},
            }
        )
    )


logging.basicConfig(
    level=logging.INFO, format="[%(levelname)s][%(asctime)s] %(message)s"
)

base = ccxt.bitflyer(
    {
        "apiKey": API_KEY,
        "secret": API_SECRET,
    }
)

position = Position.NONE
entry_price = 0
amount = 0
ohlcv = []


def process():
    """
    ローソク足を処理
    1. ローソク足からパラボリックSARを計算
    2. パラボリックSARが価格より下ならbuy、そうでないならsell
    """
    global position
    global amount
    global entry_price
    global ohlcv

    if len(ohlcv) < 2:
        return

    df = pd.DataFrame(ohlcv, columns=["high", "low", "last"])

    sar = float(talib.SAR(df["high"], df["low"])[-1:])

    if sar < float(df["last"][-1:]):
        if position == Position.BUY:
            return

        if position == Position.SELL:
            cancel_closed_orders()
            position = Position.NONE

        amount = calc_amount(last)
        logging.info("注文中...")
        try:
            create_position("buy", last, amount)
        except Exception as e:
            logging.error(f"注文に失敗しました: {e}")
            return
        logging.info("約定を待機中...")
        count = 0
        while True:
            time.sleep(1)
            count += 1
            if len(fetch_open_orders()) == 0:
                break
            if count == 10:
                logging.warning("約定しなかったため、全注文を解消します。")
                cancel_all_orders()
                return
        logging.info(f"buy {amount} BTC")
        notify(f"buy {amount} BTC")
        position = Position.BUY
    else:
        if position == Position.SELL:
            return

        if position == Position.BUY:
            cancel_closed_orders()
            position = Position.NONE

        amount = calc_amount(last)
        logging.info("注文中...")
        try:
            create_position("sell", last, amount)
        except Exception as e:
            logging.error(f"注文に失敗しました: {e}")
            return
        logging.info("約定を待機中...")
        count = 0
        while True:
            time.sleep(1)
            count += 1
            if len(fetch_open_orders()) == 0:
                break
            if count == 10:
                logging.warning("約定しなかったため、全注文を解消します。")
                cancel_all_orders()
                return
        logging.info(f"sell {amount} BTC")
        notify(f"sell {amount} BTC")
        position = Position.SELL

    entry_price = last


def reconnect_ws():
    """WebSocketを再接続"""
    ws = websocket.WebSocketApp(
        "wss://ws.lightstream.bitflyer.com/json-rpc",
        on_message=on_message,
        on_open=on_open,
    )
    while True:
        try:
            ws.run_forever()
        except Exception as e:
            logging.error(e)
        time.sleep(3)


thread = threading.Thread(target=reconnect_ws, daemon=True)
thread.start()

notify("botが起動しました")

while True:
    try:
        process()
    except Exception as e:
        try:
            notify(f"<@{DISCORD_USER_ID}> エラーが発生しました\n```{e}```")
        except:
            pass

        logging.error(e)

    time.sleep(1)
