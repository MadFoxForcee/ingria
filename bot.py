import argparse
import asyncio
from contextlib import closing
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parent
CATALOG_URL = "https://ingriafarm.com/ru"
INTERVAL = 300
PRODUCTS = {
    "/ru/product/king-usa-top-farm-full3": "КИНГ USA — 12–24+ месяцев, фарм 10–14 сессий",
    "/ru/product/king-ua-top-farm-full2": "КИНГ UA — топ фарм 12–24+ месяцев",
    "/ru/product/king-usa-top-farm-full2": "КИНГ USA — топ фарм 12–24+ месяцев",
    "/ru/product/setup-king-usa-9lichek": "Сетап KING FB USA + 8–9 USA личек",
}


class TelegramError(Exception):
    def __init__(self, status, retry_after=0):
        super().__init__(f"Telegram Bot API ({status})")
        self.status = status
        self.retry_after = retry_after


def telegram_request(token, method, payload, timeout):
    request = Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            result = json.load(response)
    except HTTPError as error:
        with error:
            try:
                parameters = json.load(error).get("parameters", {})
            except (ValueError, OSError):
                parameters = {}
        raise TelegramError(error.code, parameters.get("retry_after", 0)) from None
    except (URLError, OSError, ValueError) as error:
        raise TelegramError(type(error).__name__) from None

    if not result["ok"]:
        raise TelegramError(
            result.get("error_code", "неуспешный ответ"),
            result.get("parameters", {}).get("retry_after", 0),
        )
    return result["result"]


async def telegram(token, method, http_timeout=12, **payload):
    return await asyncio.to_thread(telegram_request, token, method, payload, http_timeout)


def parse_catalog(html):
    catalog = BeautifulSoup(html, "html.parser", from_encoding="utf-8")
    quantities = {}
    for card in catalog.select(".product"):
        link = card.select_one(".product__name[href]")
        badge = card.select_one(".product__side-info .if-badge")
        if link is None or badge is None:
            continue
        product = urlsplit(link["href"]).path.rstrip("/")
        if product not in PRODUCTS:
            continue

        label = badge.get_text(" ", strip=True).casefold()
        if label == "нет в наличии":
            quantities[product] = 0
        else:
            count = re.fullmatch(r"(\d+)\s*шт\.?", label)
            if count:
                quantities[product] = int(count[1])
    return quantities


def fetch_catalog():
    request = Request(CATALOG_URL, headers={"User-Agent": "IngriaFarmStockNotifier/1.0"})
    with urlopen(request, timeout=15) as response:
        return parse_catalog(response.read())


def open_database(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    database = sqlite3.connect(path)
    database.row_factory = sqlite3.Row
    database.executescript("""
        CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS stock (
            product TEXT PRIMARY KEY, quantity INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
            product TEXT, message TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS received_updates (
            update_id INTEGER PRIMARY KEY, received_at REAL NOT NULL
        );
    """)
    return database


def stock_message(product, quantity):
    return (
        f"🟢 Появилось в наличии\n{PRODUCTS[product]}\n"
        f"Количество: {quantity} шт.\n{urljoin(CATALOG_URL, product)}"
    )


def record_stock(database, quantities):
    with database:
        for product, quantity in quantities.items():
            previous = database.execute(
                "SELECT quantity FROM stock WHERE product = ?", (product,)
            ).fetchone()
            database.execute(
                """INSERT INTO stock VALUES (?, ?)
                   ON CONFLICT(product) DO UPDATE SET quantity = excluded.quantity""",
                (product, quantity),
            )
            if quantity == 0:
                database.execute("DELETE FROM outbox WHERE product = ?", (product,))
            elif previous is not None and previous["quantity"] == 0:
                database.execute(
                    "INSERT INTO outbox(chat_id, product, message) SELECT chat_id, ?, ? FROM subscribers",
                    (product, stock_message(product, quantity)),
                )
                logging.info("Появилось в наличии: %s, %d шт.", PRODUCTS[product], quantity)


def record_message(database, update):
    message = update.get("message", {})
    chat = message.get("chat", {})
    if chat.get("type") != "private" or message.get("from", {}).get("is_bot"):
        return
    chat_id = chat["id"]
    words = message.get("text", "").strip().split(maxsplit=1)
    command = words[0].split("@", 1)[0].lower() if words else ""
    now = time.time()

    with database:
        database.execute("DELETE FROM received_updates WHERE received_at < ?", (now - 86400,))
        saved = database.execute(
            "INSERT OR IGNORE INTO received_updates VALUES (?, ?)", (update["update_id"], now)
        )
        if not saved.rowcount:
            return

        if command == "/stop":
            database.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))
            database.execute("DELETE FROM outbox WHERE chat_id = ?", (chat_id,))
            reply = "Уведомления отключены. Отправьте /start, чтобы подписаться снова."
        else:
            subscribed = database.execute(
                "INSERT OR IGNORE INTO subscribers VALUES (?)", (chat_id,)
            ).rowcount
            if not subscribed and command != "/start":
                return
            reply = "Уведомления включены. Проверяю четыре позиции каждые 5 минут. Отписаться — /stop."

        database.execute("INSERT INTO outbox(chat_id, message) VALUES (?, ?)", (chat_id, reply))


async def monitor(database):
    while True:
        started = time.monotonic()
        try:
            quantities = await asyncio.to_thread(fetch_catalog)
        except OSError:
            pass
        else:
            record_stock(database, quantities)
        await asyncio.sleep(max(0, INTERVAL - (time.monotonic() - started)))


async def receive_updates(database, token):
    offset = None
    while True:
        payload = {"timeout": 25, "allowed_updates": ["message"]}
        if offset is not None:
            payload["offset"] = offset
        try:
            updates = await telegram(token, "getUpdates", http_timeout=35, **payload)
        except TelegramError as error:
            if error.status in (401, 404, 409):
                raise
            logging.info("%s при получении сообщений", error)
            await asyncio.sleep(max(5, error.retry_after))
            continue

        for update in updates:
            record_message(database, update)
            offset = update["update_id"] + 1


async def deliver_messages(database, token):
    while True:
        item = database.execute("SELECT * FROM outbox ORDER BY id LIMIT 1").fetchone()
        if item is None:
            await asyncio.sleep(1)
            continue

        try:
            await telegram(token, "sendMessage", chat_id=item["chat_id"], text=item["message"],
                           link_preview_options={"is_disabled": True})
        except TelegramError as error:
            if error.status in (401, 404):
                raise
            if error.status == 403:
                with database:
                    database.execute("DELETE FROM subscribers WHERE chat_id = ?", (item["chat_id"],))
                    database.execute("DELETE FROM outbox WHERE chat_id = ?", (item["chat_id"],))
            elif error.status == 400:
                with database:
                    database.execute("DELETE FROM outbox WHERE id = ?", (item["id"],))
            else:
                logging.info("%s при отправке сообщения", error)
                await asyncio.sleep(max(5, error.retry_after))
        else:
            with database:
                database.execute("DELETE FROM outbox WHERE id = ?", (item["id"],))
            await asyncio.sleep(0.08)


async def run(database_path, token):
    profile = await telegram(token, "getMe")
    logging.info("Бот запущен: https://t.me/%s", profile["username"])
    with closing(open_database(database_path)) as database:
        tasks = [asyncio.create_task(monitor(database)),
                 asyncio.create_task(receive_updates(database, token)),
                 asyncio.create_task(deliver_messages(database, token))]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description="Telegram-бот для четырёх позиций IngriaFarm")
    parser.add_argument("--token-file", type=Path, default=ROOT / ".telegram-token")
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "stock.sqlite3")
    args = parser.parse_args()

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token and args.token_file.is_file():
        token = args.token_file.read_text(encoding="utf-8")
    token = token.strip()
    if not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", token):
        parser.exit(2, "Добавьте токен Telegram в .telegram-token или укажите файл через --token-file.\n")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    logging.Formatter.converter = time.gmtime
    try:
        asyncio.run(run(args.db, token))
    except KeyboardInterrupt:
        pass
    except TelegramError as error:
        parser.exit(1, f"{error}. Проверьте токен и запустите только один экземпляр бота.\n")


if __name__ == "__main__":
    main()
