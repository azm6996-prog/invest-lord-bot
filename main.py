# main.py
import os
import logging
import asyncio
from decimal import Decimal
from typing import List, Dict, Any

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import Text
from aiogram.types import ParseMode

# -----------------------------
# Настройка логов
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------
# Переменные окружения
# -----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")  # id таблицы Google Sheets
SERVICE_JSON = os.getenv("SERVICE_JSON_FILE", "service.json")  # имя файла JSON (загрузить в репо/Render Files)

if not BOT_TOKEN or not SHEET_ID:
    logger.error("Не заданы BOT_TOKEN или SHEET_ID в переменных окружения.")
    raise SystemExit("Missing BOT_TOKEN or SHEET_ID environment variables")

# -----------------------------
# Подключение к Google Sheets
# -----------------------------
def connect_gsheet():
    # Подключение через сервисный аккаунт
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SERVICE_JSON, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    return sh

# Образец структуры листа:
# Лист с именем "portfolio" с колонками:
# account,type,symbol,qty,avg_price,currency,notes
EXPECTED_SHEET_NAME = "portfolio"

def sheet_to_dataframe(sh) -> pd.DataFrame:
    try:
        worksheet = sh.worksheet(EXPECTED_SHEET_NAME)
    except Exception as e:
        # Попробуем взять первый лист
        worksheet = sh.sheet1
    rows = worksheet.get_all_records()
    df = pd.DataFrame(rows)
    # Приводим нужные колонки
    if df.empty:
        return df
    # Поля: account,type,symbol,qty,avg_price,currency,notes
    for col in ["qty", "avg_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df

# -----------------------------
# Аналитика портфеля
# -----------------------------
def analyze_portfolio(df: pd.DataFrame) -> Dict[str, Any]:
    """Возвращает простую аналитику: по аккаунтам, по типам, общий итог"""
    result = {}
    if df.empty:
        return {"total": 0, "by_account": {}, "by_type": {}, "rows": []}

    # Расчёт текущей стоимости позиции: qty * avg_price
    df["value"] = df["qty"].fillna(0) * df["avg_price"].fillna(0)
    total = float(df["value"].sum())

    # По аккаунтам
    by_account = df.groupby("account")["value"].sum().to_dict()
    # По типу (bond/stock/other)
    by_type = df.groupby("type")["value"].sum().to_dict()

    # Доли в процентах
    by_account_pct = {k: (float(v)/total*100 if total else 0) for k, v in by_account.items()}
    by_type_pct = {k: (float(v)/total*100 if total else 0) for k, v in by_type.items()}

    result = {
        "total": total,
        "by_account": by_account,
        "by_account_pct": by_account_pct,
        "by_type": by_type,
        "by_type_pct": by_type_pct,
        "rows": df.to_dict(orient="records"),
    }
    return result

# -----------------------------
# Aiogram bot
# -----------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# -- /start
@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    text = (
        "Привет, Бро! Я — твой InvestLord бот.\n\n"
        "Доступные команды:\n"
        "/портфель — показать текущий портфель\n"
        "/анализ — краткий анализ и распределение\n"
        "/строка — вывод строк (только тест)\n"
        "/help — помощь\n\n"
        "Также доступны быстрые кнопки в будущем."
    )
    await message.answer(text)

# -- /портфель
@dp.message_handler(commands=["портфель"])
async def cmd_portfolio(message: types.Message):
    try:
        sh = connect_gsheet()
        df = sheet_to_dataframe(sh)
    except Exception as e:
        logger.exception("Ошибка чтения Google Sheets")
        await message.answer("Ошибка доступа к Google Sheets: " + str(e))
        return

    if df.empty:
        await message.answer("Портфель пуст или неверная структура листа.")
        return

    text_lines = ["📊 *Твой портфель*:\n"]
    total = 0.0
    for _, row in df.iterrows():
        symbol = row.get("symbol", "")
        qty = row.get("qty", 0)
        avg = row.get("avg_price", 0)
        value = float(qty) * float(avg)
        total += value
        text_lines.append(f"{symbol} — qty: {qty}, price: {avg} ₽, value: {value:,.2f} ₽")
    text_lines.append("\n💵 *Итого:* {:.2f} ₽".format(total))
    await message.answer("\n".join(text_lines), parse_mode=ParseMode.MARKDOWN)

# -- /анализ
@dp.message_handler(commands=["анализ"])
async def cmd_analysis(message: types.Message):
    try:
        sh = connect_gsheet()
        df = sheet_to_dataframe(sh)
    except Exception as e:
        await message.answer("Ошибка доступа к Google Sheets: " + str(e))
        return

    analysis = analyze_portfolio(df)
    if analysis["total"] == 0:
        await message.answer("Портфель пуст или нулевой.")
        return

    lines = [f"📈 *Анализ портфеля* — Итого: {analysis['total']:,.2f} ₽\n"]
    lines.append("— По счетам:")
    for acc, val in analysis["by_account"].items():
        pct = analysis["by_account_pct"].get(acc, 0)
        lines.append(f"{acc}: {val:,.2f} ₽ ({pct:.1f}%)")
    lines.append("\n— По типам:")
    for t, val in analysis["by_type"].items():
        pct = analysis["by_type_pct"].get(t, 0)
        lines.append(f"{t}: {val:,.2f} ₽ ({pct:.1f}%)")

    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

# -- /строка (печать таблицы — для теста)
@dp.message_handler(commands=["строка"])
async def cmd_rows(message: types.Message):
    try:
        sh = connect_gsheet()
        df = sheet_to_dataframe(sh)
    except Exception as e:
        await message.answer("Ошибка доступа к Google Sheets: " + str(e))
        return

    if df.empty:
        await message.answer("Пусто")
        return
    # Покажем первые 10 строк кратко
    text = "Первые строки:\n"
    for i, r in enumerate(df.to_dict(orient="records")[:10], 1):
        text += f"{i}. {r.get('symbol','')} — {r.get('qty',0)} × {r.get('avg_price',0)} = {float(r.get('qty',0))*float(r.get('avg_price',0)):,.2f} ₽\n"
    await message.answer(text)

# -----------------------------
# Функция периодического обновления (placeholder)
# -----------------------------
async def periodic_task():
    while True:
        try:
            sh = connect_gsheet()
            df = sheet_to_dataframe(sh)
            # Сейчас просто логируем общий итог. Позже можно подгружать котировки.
            analysis = analyze_portfolio(df)
            logger.info("Periodic update — total: %s", analysis.get("total"))
        except Exception as e:
            logger.exception("Periodic task error")
        await asyncio.sleep(60 * 30)  # каждые 30 минут

# -----------------------------
# on_startup
# -----------------------------
async def on_startup(dp):
    asyncio.create_task(periodic_task())
    logger.info("Bot started and periodic task created.")

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
