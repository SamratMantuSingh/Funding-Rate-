import asyncio
import aiohttp
import aiosqlite
import logging
import os
import html
import time
from datetime import datetime, timezone, timedelta
from aiohttp import web

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.getenv("PORT", 8080))
POLL_INTERVAL = 60
MAX_SAFE_LENGTH = 3900
IST = timezone(timedelta(hours=5, minutes=30))

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
DB_PATH = "/tmp/tracker.db"

async def init_db():
    try:
        db = await aiosqlite.connect(DB_PATH, timeout=30.0)
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("SELECT 1 FROM perp_state LIMIT 1")
    except Exception:
        if os.path.exists(DB_PATH): os.remove(DB_PATH)
        db = await aiosqlite.connect(DB_PATH, timeout=30.0)
        await db.execute("PRAGMA journal_mode=WAL")
    
    await db.execute("""CREATE TABLE IF NOT EXISTS perp_state (
        exchange TEXT, symbol TEXT, norm_symbol TEXT, 
        last_interval INTEGER, last_rate REAL, PRIMARY KEY (exchange, symbol))""")
    
    await db.execute("""CREATE TABLE IF NOT EXISTS signal_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, norm_sym TEXT, alert_type TEXT,
        rate REAL, price REAL, timestamp REAL, outcome TEXT, checked INTEGER DEFAULT 0)""")
    
    await db.commit()
    return db

async def get_state(db, exchange, symbol):
    async with db.execute("SELECT last_interval, last_rate FROM perp_state WHERE exchange=? AND symbol=?", (exchange, symbol)) as cursor:
        row = await cursor.fetchone()
        return (row[0], row[1]) if row else (None, None)

async def update_state(db, exchange, symbol, interval, rate):
    norm = symbol.replace("-","").replace("_","").replace("SWAP","").replace("PERP","")
    await db.execute("INSERT INTO perp_state VALUES (?, ?, ?, ?, ?) ON CONFLICT(exchange, symbol) DO UPDATE SET last_interval=excluded.last_interval, last_rate=excluded.last_rate", (exchange, symbol, norm, interval, rate))
    await db.commit()

async def record_signal(db, norm_sym, alert_type, rate, price):
    if price > 0:
        await db.execute("INSERT INTO signal_history (norm_sym, alert_type, rate, price, timestamp) VALUES (?, ?, ?, ?, ?)",
                         (norm_sym, alert_type, rate, price, time.time()))
        await db.commit()

async def get_mtf_data(db, norm_sym, current_rate):
    async with db.execute("SELECT last_rate FROM perp_state WHERE norm_symbol=?", (norm_sym,)) as cursor:
        rows = await cursor.fetchall()
    if len(rows) < 2: return 0, f"0/{len(rows)}"
    
    if current_rate < 0:
        score = sum(1 for r in rows if r[0] < 0)
        direction = "LONG"
    else:
        score = sum(1 for r in rows if r[0] > 0)
        direction = "SHORT"
    
    return score, f"{score}/{len(rows)} {direction}"

async def fetch_price(session, raw_symbol):
    base = raw_symbol.replace("USDT","").replace("BUSD","").replace("USD","").replace("_PERP","").replace("-USDT","").replace("-USD","").replace("SWAP","")
    urls = [
        f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={raw_symbol}",
        f"https://api.binance.com/api/v3/ticker/price?symbol={base}USDT",
    ]
    for url in urls:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = float(data.get("price", 0))
                    if price > 0: return price
        except: pass
    return 0

async def check_outcomes(db, session):
    async with db.execute("SELECT id, norm_sym, alert_type, price, timestamp FROM signal_history WHERE checked=0") as cursor:
        rows = await cursor.fetchall()
    
    for row in rows:
        sig_id, norm_sym, alert_type, old_price, ts = row
        if time.time() - ts < 8 * 3600: continue
        
        base_sym = norm_sym.replace("PERP","")
        if not base_sym.endswith("USDT"): base_sym += "USDT"
        new_price = await fetch_price(session, base_sym)
        if new_price == 0 or old_price == 0: continue
        
        price_change = (new_price - old_price) / old_price
        win = False
        if alert_type in ["LONG_SQUEEZE", "TREND_FLIP_BULL"] and price_change > 0.005: win = True
        elif alert_type in ["SHORT_SETUP", "TREND_FLIP_BEAR"] and price_change < -0.005: win = True
        
        await db.execute("UPDATE signal_history SET outcome=?, checked=1 WHERE id=?", 
                         ("WIN" if win else "LOSS", sig_id))
    await db.commit()

async def send_weekly_report(db, session):
    async with db.execute("SELECT outcome, COUNT(*) FROM signal_history WHERE outcome IS NOT NULL GROUP BY outcome") as cursor:
        rows = await cursor.fetchall()
    
    wins = sum(r[1] for r in rows if r[0] == "WIN")
    losses = sum(r[1] for r in rows if r[0] == "LOSS")
    total = wins + losses
    if total < 5: return
    
    win_rate = (wins / total) * 100
    status = "EXCELLENT" if win_rate > 70 else "GOOD" if win_rate > 50 else "NEEDS IMPROVEMENT"
    msg = (f"📊 <b>WEEKLY PERFORMANCE REPORT</b>\n"
           f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
           f"✅ Wins: <b>{wins}</b> | ❌ Losses: <b>{losses}</b> | Total: <b>{total}</b>\n"
           f"🏆 Win Rate: <b>{win_rate:.1f}%</b>\n\n"
           f"💡 STATUS: <b>{status}</b>")
    queue_alert(msg)

alert_queue = asyncio.Queue()
def queue_alert(msg): alert_queue.put_nowait(msg)

async def send_safe_message(session, text):
    if len(text) <= MAX_SAFE_LENGTH:
        chunks = [text]
    else:
        blocks = text.split('\n\n')
        chunks = []
        current_chunk = ""
        for block in blocks:
            if len(current_chunk) + len(block) + 2 > MAX_SAFE_LENGTH:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                if len(block) > MAX_SAFE_LENGTH:
                    lines = block.split('\n')
                    temp = ""
                    for line in lines:
                        if len(temp) + len(line) + 1 > MAX_SAFE_LENGTH - 50:
                            if temp:
                                chunks.append(temp.strip())
                            temp = line + "\n"
                        else:
                            temp += line + "\n"
                    if temp:
                        chunks.append(temp.strip())
                    current_chunk = ""
                else:
                    current_chunk = block + "\n\n"
            else:
                current_chunk += block + "\n\n"
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        total = len(chunks)
        if total > 1:
            for i in range(total):
                chunks[i] = f"<b>📄 [Part {i+1}/{total}]</b>\n\n" + chunks[i]

    for chunk in chunks:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"}
        try:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 429:
                    await asyncio.sleep(int(resp.headers.get('Retry-After', 5)))
                    await send_safe_message(session, chunk)
                elif resp.status != 200:
                    logging.error(f"Telegram Error {resp.status}")
        except Exception as e:
            logging.error(f"Network error: {e}")
        await asyncio.sleep(1.2)

async def alert_dispatcher(session):
    while True:
        try:
            message = await alert_queue.get()
            await send_safe_message(session, message)
            alert_queue.task_done()
        except Exception as e:
            logging.error(f"Dispatcher error: {e}")
            await asyncio.sleep(5)

async def fetch_json(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            if resp.status == 200: return await resp.json()
    except: pass
    return None

async def fetch_binance_usdt(session):
    info, ticker = await asyncio.gather(fetch_json(session, "https://fapi.binance.com/fapi/v1/exchangeInfo"), fetch_json(session, "https://fapi.binance.com/fapi/v1/premiumIndex"), return_exceptions=True)
    if isinstance(info, Exception) or isinstance(ticker, Exception): return {}
    intervals, rates, res = {}, {}, {}
    if info:
        for s in info.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING": intervals[s["symbol"]] = int(s.get("fundingIntervalHours", 8))
    if ticker:
        for t in ticker: rates[t["symbol"]] = float(t.get("lastFundingRate", 0))
    for sym in intervals:
        if sym in rates: res[sym] = {"interval": intervals[sym], "rate": rates[sym]}
    return res

async def fetch_binance_coinm(session):
    info, ticker = await asyncio.gather(fetch_json(session, "https://dapi.binance.com/dapi/v1/exchangeInfo"), fetch_json(session, "https://dapi.binance.com/dapi/v1/premiumIndex"), return_exceptions=True)
    if isinstance(info, Exception) or isinstance(ticker, Exception): return {}
    intervals, rates, res = {}, {}, {}
    if info:
        for s in info.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING": intervals[s["symbol"]] = int(s.get("fundingIntervalHours", 8))
    if ticker:
        for t in ticker: rates[t["symbol"]] = float(t.get("lastFundingRate", 0))
    for sym in intervals:
        if sym in rates: res[sym] = {"interval": intervals[sym], "rate": rates[sym]}
    return res

async def fetch_bybit(session):
    res = {}
    for cat in ["linear", "inverse"]:
        intervals, rates = {}, {}
        cursor = ""
        while True:
            params = {"category": cat, "limit": 1000}
            if cursor: params["cursor"] = cursor
            data = await fetch_json(session, "https://api.bybit.com/v5/market/instruments-info", params=params)
            if not data or data.get("retCode") != 0: break
            for item in data.get("result", {}).get("list", []):
                if item.get("status") == "Trading": intervals[item["symbol"]] = int(item.get("fundingInterval", 8))
            cursor = data.get("result", {}).get("nextPageCursor", "")
            if not cursor: break
        data = await fetch_json(session, "https://api.bybit.com/v5/market/tickers", params={"category": cat})
        if data and data.get("retCode") == 0:
            for item in data.get("result", {}).get("list", []): rates[item["symbol"]] = float(item.get("fundingRate", 0))
        for sym in intervals:
            if sym in rates: res[sym] = {"interval": intervals[sym], "rate": rates[sym]}
    return res

async def fetch_okx(session):
    intervals, rates, res = {}, {}, {}
    inst, tick = await asyncio.gather(fetch_json(session, "https://www.okx.com/api/v5/public/instruments", params={"instType": "SWAP"}), fetch_json(session, "https://www.okx.com/api/v5/market/tickers", params={"instType": "SWAP"}), return_exceptions=True)
    if isinstance(inst, Exception) or isinstance(tick, Exception): return {}
    if inst and inst.get("code") == "0":
        for i in inst.get("data", []):
            if i.get("state") == "live": intervals[i["instId"]] = int(i.get("fundingInterval", 8))
    if tick and tick.get("code") == "0":
        for t in tick.get("data", []): rates[t["instId"]] = float(t.get("fundingRate", 0))
    for sym in intervals:
        if sym in rates: res[sym] = {"interval": intervals[sym], "rate": rates[sym]}
    return res

async def fetch_bitget(session):
    res = {}
    for pt in ["USDT-FUTURES", "COIN-FUTURES"]:
        intervals, rates = {}, {}
        cont, tick = await asyncio.gather(fetch_json(session, "https://api.bitget.com/api/v2/mix/market/contracts", params={"productType": pt}), fetch_json(session, "https://api.bitget.com/api/v2/mix/market/tickers", params={"productType": pt}), return_exceptions=True)
        if isinstance(cont, Exception) or isinstance(tick, Exception): continue
        if cont and cont.get("code") == "00000":
            for c in cont.get("data", []):
                if c.get("symbolStatus") == "normal": intervals[c["symbol"]] = int(c.get("fundingInterval", 8))
        if tick and tick.get("code") == "00000":
            for t in tick.get("data", []): rates[t["symbol"]] = float(t.get("fundingRate", 0))
        for sym in intervals:
            if sym in rates: res[sym] = {"interval": intervals[sym], "rate": rates[sym]}
    return res

def get_countdown(interval):
    now = datetime.now(timezone.utc)
    next_hour = (now.hour // interval + 1) * interval
    if next_hour >= 24: next_hour = 0
    mins = (next_hour * 60) - (now.hour * 60 + now.minute)
    if mins <= 0: mins += 24 * 60
    return f"{mins // 60}h {mins % 60}m"

def get_star_rating(rate, exchanges_count, mtf_score):
    abs_rate = abs(rate)
    score = 0
    if abs_rate > 0.001: score += 2
    elif abs_rate > 0.0007: score += 1.5
    elif abs_rate > 0.0005: score += 1
    if exchanges_count >= 4: score += 1.5
    elif exchanges_count >= 3: score += 1
    if mtf_score >= 3: score += 1.5
    score = min(int(score), 5)
    return "⭐" * score + "☆" * (5 - score)

def get_expected_move(rate):
    abs_rate = abs(rate)
    if abs_rate > 0.001: return "5-15%"
    elif abs_rate > 0.0007: return "3-8%"
    elif abs_rate > 0.0005: return "2-5%"
    return "1-3%"

def estimate_liq_zone(price, rate):
    if price <= 0: return None
    abs_rate = abs(rate)
    if abs_rate < 0.0007: return None
    pct = min(abs_rate * 100, 5)
    if rate < 0:
        return f"~${price * (1 + pct/100):,.2f}"
    else:
        return f"~${price * (1 - pct/100):,.2f}"

def format_price(price):
    if price <= 0: return "N/A"
    if price < 0.01: return f"${price:.6f}"
    elif price < 1: return f"${price:.4f}"
    elif price < 100: return f"${price:.2f}"
    else: return f"${price:,.2f}"

async def format_consolidated_alerts(session, cycle_alerts, db):
    grouped = {}
    for (norm_sym, alert_type), data_list in cycle_alerts.items():
        if alert_type not in grouped:
            grouped[alert_type] = []
        grouped[alert_type].append((norm_sym, data_list))
    
    for alert_type, signals in grouped.items():
        if alert_type == "LONG_SQUEEZE":
            await format_long_section(session, signals, db)
        elif alert_type == "SHORT_SETUP":
            await format_short_section(session, signals, db)
        elif alert_type == "TREND_FLIP_BEAR":
            await format_flip_bear_section(session, signals, db)
        elif alert_type == "TREND_FLIP_BULL":
            await format_flip_bull_section(session, signals, db)
        elif alert_type == "INTERVAL_COMPRESS":
            await format_interval_compress_section(session, signals, db)
        elif alert_type == "INTERVAL_EXPAND":
            await format_interval_expand_section(session, signals, db)

async def format_long_section(session, signals, db):
    count = len(signals)
    msg = f"🟢 <b>LONG OPPORTUNITIES ({count})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for norm_sym, data_list in signals:
        safe_sym = html.escape(data_list[0]['symbol'])
        avg_current = sum(d['rate'] for d in data_list) / len(data_list)
        old_rates = [d['old_rate'] for d in data_list if d.get('old_rate') is not None]
        avg_previous = sum(old_rates) / len(old_rates) if old_rates else 0
        
        interval = data_list[0]['interval']
        countdown = get_countdown(interval)
        exchanges = ", ".join([d['exchange'].replace(" USDT","").replace(" COIN","") for d in data_list])
        exchanges_count = len(data_list)
        
        current_str = f"{avg_current*100:+.3f}%"
        previous_str = f"{avg_previous*100:+.3f}%" if avg_previous != 0 else "N/A"
        change = avg_current - avg_previous
        change_str = f"{'↓' if change < 0 else '↑'}{abs(change)*100:.3f}%"
        
        mtf_score, mtf_text = await get_mtf_data(db, norm_sym, avg_current)
        stars = get_star_rating(avg_current, exchanges_count, mtf_score)
        expected_move = get_expected_move(avg_current)
        
        price = await fetch_price(session, data_list[0]['symbol'])
        price_str = format_price(price)
        liq_zone = estimate_liq_zone(price, avg_current)
        
        await record_signal(db, norm_sym, "LONG_SQUEEZE", avg_current, price)
        
        msg += (f"💎 <code>{safe_sym}</code> @ <code>{price_str}</code> | {stars}\n"
                f"Rate: <code>{current_str}</code> | Prev: <code>{previous_str}</code> | Δ <code>{change_str}</code>\n"
                f"📍 {exchanges} | ⏳ {countdown} | MTF: <b>{mtf_text}</b>\n"
                f"📈 Expected: <b>{expected_move}</b> pump\n")
        if liq_zone: msg += f"🔥 Liq: {liq_zone}\n"
        msg += f"💡 <b>ENTRY: LONG NOW</b>\n\n"
    
    queue_alert(msg)

async def format_short_section(session, signals, db):
    count = len(signals)
    msg = f"🔴 <b>SHORT OPPORTUNITIES ({count})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for norm_sym, data_list in signals:
        safe_sym = html.escape(data_list[0]['symbol'])
        avg_current = sum(d['rate'] for d in data_list) / len(data_list)
        old_rates = [d['old_rate'] for d in data_list if d.get('old_rate') is not None]
        avg_previous = sum(old_rates) / len(old_rates) if old_rates else 0
        
        interval = data_list[0]['interval']
        countdown = get_countdown(interval)
        exchanges = ", ".join([d['exchange'].replace(" USDT","").replace(" COIN","") for d in data_list])
        exchanges_count = len(data_list)
        
        current_str = f"{avg_current*100:+.3f}%"
        previous_str = f"{avg_previous*100:+.3f}%" if avg_previous != 0 else "N/A"
        change = avg_current - avg_previous
        change_str = f"{'↓' if change < 0 else '↑'}{abs(change)*100:.3f}%"
        
        mtf_score, mtf_text = await get_mtf_data(db, norm_sym, avg_current)
        stars = get_star_rating(avg_current, exchanges_count, mtf_score)
        expected_move = get_expected_move(avg_current)
        
        price = await fetch_price(session, data_list[0]['symbol'])
        price_str = format_price(price)
        liq_zone = estimate_liq_zone(price, avg_current)
        
        await record_signal(db, norm_sym, "SHORT_SETUP", avg_current, price)
        
        msg += (f"💎 <code>{safe_sym}</code> @ <code>{price_str}</code> | {stars}\n"
                f"Rate: <code>{current_str}</code> | Prev: <code>{previous_str}</code> | Δ <code>{change_str}</code>\n"
                f"📍 {exchanges} | ⏳ {countdown} | MTF: <b>{mtf_text}</b>\n"
                f"📉 Expected: <b>{expected_move}</b> dump\n")
        if liq_zone: msg += f"🔥 Liq: {liq_zone}\n"
        msg += f"💡 <b>ENTRY: SHORT NOW</b>\n\n"
    
    queue_alert(msg)

async def format_flip_bear_section(session, signals, db):
    count = len(signals)
    msg = f"🔄 <b>FLIP BEARISH ({count})</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    for norm_sym, data_list in signals:
        safe_sym = html.escape(data_list[0]['symbol'])
        avg_current = sum(d['rate'] for d in data_list) / len(data_list)
        old_rates = [d['old_rate'] for d in data_list if d.get('old_rate') is not None]
        avg_previous = sum(old_rates) / len(old_rates) if old_rates else 0
        
        interval = data_list[0]['interval']
        countdown = get_countdown(interval)
        exchanges = ", ".join([d['exchange'].replace(" USDT","").replace(" COIN","") for d in data_list])
        exchanges_count = len(data_list)
        
        current_str = f"{avg_current*100:+.3f}%"
        previous_str = f"{avg_previous*100:+.3f}%" if avg_previous != 0 else "N/A"
        change = avg_current - avg_previous
        change_str = f"{'↓' if change < 0 else '↑'}{abs(change)*100:.3f}%"
        
        mtf_score, mtf_text = await get_mtf_data(db, norm_sym, avg_current)
        stars = 
