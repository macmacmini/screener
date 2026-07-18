"""
QFEX Price Screener
Monitors QFEX perpetual markets (stocks, commodities, forex, indices)
Compares best bid/ask (order book top) against QFEX's own underlier (oracle) price
Sends Telegram alerts when deviation exceeds configured threshold

The order book comparison catches dislocations that happen without any trades
(e.g. a market maker algo running the book far from the underlier), which on
QFEX is the dominant failure mode - trade volume is thin and price spikes
occur in the book alone.

Prices arrive via QFEX public market data websocket (wss://mds.qfex.com):
- 'bbo' channel: best bid/ask per symbol (continuous stream)
- 'underlier' channel: underlying asset reference price (~1s updates)
A background task keeps the websocket connected and updates an in-memory
cache; the scan loop reads the cache on the same poll/alert logic as the
other screeners.
"""

import asyncio
import os
import json
import logging
import time
from typing import Dict
from dotenv import load_dotenv
import websockets
from telegram import Bot
from telegram.error import TelegramError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

QFEX_WS_URL = "wss://mds.qfex.com"

# Ignore cached prices older than this (websocket may go quiet unnoticed)
BBO_MAX_AGE = 15          # seconds - active books stream bbo continuously; older = book gone quiet
UNDERLIER_MAX_AGE = 30    # seconds - underlier updates ~1/s, so 30s means feed problems


# Load config from JSON file
def load_config() -> dict:
    """Load all settings from config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded config: threshold={config.get('default_threshold', 0.5)}%, "
                   f"poll={config.get('poll_interval_qfex', 60)}s, "
                   f"{len(config.get('symbol_blacklist', []))} blacklisted")
        return config
    except FileNotFoundError:
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing config.json: {e}")
        return {}

CONFIG = load_config()
SYMBOL_BLACKLIST = set(CONFIG.get('symbol_blacklist', []))
CUSTOM_THRESHOLDS = CONFIG.get('custom_thresholds', {})


def base_symbol(qfex_symbol: str) -> str:
    """QFEX symbols are like 'NVDA-USD' or 'SAMSUNG-KRW' -> base is 'NVDA'/'SAMSUNG'.
    Used for blacklist and custom threshold lookups shared via config.json."""
    return qfex_symbol.split('-')[0]


class QfexPriceScreener:
    """Monitor QFEX markets: best bid/ask vs QFEX's own underlier price"""

    def __init__(self):
        self.deviation_threshold = float(CONFIG.get('default_threshold', 0.5))
        # QFEX scans read the websocket-fed in-memory cache (no REST calls),
        # so it can poll faster than the REST-based screeners
        self.poll_interval = int(CONFIG.get('poll_interval_qfex', CONFIG.get('poll_interval', 60)))

        # Telegram configuration
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        self.bot = None

        if self.telegram_token and self.telegram_chat_id:
            self.bot = Bot(token=self.telegram_token)
            logger.info("Telegram bot initialized")
        else:
            logger.warning("Telegram credentials not configured - alerts will only be logged")

        # Track last alert time to avoid spam
        self.last_alert: Dict[str, float] = {}
        self.alert_cooldown = 300  # 5 minutes between alerts for same pair

        # Track consecutive alerts for auto-blacklisting
        self.consecutive_alerts: Dict[str, int] = {}
        self.blacklisted: Dict[str, float] = {}  # market_key -> blacklist timestamp
        self.blacklist_duration = 86400  # 24 hours in seconds

        # 2-poll confirmation: alert only if deviation persists across consecutive scans
        self.pending_alerts: Dict[str, str] = {}  # alert_key -> message from previous scan

        # In-memory price cache updated by the websocket task
        # symbol -> {'bid': float, 'ask': float, 'updated': float (time.time())}
        self.bbos: Dict[str, dict] = {}
        # symbol -> {'price': float, 'source': str, 'updated': float}
        self.underliers: Dict[str, dict] = {}

        self.ws_connected = False

    async def send_alert(self, market_key: str, message: str):
        """Send alert via Telegram and/or console"""
        current_time = asyncio.get_event_loop().time()

        # Check if blacklisted
        if market_key in self.blacklisted:
            time_since_blacklist = current_time - self.blacklisted[market_key]
            if time_since_blacklist < self.blacklist_duration:
                # Still blacklisted
                remaining_hours = (self.blacklist_duration - time_since_blacklist) / 3600
                logger.debug(
                    f"Market {market_key} is blacklisted for {remaining_hours:.1f} more hours "
                    f"(too many consecutive alerts)"
                )
                return
            else:
                # Blacklist expired, remove it
                logger.info(f"Blacklist expired for {market_key}, re-enabling alerts")
                del self.blacklisted[market_key]
                self.consecutive_alerts[market_key] = 0

        logger.warning(f"ALERT [{market_key}]: {message}")

        # Track consecutive alerts (regardless of cooldown)
        self.consecutive_alerts[market_key] = self.consecutive_alerts.get(market_key, 0) + 1
        logger.info(
            f"Consecutive alerts for {market_key}: {self.consecutive_alerts[market_key]}/2"
        )

        # If we've hit 2 consecutive alerts, blacklist for 24h
        if self.consecutive_alerts[market_key] >= 2:
            self.blacklisted[market_key] = current_time
            logger.warning(
                f"Market {market_key} blacklisted for 24h due to 2 consecutive alerts"
            )
            if self.bot:
                try:
                    blacklist_msg = (
                        f"⛔ *AUTO-BLACKLISTED*\n\n"
                        f"Market: `{market_key}`\n"
                        f"Reason: 2 consecutive alerts\n"
                        f"Duration: 24 hours\n\n"
                        f"This market will be ignored until blacklist expires."
                    )
                    await self.bot.send_message(
                        chat_id=self.telegram_chat_id,
                        text=blacklist_msg,
                        parse_mode='Markdown'
                    )
                except TelegramError as e:
                    logger.error(f"Failed to send blacklist notification: {e}")
            return

        # Check cooldown
        if market_key in self.last_alert:
            if current_time - self.last_alert[market_key] < self.alert_cooldown:
                logger.debug(f"Alert cooldown active for {market_key}, skipping Telegram notification")
                return

        # Send to Telegram
        if self.bot:
            try:
                formatted_message = f"🚨 *QFEX Price Alert*\n\n{message}"
                await self.bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=formatted_message,
                    parse_mode='Markdown'
                )
                self.last_alert[market_key] = current_time
                logger.info(f"Telegram alert sent for {market_key}")
            except TelegramError as e:
                logger.error(f"Failed to send Telegram message: {e}")

    def handle_ws_message(self, msg: dict):
        """Update price cache from a websocket message"""
        msg_type = msg.get('type')
        symbol = msg.get('symbol')

        if msg_type == 'bbo' and symbol:
            try:
                # bid/ask are arrays of [price, quantity] levels; top of book is [0]
                bid_levels = msg.get('bid') or []
                ask_levels = msg.get('ask') or []
                if not bid_levels or not ask_levels:
                    return  # one-sided book, can't compare
                self.bbos[symbol] = {
                    'bid': float(bid_levels[0][0]),
                    'ask': float(ask_levels[0][0]),
                    'updated': time.time(),
                }
            except (KeyError, ValueError, IndexError, TypeError):
                logger.debug(f"Malformed bbo message: {msg}")

        elif msg_type == 'underlier' and symbol:
            try:
                self.underliers[symbol] = {
                    'price': float(msg['price']),
                    'source': msg.get('source', 'unknown'),
                    'updated': time.time(),
                }
            except (KeyError, ValueError):
                logger.debug(f"Malformed underlier message: {msg}")

        elif msg_type == 'subscribed':
            logger.info(f"Subscribed to QFEX channels: {msg.get('channels')}")

    async def websocket_loop(self):
        """Keep the market data websocket connected, reconnect on failures"""
        subscribe_msg = json.dumps({
            "type": "subscribe",
            "channels": ["bbo", "underlier"],
            "symbols": ["*"],
        })

        while True:
            try:
                async with websockets.connect(QFEX_WS_URL, ping_interval=20) as ws:
                    await ws.send(subscribe_msg)
                    self.ws_connected = True
                    logger.info("QFEX websocket connected")

                    async for raw in ws:
                        try:
                            self.handle_ws_message(json.loads(raw))
                        except json.JSONDecodeError:
                            logger.debug(f"Non-JSON websocket message: {raw[:200]}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"QFEX websocket error: {e}, reconnecting in 5s...")

            self.ws_connected = False
            await asyncio.sleep(5)

    def calculate_deviation(self, price: float, underlier_price: float) -> float:
        """Calculate percentage deviation from underlier price"""
        if underlier_price == 0:
            return 0.0
        return ((price - underlier_price) / underlier_price) * 100

    def check_qfex_market(self, symbol: str, bbo_data: dict, underlier_data: dict):
        """Check a single QFEX market: best bid/ask vs underlier price.
        Catches order book dislocations even when no trades happen."""
        try:
            base = base_symbol(symbol)

            # Skip blacklisted symbols
            if base in SYMBOL_BLACKLIST or symbol in SYMBOL_BLACKLIST:
                logger.debug(f"Skipping {symbol}: in blacklist")
                return None

            now = time.time()

            # Skip stale data - websocket may have gone quiet or market inactive
            if now - bbo_data['updated'] > BBO_MAX_AGE:
                return None
            if now - underlier_data['updated'] > UNDERLIER_MAX_AGE:
                logger.debug(f"Skipping {symbol}: underlier price stale")
                return None

            best_bid = bbo_data['bid']
            best_ask = bbo_data['ask']
            underlier_price = underlier_data['price']
            source = underlier_data['source']

            bid_deviation = self.calculate_deviation(best_bid, underlier_price)
            ask_deviation = self.calculate_deviation(best_ask, underlier_price)

            logger.debug(
                f"QFEX {symbol}: Bid={best_bid:.4f} ({bid_deviation:.2f}%), "
                f"Ask={best_ask:.4f} ({ask_deviation:.2f}%), "
                f"Underlier={underlier_price:.4f} ({source})"
            )

            # Get threshold (custom or default)
            threshold = CUSTOM_THRESHOLDS.get(base, self.deviation_threshold)

            alerts = []

            # Directional checks only: a wide spread (bid far below, ask far
            # above) is an empty book, not an opportunity. The signal is the
            # book crossing the underlier: bid too HIGH or ask too LOW.

            # Bid above underlier -> someone bids too high -> sell opportunity
            if bid_deviation >= threshold:
                message = (
                    f"📈 *QFEX - {symbol} (SELL)*\n"
                    f"Best Bid: `{best_bid:.4f}`\n"
                    f"Underlier: `{underlier_price:.4f}` ({source})\n"
                    f"Deviation: *↑{bid_deviation:.2f}%*\n"
                    f"Threshold: {threshold}%\n"
                    f"🔗 https://qfex.com/trade/{symbol}"
                )
                alerts.append((f"QF-{symbol}-BID", message))

            # Ask below underlier -> someone offers too cheap -> buy opportunity
            if ask_deviation <= -threshold:
                message = (
                    f"📉 *QFEX - {symbol} (BUY)*\n"
                    f"Best Ask: `{best_ask:.4f}`\n"
                    f"Underlier: `{underlier_price:.4f}` ({source})\n"
                    f"Deviation: *↓{abs(ask_deviation):.2f}%*\n"
                    f"Threshold: {threshold}%\n"
                    f"🔗 https://qfex.com/trade/{symbol}"
                )
                alerts.append((f"QF-{symbol}-ASK", message))

            return alerts if alerts else None

        except Exception as e:
            logger.error(f"Error checking QFEX market {symbol}: {e}")
            return None

    async def scan_all_markets(self):
        """Scan all cached QFEX markets for bid/ask vs underlier deviations"""
        if not self.ws_connected and not self.bbos:
            logger.warning("QFEX websocket not connected yet, skipping scan")
            return

        alerts = []
        scanned = 0
        now = time.time()

        for symbol, bbo_data in self.bbos.items():
            underlier_data = self.underliers.get(symbol)
            if not underlier_data:
                logger.debug(f"No underlier price for {symbol}")
                continue

            if now - bbo_data['updated'] <= BBO_MAX_AGE:
                scanned += 1
            result = self.check_qfex_market(symbol, bbo_data, underlier_data)
            if result:
                alerts.extend(result)

        logger.info(f"Scanned {scanned} QFEX markets with live order books")

        # 2-poll confirmation: only send alerts that were also detected in the previous scan
        current_alerts = {key: message for key, message in alerts}
        confirmed = []
        for alert_key, message in current_alerts.items():
            if alert_key in self.pending_alerts:
                confirmed.append((alert_key, message))
            else:
                logger.info(f"Pending confirmation: {alert_key} (will alert if persists next scan)")

        # Send confirmed alerts
        for alert_key, message in confirmed:
            await self.send_alert(alert_key, message)

        # Update pending for next scan
        self.pending_alerts = current_alerts

        if confirmed:
            logger.info(f"Scan complete - {len(confirmed)} alerts sent ({len(current_alerts)} detected)")
        elif current_alerts:
            logger.info(f"Scan complete - {len(current_alerts)} pending confirmation, 0 sent")
        else:
            logger.info("Scan complete - no deviations detected")

    async def run(self):
        """Main loop - websocket in background, scan cache on poll interval"""
        logger.info(f"Starting QFEX Price Screener")
        logger.info(f"Monitoring: QFEX perpetual markets (bid/ask vs underlier)")
        logger.info(f"Deviation threshold: {self.deviation_threshold}%")
        logger.info(f"Poll interval: {self.poll_interval} seconds")

        ws_task = asyncio.create_task(self.websocket_loop())

        # Give the websocket a moment to connect and populate the cache
        await asyncio.sleep(3)

        try:
            # Continuous monitoring loop
            while True:
                try:
                    await self.scan_all_markets()
                except Exception as e:
                    logger.error(f"Error during scan: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

                # Wait before next scan
                await asyncio.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass


async def main():
    """Entry point"""
    screener = QfexPriceScreener()
    await screener.run()


if __name__ == "__main__":
    asyncio.run(main())
