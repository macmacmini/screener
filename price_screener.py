"""
Unified Price Screener
Monitors all Lighter.xyz and Hyperliquid markets (crypto + RWA) in one process.
Each exchange is compared against its own reference price:
- Lighter: last trade price vs Lighter's own index price (all active markets, 1 bulk call)
- Hyperliquid main dex: bid/ask (impactPxs) vs Hyperliquid's own oracle (1 call)
- Hyperliquid xyz dex (RWA): bid/ask (impactPxs) vs Hyperliquid's own oracle (1 call)
Sends Telegram alerts when deviation exceeds configured threshold.

QFEX runs separately (price_screener_qfex.py) as it is websocket-based.
Replaces price_screener_binance.py and price_screener_rwa.py.
"""

import asyncio
import os
import json
import logging
from typing import Dict
from dotenv import load_dotenv
import lighter
import requests
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


# Load config from JSON file
def load_config() -> dict:
    """Load all settings from config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded config: threshold={config.get('default_threshold', 0.5)}%, "
                   f"poll={config.get('poll_interval', 60)}s, "
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

HYPERLIQUID_INFO_URL = "https://api.hyperliquid.xyz/info"

# Minimum 24h volume (USD) per Hyperliquid dex - markets below are skipped
HL_MIN_VOLUME = {
    None: 150000,   # main dex (crypto)
    'xyz': 50000,   # xyz dex (RWA)
}

# Minimum 24h volume (USD) for Lighter markets - filters out dead/illiquid
# markets whose last trade price is stale and far from index
LIGHTER_MIN_VOLUME = float(CONFIG.get('min_volume_lighter', 0))


class UnifiedPriceScreener:
    """Monitor all Lighter and Hyperliquid markets, each vs its own index/oracle price"""

    def __init__(self):
        self.deviation_threshold = float(CONFIG.get('default_threshold', 0.5))

        # Per-exchange poll intervals, tuned to stay under rate limits:
        # - Lighter: 60 req/min per IP -> 1.5s = 40/min (67%), headroom for validation calls
        # - Hyperliquid: 1200 weight/min per IP, 2 calls x 20 weight per round
        #   -> 2.5s = 24 rounds = 960 weight/min (80%)
        fallback = float(CONFIG.get('poll_interval', 60))
        self.poll_interval_lighter = float(CONFIG.get('poll_interval_lighter', fallback))
        self.poll_interval_hl = float(CONFIG.get('poll_interval_hyperliquid', fallback))

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

        # 2-poll confirmation: alert only if deviation persists across consecutive scans.
        # Separate pending dicts per exchange loop so confirmation always compares fresh data
        self.pending_lighter: Dict[str, str] = {}  # alert_key -> message from previous scan
        self.pending_hl: Dict[str, str] = {}

        # Lighter API client
        self.client = lighter.ApiClient()
        self.order_api = lighter.OrderApi(self.client)

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
                formatted_message = f"🚨 *Price Alert*\n\n{message}"
                await self.bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=formatted_message,
                    parse_mode='Markdown'
                )
                self.last_alert[market_key] = current_time
                logger.info(f"Telegram alert sent for {market_key}")
            except TelegramError as e:
                logger.error(f"Failed to send Telegram message: {e}")

    async def fetch_lighter_prices(self) -> Dict[str, dict]:
        """Fetch ALL active Lighter markets (crypto + RWA) in one bulk call.
        order_book_details includes Lighter's own index_price (oracle),
        so each market carries its own reference price."""
        try:
            details = await self.order_api.order_book_details()

            if hasattr(details, 'order_book_details'):
                order_book_details = details.order_book_details
            else:
                order_book_details = []

            prices = {}

            for detail in order_book_details:
                if isinstance(detail, dict):
                    d = detail
                else:
                    d = detail.to_dict()

                if d.get('status') != 'active':
                    continue

                symbol = d.get('symbol')
                last_price = d.get('last_trade_price')
                index_price = d.get('index_price')

                if not symbol or not last_price or not index_price:
                    continue

                # Skip markets with low volume
                volume_usd = float(d.get('daily_quote_token_volume') or 0)
                if volume_usd < LIGHTER_MIN_VOLUME:
                    logger.debug(f"Skipping {symbol}: volume ${volume_usd:.0f} < ${LIGHTER_MIN_VOLUME:.0f}")
                    continue

                try:
                    last_price = float(last_price)
                    index_price = float(index_price)
                except (ValueError, TypeError):
                    continue

                if last_price <= 0 or index_price <= 0:
                    continue

                prices[symbol] = {
                    'last_trade_price': last_price,
                    'index_price': index_price,
                    'trades_count': d.get('daily_trades_count', 0)
                }

            logger.info(f"Fetched prices for {len(prices)} Lighter markets (bulk)")
            return prices

        except Exception as e:
            logger.error(f"Error fetching Lighter prices (bulk): {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}

    def fetch_hyperliquid_prices(self, dex: str = None) -> Dict[str, dict]:
        """Fetch all markets from a Hyperliquid dex in one call.
        dex=None -> main dex (crypto), dex='xyz' -> xyz dex (RWA).
        Response includes Hyperliquid's own oracle price per market."""
        dex_label = dex or 'main'
        try:
            data = {"type": "metaAndAssetCtxs"}
            if dex:
                data["dex"] = dex

            response = requests.post(HYPERLIQUID_INFO_URL, json=data, timeout=10)
            response.raise_for_status()

            meta_data = response.json()

            if not isinstance(meta_data, list) or len(meta_data) < 2:
                logger.error(f"Unexpected metaAndAssetCtxs response format (dex={dex_label})")
                return {}

            universe = meta_data[0].get('universe', [])
            contexts = meta_data[1]
            min_volume = HL_MIN_VOLUME.get(dex, 50000)

            prices = {}
            for i, market in enumerate(universe):
                if i >= len(contexts):
                    break

                symbol = market.get('name', '')

                # Remove 'xyz:' prefix if present
                if symbol.startswith('xyz:'):
                    symbol = symbol[4:]

                ctx = contexts[i]

                oracle_px = ctx.get('oraclePx')
                impact_pxs = ctx.get('impactPxs')
                day_volume = ctx.get('dayNtlVlm')

                if not symbol or not oracle_px or not impact_pxs or len(impact_pxs) != 2:
                    continue

                # Skip markets with low volume
                volume_usd = float(day_volume) if day_volume is not None else 0
                if volume_usd < min_volume:
                    logger.debug(f"Skipping {symbol}: volume ${volume_usd:.0f} < ${min_volume}")
                    continue

                try:
                    prices[symbol] = {
                        'oracle_price': float(oracle_px),
                        'best_bid': float(impact_pxs[0]),
                        'best_ask': float(impact_pxs[1]),
                        'volume_24h': volume_usd
                    }
                except (ValueError, TypeError):
                    continue

            logger.info(f"Fetched prices for {len(prices)} Hyperliquid {dex_label} markets")
            return prices

        except Exception as e:
            logger.error(f"Error fetching Hyperliquid {dex_label} prices: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}

    def calculate_deviation(self, price: float, reference_price: float) -> float:
        """Calculate percentage deviation from reference (index/oracle) price"""
        if reference_price == 0:
            return 0.0
        return ((price - reference_price) / reference_price) * 100

    def check_lighter_market(self, symbol: str, lighter_data: dict):
        """Check a single Lighter market: last trade price vs Lighter's own index price"""
        try:
            # Skip blacklisted symbols
            if symbol in SYMBOL_BLACKLIST:
                logger.debug(f"Skipping {symbol}: in blacklist")
                return None

            lighter_price = lighter_data.get('last_trade_price')
            index_price = lighter_data.get('index_price')
            if not lighter_price or not index_price:
                return None

            # Calculate deviation
            deviation = self.calculate_deviation(lighter_price, index_price)

            logger.debug(
                f"Lighter {symbol}: Price=${lighter_price:.4f}, Index=${index_price:.4f}, "
                f"Deviation={deviation:.2f}%"
            )

            market_key = f"LT-{symbol}"

            # Get threshold (custom or default)
            threshold = CUSTOM_THRESHOLDS.get(symbol, self.deviation_threshold)

            # Alert if deviation exceeds threshold
            if abs(deviation) >= threshold:
                direction = "↑" if deviation > 0 else "↓"
                emoji = "📈" if deviation > 0 else "📉"

                message = (
                    f"{emoji} *LIGHTER - {symbol}*\n"
                    f"Last Trade: `${lighter_price:.4f}`\n"
                    f"Index: `${index_price:.4f}`\n"
                    f"Deviation: *{direction}{abs(deviation):.2f}%*\n"
                    f"Threshold: {threshold}%\n"
                    f"🔗 https://app.lighter.xyz/trade/{symbol}"
                )
                return (market_key, message)
            return None

        except Exception as e:
            logger.error(f"Error checking Lighter market {symbol}: {e}")
            return None

    def check_hyperliquid_market(self, symbol: str, hl_data: dict, dex: str = None):
        """Check a single Hyperliquid market: bid/ask vs Hyperliquid's own oracle price"""
        try:
            # Skip blacklisted symbols
            if symbol in SYMBOL_BLACKLIST:
                logger.debug(f"Skipping {symbol}: in blacklist")
                return None

            oracle_price = hl_data.get('oracle_price')
            best_bid = hl_data.get('best_bid')
            best_ask = hl_data.get('best_ask')

            if not oracle_price or not best_bid or not best_ask:
                return None

            # Calculate deviations from Hyperliquid's own oracle
            bid_deviation = self.calculate_deviation(best_bid, oracle_price)
            ask_deviation = self.calculate_deviation(best_ask, oracle_price)

            dex_tag = f"HL-{dex}" if dex else "HL"
            trade_symbol = f"{dex}:{symbol}" if dex else symbol

            logger.debug(
                f"{dex_tag} {symbol}: Bid=${best_bid:.4f} ({bid_deviation:.2f}%), "
                f"Ask=${best_ask:.4f} ({ask_deviation:.2f}%), "
                f"Oracle=${oracle_price:.4f}"
            )

            bid_key = f"{dex_tag}-{symbol}-BID"
            ask_key = f"{dex_tag}-{symbol}-ASK"
            alerts = []

            # Get threshold (custom or default)
            threshold = CUSTOM_THRESHOLDS.get(symbol, self.deviation_threshold)

            # Alert if best bid deviates from oracle (sell opportunity)
            if abs(bid_deviation) >= threshold:
                direction = "↑" if bid_deviation > 0 else "↓"
                emoji = "📈" if bid_deviation > 0 else "📉"
                message = (
                    f"{emoji} *HYPERLIQUID {dex or 'main'} - {symbol} (SELL)*\n"
                    f"Best Bid: `${best_bid:.4f}`\n"
                    f"Oracle: `${oracle_price:.4f}`\n"
                    f"Deviation: *{direction}{abs(bid_deviation):.2f}%*\n"
                    f"Threshold: {threshold}%\n"
                    f"🔗 https://app.hyperliquid.xyz/trade/{trade_symbol}"
                )
                alerts.append((bid_key, message))

            # Alert if best ask deviates from oracle (buy opportunity)
            if abs(ask_deviation) >= threshold:
                direction = "↑" if ask_deviation > 0 else "↓"
                emoji = "📈" if ask_deviation > 0 else "📉"
                message = (
                    f"{emoji} *HYPERLIQUID {dex or 'main'} - {symbol} (BUY)*\n"
                    f"Best Ask: `${best_ask:.4f}`\n"
                    f"Oracle: `${oracle_price:.4f}`\n"
                    f"Deviation: *{direction}{abs(ask_deviation):.2f}%*\n"
                    f"Threshold: {threshold}%\n"
                    f"🔗 https://app.hyperliquid.xyz/trade/{trade_symbol}"
                )
                alerts.append((ask_key, message))

            return alerts if alerts else None

        except Exception as e:
            logger.error(f"Error checking Hyperliquid market {symbol}: {e}")
            return None

    async def scan_lighter_markets(self):
        """Scan all Lighter markets vs Lighter's own index price (1 API call)"""
        lighter_prices = await self.fetch_lighter_prices()

        if not lighter_prices:
            logger.warning("No Lighter prices available, skipping scan")
            return

        alerts = []
        for symbol, lighter_data in lighter_prices.items():
            result = self.check_lighter_market(symbol, lighter_data)
            if result:
                alerts.append(result)

        # 2-poll confirmation: only send alerts that were also detected in the previous scan
        current_alerts = {key: message for key, message in alerts}
        confirmed = []
        for alert_key, message in current_alerts.items():
            if alert_key in self.pending_lighter:
                confirmed.append((alert_key, message))
            else:
                logger.info(f"Pending confirmation: {alert_key} (will alert if persists next scan)")

        for alert_key, message in confirmed:
            await self.send_alert(alert_key, message)

        self.pending_lighter = current_alerts

        if confirmed:
            logger.info(f"Lighter scan: {len(confirmed)} alerts sent ({len(current_alerts)} detected, {len(lighter_prices)} markets)")
        elif current_alerts:
            logger.info(f"Lighter scan: {len(current_alerts)} pending confirmation ({len(lighter_prices)} markets)")

    async def scan_hyperliquid_markets(self):
        """Scan Hyperliquid main + xyz markets vs Hyperliquid's own oracle (2 API calls)"""
        # requests is blocking - run in a thread so the Lighter loop isn't stalled
        hl_main_prices = await asyncio.to_thread(self.fetch_hyperliquid_prices)
        hl_xyz_prices = await asyncio.to_thread(self.fetch_hyperliquid_prices, 'xyz')

        if not hl_main_prices and not hl_xyz_prices:
            logger.warning("No Hyperliquid prices available, skipping scan")
            return

        alerts = []
        for symbol, hl_data in hl_main_prices.items():
            result = self.check_hyperliquid_market(symbol, hl_data)
            if result:
                alerts.extend(result)

        for symbol, hl_data in hl_xyz_prices.items():
            result = self.check_hyperliquid_market(symbol, hl_data, dex='xyz')
            if result:
                alerts.extend(result)

        # 2-poll confirmation: only send alerts that were also detected in the previous scan
        current_alerts = {key: message for key, message in alerts}
        confirmed = []
        for alert_key, message in current_alerts.items():
            if alert_key in self.pending_hl:
                confirmed.append((alert_key, message))
            else:
                logger.info(f"Pending confirmation: {alert_key} (will alert if persists next scan)")

        for alert_key, message in confirmed:
            await self.send_alert(alert_key, message)

        self.pending_hl = current_alerts

        if confirmed:
            logger.info(f"HL scan: {len(confirmed)} alerts sent ({len(current_alerts)} detected, "
                        f"{len(hl_main_prices)}+{len(hl_xyz_prices)} markets)")
        elif current_alerts:
            logger.info(f"HL scan: {len(current_alerts)} pending confirmation "
                        f"({len(hl_main_prices)}+{len(hl_xyz_prices)} markets)")

    async def _scan_loop(self, name: str, interval: float, scan_fn):
        """Run one exchange's scan function on its own interval.
        Scan duration is subtracted from the sleep so the interval holds
        the actual API call rate, not interval + scan time."""
        loop = asyncio.get_event_loop()
        while True:
            started = loop.time()
            try:
                await scan_fn()
            except Exception as e:
                logger.error(f"Error during {name} scan: {e}")
                import traceback
                logger.error(traceback.format_exc())

            elapsed = loop.time() - started
            await asyncio.sleep(max(0.1, interval - elapsed))

    async def run(self):
        """Run both exchange loops concurrently, each at its own poll interval"""
        logger.info(f"Starting Unified Price Screener")
        logger.info(f"Monitoring: Lighter.xyz (all markets) + Hyperliquid main + Hyperliquid xyz")
        logger.info(f"Reference: each exchange's own index/oracle price")
        logger.info(f"Deviation threshold: {self.deviation_threshold}%")
        logger.info(f"Poll intervals: Lighter {self.poll_interval_lighter}s, "
                    f"Hyperliquid {self.poll_interval_hl}s")

        try:
            await asyncio.gather(
                self._scan_loop("Lighter", self.poll_interval_lighter, self.scan_lighter_markets),
                self._scan_loop("Hyperliquid", self.poll_interval_hl, self.scan_hyperliquid_markets),
            )
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            await self.client.close()

    async def close(self):
        """Cleanup resources"""
        await self.client.close()


async def main():
    """Entry point"""
    screener = UnifiedPriceScreener()
    try:
        await screener.run()
    finally:
        await screener.close()


if __name__ == "__main__":
    asyncio.run(main())
