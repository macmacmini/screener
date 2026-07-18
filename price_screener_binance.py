"""
Crypto Price Screener
Compares Lighter.xyz last trade prices against Lighter's own index price
and Hyperliquid bid/ask against Hyperliquid's own oracle price
Alerts when significant deviations are detected
"""

import asyncio
import os
import json
import logging
from datetime import datetime
from typing import Dict, Optional, Set
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

# Lighter strategy_index for crypto markets
# (3-7 = RWA markets, covered by the RWA screener; 0 = inactive/delisted)
CRYPTO_STRATEGY_INDEX = 2


class CryptoPriceScreener:
    """Monitor crypto markets: Lighter vs its own index price, Hyperliquid vs its own oracle"""

    def __init__(self):
        self.deviation_threshold = float(CONFIG.get('default_threshold', 0.5))
        self.poll_interval = int(CONFIG.get('poll_interval', 60))

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

        # Lighter API client
        self.client = lighter.ApiClient()
        self.order_api = lighter.OrderApi(self.client)

        # Symbol -> market_id mapping for recent_trades validation
        self.symbol_to_market_id: Dict[str, int] = {}

    async def fetch_all_lighter_prices(self) -> Dict[str, dict]:
        """Fetch all crypto markets from Lighter in one bulk call.
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
                if d.get('strategy_index') != CRYPTO_STRATEGY_INDEX:
                    continue

                symbol = d.get('symbol')
                last_price = d.get('last_trade_price')
                index_price = d.get('index_price')

                if not symbol or not last_price or not index_price:
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

            logger.info(f"Fetched prices for {len(prices)} Lighter crypto markets (bulk)")
            return prices

        except Exception as e:
            logger.error(f"Error fetching Lighter prices (bulk): {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}

    async def validate_lighter_price(self, symbol: str, expected_price: float) -> bool:
        """Validate Lighter last_trade_price by checking recent_trades endpoint.
        Returns True if price is confirmed, False if it looks wrong or can't be validated."""
        market_id = self.symbol_to_market_id.get(symbol)
        if market_id is None:
            logger.warning(f"No market_id for {symbol}, blocking alert (can't validate)")
            return False

        try:
            trades = await self.order_api.recent_trades(market_id=market_id, limit=5)
            if not hasattr(trades, 'trades') or not trades.trades:
                logger.warning(f"No recent trades for {symbol}, blocking alert")
                return False

            latest_trade = trades.trades[0]
            if isinstance(latest_trade, dict):
                real_price = float(latest_trade.get('price', 0))
            else:
                real_price = float(getattr(latest_trade, 'price', 0))

            if real_price <= 0:
                return False

            diff_pct = abs(expected_price - real_price) / real_price * 100
            if diff_pct > 1.0:
                logger.warning(
                    f"REJECTED {symbol}: exchange_stats=${expected_price:.6f} vs "
                    f"recent_trades=${real_price:.6f} (diff {diff_pct:.2f}%) - stale data"
                )
                return False

            logger.info(f"VALIDATED {symbol}: exchange_stats=${expected_price:.6f} matches recent_trades=${real_price:.6f}")
            return True

        except Exception as e:
            logger.error(f"Error validating {symbol} price: {e}")
            return False

    async def fetch_hyperliquid_prices(self) -> Dict[str, dict]:
        """Fetch bid/ask prices for ALL Hyperliquid perp markets using metaAndAssetCtxs endpoint"""
        try:
            url = "https://api.hyperliquid.xyz/info"
            data = {"type": "metaAndAssetCtxs"}

            response = requests.post(url, json=data, timeout=10)
            response.raise_for_status()

            meta_data = response.json()

            # meta_data[0] = universe (market definitions)
            # meta_data[1] = contexts (market data)
            if not isinstance(meta_data, list) or len(meta_data) < 2:
                logger.error("Unexpected metaAndAssetCtxs response format")
                return {}

            universe = meta_data[0].get('universe', [])
            contexts = meta_data[1]

            perp_prices = {}
            for i, market in enumerate(universe):
                if i >= len(contexts):
                    break

                symbol = market.get('name')
                ctx = contexts[i]

                # Extract bid/ask from impactPxs and Hyperliquid's own oracle price
                impact_pxs = ctx.get('impactPxs')
                mid_px = ctx.get('midPx')
                oracle_px = ctx.get('oraclePx')
                day_volume = ctx.get('dayNtlVlm')  # Daily notional volume in USD

                if not symbol or not impact_pxs or len(impact_pxs) != 2:
                    continue

                # Skip if no mid price (market might be inactive) or no oracle price
                if mid_px is None or oracle_px is None:
                    continue

                # Skip markets with low volume (< $150k in 24h)
                volume_usd = float(day_volume) if day_volume is not None else 0
                if volume_usd < 150000:
                    logger.debug(f"Skipping {symbol}: volume ${volume_usd:.0f} < $150k")
                    continue

                try:
                    perp_prices[symbol] = {
                        'best_bid': float(impact_pxs[0]),
                        'best_ask': float(impact_pxs[1]),
                        'mid_price': float(mid_px),
                        'oracle_price': float(oracle_px),
                        'volume_24h': volume_usd
                    }
                except (ValueError, TypeError):
                    continue

            logger.info(f"Fetched prices for {len(perp_prices)} Hyperliquid perp markets (bulk)")
            return perp_prices

        except Exception as e:
            logger.error(f"Error fetching Hyperliquid prices: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}

    def calculate_deviation(self, price: float, reference_price: float) -> float:
        """Calculate percentage deviation from reference (index/oracle) price"""
        if reference_price == 0:
            return 0.0
        return ((price - reference_price) / reference_price) * 100

    async def send_alert(self, market_id: int, message: str):
        """Send alert via Telegram and/or console"""
        current_time = asyncio.get_event_loop().time()
        alert_key = str(market_id)

        # Check if blacklisted
        if alert_key in self.blacklisted:
            time_since_blacklist = current_time - self.blacklisted[alert_key]
            if time_since_blacklist < self.blacklist_duration:
                remaining_hours = (self.blacklist_duration - time_since_blacklist) / 3600
                logger.debug(
                    f"Market {alert_key} is blacklisted for {remaining_hours:.1f} more hours"
                )
                return
            else:
                logger.info(f"Blacklist expired for {alert_key}, re-enabling alerts")
                del self.blacklisted[alert_key]
                self.consecutive_alerts[alert_key] = 0

        logger.warning(f"ALERT [Market {market_id}]: {message}")

        # Track consecutive alerts (regardless of cooldown)
        self.consecutive_alerts[alert_key] = self.consecutive_alerts.get(alert_key, 0) + 1
        logger.info(
            f"Consecutive alerts for {alert_key}: {self.consecutive_alerts[alert_key]}/2"
        )

        # If we've hit 2 consecutive alerts, blacklist for 24h
        if self.consecutive_alerts[alert_key] >= 2:
            self.blacklisted[alert_key] = current_time
            logger.warning(
                f"Market {alert_key} blacklisted for 24h due to 2 consecutive alerts"
            )
            if self.bot:
                try:
                    blacklist_msg = (
                        f"⛔ *AUTO-BLACKLISTED*\n\n"
                        f"Market: `{alert_key}`\n"
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
        if alert_key in self.last_alert:
            if current_time - self.last_alert[alert_key] < self.alert_cooldown:
                logger.debug(f"Alert cooldown active for {market_id}, skipping Telegram notification")
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
                self.last_alert[alert_key] = current_time
                logger.info(f"Telegram alert sent for market {market_id}")
            except TelegramError as e:
                logger.error(f"Failed to send Telegram message: {e}")

    def check_market(self, symbol: str, lighter_data: dict):
        """Check a single Lighter market: last trade price vs Lighter's own index price"""
        try:
            # Skip blacklisted symbols
            if symbol in SYMBOL_BLACKLIST:
                logger.debug(f"Skipping {symbol}: in blacklist")
                return None

            lighter_price = lighter_data.get('last_trade_price')
            index_price = lighter_data.get('index_price')
            if not lighter_price or not index_price:
                logger.debug(f"No Lighter price/index for {symbol}")
                return None

            # Calculate deviation
            deviation = self.calculate_deviation(lighter_price, index_price)

            logger.debug(
                f"{symbol}: Lighter=${lighter_price:.4f}, Index=${index_price:.4f}, "
                f"Deviation={deviation:.2f}%"
            )

            # Get threshold (custom or default)
            threshold = CUSTOM_THRESHOLDS.get(symbol, self.deviation_threshold)

            # Alert if deviation exceeds threshold
            if abs(deviation) >= threshold:
                direction = "↑" if deviation > 0 else "↓"
                emoji = "📈" if deviation > 0 else "📉"

                message = (
                    f"{emoji} *TRADE OPPORTUNITY*\n"
                    f"*{symbol}* @ Lighter\n"
                    f"Last Trade: `${lighter_price:.4f}`\n"
                    f"Index: `${index_price:.4f}`\n"
                    f"Deviation: *{direction}{abs(deviation):.2f}%*\n"
                    f"🔗 https://app.lighter.xyz/trade/{symbol}"
                )
                return (f"LT-{symbol}", message)

            return None

        except Exception as e:
            logger.error(f"Error checking market {symbol}: {e}")
            return None

    def check_hyperliquid_market(self, symbol: str, hl_data: dict):
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
                logger.debug(f"No Hyperliquid bid/ask/oracle for {symbol}")
                return None

            # Calculate deviations from Hyperliquid's own oracle
            bid_deviation = self.calculate_deviation(best_bid, oracle_price)
            ask_deviation = self.calculate_deviation(best_ask, oracle_price)

            logger.debug(
                f"{symbol}: Bid=${best_bid:.4f} ({bid_deviation:.2f}%), "
                f"Ask=${best_ask:.4f} ({ask_deviation:.2f}%), "
                f"Oracle=${oracle_price:.4f}"
            )

            # Get threshold (custom or default)
            threshold = CUSTOM_THRESHOLDS.get(symbol, self.deviation_threshold)

            # Check if either bid or ask deviates beyond threshold
            alerts = []

            if abs(bid_deviation) >= threshold:
                direction = "↑" if bid_deviation > 0 else "↓"
                emoji = "📈" if bid_deviation > 0 else "📉"
                message = (
                    f"{emoji} *SELL OPPORTUNITY*\n"
                    f"*{symbol}* @ Hyperliquid\n"
                    f"Best Bid: `${best_bid:.4f}`\n"
                    f"Oracle: `${oracle_price:.4f}`\n"
                    f"Deviation: *{direction}{abs(bid_deviation):.2f}%*\n"
                    f"🔗 https://app.hyperliquid.xyz/trade/{symbol}"
                )
                alerts.append((f"HL-{symbol}-BID", message))

            if abs(ask_deviation) >= threshold:
                direction = "↑" if ask_deviation > 0 else "↓"
                emoji = "📈" if ask_deviation > 0 else "📉"
                message = (
                    f"{emoji} *BUY OPPORTUNITY*\n"
                    f"*{symbol}* @ Hyperliquid\n"
                    f"Best Ask: `${best_ask:.4f}`\n"
                    f"Oracle: `${oracle_price:.4f}`\n"
                    f"Deviation: *{direction}{abs(ask_deviation):.2f}%*\n"
                    f"🔗 https://app.hyperliquid.xyz/trade/{symbol}"
                )
                alerts.append((f"HL-{symbol}-ASK", message))

            return alerts if alerts else None

        except Exception as e:
            logger.error(f"Error checking Hyperliquid market {symbol}: {e}")
            return None

    async def scan_all_markets(self):
        """Scan all markets: Lighter vs its own index price, Hyperliquid vs its own oracle"""
        # Fetch ALL Lighter prices in ONE call (includes Lighter's own index price)
        lighter_prices = await self.fetch_all_lighter_prices()

        # Fetch ALL Hyperliquid prices in ONE call (includes Hyperliquid's own oracle)
        hyperliquid_prices = await self.fetch_hyperliquid_prices()

        if not lighter_prices and not hyperliquid_prices:
            logger.warning("No exchange prices available, skipping scan")
            return

        logger.info(f"Scanning {len(lighter_prices)} Lighter + {len(hyperliquid_prices)} Hyperliquid markets...")

        # Check markets synchronously (data already fetched)
        alerts = []

        # Check Lighter markets against Lighter's own index price
        for symbol, lighter_data in lighter_prices.items():
            result = self.check_market(symbol, lighter_data)
            if result:
                alerts.append(result)

        # Check Hyperliquid markets against Hyperliquid's own oracle
        for symbol, hl_data in hyperliquid_prices.items():
            result = self.check_hyperliquid_market(symbol, hl_data)
            if result:
                # Result is a list of alerts (bid and/or ask)
                alerts.extend(result)

        # 2-poll confirmation: only send alerts that were also detected in the previous scan
        current_alerts = {key: message for key, message in alerts}
        confirmed = []
        for alert_key, message in current_alerts.items():
            if alert_key in self.pending_alerts:
                confirmed.append((alert_key, message))
            else:
                logger.info(f"Pending confirmation: {alert_key} (will alert if persists next scan)")

        # Validate confirmed Lighter alerts with recent_trades before sending
        validated = []
        for alert_key, message in confirmed:
            if alert_key.startswith("LT-"):
                symbol = alert_key[3:]  # "LT-COIN" -> "COIN"
                price = lighter_prices.get(symbol, {}).get('last_trade_price')
                if price and not await self.validate_lighter_price(symbol, price):
                    continue  # rejected by recent_trades validation
            validated.append((alert_key, message))

        # Send only validated alerts
        for alert_key, message in validated:
            await self.send_alert(alert_key, message)

        # Update pending for next scan
        self.pending_alerts = current_alerts

        if validated:
            logger.info(f"Scan complete - {len(validated)} validated alerts sent ({len(current_alerts)} detected)")
        elif current_alerts:
            logger.info(f"Scan complete - {len(current_alerts)} pending confirmation, 0 sent")
        else:
            logger.info("Scan complete - no deviations detected")

    async def _build_market_id_mapping(self):
        """Fetch order_books once to build symbol -> market_id mapping for recent_trades validation"""
        try:
            orderbooks_response = await self.order_api.order_books()
            if hasattr(orderbooks_response, 'order_books'):
                obs = orderbooks_response.order_books
            elif hasattr(orderbooks_response, 'data'):
                obs = orderbooks_response.data
            else:
                obs = orderbooks_response

            if isinstance(obs, list):
                for ob in obs:
                    if isinstance(ob, dict):
                        symbol = ob.get('symbol', '')
                        market_id = ob.get('market_id')
                    else:
                        symbol = getattr(ob, 'symbol', '')
                        market_id = getattr(ob, 'market_id', None)
                    if symbol and market_id is not None:
                        self.symbol_to_market_id[symbol] = int(market_id)

            logger.info(f"Built market_id mapping for {len(self.symbol_to_market_id)} symbols")
        except Exception as e:
            logger.error(f"Error building market_id mapping: {e}")

    async def run(self):
        """Main loop - continuously monitor markets"""
        logger.info(f"Starting Crypto Price Screener")
        logger.info(f"Monitoring: Lighter.xyz + Hyperliquid")
        logger.info(f"Reference: Lighter index price / Hyperliquid oracle (each vs its own)")
        logger.info(f"Deviation threshold: {self.deviation_threshold}%")
        logger.info(f"Poll interval: {self.poll_interval} seconds")

        # Build symbol -> market_id mapping once at startup
        await self._build_market_id_mapping()

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
            await self.client.close()

    async def close(self):
        """Cleanup resources"""
        await self.client.close()


async def main():
    """Entry point"""
    screener = CryptoPriceScreener()
    try:
        await screener.run()
    finally:
        await screener.close()


if __name__ == "__main__":
    asyncio.run(main())
