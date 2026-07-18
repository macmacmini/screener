# Multi-Exchange Price Screener

Real-time price monitoring tool that compares prices across exchanges and alerts via Telegram when significant deviations are detected.

## Screeners

### 1. Unified Screener (`price_screener.py`)
Monitors ALL Lighter.xyz and Hyperliquid markets (crypto + RWA) in one process.
Each exchange is compared against its own index/oracle price - no cross-exchange
symbol mapping needed.

- Lighter: last trade vs Lighter's own index price (~185 markets, 1 bulk call)
- Hyperliquid main dex: bid/ask vs own oracle (~127 crypto markets, 1 call)
- Hyperliquid xyz dex: bid/ask vs own oracle (~86 RWA markets, 1 call)
- 3 API calls per scan, auto-blacklisting, 2-poll confirmation

```bash
python price_screener.py
```

### 2. QFEX Screener (`price_screener_qfex.py`)
Monitors QFEX perpetual markets (stocks, commodities, forex, indices) via public websocket.

- Monitors all ~138 QFEX markets (NVDA, TSLA, GOLD, EUR-USD, US500, etc.)
- Compares executed trade prices vs QFEX's own underlier (oracle) price
- Underlier source shown in alerts: `external` (market hours) / `internal` (order-book derived, off hours)
- Websocket feed (wss://mds.qfex.com), no API key needed; stale prices are skipped

```bash
python price_screener_qfex.py
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Create Telegram Bot

1. Open Telegram and search for [@BotFather](https://t.me/botfather)
2. Send `/newbot` and follow the instructions
3. Copy the **Bot Token**
4. Send a message to your new bot
5. Get your Chat ID from: `https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates`

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` (only Telegram credentials - not synced via git):

```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

## Configuration

### config.json (synced via git)

All settings except Telegram credentials:

```json
{
  "default_threshold": 4.0,
  "poll_interval_lighter": 1.5,
  "poll_interval_hyperliquid": 2.5,
  "poll_interval_qfex": 1,
  "min_volume_lighter": 10000,
  "symbol_blacklist": ["SYMBOL1", "SYMBOL2"],
  "custom_thresholds": {
    "BTC": 0.3,
    "ETH": 0.4
  }
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `default_threshold` | 4.0 | Alert threshold percentage |
| `poll_interval_lighter` | 1.5 | Seconds between Lighter scans. Limit: 60 req/min per IP; 1.5s = 40/min (67%), leaving headroom for recent_trades validation calls |
| `poll_interval_hyperliquid` | 2.5 | Seconds between Hyperliquid scans (main + xyz = 2 calls x 20 weight). Limit: 1200 weight/min per IP; 2.5s = 960/min (80%) |
| `poll_interval_qfex` | 1 | Seconds between QFEX scans (websocket-fed, no REST calls, safe to keep at 1) |
| `min_volume_lighter` | 10000 | Minimum 24h USD volume for Lighter markets; filters out dead markets whose stale last trade sits far from index (Hyperliquid equivalents are hardcoded: 150k main / 50k xyz) |
| `symbol_blacklist` | [] | Symbols to ignore |
| `custom_thresholds` | {} | Per-symbol thresholds |

## Files

- `price_screener.py` - Unified screener: all Lighter + Hyperliquid markets (crypto + RWA)
- `price_screener_qfex.py` - QFEX screener (trades vs underlier price, websocket-based)
- `config.json` - Blacklist and custom thresholds
- `requirements.txt` - Python dependencies
- `.env` - Environment configuration

## Running in Background

**Linux/Mac:**
```bash
nohup python price_screener.py > screener.log 2>&1 &
nohup python price_screener_qfex.py > screener_qfex.log 2>&1 &
```

**Windows (Task Scheduler):**
```bash
pythonw price_screener.py
pythonw price_screener_qfex.py
```

## License

This project is provided as-is for educational and personal use.
