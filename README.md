# Multi-Exchange Price Screener

Real-time price monitoring tool that compares prices across exchanges and alerts via Telegram when significant deviations are detected.

## Screeners

### 1. Crypto Screener (`price_screener_binance.py`)
Monitors cryptocurrency markets on Lighter.xyz and Hyperliquid against Binance Futures mark prices.

- Compares Lighter last trade prices vs Binance mark prices
- Compares Hyperliquid bid/ask vs Binance mark prices
- Uses bulk API endpoints (only 3 API calls per scan)
- Auto-blacklisting for markets with repeated alerts

```bash
python price_screener_binance.py
```

### 2. RWA Screener (`price_screener_rwa.py`)
Monitors Real World Asset markets (stocks, commodities, forex) using Hyperliquid oracle prices.

- Monitors: TSLA, AAPL, GOOGL, NVDA, XAU, EURUSD, etc.
- Compares Lighter prices vs Hyperliquid oracle
- Compares Hyperliquid xyz bid/ask vs oracle

```bash
python price_screener_rwa.py
```

### 3. QFEX Screener (`price_screener_qfex.py`)
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
  "poll_interval": 1,
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
| `poll_interval` | 1 | Seconds between scans |
| `symbol_blacklist` | [] | Symbols to ignore |
| `custom_thresholds` | {} | Per-symbol thresholds |

## Files

- `price_screener_binance.py` - Crypto screener (Lighter + Hyperliquid vs Binance)
- `price_screener_rwa.py` - RWA screener (stocks, commodities, forex)
- `price_screener_qfex.py` - QFEX screener (trades vs underlier price)
- `config.json` - Blacklist and custom thresholds
- `requirements.txt` - Python dependencies
- `.env` - Environment configuration

## Running in Background

**Linux/Mac:**
```bash
nohup python price_screener_binance.py > screener.log 2>&1 &
```

**Windows (Task Scheduler):**
```bash
pythonw price_screener_binance.py
```

## License

This project is provided as-is for educational and personal use.
