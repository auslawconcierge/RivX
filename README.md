# RivX AutoTrader

Autonomous AI trading bot. Claude is the brain.
Runs 24/7 on Render. Trades US stocks and crypto autonomously overnight.

## The daily flow

| Time (AEST) | What happens |
|---|---|
| 9:00pm | RivX analyses market, sends you tonight's plan on Telegram |
| 9:00-10:00pm | You reply YES or NO (auto-approves after 1 hour) |
| 11:30pm-6am | Bot trades autonomously while you sleep |
| Every 2 mins | Intraday checks — takes profits, adapts to news |
| Every 5 mins | Crypto checks — 24/7 BTC/ETH monitoring |
| 6:30am | Morning summary sent to Telegram with overnight results |

## Portfolio

| Tier | % | Amount | Assets |
|---|---|---|---|
| Conservative | 50% | $2,500 | SPY, QQQ |
| Moderate | 30% | $1,500 | NVDA, TSLA, META |
| High risk | 20% | $1,000 | BTC, ETH |

## Setup

### 1. Create GitHub repo
- Create a new **private** repo called `rivx-autotrader`
- Push all this code into it

### 2. Add Supabase tables
In your OceanaSydney Supabase project → SQL Editor → run:

```sql
create table trades (
  id bigserial primary key, symbol text, action text,
  aud_amount numeric, score numeric, details text,
  raw_signals jsonb, order_id text, order_status text,
  pnl_pct numeric, created_at timestamptz default now()
);
create table positions (
  id bigserial primary key, symbol text, entry_price numeric,
  exit_price numeric, aud_amount numeric, market text,
  status text default 'open', pnl_pct numeric,
  created_at timestamptz default now(), closed_at timestamptz
);
create table signal_weights (
  id bigserial primary key, rsi numeric default 0.2,
  macd numeric default 0.2, bollinger numeric default 0.2,
  volume numeric default 0.2, ma_cross numeric default 0.2,
  updated_at timestamptz default now()
);
create table snapshots (
  id bigserial primary key, date date unique,
  total_aud numeric, day_pnl numeric, total_pnl numeric
);
create table approved_plan (
  id bigserial primary key, plan text,
  updated_at timestamptz default now()
);
insert into signal_weights (rsi, macd, bollinger, volume, ma_cross)
values (0.2, 0.2, 0.2, 0.2, 0.2);
```

### 3. Deploy on Render
- Go to render.com → New → Web Service
- Connect your GitHub repo
- Render detects render.yaml automatically
- Add all environment variables (see below)
- Deploy

### 4. Environment variables to add in Render

```
PAPER_MODE          = true
ALPACA_API_KEY      = your alpaca paper key
ALPACA_SECRET_KEY   = your alpaca paper secret
COINSPOT_API_KEY    = your coinspot key
COINSPOT_SECRET_KEY = your coinspot secret
SUPABASE_URL        = https://xxxx.supabase.co
SUPABASE_API_KEY    = your supabase anon key
TELEGRAM_TOKEN      = your bot token from BotFather
TELEGRAM_CHAT_ID    = your chat id from userinfobot
ANTHROPIC_API_KEY   = your anthropic key
```

### 5. Paper trade for 3 nights
Watch it run in paper mode. Check the morning summaries.
When you're happy, change PAPER_MODE to false in Render,
fund Alpaca via Wise, and go live.

## Kill switch
Send **STOP ALL** to RivX_trading_bot on Telegram at any time.
Bot checks for this message every 30 seconds and halts immediately.

## Going live
1. Change PAPER_MODE to false in Render environment variables
2. Transfer AUD to Wise → convert to USD → send to Alpaca
3. Top up CoinSpot with AUD via PayID
4. Redeploy on Render
