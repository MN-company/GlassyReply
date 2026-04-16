# GlassyReply

GlassyReply is a Telegram bot for one-user Gmail triage and AI-assisted replies.

It watches Gmail, forwards each new inbox message to Telegram, streams a Gemini draft reply, and lets you send, save draft, trash, star, label, forward, and download attachments from inline keyboards. It also exposes a public landing page, a private Telegram-signed config dashboard, and an optional multilayer tracking pipeline for outbound HTML replies.

## Architecture

```text
                +----------------------+
                |      Gmail API       |
                +----------+-----------+
                           |
          push wake-up + polling fallback + actions
                           |
+-------------+     +-----v-------------------+      +------------------+
| Telegram App | <-> | GlassyReply Python bot | <--> | Gemini API       |
+-------------+     | tg_email.py             |      +------------------+
                    | SQLite state + Quart    |
                    | /landing + dashboard    |
                    | /track/* self-hosted    |
                    +-------------------------+
```

## What changed in this hardening pass

- SQLite replaced in-memory bot state.
- Gmail API calls now self-heal on `401/403` by rebuilding the client and retrying once.
- Telegram access is locked to the claimed owner user ID.
- Configuration is centralized in `Config.from_env()`.
- Runtime config is persisted in SQLite and edited from the dashboard, not by hand in `.env`.
- The first Telegram `/start` can claim the bot owner, and `/setup` can finish most of the configuration in-chat.
- Bot runtime supports `--mode polling` and `--mode webhook`.
- Docker, Docker Compose, Fly.io config, tests, and first-run docs were added.
- Pixel tracking moved from a single raw image URL to a signed multilayer bundle:
  - primary `img` beacon
  - CSS background-image beacon
  - dark-mode CSS beacon
  - experimental font beacon

## Requirements

- Python 3.11+ recommended
- Google OAuth Web client credentials for Gmail
- Telegram bot token
- Optional bootstrap owner Telegram user ID
- Optional bootstrap Gemini API key
- Optional: Fly.io CLI for bot deployment

## Quick start

### 1. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create your environment file

```bash
cp .env.example .env
```

Required values:

- `TELEGRAM_BOT_TOKEN`

Optional bootstrap values:

- `TELEGRAM_CHAT_ID`
- `GOOGLE_API_KEY`
- `GOOGLE_OAUTH_CREDENTIALS_JSON`
- `GOOGLE_OAUTH_TOKEN_JSON`

If you leave them empty, the bot can still start. Then:

- the first Telegram user who sends `/start` claims ownership
- `/setup` guides the rest of the configuration

Recommended local paths:

- `DATA_DIR=./data`
- `STATE_DB_PATH=./data/state.db`
- `GMAIL_TOKEN_PATH=./data/token.json`
- `GMAIL_CREDENTIALS_PATH=./data/credentials.json`

Optional runtime values:

- `ENABLE_PIXEL=true`
- `PIXEL_WEBHOOK_SECRET=shared-secret`
- `PIXEL_BASE_URL=https://another-host.example.com` only if you want an external tracker instead of the built-in Fly routes
- `GMAIL_PUSH_TOPIC=projects/my-project/topics/glassyreply-mail`
- `GMAIL_PUSH_WEBHOOK_SECRET=a-random-secret-used-in-the-push-url`
- `TIMEZONE=Europe/Rome`
- `TELEGRAM_WEBHOOK_URL=https://your-public-host`
- `TELEGRAM_WEBHOOK_SECRET=telegram-secret`

### 3. Configure from Telegram

Start the bot:

```bash
python3 tg_email.py --mode polling
```

Then in Telegram:

1. Send `/start` to claim the bot owner.
2. Send `/setup` to see the next setup step only.
3. Set `PUBLIC_BASE_URL` if the bot runs on Fly or another public host.
4. Set the Gemini key from the bot or with `/set google_api_key ...`.
5. Upload the Google OAuth Web client JSON from the setup flow.
6. Tap `Connect Gmail` or use `/gmail_login`.

### 4. Google OAuth for Gmail

For the bot-based Gmail login you need a Google OAuth client of type `Web application`, not `Desktop app`.

The redirect URI must include:

- `https://your-domain.example/oauth/google/callback`

or on Fly:

- `https://glassyreply-bot.fly.dev/oauth/google/callback`

After you upload the JSON and tap `Connect Gmail`, Google redirects back to the bot callback URL and the refresh token is stored automatically.

If you see `(insecure_transport) OAuth 2 MUST utilize https`, the bot is still building the callback with a non-HTTPS public base URL. On Fly, `PUBLIC_BASE_URL` must be the exact `https://...fly.dev` address of the app.

What happens:

1. The bot generates a Google authorization URL.
2. You log in with Google in the browser.
3. Google redirects back to `/oauth/google/callback`.
4. The bot stores the token in SQLite and `/app/data/token.json`.
5. Future runs refresh tokens automatically.

You can still bootstrap remote hosts with `GOOGLE_OAUTH_TOKEN_JSON` if you already have a valid token, but it is no longer the only path.

### 5. Open the dashboard

After the bot is running, send `/setup`, `/config`, or `/dashboard` to the Telegram bot.

It replies with a signed link to the private dashboard where you can:

- inspect which settings are saved in SQLite
- change runtime settings without editing `.env`
- keep the live bot config aligned across restarts

The link is signed and time-limited, so you can ask the bot for a fresh one whenever you need it.

## Running locally

### Polling mode

```bash
python3 tg_email.py --mode polling
```

`--interval` and `--lang` are optional startup overrides. The dashboard-saved values win for normal operation.

### Webhook mode

Use webhook mode only when the bot is reachable from Telegram:

```bash
python3 tg_email.py --mode webhook
```

You must set:

- `TELEGRAM_WEBHOOK_URL`
- `TELEGRAM_WEBHOOK_SECRET`

## Docker

Build:

```bash
docker build -t glassyreply .
```

Run:

```bash
docker run --rm \
  --env-file .env \
  -p 8080:8080 \
  -v "$(pwd)/data:/app/data" \
  glassyreply
```

Or with Compose:

```bash
docker compose up --build
```

## Fly.io deployment

The bot is designed to run on Fly in polling mode with a persistent volume mounted at `/app/data`.

### 1. Create the app and volume

```bash
flyctl apps create glassyreply-bot
flyctl volumes create data --size 1 --region fra --app glassyreply-bot
```

If the name is taken, pick a close variant and update [`fly.toml`](/Users/mnbrain/GlassyReply/fly.toml).

### 2. Set secrets

```bash
flyctl secrets set \
  TELEGRAM_BOT_TOKEN=... \
  --app glassyreply-bot
```

Optional bootstrap secrets you can add now or later from the bot:

- `TELEGRAM_CHAT_ID`
- `GOOGLE_API_KEY`
- `GOOGLE_OAUTH_CREDENTIALS_JSON`
- `GOOGLE_OAUTH_TOKEN_JSON`
- `GMAIL_PUSH_TOPIC`
- `GMAIL_PUSH_WEBHOOK_SECRET`
- pixel-related secrets

You can leave most runtime knobs for `/setup`, `/set`, or the dashboard after the app is live.

### 3. Bootstrap `token.json`

If you prefer not to paste OAuth secrets into Fly, you can deploy first and then finish setup from Telegram:

1. Set only `TELEGRAM_BOT_TOKEN`.
2. Deploy the app.
3. Claim ownership with `/start`.
4. Run `/setup`.
5. Upload the Google OAuth Web client JSON.
6. Complete `/gmail_login` in the browser.

If you already have a valid Gmail token and want a pre-bootstrapped deploy:

1. Run the bot locally once with the same Gmail OAuth client.
2. Confirm `data/token.json` exists.
3. Seed `GOOGLE_OAUTH_TOKEN_JSON`.
4. Deploy the app.

```bash
flyctl deploy --app glassyreply-bot
```

On first boot, the container writes the token into `/app/data/token.json`. After that, restarts preserve both `state.db` and the refreshed token on the volume.

### 4. Wake on mail with Fly autosleep

`auto_stop_machines = "suspend"` is only useful for Gmail if the mailbox can wake the app with an inbound HTTP request.

GlassyReply now supports that through Gmail Push:

1. Create a Pub/Sub topic in the same Google Cloud project used by the Gmail OAuth client.
2. Grant Gmail publish access to the topic.
   Use the Gmail publisher service account: `gmail-api-push@system.gserviceaccount.com`.
3. Save `GMAIL_PUSH_TOPIC` in the bot settings or dashboard.
4. Save `GMAIL_PUSH_WEBHOOK_SECRET` in the bot settings or dashboard.
5. Create a Pub/Sub push subscription that points to:

```text
https://your-fly-app.fly.dev/gmail/push?secret=YOUR_SECRET
```

For the default Fly app name in this repo, that becomes:

```text
https://glassyreply-bot.fly.dev/gmail/push?secret=YOUR_SECRET
```

Recommended topic example:

```text
projects/my-project/topics/glassyreply-mail
```

Notes:

- Gmail `users.watch` is renewed automatically by the bot while it is awake.
- Gmail history IDs are used to recover the exact new messages instead of trusting the webhook payload alone.
- If Gmail Push is not configured, the bot falls back to polling, so Fly suspend will not wake it on new mail.
- Gmail history IDs are usually valid for about a week, so if the app stays completely idle for many days, refresh the watch from Telegram settings before relying on autosleep again.

### 5. Health check

Fly checks:

- `GET /healthz`
- `GET /` public landing page
- `GET /dashboard?token=...` private dashboard
- `GET /oauth/google/callback` Google OAuth callback

## Pixel tracker

Pixel tracking is optional and intentionally classified as telemetry, not truth.

Telegram stats now distinguish between:

- proxy fetches
- probable user opens
- probable reopens
- confidence score + layer used for the latest useful signal

Readable timestamps use the configured `TIMEZONE` and fall back to a language-based default when the timezone is unset.

### Recommended setup: self-hosted on Fly

The bot can now serve the tracking assets directly from the same Fly app:

- `/track/img/...`
- `/track/bg/...`
- `/track/dark/...`
- `/track/font/...`

That means the simplest production setup is:

- enable the pixel in Telegram settings
- set `PIXEL_WEBHOOK_SECRET`
- leave `PIXEL_BASE_URL` empty

When `PIXEL_BASE_URL` is empty, GlassyReply automatically uses `PUBLIC_BASE_URL`, so the pixel stays fully proprietary on your Fly deployment.

### Why multilayer?

- Gmail image proxy still fetches images, but it is a proxy signal.
- Apple Mail Privacy Protection can preload remote assets before a human open.
- CSS background assets still provide extra evidence in clients that resolve them.
- Font fetches are niche and diagnostic only.

Research notes: [docs/pixel-tracker-research.md](/Users/mnbrain/GlassyReply/docs/pixel-tracker-research.md)

### Self-hosted smoke check

Once Fly is live, the built-in tracker is served by the same app under:

```bash
https://glassyreply-bot.fly.dev/track/img/2x1/<signed-token>.png
https://glassyreply-bot.fly.dev/track/font/<signed-token>.woff2
```

The token must be signed by the bot, so the easiest real test is still:

1. enable the pixel
2. create `Email Tracciata`
3. open/send the draft from Gmail
4. watch `Stats` in Telegram update

Tracked draft note:

- outbound tracked emails now start as clean Gmail drafts without active remote beacons
- finish editing and let Gmail save the draft
- then press `📨 Invia con tracking` in Telegram
- this avoids counting your own Gmail draft editing as an open event

### Optional Cloudflare Worker mode

Cloudflare is no longer required. The Worker remains optional if you specifically want a separate edge-hosted tracker.

### Local worker dev

```bash
npx wrangler dev \
  --ip 127.0.0.1 \
  --port 8790 \
  --var PIXEL_WEBHOOK_URL:http://127.0.0.1:8788/pixel_status \
  --var PIXEL_WEBHOOK_SECRET:probe-secret
```

### Independent pixel lab

Start a webhook capture server:

```bash
python3 scripts/pixel_smoke_test.py capture \
  --host 127.0.0.1 \
  --port 8788 \
  --secret probe-secret
```

Generate a standalone fixture:

```bash
python3 scripts/pixel_smoke_test.py bundle \
  --base-url http://127.0.0.1:8790 \
  --secret probe-secret \
  --tg-id 424242
```

This gives you:

- a standalone HTML file
- direct `curl` probes
- signed image / CSS / font URLs

### Cloudflare Worker deploy

```bash
npx wrangler deploy
```

Set worker secrets first:

```bash
wrangler secret put PIXEL_WEBHOOK_SECRET
wrangler secret put PIXEL_WEBHOOK_URL
```

## Tests

Python unit tests:

```bash
python3 -m unittest discover -s tests -v
```

Worker bundle check:

```bash
npm run worker:check
```

Browser-based pixel smoke:

```bash
playwright screenshot -b chromium \
  file:///absolute/path/to/output/pixel-smoke/pixel-fixture.html \
  output/playwright/pixel-smoke.png
```

## Security notes

- Only the configured Telegram owner can use bot handlers.
- `PIXEL_WEBHOOK_SECRET` is checked before parsing pixel webhook JSON.
- `GMAIL_PUSH_WEBHOOK_SECRET` is checked before parsing Gmail Pub/Sub JSON.
- Pixel tokens are signed.
- The same Fly app can now serve the signed tracking assets directly.
- Gmail state and pending Telegram follow-ups persist in SQLite.

## Files worth knowing

- [tg_email.py](/Users/mnbrain/GlassyReply/tg_email.py): bot runtime, Quart server, SQLite state, Gmail wrapper
- [src/index.ts](/Users/mnbrain/GlassyReply/src/index.ts): Cloudflare Worker pixel tracker
- [scripts/pixel_smoke_test.py](/Users/mnbrain/GlassyReply/scripts/pixel_smoke_test.py): independent pixel lab helpers
- [docs/pixel-tracker-research.md](/Users/mnbrain/GlassyReply/docs/pixel-tracker-research.md): current tracking constraints and strategy
