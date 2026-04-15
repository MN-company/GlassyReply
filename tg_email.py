from __future__ import annotations

import argparse
import asyncio
import base64
import email
import hashlib
import hmac
import html as ihtml
import json
import logging
import os
import re
import signal
import sqlite3
import threading
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.utils import parseaddr
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from uuid import uuid4

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.generativeai as genai
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig
from quart import Quart, jsonify, request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

LOGGER = logging.getLogger("glassyreply")

AI_MODEL = "gemini-1.5-flash"
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.modify",
]
MAX_CHARS = 20_000
TELEGRAM_MAX = 4_000
PAGE_SIZE = 30
STATE_RETENTION_DAYS = 30
LAST_SEEN_KEY = "last_seen_gmail_message_id"
GOOGLE_OAUTH_STATE_KEY = "google_oauth_pending_state"
GMAIL_INITIAL_SYNC_KEY = "gmail_initial_sync_pending"
PREDEF_FWD = ["redazione@example.com", "boss@example.com"]
DEFAULT_PROMPT = (
    "Sei un assistente professionale. Scrivi una risposta "
    "educata, chiara e concisa all'e-mail seguente. Includi saluti "
    "e firma se opportuno."
)
DASHBOARD_TOKEN_TTL_SECONDS = 24 * 60 * 60
TRACKED_DRAFT_RECIPIENT_KEY = "tracked_draft_recipient"
TRACKED_DRAFT_SUBJECT_KEY = "tracked_draft_subject"
MENU_TRACKED_EMAIL = "Email Tracciata"
MENU_STATS = "Stats"
MENU_SETTINGS = "Impostazioni"
GMAIL_MONITOR_LABEL_CHOICES = [
    ("INBOX", "Inbox"),
    ("CATEGORY_PERSONAL", "Primaria"),
    ("CATEGORY_PROMOTIONS", "Promozioni"),
    ("CATEGORY_SOCIAL", "Social"),
    ("CATEGORY_UPDATES", "Aggiornamenti"),
    ("CATEGORY_FORUMS", "Forum"),
    ("IMPORTANT", "Importanti"),
    ("STARRED", "Speciali"),
]


class ConfigError(RuntimeError):
    pass


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_bool(raw: str | None, default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    return int(raw)


def parse_list(raw: str | None, default: List[str]) -> List[str]:
    if raw is None:
        return list(default)
    raw = raw.strip()
    if not raw:
        return list(default)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            items = [str(item).strip() for item in parsed if str(item).strip()]
            return items
    except json.JSONDecodeError:
        pass
    items = [item.strip() for item in re.split(r"[,\n]+", raw) if item.strip()]
    return items or list(default)


@dataclass(slots=True)
class Config:
    bot_token: str
    chat_id: int
    google_api_key: str
    public_base_url: str
    data_dir: Path
    state_db_path: Path
    gmail_token_path: Path
    gmail_credentials_path: Path
    google_oauth_credentials_json: str
    google_oauth_token_json: str
    enable_pixel: bool
    pixel_base_url: str
    pixel_webhook_secret: str
    pixel_webhook_url: str
    host: str
    port: int
    watch_interval: int
    lang: str
    ai_model: str
    system_prompt: str
    gmail_monitor_labels: List[str]
    predef_fwd: List[str]
    state_retention_days: int
    telegram_webhook_url: str
    telegram_webhook_secret: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        load_dotenv()
        source = dict(os.environ if env is None else env)

        bot_token = source.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id_raw = source.get("TELEGRAM_CHAT_ID", "").strip()
        google_api_key = source.get("GOOGLE_API_KEY", "").strip()
        fly_app_name = source.get("FLY_APP_NAME", "").strip()
        if chat_id_raw:
            try:
                chat_id = int(chat_id_raw)
            except ValueError as exc:
                raise ConfigError("TELEGRAM_CHAT_ID must be integer") from exc
        else:
            chat_id = 0

        public_base_url = source.get("PUBLIC_BASE_URL", "").strip()
        if not public_base_url:
            public_base_url = (
                f"https://{fly_app_name}.fly.dev" if fly_app_name else "http://127.0.0.1:8080"
            )
        data_dir = Path(source.get("DATA_DIR") or (Path.cwd() / "data"))
        state_db_path = Path(source.get("STATE_DB_PATH") or (data_dir / "state.db"))
        gmail_token_path = Path(source.get("GMAIL_TOKEN_PATH") or (data_dir / "token.json"))
        gmail_credentials_path = Path(
            source.get("GMAIL_CREDENTIALS_PATH") or (data_dir / "credentials.json")
        )
        google_oauth_credentials_json = source.get("GOOGLE_OAUTH_CREDENTIALS_JSON", "").strip()
        google_oauth_token_json = source.get("GOOGLE_OAUTH_TOKEN_JSON", "").strip()
        enable_pixel = parse_bool(source.get("ENABLE_PIXEL"), default=False)
        pixel_base_url = source.get("PIXEL_BASE_URL", "").strip().rstrip("/")
        pixel_webhook_secret = source.get("PIXEL_WEBHOOK_SECRET", "").strip()
        pixel_webhook_url = source.get("PIXEL_WEBHOOK_URL", "").strip()
        host = source.get("HOST", "0.0.0.0").strip() or "0.0.0.0"
        port_raw = source.get("PORT", "8080").strip()
        watch_interval_raw = source.get("WATCH_INTERVAL", "15").strip()
        lang = source.get("LANG", "it").strip() or "it"
        ai_model = source.get("AI_MODEL", AI_MODEL).strip() or AI_MODEL
        system_prompt = source.get("SYSTEM_PROMPT", DEFAULT_PROMPT).strip() or DEFAULT_PROMPT
        gmail_monitor_labels = parse_list(source.get("GMAIL_MONITOR_LABELS"), ["INBOX"])
        predef_fwd = parse_list(source.get("PREDEF_FWD"), PREDEF_FWD)
        state_retention_days_raw = source.get("STATE_RETENTION_DAYS", str(STATE_RETENTION_DAYS)).strip()
        telegram_webhook_url = source.get("TELEGRAM_WEBHOOK_URL", "").strip()
        telegram_webhook_secret = source.get("TELEGRAM_WEBHOOK_SECRET", "").strip()

        if not bot_token:
            raise ConfigError("Missing TELEGRAM_BOT_TOKEN")
        try:
            port = int(port_raw)
        except ValueError as exc:
            raise ConfigError("PORT must be integer") from exc
        if enable_pixel and not pixel_base_url:
            raise ConfigError("ENABLE_PIXEL=true requires PIXEL_BASE_URL")
        if enable_pixel and not pixel_webhook_secret:
            raise ConfigError("ENABLE_PIXEL=true requires PIXEL_WEBHOOK_SECRET")
        try:
            watch_interval = int(watch_interval_raw)
        except ValueError as exc:
            raise ConfigError("WATCH_INTERVAL must be integer") from exc
        try:
            state_retention_days = int(state_retention_days_raw)
        except ValueError as exc:
            raise ConfigError("STATE_RETENTION_DAYS must be integer") from exc

        return cls(
            bot_token=bot_token,
            chat_id=chat_id,
            google_api_key=google_api_key,
            public_base_url=public_base_url,
            data_dir=data_dir,
            state_db_path=state_db_path,
            gmail_token_path=gmail_token_path,
            gmail_credentials_path=gmail_credentials_path,
            google_oauth_credentials_json=google_oauth_credentials_json,
            google_oauth_token_json=google_oauth_token_json,
            enable_pixel=enable_pixel,
            pixel_base_url=pixel_base_url,
            pixel_webhook_secret=pixel_webhook_secret,
            pixel_webhook_url=pixel_webhook_url,
            host=host,
            port=port,
            watch_interval=watch_interval,
            lang=lang,
            ai_model=ai_model,
            system_prompt=system_prompt,
            gmail_monitor_labels=gmail_monitor_labels or ["INBOX"],
            predef_fwd=predef_fwd,
            state_retention_days=state_retention_days,
            telegram_webhook_url=telegram_webhook_url,
            telegram_webhook_secret=telegram_webhook_secret,
        )

    def ensure_storage(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.gmail_token_path.parent.mkdir(parents=True, exist_ok=True)
        self.gmail_credentials_path.parent.mkdir(parents=True, exist_ok=True)

    def materialize_google_credentials(self) -> None:
        self.ensure_storage()
        if not self.google_oauth_credentials_json:
            return
        payload = json.loads(self.google_oauth_credentials_json)
        content = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        current = self.gmail_credentials_path.read_text() if self.gmail_credentials_path.exists() else ""
        if current != content:
            self.gmail_credentials_path.write_text(content)

    def materialize_gmail_token(self) -> None:
        self.ensure_storage()
        if not self.google_oauth_token_json:
            return
        payload = json.loads(self.google_oauth_token_json)
        content = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        current = self.gmail_token_path.read_text() if self.gmail_token_path.exists() else ""
        if current != content:
            self.gmail_token_path.write_text(content)

    def validate_mode(self, mode: str) -> None:
        self.validate_effective(mode)

    def validate_effective(self, mode: str | None = None) -> None:
        if self.chat_id < 0:
            raise ConfigError("TELEGRAM_CHAT_ID must be >= 0")
        if self.port <= 0:
            raise ConfigError("PORT must be > 0")
        if self.watch_interval <= 0:
            raise ConfigError("WATCH_INTERVAL must be > 0")
        if self.state_retention_days <= 0:
            raise ConfigError("STATE_RETENTION_DAYS must be > 0")
        if not self.gmail_monitor_labels:
            raise ConfigError("GMAIL_MONITOR_LABELS must contain at least one label")
        if self.enable_pixel and not self.pixel_base_url:
            raise ConfigError("ENABLE_PIXEL=true requires PIXEL_BASE_URL")
        if self.enable_pixel and not self.pixel_webhook_secret:
            raise ConfigError("ENABLE_PIXEL=true requires PIXEL_WEBHOOK_SECRET")
        if mode == "webhook":
            if not self.telegram_webhook_secret:
                raise ConfigError("Webhook mode requires TELEGRAM_WEBHOOK_SECRET")
            if not self.telegram_webhook_url:
                raise ConfigError("Webhook mode requires TELEGRAM_WEBHOOK_URL")

    def resolved_telegram_webhook_url(self) -> str:
        if self.telegram_webhook_url.endswith("/telegram/webhook"):
            return self.telegram_webhook_url
        return self.telegram_webhook_url.rstrip("/") + "/telegram/webhook"

    def resolved_public_base_url(self) -> str:
        return self.public_base_url.rstrip("/")

    def with_overrides(self, overrides: Mapping[str, str]) -> "Config":
        if not overrides:
            return self

        data = {
            "chat_id": self.chat_id,
            "google_api_key": self.google_api_key,
            "enable_pixel": self.enable_pixel,
            "pixel_base_url": self.pixel_base_url,
            "pixel_webhook_secret": self.pixel_webhook_secret,
            "pixel_webhook_url": self.pixel_webhook_url,
            "host": self.host,
            "port": self.port,
            "watch_interval": self.watch_interval,
            "lang": self.lang,
            "ai_model": self.ai_model,
            "system_prompt": self.system_prompt,
            "gmail_monitor_labels": list(self.gmail_monitor_labels),
            "predef_fwd": list(self.predef_fwd),
            "state_retention_days": self.state_retention_days,
            "public_base_url": self.public_base_url,
            "google_oauth_credentials_json": self.google_oauth_credentials_json,
            "google_oauth_token_json": self.google_oauth_token_json,
            "telegram_webhook_url": self.telegram_webhook_url,
            "telegram_webhook_secret": self.telegram_webhook_secret,
        }

        if "TELEGRAM_CHAT_ID" in overrides:
            data["chat_id"] = parse_int(overrides["TELEGRAM_CHAT_ID"], data["chat_id"])
        if "GOOGLE_API_KEY" in overrides:
            data["google_api_key"] = overrides["GOOGLE_API_KEY"].strip()
        if "PUBLIC_BASE_URL" in overrides:
            data["public_base_url"] = overrides["PUBLIC_BASE_URL"].strip() or data["public_base_url"]
        if "GOOGLE_OAUTH_CREDENTIALS_JSON" in overrides:
            data["google_oauth_credentials_json"] = overrides["GOOGLE_OAUTH_CREDENTIALS_JSON"].strip()
        if "GOOGLE_OAUTH_TOKEN_JSON" in overrides:
            data["google_oauth_token_json"] = overrides["GOOGLE_OAUTH_TOKEN_JSON"].strip()
        if "ENABLE_PIXEL" in overrides:
            data["enable_pixel"] = parse_bool(overrides["ENABLE_PIXEL"], data["enable_pixel"])
        if "PIXEL_BASE_URL" in overrides:
            data["pixel_base_url"] = overrides["PIXEL_BASE_URL"].strip()
        if "PIXEL_WEBHOOK_SECRET" in overrides:
            data["pixel_webhook_secret"] = overrides["PIXEL_WEBHOOK_SECRET"].strip()
        if "PIXEL_WEBHOOK_URL" in overrides:
            data["pixel_webhook_url"] = overrides["PIXEL_WEBHOOK_URL"].strip()
        if "HOST" in overrides:
            data["host"] = overrides["HOST"].strip() or data["host"]
        if "PORT" in overrides:
            data["port"] = parse_int(overrides["PORT"], data["port"])
        if "WATCH_INTERVAL" in overrides:
            data["watch_interval"] = parse_int(overrides["WATCH_INTERVAL"], data["watch_interval"])
        if "LANG" in overrides:
            data["lang"] = overrides["LANG"].strip() or data["lang"]
        if "AI_MODEL" in overrides:
            data["ai_model"] = overrides["AI_MODEL"].strip() or data["ai_model"]
        if "SYSTEM_PROMPT" in overrides:
            data["system_prompt"] = overrides["SYSTEM_PROMPT"].strip() or data["system_prompt"]
        if "GMAIL_MONITOR_LABELS" in overrides:
            data["gmail_monitor_labels"] = parse_list(
                overrides["GMAIL_MONITOR_LABELS"], data["gmail_monitor_labels"]
            ) or ["INBOX"]
        if "PREDEF_FWD" in overrides:
            data["predef_fwd"] = parse_list(overrides["PREDEF_FWD"], data["predef_fwd"])
        if "STATE_RETENTION_DAYS" in overrides:
            data["state_retention_days"] = parse_int(
                overrides["STATE_RETENTION_DAYS"], data["state_retention_days"]
            )
        if "TELEGRAM_WEBHOOK_URL" in overrides:
            data["telegram_webhook_url"] = overrides["TELEGRAM_WEBHOOK_URL"].strip()
        if "TELEGRAM_WEBHOOK_SECRET" in overrides:
            data["telegram_webhook_secret"] = overrides["TELEGRAM_WEBHOOK_SECRET"].strip()

        return replace(self, **data)


@dataclass(slots=True)
class EmailState:
    tg_message_id: int
    gmail_message_id: str
    gmail_thread_id: str
    sender: str
    subject: str
    body: str
    header: str
    attachments: List[dict]
    starred: bool
    lang: str
    ai_body: str = ""
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "EmailState":
        return cls(
            tg_message_id=row["tg_message_id"],
            gmail_message_id=row["gmail_message_id"],
            gmail_thread_id=row["gmail_thread_id"] or "",
            sender=row["sender"] or "",
            subject=row["subject"] or "",
            body=row["body"] or "",
            header=row["header"] or "",
            attachments=json.loads(row["attachments_json"] or "[]"),
            starred=bool(row["starred"]),
            lang=row["lang"] or "it",
            ai_body=row["ai_body"] or "",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
        )


@dataclass(slots=True)
class TrackedEmail:
    tg_message_id: int
    draft_id: str
    recipient: str
    subject: str
    open_count: int
    first_opened_at: str
    last_opened_at: str
    last_classification: str
    last_layer: str
    last_dimensions: str
    last_confidence: float | None
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TrackedEmail":
        confidence = row["last_confidence"]
        return cls(
            tg_message_id=row["tg_message_id"],
            draft_id=row["draft_id"] or "",
            recipient=row["recipient"] or "",
            subject=row["subject"] or "",
            open_count=row["open_count"] or 0,
            first_opened_at=row["first_opened_at"] or "",
            last_opened_at=row["last_opened_at"] or "",
            last_classification=row["last_classification"] or "",
            last_layer=row["last_layer"] or "",
            last_dimensions=row["last_dimensions"] or "",
            last_confidence=float(confidence) if confidence is not None else None,
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
        )


@dataclass(slots=True)
class PendingAction:
    prompt_message_id: int
    root_tg_message_id: int
    action_kind: str
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "PendingAction":
        return cls(
            prompt_message_id=row["prompt_message_id"],
            root_tg_message_id=row["root_tg_message_id"],
            action_kind=row["action_kind"],
            created_at=row["created_at"],
        )


@dataclass(slots=True)
class InteractivePrompt:
    prompt_message_id: int
    action_kind: str
    created_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "InteractivePrompt":
        return cls(
            prompt_message_id=row["prompt_message_id"],
            action_kind=row["action_kind"],
            created_at=row["created_at"],
        )


class StateStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS email_state (
                    tg_message_id INTEGER PRIMARY KEY,
                    gmail_message_id TEXT NOT NULL,
                    gmail_thread_id TEXT,
                    sender TEXT,
                    subject TEXT,
                    body TEXT,
                    header TEXT,
                    attachments_json TEXT NOT NULL,
                    starred INTEGER NOT NULL DEFAULT 0,
                    lang TEXT NOT NULL,
                    ai_body TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pending_actions (
                    prompt_message_id INTEGER PRIMARY KEY,
                    root_tg_message_id INTEGER NOT NULL,
                    action_kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(root_tg_message_id) REFERENCES email_state(tg_message_id)
                );

                CREATE TABLE IF NOT EXISTS interactive_prompts (
                    prompt_message_id INTEGER PRIMARY KEY,
                    action_kind TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tracked_emails (
                    tg_message_id INTEGER PRIMARY KEY,
                    draft_id TEXT,
                    recipient TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    open_count INTEGER NOT NULL DEFAULT 0,
                    first_opened_at TEXT,
                    last_opened_at TEXT,
                    last_classification TEXT,
                    last_layer TEXT,
                    last_dimensions TEXT,
                    last_confidence REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pixel_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_message_id INTEGER NOT NULL,
                    classification TEXT,
                    layer TEXT,
                    dimensions TEXT,
                    confidence REAL,
                    is_user_open INTEGER,
                    email_subject TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(tg_message_id) REFERENCES tracked_emails(tg_message_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_pixel_events_tg_message_id
                ON pixel_events (tg_message_id);
                """
            )

    def purge_old_rows(self, days: int = STATE_RETENTION_DAYS) -> None:
        cutoff = (utcnow() - timedelta(days=days)).isoformat()
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM pending_actions WHERE created_at < ?", (cutoff,))
            self._conn.execute("DELETE FROM interactive_prompts WHERE created_at < ?", (cutoff,))
            self._conn.execute("DELETE FROM email_state WHERE updated_at < ?", (cutoff,))
            self._conn.execute("DELETE FROM pixel_events WHERE created_at < ?", (cutoff,))
            self._conn.execute("DELETE FROM tracked_emails WHERE updated_at < ?", (cutoff,))
            self._conn.execute(
                """
                DELETE FROM pending_actions
                WHERE root_tg_message_id NOT IN (SELECT tg_message_id FROM email_state)
                """
            )

    def upsert_email_state(self, state: EmailState) -> None:
        created_at = state.created_at or utcnow_iso()
        updated_at = utcnow_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO email_state (
                    tg_message_id, gmail_message_id, gmail_thread_id, sender, subject,
                    body, header, attachments_json, starred, lang, ai_body,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tg_message_id) DO UPDATE SET
                    gmail_message_id=excluded.gmail_message_id,
                    gmail_thread_id=excluded.gmail_thread_id,
                    sender=excluded.sender,
                    subject=excluded.subject,
                    body=excluded.body,
                    header=excluded.header,
                    attachments_json=excluded.attachments_json,
                    starred=excluded.starred,
                    lang=excluded.lang,
                    ai_body=excluded.ai_body,
                    updated_at=excluded.updated_at
                """,
                (
                    state.tg_message_id,
                    state.gmail_message_id,
                    state.gmail_thread_id,
                    state.sender,
                    state.subject,
                    state.body,
                    state.header,
                    json.dumps(state.attachments),
                    int(state.starred),
                    state.lang,
                    state.ai_body,
                    created_at,
                    updated_at,
                ),
            )

    def get_email_state(self, tg_message_id: int) -> EmailState | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM email_state WHERE tg_message_id = ?",
                (tg_message_id,),
            ).fetchone()
        return EmailState.from_row(row) if row else None

    def upsert_tracked_email(self, tracked: TrackedEmail) -> None:
        created_at = tracked.created_at or utcnow_iso()
        updated_at = utcnow_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO tracked_emails (
                    tg_message_id, draft_id, recipient, subject, open_count,
                    first_opened_at, last_opened_at, last_classification,
                    last_layer, last_dimensions, last_confidence, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tg_message_id) DO UPDATE SET
                    draft_id=excluded.draft_id,
                    recipient=excluded.recipient,
                    subject=excluded.subject,
                    open_count=excluded.open_count,
                    first_opened_at=excluded.first_opened_at,
                    last_opened_at=excluded.last_opened_at,
                    last_classification=excluded.last_classification,
                    last_layer=excluded.last_layer,
                    last_dimensions=excluded.last_dimensions,
                    last_confidence=excluded.last_confidence,
                    updated_at=excluded.updated_at
                """,
                (
                    tracked.tg_message_id,
                    tracked.draft_id,
                    tracked.recipient,
                    tracked.subject,
                    tracked.open_count,
                    tracked.first_opened_at,
                    tracked.last_opened_at,
                    tracked.last_classification,
                    tracked.last_layer,
                    tracked.last_dimensions,
                    tracked.last_confidence,
                    created_at,
                    updated_at,
                ),
            )

    def get_tracked_email(self, tg_message_id: int) -> TrackedEmail | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tracked_emails WHERE tg_message_id = ?",
                (tg_message_id,),
            ).fetchone()
        return TrackedEmail.from_row(row) if row else None

    def list_tracked_emails(self, limit: int = 10) -> List[TrackedEmail]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tracked_emails ORDER BY updated_at DESC, tg_message_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [TrackedEmail.from_row(row) for row in rows]

    def record_pixel_event(
        self,
        *,
        tg_message_id: int,
        classification: str,
        layer: str,
        dimensions: str,
        confidence: float | None,
        is_user_open: bool | None,
        email_subject: str,
    ) -> TrackedEmail | None:
        tracked = self.get_tracked_email(tg_message_id)
        if tracked is None:
            return None
        event_time = utcnow_iso()
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO pixel_events (
                    tg_message_id, classification, layer, dimensions, confidence,
                    is_user_open, email_subject, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tg_message_id,
                    classification,
                    layer,
                    dimensions,
                    confidence,
                    None if is_user_open is None else int(is_user_open),
                    email_subject,
                    event_time,
                ),
            )
            first_opened_at = tracked.first_opened_at or event_time
            open_count = tracked.open_count + 1
            self._conn.execute(
                """
                UPDATE tracked_emails
                SET open_count = ?, first_opened_at = ?, last_opened_at = ?,
                    last_classification = ?, last_layer = ?, last_dimensions = ?,
                    last_confidence = ?, updated_at = ?
                WHERE tg_message_id = ?
                """,
                (
                    open_count,
                    first_opened_at,
                    event_time,
                    classification,
                    layer,
                    dimensions,
                    confidence,
                    event_time,
                    tg_message_id,
                ),
            )
        return self.get_tracked_email(tg_message_id)

    def update_ai_body(self, tg_message_id: int, ai_body: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE email_state SET ai_body = ?, updated_at = ? WHERE tg_message_id = ?",
                (ai_body, utcnow_iso(), tg_message_id),
            )

    def update_starred(self, tg_message_id: int, starred: bool) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE email_state SET starred = ?, updated_at = ? WHERE tg_message_id = ?",
                (int(starred), utcnow_iso(), tg_message_id),
            )

    def add_pending_action(self, prompt_message_id: int, root_tg_message_id: int, action_kind: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO pending_actions (prompt_message_id, root_tg_message_id, action_kind, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(prompt_message_id) DO UPDATE SET
                    root_tg_message_id=excluded.root_tg_message_id,
                    action_kind=excluded.action_kind,
                    created_at=excluded.created_at
                """,
                (prompt_message_id, root_tg_message_id, action_kind, utcnow_iso()),
            )

    def get_pending_action(self, prompt_message_id: int) -> PendingAction | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM pending_actions WHERE prompt_message_id = ?",
                (prompt_message_id,),
            ).fetchone()
        return PendingAction.from_row(row) if row else None

    def pop_pending_action(self, prompt_message_id: int) -> PendingAction | None:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM pending_actions WHERE prompt_message_id = ?",
                (prompt_message_id,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "DELETE FROM pending_actions WHERE prompt_message_id = ?",
                (prompt_message_id,),
            )
        return PendingAction.from_row(row)

    def add_interactive_prompt(self, prompt_message_id: int, action_kind: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO interactive_prompts (prompt_message_id, action_kind, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(prompt_message_id) DO UPDATE SET
                    action_kind=excluded.action_kind,
                    created_at=excluded.created_at
                """,
                (prompt_message_id, action_kind, utcnow_iso()),
            )

    def pop_interactive_prompt(self, prompt_message_id: int) -> InteractivePrompt | None:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT * FROM interactive_prompts WHERE prompt_message_id = ?",
                (prompt_message_id,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "DELETE FROM interactive_prompts WHERE prompt_message_id = ?",
                (prompt_message_id,),
            )
        return InteractivePrompt.from_row(row)

    def set_bot_state(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO bot_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, utcnow_iso()),
            )

    def get_bot_state(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM bot_state WHERE key = ?",
                (key,),
            ).fetchone()
        return row["value"] if row else None

    def set_app_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, utcnow_iso()),
            )

    def delete_app_setting(self, key: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM app_settings WHERE key = ?", (key,))

    def get_app_settings(self) -> Dict[str, str]:
        with self._lock:
            rows = self._conn.execute("SELECT key, value FROM app_settings ORDER BY key").fetchall()
        return {row["key"]: row["value"] or "" for row in rows}

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def gmail_http_status(exc: HttpError) -> int | None:
    response = getattr(exc, "resp", None)
    return getattr(response, "status", None)


class GmailClient:
    def __init__(self, config: Config, service_factory: Callable[[], Any] | None = None):
        self.config = config
        self._lock = threading.RLock()
        self._service: Any = None
        self._labels: Dict[str, str] = {}
        self._service_factory = service_factory or self._build_service

    def _load_credentials(self) -> Credentials:
        self.config.materialize_google_credentials()
        self.config.materialize_gmail_token()
        creds: Optional[Credentials] = None

        if self.config.gmail_token_path.exists():
            creds = Credentials.from_authorized_user_file(str(self.config.gmail_token_path), SCOPES)

        if creds and creds.valid:
            return creds

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self.config.gmail_token_path.write_text(creds.to_json())
            return creds

        if not self.config.gmail_credentials_path.exists():
            raise FileNotFoundError(
                f"Missing Gmail credentials file at {self.config.gmail_credentials_path}"
            )

        flow = InstalledAppFlow.from_client_secrets_file(
            str(self.config.gmail_credentials_path),
            SCOPES,
        )
        creds = flow.run_local_server(port=0)
        self.config.gmail_token_path.write_text(creds.to_json())
        return creds

    def _build_service(self) -> Any:
        creds = self._load_credentials()
        return build("gmail", "v1", credentials=creds, cache_discovery=False)

    def invalidate(self) -> None:
        with self._lock:
            self._service = None
            self._labels = {}

    def get_service(self, force_reinit: bool = False) -> Any:
        with self._lock:
            if force_reinit or self._service is None:
                self._service = self._service_factory()
                self._labels = {}
            return self._service

    def refresh_labels(self, force: bool = False) -> Dict[str, str]:
        with self._lock:
            if self._labels and not force:
                return dict(self._labels)
        labels = self.call(self._fetch_labels_once)
        mapping = {item["id"]: item["name"] for item in labels}
        with self._lock:
            self._labels = mapping
        return dict(mapping)

    @staticmethod
    def _fetch_labels_once(service: Any) -> list[dict]:
        return service.users().labels().list(userId="me").execute().get("labels", [])

    def call(self, fn: Callable[[Any], Any]) -> Any:
        last_error: HttpError | None = None
        for attempt in range(2):
            service = self.get_service(force_reinit=False)
            try:
                return fn(service)
            except HttpError as exc:
                last_error = exc
                status = gmail_http_status(exc)
                if attempt == 0 and status in {401, 403}:
                    LOGGER.warning("Gmail auth failed with %s. Reinitializing service.", status)
                    reinitialized = self.get_service(force_reinit=True)
                    try:
                        labels = self._fetch_labels_once(reinitialized)
                        with self._lock:
                            self._labels = {item["id"]: item["name"] for item in labels}
                    except Exception:
                        LOGGER.exception("Failed to refresh Gmail label cache after reinit.")
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("gmail_call failed without HttpError")

    def latest_inbox_id(self) -> str | None:
        message_ids = self.list_recent_monitored_ids(self.config.gmail_monitor_labels, limit=1)
        return message_ids[0] if message_ids else None

    def list_recent_label_ids(self, label_id: str, limit: int = 100) -> List[str]:
        def fetch(service: Any) -> List[str]:
            message_ids: List[str] = []
            page_token: str | None = None
            while len(message_ids) < limit:
                payload = (
                    service.users()
                    .messages()
                    .list(
                        userId="me",
                        labelIds=[label_id],
                        maxResults=min(100, limit - len(message_ids)),
                        includeSpamTrash=False,
                        pageToken=page_token,
                    )
                    .execute()
                )
                for item in payload.get("messages") or []:
                    message_id = item.get("id")
                    if message_id:
                        message_ids.append(message_id)
                        if len(message_ids) >= limit:
                            break
                page_token = payload.get("nextPageToken")
                if not page_token or not payload.get("messages"):
                    break
            return message_ids

        return self.call(fetch)

    def get_internal_date(self, gmail_message_id: str) -> int:
        payload = self.call(
            lambda svc: svc.users()
            .messages()
            .get(userId="me", id=gmail_message_id, format="minimal")
            .execute()
        )
        try:
            return int(payload.get("internalDate", "0"))
        except (TypeError, ValueError):
            return 0

    def list_recent_monitored_ids(self, label_ids: List[str], limit: int = 100) -> List[str]:
        labels = [label for label in label_ids if label] or ["INBOX"]
        if len(labels) == 1:
            return self.list_recent_label_ids(labels[0], limit=limit)

        per_label_limit = max(10, min(limit, 30))
        seen: set[str] = set()
        candidates: List[str] = []
        for label_id in labels:
            for message_id in self.list_recent_label_ids(label_id, limit=per_label_limit):
                if message_id not in seen:
                    seen.add(message_id)
                    candidates.append(message_id)
        ranked = sorted(
            candidates,
            key=self.get_internal_date,
            reverse=True,
        )
        return ranked[:limit]

    def get_full_message(self, gmail_message_id: str) -> dict:
        return self.call(
            lambda svc: svc.users()
            .messages()
            .get(userId="me", id=gmail_message_id, format="full")
            .execute()
        )

    def get_raw_message(self, gmail_message_id: str) -> dict:
        return self.call(
            lambda svc: svc.users()
            .messages()
            .get(userId="me", id=gmail_message_id, format="raw")
            .execute()
        )

    def get_attachment_data(self, gmail_message_id: str, attachment_id: str) -> str:
        payload = self.call(
            lambda svc: svc.users()
            .messages()
            .attachments()
            .get(userId="me", messageId=gmail_message_id, id=attachment_id)
            .execute()
        )
        return payload["data"]

    def send_raw_message(self, raw: str, thread_id: str) -> None:
        body = {"raw": raw}
        if thread_id:
            body["threadId"] = thread_id
        self.call(
            lambda svc: svc.users()
            .messages()
            .send(userId="me", body=body)
            .execute()
        )

    def create_draft(self, raw: str, thread_id: str) -> dict:
        message_body = {"raw": raw}
        if thread_id:
            message_body["threadId"] = thread_id
        return self.call(
            lambda svc: svc.users()
            .drafts()
            .create(userId="me", body={"message": message_body})
            .execute()
        )

    def modify_message(self, gmail_message_id: str, add: List[str] | None = None, rem: List[str] | None = None) -> None:
        self.call(
            lambda svc: svc.users()
            .messages()
            .modify(
                userId="me",
                id=gmail_message_id,
                body={"addLabelIds": add or [], "removeLabelIds": rem or []},
            )
            .execute()
        )


@dataclass(slots=True)
class Runtime:
    base_config: Config
    config: Config
    startup_overrides: Dict[str, str]
    store: StateStore
    gmail: GmailClient
    model: Any
    shutdown_event: asyncio.Event
    mode: str


@dataclass(frozen=True, slots=True)
class DashboardField:
    key: str
    attr: str
    label: str
    kind: str
    help_text: str = ""
    secret: bool = False
    restart_required: bool = False


def decode_hdr(value: str | None) -> str:
    if not value:
        return ""
    return "".join(
        (
            chunk.decode(encoding or "utf-8", "replace") if isinstance(chunk, bytes) else chunk
            for chunk, encoding in decode_header(value)
        )
    )


def decode_base64_body(data: str | None) -> str | None:
    if not data:
        return None
    try:
        return base64.urlsafe_b64decode(data).decode("utf-8", "replace")
    except Exception:
        LOGGER.exception("Failed to decode base64 Gmail body chunk.")
        return None


class EmailHTMLTextExtractor(HTMLParser):
    BLOCK_TAGS = {
        "address",
        "article",
        "aside",
        "blockquote",
        "div",
        "figcaption",
        "figure",
        "footer",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "li",
        "main",
        "nav",
        "p",
        "section",
        "table",
        "tr",
        "td",
        "th",
        "ul",
        "ol",
    }
    SKIP_TAGS = {"head", "meta", "script", "style", "svg", "title", "noscript"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: List[str] = []
        self._skip_depth = 0

    def _append_break(self) -> None:
        if not self.parts:
            return
        if self.parts[-1].endswith("\n"):
            return
        self.parts.append("\n")

    def handle_starttag(self, tag: str, attrs: List[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        if tag == "br":
            self._append_break()
            return
        if tag in self.BLOCK_TAGS:
            self._append_break()
            if tag == "li":
                self.parts.append("• ")
            return
        if tag == "img":
            attributes = dict(attrs)
            alt = (attributes.get("alt") or "").strip()
            if alt and len(alt) <= 80:
                self.parts.append(f"[immagine: {alt}]")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self.SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
            return
        if self._skip_depth:
            return
        if tag in self.BLOCK_TAGS:
            self._append_break()

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        cleaned = re.sub(r"\s+", " ", data.replace("\xa0", " ")).strip()
        if not cleaned:
            return
        self.parts.append(cleaned)

    def get_text(self) -> str:
        text = "".join(self.parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def normalize_email_text(text: str) -> str:
    cleaned = text.replace("\r", "").replace("\xa0", " ")
    cleaned = re.sub(r"[\u200b-\u200d\ufeff]", "", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in cleaned.splitlines()]
    normalized: List[str] = []
    blank_run = 0
    for line in lines:
        if not line:
            blank_run += 1
            if blank_run <= 1:
                normalized.append("")
            continue
        blank_run = 0
        normalized.append(line)
    return "\n".join(normalized).strip()


def html_to_text(html: str) -> str:
    parser = EmailHTMLTextExtractor()
    parser.feed(html)
    parser.close()
    return normalize_email_text(parser.get_text())


def is_useful_email_text(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    if len(compact) < 20:
        return False
    alpha_count = sum(char.isalpha() for char in compact)
    return alpha_count >= min(20, len(compact) // 4)


def payload_text(payload: dict) -> str:
    plain: str | None = None
    html: str | None = None

    def visit(part: dict) -> None:
        nonlocal plain, html
        mime = part.get("mimeType", "")
        text = decode_base64_body(part.get("body", {}).get("data"))
        if text:
            if mime.startswith("text/plain") and plain is None:
                plain = text
            elif mime.startswith("text/html") and html is None:
                html = text
        for child in part.get("parts", []):
            visit(child)

    visit(payload)

    normalized_plain = normalize_email_text(plain) if plain else ""
    if normalized_plain and is_useful_email_text(normalized_plain):
        return normalized_plain
    if html:
        normalized_html = html_to_text(html)
        if normalized_html:
            return normalized_html
    if normalized_plain:
        return normalized_plain
    return "(corpo non disponibile)"


def list_attachments(payload: dict) -> List[dict]:
    attachments: List[dict] = []

    def visit(part: dict) -> None:
        filename = part.get("filename")
        body = part.get("body", {})
        if filename:
            attachments.append(
                {
                    "id": body.get("attachmentId"),
                    "data": body.get("data"),
                    "filename": filename,
                    "size": body.get("size", 0),
                }
            )
        for child in part.get("parts", []):
            visit(child)

    visit(payload)
    return attachments


def extract_header(headers: List[dict], name: str, default: str = "") -> str:
    match = next((item.get("value", "") for item in headers if item.get("name", "").lower() == name.lower()), None)
    return match if match is not None else default


def build_raw(to_addr: str, subject: str, plain: str, tracking_markup: str | None = None) -> str:
    message = email.message.EmailMessage()
    message["To"] = to_addr
    message["Subject"] = subject
    message.set_content(plain)
    if tracking_markup:
        html_body = ihtml.escape(plain).replace("\n", "<br>")
        html_body += tracking_markup
        message.add_alternative(html_body, subtype="html")
    return base64.urlsafe_b64encode(message.as_bytes()).decode()


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[MENU_TRACKED_EMAIL, MENU_STATS], [MENU_SETTINGS]],
        resize_keyboard=True,
    )


def gmail_call(runtime: Runtime, fn: Callable[[Any], Any]) -> Any:
    return runtime.gmail.call(fn)


def gmail_forward(runtime: Runtime, gmail_message_id: str, to_addr: str) -> None:
    payload = runtime.gmail.get_raw_message(gmail_message_id)
    original = email.message_from_bytes(base64.urlsafe_b64decode(payload["raw"].encode()))
    body = "Inoltro automatico.\n\n--- Messaggio originale ---\n" + payload_text(payload["payload"])
    raw = build_raw(to_addr, "Fwd: " + original.get("Subject", ""), body, None)
    runtime.gmail.call(
        lambda svc: svc.users().messages().send(userId="me", body={"raw": raw}).execute()
    )


def format_email_text(
    state: EmailState,
    *,
    body_override: str | None = None,
    status_line: str | None = None,
) -> str:
    body_text = (body_override if body_override is not None else state.body)[:TELEGRAM_MAX]
    parts = [
        f"📧 <b>{ihtml.escape(state.subject or '(senza oggetto)')}</b>",
        "",
        ihtml.escape(body_text),
    ]
    if status_line:
        parts.extend(["", "---", ihtml.escape(status_line)])
    return "\n".join(parts)


def settings_message_text(runtime: Runtime) -> str:
    prompt_preview = ihtml.escape(shorten_text(runtime.config.system_prompt, limit=160))
    public_base_url = ihtml.escape(runtime.config.resolved_public_base_url() or "missing")
    monitor_labels = ihtml.escape(", ".join(runtime.config.gmail_monitor_labels))
    lines = [
        "Impostazioni rapide:",
        "",
        f"- Gemini key: {'set' if ai_configured(runtime.config) else 'missing'}",
        f"- Gmail OAuth: {'set' if google_credentials_available(runtime.config) else 'missing'}",
        f"- Gmail token: {'set' if google_token_available(runtime.config) else 'missing'}",
        f"- Cartelle Gmail: {monitor_labels}",
        f"- Pixel: {'enabled' if runtime.config.enable_pixel else 'disabled'}",
        f"- Public URL: {public_base_url}",
        "",
        "Prompt di sistema Gemini:",
        prompt_preview,
    ]
    return "\n".join(lines)


def tracked_email_status_summary(tracked: TrackedEmail) -> str:
    if tracked.open_count <= 0:
        return "Mai aperta"
    reopen_count = max(0, tracked.open_count - 1)
    security = "bassa"
    if tracked.last_classification == "human_browser":
        if tracked.last_confidence is not None and tracked.last_confidence >= 0.85:
            security = "alta"
        else:
            security = "media"
    elif tracked.last_confidence is not None and tracked.last_confidence >= 0.75:
        security = "media"
    pieces = [f"Aperta {tracked.open_count} volta{'e' if tracked.open_count != 1 else ''}"]
    if reopen_count:
        pieces.append(f"riaperta {reopen_count} volta{'e' if reopen_count != 1 else ''}")
    if tracked.last_classification:
        pieces.append(tracked.last_classification.replace("_", " "))
    if tracked.last_layer:
        pieces.append(f"via {tracked.last_layer}")
    if tracked.last_confidence is not None:
        pieces.append(f"sicurezza {security} ({tracked.last_confidence:.2f})")
    else:
        pieces.append(f"sicurezza {security}")
    return " · ".join(pieces)


def format_tracked_email_text(tracked: TrackedEmail, note: str | None = None) -> str:
    lines = [
        f"🛰️ <b>{ihtml.escape(tracked.subject or '(senza oggetto)')}</b>",
        f"To: <code>{ihtml.escape(tracked.recipient)}</code>",
        "",
        ihtml.escape(tracked_email_status_summary(tracked)),
    ]
    if tracked.last_opened_at:
        lines.append(ihtml.escape(f"Ultima apertura: {tracked.last_opened_at}"))
    if note:
        lines.extend(["", "---", ihtml.escape(note)])
    return "\n".join(lines)


def tracked_stats_text(tracked_items: List[TrackedEmail]) -> str:
    if not tracked_items:
        return "📊 Nessuna email tracciata ancora."
    lines = ["📊 <b>Email tracciate</b>", ""]
    for index, tracked in enumerate(tracked_items, start=1):
        lines.append(f"{index}. <b>{ihtml.escape(tracked.subject or '(senza oggetto)')}</b>")
        lines.append(f"To: <code>{ihtml.escape(tracked.recipient)}</code>")
        lines.append(ihtml.escape(tracked_email_status_summary(tracked)))
        if tracked.last_opened_at:
            lines.append(ihtml.escape(f"Ultima apertura: {tracked.last_opened_at}"))
        lines.append("")
    return "\n".join(lines).rstrip()


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def make_tracking_token(config: Config, tg_message_id: int) -> str:
    payload = b64url_encode(
        json.dumps(
            {"tg": tg_message_id, "nonce": uuid4().hex},
            separators=(",", ":"),
        ).encode()
    )
    signature = b64url_encode(
        hmac.new(
            config.pixel_webhook_secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).digest()
    )
    return f"{payload}.{signature}"


def make_dashboard_token(config: Config) -> str:
    now = int(utcnow().timestamp())
    payload = {
        "chat_id": config.chat_id,
        "iat": now,
        "exp": now + DASHBOARD_TOKEN_TTL_SECONDS,
        "nonce": uuid4().hex,
    }
    encoded = b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    signature = b64url_encode(
        hmac.new(config.bot_token.encode(), encoded.encode(), hashlib.sha256).digest()
    )
    return f"{encoded}.{signature}"


def verify_dashboard_token(config: Config, token: str | None) -> bool:
    if not token or "." not in token:
        return False
    try:
        payload_part, signature_part = token.rsplit(".", 1)
        expected = b64url_encode(
            hmac.new(config.bot_token.encode(), payload_part.encode(), hashlib.sha256).digest()
        )
        if not hmac.compare_digest(expected, signature_part):
            return False
        payload = json.loads(b64url_decode(payload_part))
        if int(payload.get("chat_id", 0)) != config.chat_id:
            return False
        if int(payload.get("exp", 0)) < int(utcnow().timestamp()):
            return False
        return True
    except Exception:
        return False


def dashboard_url(config: Config) -> str:
    return f"{config.resolved_public_base_url()}/dashboard?token={make_dashboard_token(config)}"


def apply_runtime_overrides(runtime: Runtime) -> None:
    overrides = runtime.store.get_app_settings()
    merged = runtime.base_config.with_overrides(runtime.startup_overrides).with_overrides(overrides)
    merged.validate_effective(runtime.mode)
    runtime.config = merged
    runtime.gmail.config = runtime.config
    runtime.config.materialize_google_credentials()
    runtime.config.materialize_gmail_token()
    runtime.gmail.invalidate()
    if runtime.config.google_api_key:
        genai.configure(api_key=runtime.config.google_api_key)
        runtime.model = genai.GenerativeModel(runtime.config.ai_model)
    else:
        runtime.model = None


def build_candidate_config(runtime: Runtime, overrides: Mapping[str, str]) -> Config:
    candidate = runtime.base_config.with_overrides(runtime.startup_overrides).with_overrides(overrides)
    candidate.validate_effective(runtime.mode)
    return candidate


def sync_dashboard_overrides(runtime: Runtime, overrides: Mapping[str, str]) -> None:
    editable_keys = {field.key for field in EDITABLE_DASHBOARD_FIELDS}
    current = runtime.store.get_app_settings()
    for key in editable_keys:
        if key in overrides:
            if current.get(key) != overrides[key]:
                runtime.store.set_app_setting(key, overrides[key])
        elif key in current:
            runtime.store.delete_app_setting(key)
    apply_runtime_overrides(runtime)


def parse_dashboard_overrides(form_data: Mapping[str, Any], current: Mapping[str, str]) -> Dict[str, str]:
    overrides = dict(current)
    for field in EDITABLE_DASHBOARD_FIELDS:
        raw = form_data.get(field.key)
        if field.kind == "checkbox":
            overrides[field.key] = "1" if raw else "0"
            continue
        if raw is None:
            continue
        text = str(raw).strip()
        if field.secret:
            if text:
                overrides[field.key] = text
            continue
        if text:
            overrides[field.key] = text
        else:
            overrides.pop(field.key, None)
    return overrides


def owner_configured(config: Config) -> bool:
    return config.chat_id > 0


def ai_configured(config: Config) -> bool:
    return bool(config.google_api_key)


def google_credentials_available(config: Config) -> bool:
    return bool(config.google_oauth_credentials_json) or config.gmail_credentials_path.exists()


def google_token_available(config: Config) -> bool:
    return bool(config.google_oauth_token_json) or config.gmail_token_path.exists()


def gmail_ready_for_watch(config: Config) -> bool:
    return owner_configured(config) and google_credentials_available(config) and google_token_available(config)


def google_oauth_redirect_url(config: Config) -> str:
    return config.resolved_public_base_url() + "/oauth/google/callback"


def google_oauth_authorization_response(config: Config, query_string: str) -> str:
    if query_string:
        return google_oauth_redirect_url(config) + "?" + query_string
    return google_oauth_redirect_url(config)


def google_client_config(config: Config) -> dict:
    if config.google_oauth_credentials_json:
        return json.loads(config.google_oauth_credentials_json)
    if config.gmail_credentials_path.exists():
        return json.loads(config.gmail_credentials_path.read_text())
    raise ConfigError("Google OAuth client JSON not configured yet.")


def google_web_client_config(config: Config) -> dict:
    client_config = google_client_config(config)
    web_config = client_config.get("web")
    if not isinstance(web_config, dict):
        raise ConfigError(
            "Bot-based Gmail login needs a Google OAuth Web application JSON under the 'web' key."
        )
    missing = [key for key in ("client_id", "client_secret", "auth_uri", "token_uri") if not web_config.get(key)]
    if missing:
        raise ConfigError(
            "Google OAuth Web client JSON is missing required fields: " + ", ".join(missing)
        )
    return client_config


def setup_status_lines(runtime: Runtime) -> List[str]:
    config = runtime.config
    owner_line = (
        f"Owner Telegram ID: {config.chat_id}"
        if owner_configured(config)
        else "Owner Telegram ID: not claimed yet"
    )
    return [
        owner_line,
        f"Gemini API key: {'set' if ai_configured(config) else 'missing'}",
        f"Public base URL: {config.resolved_public_base_url() or 'missing'}",
        f"Google OAuth client JSON: {'set' if google_credentials_available(config) else 'missing'}",
        f"Gmail refresh token: {'set' if google_token_available(config) else 'missing'}",
        f"Gmail monitor labels: {', '.join(config.gmail_monitor_labels)}",
        f"Pixel tracking: {'enabled' if config.enable_pixel else 'disabled'}",
    ]


def setup_keyboard(runtime: Runtime) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Status", callback_data="setup|status"),
            InlineKeyboardButton("Gemini key", callback_data="setup|google_api_key"),
        ],
        [
            InlineKeyboardButton("Public URL", callback_data="setup|public_base_url"),
            InlineKeyboardButton("Upload Google OAuth JSON", callback_data="setup|oauth_json"),
        ],
    ]
    if google_credentials_available(runtime.config):
        rows.append([InlineKeyboardButton("Connect Gmail", callback_data="setup|gmail_login")])
    rows.append([InlineKeyboardButton("Open dashboard", callback_data="setup|dashboard")])
    return InlineKeyboardMarkup(rows)


def settings_keyboard(runtime: Runtime) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Status", callback_data="settings|status"),
            InlineKeyboardButton("Prompt Gemini", callback_data="settings|system_prompt"),
        ],
        [
            InlineKeyboardButton("Gemini key", callback_data="settings|google_api_key"),
            InlineKeyboardButton("Public URL", callback_data="settings|public_base_url"),
        ],
        [
            InlineKeyboardButton("Cartelle Gmail", callback_data="settings|monitor_labels"),
            InlineKeyboardButton("OAuth JSON", callback_data="settings|oauth_json"),
        ],
        [
            InlineKeyboardButton("Pixel URL", callback_data="settings|pixel_base_url"),
            InlineKeyboardButton("Pixel secret", callback_data="settings|pixel_webhook_secret"),
        ],
        [
            InlineKeyboardButton(
                "Pixel ON/OFF",
                callback_data="settings|toggle_pixel",
            ),
            InlineKeyboardButton("Open dashboard", callback_data="settings|dashboard"),
        ],
    ]
    if google_credentials_available(runtime.config):
        rows.append([InlineKeyboardButton("Connect Gmail", callback_data="settings|gmail_login")])
    return InlineKeyboardMarkup(rows)


def gmail_monitor_keyboard(runtime: Runtime) -> InlineKeyboardMarkup:
    selected = set(runtime.config.gmail_monitor_labels)
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for label_id, label_name in GMAIL_MONITOR_LABEL_CHOICES:
        prefix = "✅ " if label_id in selected else ""
        current_row.append(
            InlineKeyboardButton(
                prefix + label_name,
                callback_data=f"settings|toggle_label|{label_id}",
            )
        )
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    rows.append([InlineKeyboardButton("⬅️ Torna", callback_data="settings|status")])
    return InlineKeyboardMarkup(rows)


def stats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Aggiorna", callback_data="stats|refresh"),
            InlineKeyboardButton("Nuova email tracciata", callback_data="tracked|new"),
        ]]
    )


def setup_message_text(runtime: Runtime) -> str:
    redirect_uri = google_oauth_redirect_url(runtime.config)
    lines = ["Self-hosted setup status:", ""] + [f"- {line}" for line in setup_status_lines(runtime)]
    lines.extend(
        [
            "",
            "Quick flow:",
            "1. Send /start to claim the bot owner if it is still unclaimed.",
            "2. Set the Gemini key.",
            "3. Set the public base URL of this app.",
            "4. Upload the Google OAuth Web client JSON.",
            f"5. In Google Cloud Console, add this redirect URI: {redirect_uri}",
            "6. Tap Connect Gmail and finish the Google login in the browser.",
        ]
    )
    return "\n".join(lines)


def startup_notice_text(runtime: Runtime, gmail_error: str | None = None) -> str | None:
    if not owner_configured(runtime.config):
        return None

    missing: List[str] = []
    if not ai_configured(runtime.config):
        missing.append("Gemini API key missing")
    if not google_credentials_available(runtime.config):
        missing.append("Google OAuth client JSON missing")
    if not google_token_available(runtime.config):
        missing.append("Gmail token missing")
    if gmail_error:
        missing.append(f"Gmail auth failed at startup: {gmail_error}")

    if not missing:
        return None

    return "\n".join(
        [
            "GlassyReply is running, but setup is still incomplete.",
            "",
            "Missing pieces:",
            *[f"- {item}" for item in missing],
            "",
            "Open /setup to finish the configuration, /gmail_login to reconnect Gmail, or /dashboard to inspect saved values.",
        ]
    )


def save_runtime_settings(runtime: Runtime, updates: Mapping[str, str]) -> None:
    current = runtime.store.get_app_settings()
    merged = dict(current)
    merged.update(updates)
    candidate = build_candidate_config(runtime, merged)
    if "GOOGLE_OAUTH_CREDENTIALS_JSON" in updates:
        google_web_client_config(candidate)
    if "GOOGLE_OAUTH_TOKEN_JSON" in updates:
        json.loads(updates["GOOGLE_OAUTH_TOKEN_JSON"])
    for key, value in updates.items():
        runtime.store.set_app_setting(key, value)
    apply_runtime_overrides(runtime)
    if "GMAIL_MONITOR_LABELS" in updates:
        mark_gmail_initial_sync_pending(runtime)


def claim_owner(runtime: Runtime, user_id: int) -> None:
    save_runtime_settings(runtime, {"TELEGRAM_CHAT_ID": str(user_id)})


def clear_google_oauth_state(runtime: Runtime) -> None:
    runtime.store.set_bot_state(GOOGLE_OAUTH_STATE_KEY, "")


def mark_gmail_initial_sync_pending(runtime: Runtime) -> None:
    runtime.store.set_bot_state(GMAIL_INITIAL_SYNC_KEY, "1")
    runtime.store.set_bot_state(LAST_SEEN_KEY, "")


def gmail_initial_sync_pending(runtime: Runtime) -> bool:
    return runtime.store.get_bot_state(GMAIL_INITIAL_SYNC_KEY) == "1"


def clear_gmail_initial_sync_pending(runtime: Runtime) -> None:
    runtime.store.set_bot_state(GMAIL_INITIAL_SYNC_KEY, "")


def google_oauth_state_payload(state: str, code_verifier: str | None) -> str:
    payload = {"state": state, "created_at": utcnow_iso()}
    if code_verifier:
        payload["code_verifier"] = code_verifier
    return json.dumps(payload)


def parse_google_oauth_state_payload(raw: str) -> dict:
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not payload.get("state"):
        raise ConfigError("Google OAuth state payload is invalid.")
    return payload


def start_google_oauth(runtime: Runtime) -> str:
    client_config = google_web_client_config(runtime.config)
    flow = Flow.from_client_config(
        client_config,
        scopes=SCOPES,
        redirect_uri=google_oauth_redirect_url(runtime.config),
    )
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    runtime.store.set_bot_state(
        GOOGLE_OAUTH_STATE_KEY,
        google_oauth_state_payload(state, getattr(flow, "code_verifier", None)),
    )
    return auth_url


BOT_CONFIG_ALIASES = {
    "owner_chat_id": "TELEGRAM_CHAT_ID",
    "telegram_chat_id": "TELEGRAM_CHAT_ID",
    "google_api_key": "GOOGLE_API_KEY",
    "public_base_url": "PUBLIC_BASE_URL",
    "enable_pixel": "ENABLE_PIXEL",
    "pixel_base_url": "PIXEL_BASE_URL",
    "pixel_webhook_secret": "PIXEL_WEBHOOK_SECRET",
    "pixel_webhook_url": "PIXEL_WEBHOOK_URL",
    "watch_interval": "WATCH_INTERVAL",
    "lang": "LANG",
    "ai_model": "AI_MODEL",
    "system_prompt": "SYSTEM_PROMPT",
    "gmail_monitor_labels": "GMAIL_MONITOR_LABELS",
    "predef_fwd": "PREDEF_FWD",
    "state_retention_days": "STATE_RETENTION_DAYS",
    "telegram_webhook_url": "TELEGRAM_WEBHOOK_URL",
    "telegram_webhook_secret": "TELEGRAM_WEBHOOK_SECRET",
    "google_oauth_credentials_json": "GOOGLE_OAUTH_CREDENTIALS_JSON",
    "google_oauth_token_json": "GOOGLE_OAUTH_TOKEN_JSON",
}


def normalize_bot_config_key(raw: str) -> str:
    key = raw.strip().lower()
    normalized = BOT_CONFIG_ALIASES.get(key)
    if not normalized:
        raise ConfigError(f"Unsupported setup key: {raw}")
    return normalized


def build_tracking_markup_for_message_id(config: Config, tg_message_id: int) -> str:
    if not (config.enable_pixel and config.pixel_base_url):
        return ""
    base_url = config.pixel_base_url
    token = make_tracking_token(config, tg_message_id)
    nonce = uuid4().hex[:10]
    img_url = f"{base_url}/track/img/2x1/{token}.png"
    bg_url = f"{base_url}/track/bg/2x1/{token}.png"
    dark_url = f"{base_url}/track/dark/2x1/{token}.png"
    font_url = f"{base_url}/track/font/{token}.woff2"
    return (
        '<div style="line-height:1px;max-height:1px;overflow:hidden;">'
        f'<img src="{img_url}" width="2" height="1" alt="" '
        'style="display:block;border:0;outline:none;text-decoration:none;opacity:0.01;">'
        f'<div class="gr-bg-{nonce}" style="background-image:url(\'{bg_url}\');'
        'background-repeat:no-repeat;background-size:2px 1px;width:2px;height:1px;'
        'max-height:1px;overflow:hidden;opacity:0.01;">&nbsp;</div>'
        f'<div class="gr-dark-{nonce}" style="background-image:url(\'{bg_url}\');'
        'background-repeat:no-repeat;background-size:2px 1px;width:2px;height:1px;'
        'max-height:1px;overflow:hidden;opacity:0.01;">&nbsp;</div>'
        f'<span class="gr-font-{nonce}" style="font-family:\'grtrack-{nonce}\',Arial,sans-serif;'
        'font-size:1px;line-height:1px;color:transparent;display:inline-block;max-height:0;overflow:hidden;">.</span>'
        '</div>'
        "<style>"
        f"@media (prefers-color-scheme: dark) {{ .gr-dark-{nonce} {{ background-image:url('{dark_url}') !important; }} }}"
        f"@font-face {{ font-family:'grtrack-{nonce}'; src:url('{font_url}') format('woff2'); font-style:normal; font-weight:400; }}"
        "</style>"
    )


def build_tracking_markup(config: Config, state: EmailState) -> str:
    return build_tracking_markup_for_message_id(config, state.tg_message_id)


def split_unseen_inbox_ids(recent_ids: List[str], last_seen: str | None) -> tuple[List[str], str | None]:
    if not recent_ids:
        return [], last_seen
    newest_seen = recent_ids[0]
    if not last_seen:
        return [], newest_seen
    try:
        index = recent_ids.index(last_seen)
    except ValueError:
        return [], newest_seen
    if index <= 0:
        return [], newest_seen
    return list(reversed(recent_ids[:index])), newest_seen


EDITABLE_DASHBOARD_FIELDS = [
    DashboardField(
        key="PUBLIC_BASE_URL",
        attr="public_base_url",
        label="Public base URL",
        kind="text",
        help_text="Used for dashboard links and externally visible URLs.",
    ),
    DashboardField(
        key="ENABLE_PIXEL",
        attr="enable_pixel",
        label="Enable pixel tracker",
        kind="checkbox",
        help_text="Turns on outbound telemetry markup generation.",
    ),
    DashboardField(
        key="PIXEL_BASE_URL",
        attr="pixel_base_url",
        label="Pixel base URL",
        kind="text",
        help_text="Cloudflare Worker or other tracking endpoint base URL.",
    ),
    DashboardField(
        key="PIXEL_WEBHOOK_SECRET",
        attr="pixel_webhook_secret",
        label="Pixel webhook secret",
        kind="password",
        secret=True,
        help_text="Shared secret used by the pixel capture endpoint.",
    ),
    DashboardField(
        key="PIXEL_WEBHOOK_URL",
        attr="pixel_webhook_url",
        label="Pixel webhook URL",
        kind="text",
        help_text="External callback URL for pixel status events.",
    ),
    DashboardField(
        key="HOST",
        attr="host",
        label="HTTP host",
        kind="text",
        restart_required=True,
        help_text="The process binds to this host on startup.",
    ),
    DashboardField(
        key="PORT",
        attr="port",
        label="HTTP port",
        kind="number",
        restart_required=True,
        help_text="The process listens on this port on startup.",
    ),
    DashboardField(
        key="WATCH_INTERVAL",
        attr="watch_interval",
        label="Watch interval (seconds)",
        kind="number",
        help_text="Polling delay between Gmail inbox checks.",
    ),
    DashboardField(
        key="LANG",
        attr="lang",
        label="Reply language",
        kind="text",
        help_text="Language used for Gemini reply drafts.",
    ),
    DashboardField(
        key="AI_MODEL",
        attr="ai_model",
        label="AI model",
        kind="text",
        help_text="Gemini model name used for reply generation.",
    ),
    DashboardField(
        key="SYSTEM_PROMPT",
        attr="system_prompt",
        label="Gemini system prompt",
        kind="textarea",
        help_text="Default instruction used for automatic AI drafts.",
    ),
    DashboardField(
        key="GMAIL_MONITOR_LABELS",
        attr="gmail_monitor_labels",
        label="Gmail monitor labels",
        kind="textarea",
        help_text="One Gmail label per line. Example: INBOX, CATEGORY_PROMOTIONS, CATEGORY_UPDATES.",
    ),
    DashboardField(
        key="PREDEF_FWD",
        attr="predef_fwd",
        label="Preset forward addresses",
        kind="textarea",
        help_text="One address per line or comma-separated. Use [] to clear the list.",
    ),
    DashboardField(
        key="STATE_RETENTION_DAYS",
        attr="state_retention_days",
        label="State retention days",
        kind="number",
        help_text="Rows older than this are purged from SQLite.",
    ),
    DashboardField(
        key="TELEGRAM_WEBHOOK_URL",
        attr="telegram_webhook_url",
        label="Telegram webhook URL",
        kind="text",
        restart_required=True,
        help_text="Base URL used when switching the bot to webhook mode.",
    ),
    DashboardField(
        key="TELEGRAM_WEBHOOK_SECRET",
        attr="telegram_webhook_secret",
        label="Telegram webhook secret",
        kind="password",
        secret=True,
        restart_required=True,
        help_text="Secret token checked by the Telegram webhook route.",
    ),
]


def mask_secret(value: str, keep: int = 4) -> str:
    if not value:
        return "missing"
    if len(value) <= keep * 2:
        return "set"
    return f"{value[:keep]}…{value[-keep:]}"


def shorten_text(value: str, limit: int = 100) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)] + "…"


def field_display_value(field: DashboardField, value: Any) -> str:
    if field.kind == "checkbox":
        return "enabled" if bool(value) else "disabled"
    if field.secret:
        return "set" if value else "missing"
    if field.key in {"PREDEF_FWD", "GMAIL_MONITOR_LABELS"}:
        if isinstance(value, list):
            if not value:
                return "unset"
            preview = ", ".join(value[:3])
            if len(value) > 3:
                preview += f" (+{len(value) - 3} more)"
            return shorten_text(preview)
        return shorten_text(str(value))
    if value is None or value == "":
        return "unset"
    return shorten_text(str(value))


def render_status_badge(text: str, kind: str = "neutral") -> str:
    return f'<span class="badge badge-{kind}">{ihtml.escape(text)}</span>'


def render_setting_row(
    label: str,
    value: str,
    source: str,
    note: str = "",
    *,
    secret: bool = False,
) -> str:
    value_html = ihtml.escape(value)
    note_html = f'<div class="note">{ihtml.escape(note)}</div>' if note else ""
    source_badge = render_status_badge(source, "saved" if source == "saved" else "soft")
    return (
        '<div class="row">'
        f"<div><strong>{ihtml.escape(label)}</strong>{note_html}</div>"
        f"<div class=\"mono\">{value_html}</div>"
        f"<div>{source_badge}</div>"
        "</div>"
    )


def render_dashboard_table(rows: List[dict]) -> str:
    body = "".join(
        render_setting_row(
            row["label"],
            row["value"],
            row["source"],
            row.get("note", ""),
            secret=row.get("secret", False),
        )
        for row in rows
    )
    return (
        '<div class="table">'
        '<div class="row head"><div>Setting</div><div>Value</div><div>Source</div></div>'
        f"{body}"
        "</div>"
    )


def build_runtime_rows(runtime: Runtime) -> List[dict]:
    overrides = runtime.store.get_app_settings()
    rows: List[dict] = []
    for field in EDITABLE_DASHBOARD_FIELDS:
        value = getattr(runtime.config, field.attr)
        if field.key in overrides:
            source = "saved"
        elif field.key in runtime.startup_overrides:
            source = "startup"
        else:
            source = "env/default"
        rows.append(
            {
                "label": field.label,
                "value": field_display_value(field, value),
                "source": source,
                "note": field.help_text + (" Restart required." if field.restart_required else ""),
                "secret": field.secret,
            }
        )
    return rows


def build_bootstrap_rows(runtime: Runtime) -> List[dict]:
    config = runtime.config
    overrides = runtime.store.get_app_settings()
    rows = [
        {
            "label": "Telegram bot token",
            "value": mask_secret(config.bot_token),
            "source": "env",
            "note": "Required to talk to Telegram.",
            "secret": True,
        },
        {
            "label": "Telegram owner ID",
            "value": str(config.chat_id) if owner_configured(config) else "missing",
            "source": "saved" if "TELEGRAM_CHAT_ID" in overrides else "env",
            "note": "The first /start can claim the owner automatically.",
        },
        {
            "label": "Google API key",
            "value": "set" if config.google_api_key else "missing",
            "source": "saved" if "GOOGLE_API_KEY" in overrides else "env",
            "note": "Used for Gemini requests and configurable from Telegram.",
            "secret": True,
        },
        {
            "label": "OAuth credentials JSON",
            "value": "set" if config.google_oauth_credentials_json or config.gmail_credentials_path.exists() else "missing",
            "source": "saved" if "GOOGLE_OAUTH_CREDENTIALS_JSON" in overrides else "env/filesystem",
            "note": "Google OAuth Web client JSON for Gmail login.",
            "secret": True,
        },
        {
            "label": "OAuth token JSON",
            "value": "set" if config.google_oauth_token_json or config.gmail_token_path.exists() else "missing",
            "source": "saved" if "GOOGLE_OAUTH_TOKEN_JSON" in overrides else "env/filesystem",
            "note": "Stored after the browser-based Gmail OAuth flow.",
            "secret": True,
        },
        {
            "label": "Gmail token file",
            "value": "present" if config.gmail_token_path.exists() else "missing",
            "source": "filesystem",
            "note": "Refreshed token saved on disk.",
        },
        {
            "label": "Gmail credentials file",
            "value": "present" if config.gmail_credentials_path.exists() else "missing",
            "source": "filesystem",
            "note": "OAuth client JSON materialized on disk.",
        },
    ]
    return rows


def dashboard_page(
    runtime: Runtime,
    *,
    title: str,
    message: str = "",
    errors: List[str] | None = None,
    token: str = "",
) -> str:
    runtime_rows = render_dashboard_table(build_runtime_rows(runtime))
    bootstrap_rows = render_dashboard_table(build_bootstrap_rows(runtime))
    form_inputs: List[str] = []
    for field in EDITABLE_DASHBOARD_FIELDS:
        value = getattr(runtime.config, field.attr)
        if field.kind == "checkbox":
            checked = " checked" if bool(value) else ""
            form_inputs.append(
                f"""
                <label class="field field-inline">
                  <input type="checkbox" name="{field.key}" value="1"{checked}>
                  <span>{ihtml.escape(field.label)}</span>
                </label>
                <div class="help">{ihtml.escape(field.help_text)}</div>
                """
            )
            continue
        if field.kind == "textarea":
            current_value = "\n".join(value) if isinstance(value, list) else str(value if value is not None else "")
            if field.secret:
                current_value = ""
            input_html = f'<textarea name="{field.key}" rows="3" placeholder="leave blank to keep current">{ihtml.escape(current_value)}</textarea>'
        else:
            input_type = "password" if field.secret else ("number" if field.kind == "number" else "text")
            current_value = "" if field.secret else str(value if value is not None else "")
            input_html = f'<input type="{input_type}" name="{field.key}" value="{ihtml.escape(current_value)}" placeholder="leave blank to keep current">'
        restart_note = "Restart required." if field.restart_required else ""
        form_inputs.append(
            f"""
            <label class="field">
              <span>{ihtml.escape(field.label)}</span>
              {input_html}
              <div class="help">{ihtml.escape(field.help_text)} {ihtml.escape(restart_note)}</div>
            </label>
            """
        )

    message_html = f'<div class="flash flash-ok">{ihtml.escape(message)}</div>' if message else ""
    error_html = ""
    if errors:
        error_html = "".join(f'<div class="flash flash-error">{ihtml.escape(err)}</div>' for err in errors)

    form_html = "".join(form_inputs)
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{ihtml.escape(title)}</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #f3efe7;
        --panel: rgba(255, 255, 255, 0.82);
        --text: #1f2328;
        --muted: #667085;
        --border: rgba(31, 35, 40, 0.12);
        --accent: #d97706;
        --good: #0f766e;
        --warn: #b45309;
        --bad: #b91c1c;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: radial-gradient(circle at top left, rgba(217, 119, 6, 0.14), transparent 28%),
                    radial-gradient(circle at bottom right, rgba(15, 118, 110, 0.10), transparent 24%),
                    var(--bg);
        color: var(--text);
      }}
      .wrap {{ max-width: 1160px; margin: 0 auto; padding: 32px 20px 56px; }}
      .hero {{ padding: 28px; border: 1px solid var(--border); border-radius: 24px; background: var(--panel); backdrop-filter: blur(12px); box-shadow: 0 20px 60px rgba(15, 23, 42, 0.08); }}
      h1 {{ margin: 0 0 8px; font-size: clamp(2rem, 3vw, 3rem); }}
      .sub {{ color: var(--muted); margin: 0 0 16px; line-height: 1.6; }}
      .chips {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 16px 0 0; }}
      .badge {{ display: inline-flex; align-items: center; gap: 6px; border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: 700; letter-spacing: .02em; border: 1px solid var(--border); }}
      .badge-neutral {{ background: #f8fafc; }}
      .badge-saved {{ background: rgba(15, 118, 110, 0.10); color: var(--good); }}
      .badge-soft {{ background: rgba(217, 119, 6, 0.10); color: var(--warn); }}
      .grid {{ display: grid; grid-template-columns: 1.05fr .95fr; gap: 18px; margin-top: 18px; }}
      .card {{ padding: 22px; border-radius: 22px; border: 1px solid var(--border); background: var(--panel); backdrop-filter: blur(10px); }}
      .card h2 {{ margin: 0 0 14px; font-size: 1.2rem; }}
      .table {{ display: grid; gap: 10px; }}
      .row {{ display: grid; grid-template-columns: 1.35fr 1fr .6fr; gap: 12px; align-items: start; padding: 12px 14px; border: 1px solid var(--border); border-radius: 16px; background: rgba(255,255,255,0.58); }}
      .row.head {{ background: transparent; border: none; padding-top: 0; font-size: 12px; text-transform: uppercase; color: var(--muted); letter-spacing: .08em; }}
      .note {{ margin-top: 6px; color: var(--muted); font-size: 13px; line-height: 1.5; }}
      .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; word-break: break-word; }}
      .flash {{ padding: 12px 14px; border-radius: 14px; margin: 14px 0; border: 1px solid var(--border); }}
      .flash-ok {{ background: rgba(15, 118, 110, 0.08); color: var(--good); }}
      .flash-error {{ background: rgba(185, 28, 28, 0.08); color: var(--bad); }}
      .form {{ display: grid; gap: 14px; }}
      .field {{ display: grid; gap: 8px; }}
      .field-inline {{ display: flex; align-items: center; gap: 10px; }}
      .field input[type="text"], .field input[type="password"], .field input[type="number"], .field textarea {{
        width: 100%;
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 11px 13px;
        background: rgba(255,255,255,0.78);
        color: var(--text);
        font: inherit;
      }}
      .field textarea {{ min-height: 88px; resize: vertical; }}
      .help {{ color: var(--muted); font-size: 13px; line-height: 1.5; }}
      .actions {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 6px; }}
      .btn {{
        appearance: none;
        border: none;
        background: linear-gradient(135deg, #b45309, #d97706);
        color: white;
        padding: 12px 16px;
        border-radius: 14px;
        font: inherit;
        font-weight: 700;
        cursor: pointer;
        text-decoration: none;
        display: inline-flex;
        align-items: center;
        justify-content: center;
      }}
      .btn.secondary {{ background: #111827; }}
      .mini {{ font-size: 13px; color: var(--muted); line-height: 1.55; }}
      .stack {{ display: grid; gap: 16px; }}
      .split {{ display: grid; gap: 16px; }}
      @media (max-width: 920px) {{
        .grid {{ grid-template-columns: 1fr; }}
        .row {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="hero">
        <h1>{ihtml.escape(title)}</h1>
        <p class="sub">GlassyReply status and configuration dashboard. Runtime settings are saved in SQLite; bootstrap secrets stay visible here only as status, not in cleartext.</p>
        <div class="chips">
          {render_status_badge(f"mode: {runtime.mode}", "neutral")}
          {render_status_badge(f"watch: {runtime.config.watch_interval}s", "neutral")}
          {render_status_badge(f"lang: {runtime.config.lang}", "neutral")}
          {render_status_badge(f"model: {runtime.config.ai_model}", "neutral")}
          {render_status_badge("pixel on" if runtime.config.enable_pixel else "pixel off", "saved" if runtime.config.enable_pixel else "soft")}
        </div>
      </div>
      {message_html}
      {error_html}
      <div class="grid">
        <div class="card">
          <h2>Live runtime settings</h2>
          <p class="mini">These values come from env plus SQLite overrides. Save changes to persist them without editing `.env`.</p>
          {runtime_rows}
        </div>
        <div class="card">
          <h2>Bootstrap status</h2>
          <p class="mini">What is required for the app to start or talk to external services.</p>
          {bootstrap_rows}
        </div>
      </div>
      <div class="grid">
        <div class="card">
          <h2>Edit settings</h2>
          <p class="mini">Leave a text field blank to keep the current value. Checkbox changes are saved immediately. Host/port changes are stored, but the running process still needs a restart to rebind.</p>
          <form class="form" method="post" action="/dashboard?token={ihtml.escape(token)}">
            <input type="hidden" name="token" value="{ihtml.escape(token)}">
            {form_html}
            <div class="actions">
              <button class="btn" type="submit">Save configuration</button>
              <a class="btn secondary" href="/healthz">Health check</a>
            </div>
          </form>
        </div>
        <div class="card">
          <h2>How to open</h2>
          <p class="mini">Use the Telegram command <code>/setup</code>, <code>/config</code>, or <code>/dashboard</code> to receive the private controls for this bot.</p>
          <p class="mini">If you change Gmail bootstrap secrets or the bot token, restart the process so the new runtime files are re-materialized.</p>
        </div>
      </div>
    </div>
  </body>
</html>
"""


def landing_page(runtime: Runtime) -> str:
    return f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>GlassyReply</title>
    <style>
      :root {{
        color-scheme: light;
        --bg: #f4efe4;
        --panel: rgba(255,255,255,0.84);
        --text: #1f2937;
        --muted: #6b7280;
        --border: rgba(31,41,55,0.12);
        --good: #0f766e;
        --warn: #b45309;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: radial-gradient(circle at top left, rgba(180, 83, 9, 0.16), transparent 32%),
                    radial-gradient(circle at bottom right, rgba(15, 118, 110, 0.12), transparent 26%),
                    var(--bg);
        color: var(--text);
      }}
      .wrap {{ max-width: 1120px; margin: 0 auto; padding: 30px 18px 56px; }}
      .hero {{ padding: 28px; border: 1px solid var(--border); border-radius: 24px; background: var(--panel); backdrop-filter: blur(10px); box-shadow: 0 20px 60px rgba(15,23,42,0.08); }}
      h1 {{ margin: 0 0 8px; font-size: clamp(2rem, 4vw, 3rem); }}
      .sub {{ margin: 0; color: var(--muted); line-height: 1.6; }}
      .chips {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 16px; }}
      .badge {{ display: inline-flex; align-items: center; border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: 700; border: 1px solid var(--border); }}
      .badge-neutral {{ background: #f8fafc; }}
      .badge-saved {{ background: rgba(15,118,110,0.10); color: var(--good); }}
      .badge-soft {{ background: rgba(180,83,9,0.10); color: var(--warn); }}
      .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }}
      .card {{ padding: 22px; border-radius: 22px; border: 1px solid var(--border); background: var(--panel); backdrop-filter: blur(10px); }}
      .card h2 {{ margin: 0 0 14px; font-size: 1.15rem; }}
      .mini {{ color: var(--muted); line-height: 1.55; font-size: 14px; }}
      .link {{ color: inherit; }}
      @media (max-width: 920px) {{
        .grid {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="hero">
        <h1>GlassyReply</h1>
        <p class="sub">Telegram Gmail assistant with SQLite-backed state, AI replies, and a protected config dashboard. The bot root is now a real landing page, not a 404.</p>
        <div class="chips">
          {render_status_badge(f"mode: {runtime.mode}", "neutral")}
          {render_status_badge(f"watch: {runtime.config.watch_interval}s", "neutral")}
          {render_status_badge(f"lang: {runtime.config.lang}", "neutral")}
          {render_status_badge("pixel on" if runtime.config.enable_pixel else "pixel off", "saved" if runtime.config.enable_pixel else "soft")}
        </div>
      </div>
      <div class="grid">
        <div class="card">
          <h2>What this page is</h2>
          <p class="mini">This public landing page only proves the app is alive. The full configuration view lives behind the signed Telegram dashboard link.</p>
          <p class="mini">Open Telegram and send <code>/setup</code> first if the bot is not configured yet, or <code>/config</code> / <code>/dashboard</code> once the owner is already claimed.</p>
        </div>
        <div class="card">
          <h2>Open the dashboard</h2>
          <p class="mini">Use <code>/setup</code> for the in-bot wizard and <code>/config</code> or <code>/dashboard</code> for the private web dashboard. Both avoid editing `.env` by hand.</p>
          <p class="mini"><a class="link" href="/healthz">/healthz</a> is the lightweight uptime check used by Fly.</p>
        </div>
        <div class="card">
          <h2>Status</h2>
          <p class="mini">If Gmail OAuth is missing or invalid, the bot will report it in logs instead of crashing silently.</p>
        </div>
        <div class="card">
          <h2>Notes</h2>
          <p class="mini">The detailed settings page stays private. That keeps runtime knobs and bootstrap secrets out of the public root response.</p>
        </div>
      </div>
    </div>
  </body>
</html>
"""


def kb_main(tg_message_id: int, starred: bool, attachments: List[dict]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📨 Invia", callback_data=f"send|{tg_message_id}"),
            InlineKeyboardButton("💾 Bozza", callback_data=f"draft|{tg_message_id}"),
        ],
        [
            InlineKeyboardButton("🤖 Analizza AI", callback_data=f"analyze|{tg_message_id}"),
            InlineKeyboardButton("✏️ Prompt AI", callback_data=f"ask|{tg_message_id}"),
        ],
        [
            InlineKeyboardButton("❌ Rifiuta", callback_data=f"reject|{tg_message_id}"),
            InlineKeyboardButton("🗑️ Cestino", callback_data=f"trash|{tg_message_id}"),
        ],
        [
            InlineKeyboardButton(
                "⭐ Unstar" if starred else "⭐ Star",
                callback_data=f"starT|{tg_message_id}",
            )
        ],
        [
            InlineKeyboardButton("🔁 Inoltra", callback_data=f"fwd|{tg_message_id}"),
            InlineKeyboardButton("🏷️ Tag ➜", callback_data=f"tag|{tg_message_id}|0"),
        ],
    ]
    if attachments:
        rows.append(
            [InlineKeyboardButton("📎 Allegati ➜", callback_data=f"attmenu|{tg_message_id}")]
        )
    return InlineKeyboardMarkup(rows)


def kb_tag(tg_message_id: int, page: int, labels: Dict[str, str]) -> InlineKeyboardMarkup:
    valid = [
        (label_id, name)
        for label_id, name in labels.items()
        if label_id not in {"INBOX", "SENT", "TRASH", "SPAM", "DRAFT"}
        and not label_id.startswith("CATEGORY_")
    ]
    start = page * PAGE_SIZE
    chunk = valid[start : start + PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(f"🏷️ {name[:20]}", callback_data=f"tagset|{tg_message_id}|{label_id}")]
        for label_id, name in chunk
    ]
    nav: List[InlineKeyboardButton] = []
    if page:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"tag|{tg_message_id}|{page - 1}"))
    if start + PAGE_SIZE < len(valid):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"tag|{tg_message_id}|{page + 1}"))
    nav.append(InlineKeyboardButton("⬅️ Back", callback_data=f"back|{tg_message_id}"))
    rows.append(nav)
    return InlineKeyboardMarkup(rows)


def kb_att(tg_message_id: int, attachments: List[dict]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                f"⬇️ {attachment['filename'][:25]}",
                callback_data=f"att|{tg_message_id}|{index}",
            )
        ]
        for index, attachment in enumerate(attachments)
    ]
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"back|{tg_message_id}")])
    return InlineKeyboardMarkup(rows)


def kb_fwd(config: Config, tg_message_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(address, callback_data=f"fwdto|{tg_message_id}|{address}")]
        for address in config.predef_fwd
    ]
    rows.append([InlineKeyboardButton("✉️ Altro…", callback_data=f"fwdother|{tg_message_id}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"back|{tg_message_id}")])
    return InlineKeyboardMarkup(rows)


def get_runtime(application: Application) -> Runtime:
    runtime = application.bot_data.get("runtime")
    if runtime is None:
        raise RuntimeError("Runtime not attached to application")
    return runtime


def is_authorized_update(update: Update, config: Config) -> bool:
    user = update.effective_user
    if not owner_configured(config):
        return False
    return bool(user and user.id == config.chat_id)


async def deny_unauthorized(update: Update) -> None:
    query = update.callback_query
    if query:
        await query.answer("Unauthorized", show_alert=True)
        return
    message = update.effective_message
    if message:
        await message.reply_text("Unauthorized")


async def ensure_authorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    runtime = get_runtime(context.application)
    if is_authorized_update(update, runtime.config):
        return True
    if not owner_configured(runtime.config):
        message = update.effective_message
        if message:
            await message.reply_text("Questo bot non ha ancora un owner. Usa /start per reclamarlo.")
        return False
    LOGGER.warning(
        "Rejected unauthorized Telegram user. user_id=%s",
        getattr(update.effective_user, "id", None),
    )
    await deny_unauthorized(update)
    return False


async def claim_owner_if_needed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    runtime = get_runtime(context.application)
    user = update.effective_user
    if user is None:
        return False
    if owner_configured(runtime.config):
        return user.id == runtime.config.chat_id
    claim_owner(runtime, user.id)
    LOGGER.info("Claimed bot owner user_id=%s", user.id)
    return True


async def send_setup_message(message, runtime: Runtime, text: str | None = None) -> None:
    await message.reply_text(
        text or setup_message_text(runtime),
        reply_markup=setup_keyboard(runtime),
        disable_web_page_preview=True,
    )


async def send_main_menu(message, text: str = "Menu pronto.") -> None:
    await message.reply_text(text, reply_markup=main_menu_keyboard())


async def send_settings_message(message, runtime: Runtime, text: str | None = None) -> None:
    await message.reply_text(
        text or settings_message_text(runtime),
        reply_markup=settings_keyboard(runtime),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def send_tracked_stats(message, runtime: Runtime) -> None:
    tracked_items = runtime.store.list_tracked_emails(limit=10)
    await message.reply_text(
        tracked_stats_text(tracked_items),
        reply_markup=stats_keyboard(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def prompt_for_setup_value(
    context: ContextTypes.DEFAULT_TYPE,
    runtime: Runtime,
    *,
    prompt_text: str,
    action_kind: str,
    reply_to_message_id: int | None = None,
) -> None:
    prompt_message = await context.bot.send_message(
        chat_id=runtime.config.chat_id,
        text=prompt_text,
        reply_to_message_id=reply_to_message_id,
        disable_web_page_preview=True,
    )
    runtime.store.add_interactive_prompt(prompt_message.message_id, action_kind)
    runtime.store.purge_old_rows(runtime.config.state_retention_days)


async def safe_edit(
    message,
    *,
    text: str | None = None,
    markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        if text is not None:
            await message.edit_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
                disable_web_page_preview=True,
            )
        else:
            await message.edit_reply_markup(reply_markup=markup)
    except BadRequest as exc:
        if "not modified" not in str(exc).lower():
            raise


async def ai_stream(model: Any, prompt: str, context: str, lang: str):
    full_prompt = f"{prompt}\n\nRispondi in {lang}.\n\n{context[:MAX_CHARS]}"
    for chunk in model.generate_content(full_prompt, stream=True):
        text = getattr(chunk, "text", "") or "".join(
            part.text for part in getattr(chunk, "parts", []) if getattr(part, "text", "")
        )
        if text.strip():
            yield text
        await asyncio.sleep(0)


async def ai_reply_stream(
    application: Application,
    runtime: Runtime,
    state: EmailState,
    prompt: str,
) -> None:
    if runtime.model is None:
        return
    progress = await application.bot.send_message(
        chat_id=runtime.config.chat_id,
        text="⌛ AI…",
        reply_to_message_id=state.tg_message_id,
    )
    accumulated = ""
    effective_prompt = prompt or runtime.config.system_prompt or DEFAULT_PROMPT
    async for chunk in ai_stream(runtime.model, effective_prompt, state.body, state.lang):
        accumulated += chunk
        if len(accumulated) >= TELEGRAM_MAX:
            break
        await safe_edit(progress, text=format_email_text(state, body_override=accumulated))
        await asyncio.sleep(0.4)

    final_text = accumulated[:TELEGRAM_MAX]
    state.ai_body = final_text
    runtime.store.update_ai_body(state.tg_message_id, final_text)
    runtime.store.purge_old_rows(runtime.config.state_retention_days)
    await safe_edit(progress, text=format_email_text(state, body_override=final_text))


def validate_email_address(raw: str) -> str:
    email_addr = parseaddr(raw)[1].strip()
    if not email_addr or "@" not in email_addr:
        raise ConfigError("Inserisci un indirizzo email valido.")
    return email_addr


async def begin_tracked_email_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    runtime: Runtime,
) -> None:
    if not runtime.config.enable_pixel or not runtime.config.pixel_base_url:
        await message.reply_text(
            "Il pixel non è configurato. Apri Impostazioni o /dashboard e completa Pixel URL + secret prima di creare email tracciate."
        )
        return
    await prompt_for_setup_value(
        context,
        runtime,
        prompt_text="Rispondi a questo messaggio con l'indirizzo destinatario della bozza tracciata.",
        action_kind="tracked_email_recipient",
        reply_to_message_id=message.message_id,
    )


async def create_tracked_draft(
    context: ContextTypes.DEFAULT_TYPE,
    runtime: Runtime,
    *,
    recipient: str,
    subject: str,
) -> None:
    placeholder = await context.bot.send_message(
        chat_id=runtime.config.chat_id,
        text="🛰️ Creo la bozza tracciata…",
        parse_mode=ParseMode.HTML,
    )
    tracking_markup = build_tracking_markup_for_message_id(runtime.config, placeholder.message_id)
    raw = build_raw(recipient, subject, "", tracking_markup)
    try:
        draft_payload = await asyncio.to_thread(runtime.gmail.create_draft, raw, "")
    except Exception as exc:
        LOGGER.exception("Tracked draft creation failed.")
        await safe_edit(
            placeholder,
            text=f"🛰️ <b>{ihtml.escape(subject)}</b>\n\nErrore nella creazione della bozza: {ihtml.escape(str(exc))}",
        )
        return

    tracked = TrackedEmail(
        tg_message_id=placeholder.message_id,
        draft_id=(draft_payload or {}).get("id", ""),
        recipient=recipient,
        subject=subject,
        open_count=0,
        first_opened_at="",
        last_opened_at="",
        last_classification="",
        last_layer="",
        last_dimensions="",
        last_confidence=None,
    )
    runtime.store.upsert_tracked_email(tracked)
    runtime.store.purge_old_rows(runtime.config.state_retention_days)
    note = "Bozza creata. Apri Gmail > Drafts, completa il testo e inviala."
    await safe_edit(placeholder, text=format_tracked_email_text(tracked, note=note), markup=stats_keyboard())


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    claimed = await claim_owner_if_needed(update, context)
    if not claimed:
        await deny_unauthorized(update)
        return
    runtime = get_runtime(context.application)
    await send_main_menu(update.effective_message, "Menu Telegram attivato.")
    await send_setup_message(
        update.effective_message,
        runtime,
        "Owner registrato. Da qui possiamo finire tutta la configurazione del bot.",
    )


async def cmd_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    claimed = await claim_owner_if_needed(update, context)
    if not claimed:
        await deny_unauthorized(update)
        return
    runtime = get_runtime(context.application)
    await send_main_menu(update.effective_message)
    await send_setup_message(update.effective_message, runtime)


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    await send_main_menu(update.effective_message)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    await send_setup_message(update.effective_message, runtime)


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    await send_settings_message(update.effective_message, runtime)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    await send_tracked_stats(update.effective_message, runtime)


async def cmd_tracked_email(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    await begin_tracked_email_flow(update.effective_message, context, runtime)


async def cmd_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    url = dashboard_url(runtime.config)
    await update.effective_message.reply_text(
        "Dashboard pronta.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Open dashboard", url=url)]]
        ),
        disable_web_page_preview=True,
    )


async def cmd_gmail_login(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    try:
        auth_url = start_google_oauth(runtime)
    except Exception as exc:
        await update.effective_message.reply_text(f"Gmail login non pronto: {exc}")
        return
    await update.effective_message.reply_text(
        "Apri questo link, fai login su Google e autorizza Gmail. Quando Google torna qui il token viene salvato automaticamente.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Connect Gmail", url=auth_url)]]),
        disable_web_page_preview=True,
    )


async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    if len(context.args) < 2:
        keys = ", ".join(sorted(BOT_CONFIG_ALIASES))
        await update.effective_message.reply_text(
            f"Uso: /set <chiave> <valore>\nChiavi supportate: {keys}"
        )
        return
    raw_key = context.args[0]
    raw_value = " ".join(context.args[1:]).strip()
    try:
        save_runtime_settings(runtime, {normalize_bot_config_key(raw_key): raw_value})
    except Exception as exc:
        await update.effective_message.reply_text(f"Config non salvata: {exc}")
        return
    await send_setup_message(update.effective_message, runtime, f"Salvato {raw_key}.")


async def cmd_unset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    runtime = get_runtime(context.application)
    if not context.args:
        await update.effective_message.reply_text("Uso: /unset <chiave>")
        return
    try:
        key = normalize_bot_config_key(context.args[0])
    except Exception as exc:
        await update.effective_message.reply_text(str(exc))
        return
    runtime.store.delete_app_setting(key)
    apply_runtime_overrides(runtime)
    await send_setup_message(update.effective_message, runtime, f"Rimosso {context.args[0]}.")


async def handle_setup_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    runtime: Runtime,
    action: str,
) -> bool:
    query = update.callback_query
    if query is None or query.message is None:
        return False
    if action == "status":
        await safe_edit(query.message, text=setup_message_text(runtime), markup=setup_keyboard(runtime))
        await query.answer()
        return True
    if action == "dashboard":
        await query.answer()
        await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text="Dashboard privata pronta.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Open dashboard", url=dashboard_url(runtime.config))]]
            ),
            disable_web_page_preview=True,
        )
        return True
    if action == "google_api_key":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi a questo messaggio con la tua `GOOGLE_API_KEY`.",
            action_kind="setup_google_api_key",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "public_base_url":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi con la `PUBLIC_BASE_URL` pubblica del bot, per esempio https://glassyreply-bot.fly.dev",
            action_kind="setup_public_base_url",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "oauth_json":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Carica qui il file JSON del client OAuth Google di tipo Web, oppure incolla il JSON completo come testo.",
            action_kind="setup_google_oauth_json",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "gmail_login":
        try:
            auth_url = start_google_oauth(runtime)
        except Exception as exc:
            await query.answer(f"Non pronto: {exc}", show_alert=True)
            return True
        await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text="Apri il link per collegare Gmail. Il token viene salvato al ritorno da Google.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Connect Gmail", url=auth_url)]]),
            disable_web_page_preview=True,
        )
        await query.answer()
        return True
    return False


async def handle_settings_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    runtime: Runtime,
    action: str,
) -> bool:
    query = update.callback_query
    if query is None or query.message is None:
        return False
    if action == "status":
        await safe_edit(query.message, text=settings_message_text(runtime), markup=settings_keyboard(runtime))
        await query.answer()
        return True
    if action == "monitor_labels":
        summary = "Seleziona le cartelle/categorie Gmail da inoltrare su Telegram."
        await safe_edit(query.message, text=summary, markup=gmail_monitor_keyboard(runtime))
        await query.answer()
        return True
    if action == "dashboard":
        await query.answer()
        await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text="Dashboard privata pronta.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("Open dashboard", url=dashboard_url(runtime.config))]]
            ),
            disable_web_page_preview=True,
        )
        return True
    if action == "system_prompt":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi con il nuovo prompt di sistema di Gemini. Verra' usato per le bozze AI automatiche.",
            action_kind="setup_system_prompt",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "toggle_label":
        if len(query.data.split("|")) < 3:
            await query.answer("Label mancante", show_alert=True)
            return True
        label_id = query.data.split("|", 2)[2]
        selected = list(runtime.config.gmail_monitor_labels)
        if label_id in selected:
            if len(selected) == 1:
                await query.answer("Lascia almeno una cartella attiva.", show_alert=True)
                return True
            selected = [item for item in selected if item != label_id]
        else:
            selected.append(label_id)
        try:
            save_runtime_settings(runtime, {"GMAIL_MONITOR_LABELS": json.dumps(selected)})
        except Exception as exc:
            await query.answer(f"Config err: {exc}", show_alert=True)
            return True
        await safe_edit(
            query.message,
            text="Seleziona le cartelle/categorie Gmail da inoltrare su Telegram.",
            markup=gmail_monitor_keyboard(runtime),
        )
        await query.answer("Cartelle aggiornate")
        return True
    if action == "google_api_key":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi a questo messaggio con la tua `GOOGLE_API_KEY`.",
            action_kind="setup_google_api_key",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "public_base_url":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi con la `PUBLIC_BASE_URL` pubblica del bot.",
            action_kind="setup_public_base_url",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "pixel_base_url":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi con la `PIXEL_BASE_URL`, ad esempio il tuo Worker/endpoint pubblico.",
            action_kind="setup_pixel_base_url",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "pixel_webhook_secret":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Rispondi con la `PIXEL_WEBHOOK_SECRET` condivisa col pixel worker.",
            action_kind="setup_pixel_webhook_secret",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "toggle_pixel":
        try:
            new_value = "0" if runtime.config.enable_pixel else "1"
            save_runtime_settings(runtime, {"ENABLE_PIXEL": new_value})
        except Exception as exc:
            await query.answer(f"Config err: {exc}", show_alert=True)
            return True
        await safe_edit(query.message, text=settings_message_text(runtime), markup=settings_keyboard(runtime))
        await query.answer("Pixel aggiornato")
        return True
    if action == "oauth_json":
        await prompt_for_setup_value(
            context,
            runtime,
            prompt_text="Carica qui il file JSON del client OAuth Google di tipo Web, oppure incolla il JSON completo come testo.",
            action_kind="setup_google_oauth_json",
            reply_to_message_id=query.message.message_id,
        )
        await query.answer()
        return True
    if action == "gmail_login":
        try:
            auth_url = start_google_oauth(runtime)
        except Exception as exc:
            await query.answer(f"Non pronto: {exc}", show_alert=True)
            return True
        await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text="Apri il link per collegare Gmail. Il token viene salvato al ritorno da Google.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Connect Gmail", url=auth_url)]]),
            disable_web_page_preview=True,
        )
        await query.answer()
        return True
    return False


async def handle_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    message = update.effective_message
    if message is None or not message.text or message.reply_to_message is not None:
        return
    runtime = get_runtime(context.application)
    if message.text == MENU_TRACKED_EMAIL:
        await begin_tracked_email_flow(message, context, runtime)
        return
    if message.text == MENU_STATS:
        await send_tracked_stats(message, runtime)
        return
    if message.text == MENU_SETTINGS:
        await send_settings_message(message, runtime)


async def txt_followup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return

    message = update.effective_message
    if not message or not message.reply_to_message:
        return

    runtime = get_runtime(context.application)
    interactive = runtime.store.pop_interactive_prompt(message.reply_to_message.message_id)
    if interactive is not None:
        raw_text = message.text.strip() if message.text else ""
        document_bytes: bytes | None = None
        if message.document:
            telegram_file = await message.document.get_file()
            document_bytes = bytes(await telegram_file.download_as_bytearray())
        try:
            if interactive.action_kind == "setup_google_api_key":
                if not raw_text:
                    raise ConfigError("Serve una Google API key testuale.")
                save_runtime_settings(runtime, {"GOOGLE_API_KEY": raw_text})
                await send_setup_message(message, runtime, "Google API key salvata.")
                return
            if interactive.action_kind == "setup_public_base_url":
                if not raw_text:
                    raise ConfigError("Serve una PUBLIC_BASE_URL testuale.")
                save_runtime_settings(runtime, {"PUBLIC_BASE_URL": raw_text})
                await send_setup_message(message, runtime, "Public base URL salvata.")
                return
            if interactive.action_kind == "setup_pixel_base_url":
                if not raw_text:
                    raise ConfigError("Serve una PIXEL_BASE_URL testuale.")
                save_runtime_settings(runtime, {"PIXEL_BASE_URL": raw_text})
                await send_settings_message(message, runtime, "Pixel base URL salvata.")
                return
            if interactive.action_kind == "setup_pixel_webhook_secret":
                if not raw_text:
                    raise ConfigError("Serve una PIXEL_WEBHOOK_SECRET testuale.")
                save_runtime_settings(runtime, {"PIXEL_WEBHOOK_SECRET": raw_text})
                await send_settings_message(message, runtime, "Pixel webhook secret salvata.")
                return
            if interactive.action_kind == "setup_system_prompt":
                if not raw_text:
                    raise ConfigError("Serve un prompt testuale.")
                save_runtime_settings(runtime, {"SYSTEM_PROMPT": raw_text})
                await send_settings_message(message, runtime, "Prompt Gemini salvato.")
                return
            if interactive.action_kind == "setup_google_oauth_json":
                if document_bytes is not None:
                    raw_text = document_bytes.decode("utf-8")
                if not raw_text:
                    raise ConfigError("Serve un file JSON o il JSON incollato come testo.")
                parsed = json.loads(raw_text)
                if not isinstance(parsed, dict):
                    raise ConfigError("Il JSON OAuth deve essere un oggetto.")
                save_runtime_settings(
                    runtime,
                    {
                        "GOOGLE_OAUTH_CREDENTIALS_JSON": json.dumps(
                            parsed,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )
                    },
                )
                await send_setup_message(
                    message,
                    runtime,
                    "Google OAuth client JSON salvato. Ora puoi usare /gmail_login o il bottone Connect Gmail.",
                )
                return
            if interactive.action_kind == "tracked_email_recipient":
                recipient = validate_email_address(raw_text)
                runtime.store.set_bot_state(TRACKED_DRAFT_RECIPIENT_KEY, recipient)
                await prompt_for_setup_value(
                    context,
                    runtime,
                    prompt_text="Perfetto. Ora rispondi con l'oggetto della bozza tracciata.",
                    action_kind="tracked_email_subject",
                    reply_to_message_id=message.message_id,
                )
                return
            if interactive.action_kind == "tracked_email_subject":
                subject = raw_text.strip()
                if not subject:
                    raise ConfigError("Serve un oggetto non vuoto.")
                recipient = runtime.store.get_bot_state(TRACKED_DRAFT_RECIPIENT_KEY) or ""
                if not recipient:
                    raise ConfigError("Destinatario non trovato. Premi di nuovo Email Tracciata.")
                runtime.store.set_bot_state(TRACKED_DRAFT_SUBJECT_KEY, subject)
                await create_tracked_draft(context, runtime, recipient=recipient, subject=subject)
                runtime.store.set_bot_state(TRACKED_DRAFT_RECIPIENT_KEY, "")
                runtime.store.set_bot_state(TRACKED_DRAFT_SUBJECT_KEY, "")
                return
            await message.reply_text("Prompt interattivo non riconosciuto.")
            return
        except Exception as exc:
            runtime.store.add_interactive_prompt(message.reply_to_message.message_id, interactive.action_kind)
            await message.reply_text(f"Config non salvata: {exc}")
            return

    if not message.text:
        return

    pending = runtime.store.pop_pending_action(message.reply_to_message.message_id)
    if pending is None:
        return

    state = runtime.store.get_email_state(pending.root_tg_message_id)
    if state is None:
        await message.reply_text("Contesto email non trovato.")
        return

    try:
        if pending.action_kind == "ask":
            await ai_reply_stream(context.application, runtime, state, message.text.strip())
        elif pending.action_kind == "forward":
            await asyncio.to_thread(gmail_forward, runtime, state.gmail_message_id, message.text.strip())
            await context.bot.send_message(
                chat_id=runtime.config.chat_id,
                text=f"Inoltrata a {message.text.strip()}",
                reply_to_message_id=state.tg_message_id,
            )
    except Exception as exc:
        LOGGER.exception("Follow-up action failed.")
        await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text=f"⚠️ Action err: {exc}",
            reply_to_message_id=state.tg_message_id,
        )


async def cb_btn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return

    query = update.callback_query
    if query is None or not query.data:
        return

    runtime = get_runtime(context.application)
    parts = query.data.split("|")
    action = parts[0]
    if action == "setup":
        handled = await handle_setup_callback(update, context, runtime, parts[1] if len(parts) > 1 else "")
        if not handled:
            await query.answer("Unsupported", show_alert=True)
        return
    if action == "settings":
        handled = await handle_settings_callback(update, context, runtime, parts[1] if len(parts) > 1 else "")
        if not handled:
            await query.answer("Unsupported", show_alert=True)
        return
    if action == "stats":
        if query.message is not None:
            await safe_edit(query.message, text=tracked_stats_text(runtime.store.list_tracked_emails(limit=10)), markup=stats_keyboard())
        await query.answer()
        return
    if action == "tracked":
        if query.message is not None:
            await begin_tracked_email_flow(query.message, context, runtime)
        await query.answer()
        return
    tg_message_id = int(parts[1])
    state = runtime.store.get_email_state(tg_message_id)

    if state is None:
        await query.answer("Not found", show_alert=True)
        return

    message = query.message
    if message is None:
        await query.answer()
        return

    if action == "tag":
        page = int(parts[2])
        labels = await asyncio.to_thread(runtime.gmail.refresh_labels)
        await safe_edit(message, markup=kb_tag(tg_message_id, page, labels))
        await query.answer()
        return

    if action == "tagset":
        label_id = parts[2]
        labels = await asyncio.to_thread(runtime.gmail.refresh_labels)
        await asyncio.to_thread(runtime.gmail.modify_message, state.gmail_message_id, [label_id], None)
        await safe_edit(message, markup=kb_main(tg_message_id, state.starred, state.attachments))
        await query.answer(f"🏷️ {labels.get(label_id, label_id)}")
        return

    if action == "back":
        await safe_edit(message, markup=kb_main(tg_message_id, state.starred, state.attachments))
        await query.answer()
        return

    if action == "ask":
        prompt_message = await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text="✏️ Scrivi il prompt per l'AI su questa mail (rispondi qui).",
            reply_to_message_id=message.message_id,
        )
        runtime.store.add_pending_action(prompt_message.message_id, tg_message_id, "ask")
        runtime.store.purge_old_rows(runtime.config.state_retention_days)
        await query.answer()
        return

    if action == "analyze":
        if runtime.model is None:
            await query.answer("Gemini non configurato.", show_alert=True)
            return
        await query.answer("Analisi AI in corso…")
        await ai_reply_stream(context.application, runtime, state, runtime.config.system_prompt)
        return

    if action == "starT":
        new_state = not state.starred
        add = ["STARRED"] if new_state else None
        rem = None if new_state else ["STARRED"]
        await asyncio.to_thread(runtime.gmail.modify_message, state.gmail_message_id, add, rem)
        state.starred = new_state
        runtime.store.update_starred(tg_message_id, new_state)
        await safe_edit(message, markup=kb_main(tg_message_id, state.starred, state.attachments))
        await query.answer("⭐ on" if new_state else "⭐ off")
        return

    if action == "attmenu":
        await safe_edit(message, markup=kb_att(tg_message_id, state.attachments))
        await query.answer()
        return

    if action == "att":
        index = int(parts[2])
        attachment = state.attachments[index]
        try:
            data64 = attachment.get("data")
            if not data64 and attachment.get("id"):
                data64 = await asyncio.to_thread(
                    runtime.gmail.get_attachment_data,
                    state.gmail_message_id,
                    attachment["id"],
                )
            if not data64:
                raise RuntimeError("Attachment data unavailable")
            decoded = base64.urlsafe_b64decode(data64)
            await context.bot.send_document(
                chat_id=runtime.config.chat_id,
                document=InputFile(BytesIO(decoded), filename=attachment["filename"]),
            )
            await query.answer()
        except Exception as exc:
            LOGGER.exception("Attachment download failed.")
            await query.answer(f"Err: {exc}", show_alert=True)
        return

    if action == "fwd":
        await safe_edit(message, markup=kb_fwd(runtime.config, tg_message_id))
        await query.answer()
        return

    if action == "fwdto":
        await asyncio.to_thread(gmail_forward, runtime, state.gmail_message_id, parts[2])
        await safe_edit(message, markup=kb_main(tg_message_id, state.starred, state.attachments))
        await query.answer("Inoltrata")
        return

    if action == "fwdother":
        prompt_message = await context.bot.send_message(
            chat_id=runtime.config.chat_id,
            text="✉️ Rispondi con indirizzo.",
            reply_to_message_id=message.message_id,
        )
        runtime.store.add_pending_action(prompt_message.message_id, tg_message_id, "forward")
        runtime.store.purge_old_rows(runtime.config.state_retention_days)
        await query.answer()
        return

    tracking_markup = build_tracking_markup(runtime.config, state)
    body_to_send = state.ai_body or state.body

    try:
        if action == "send":
            if not state.ai_body:
                await query.answer("Premi prima 🤖 Analizza AI o usa Prompt AI.", show_alert=True)
                return
            raw = build_raw(state.sender, "Re: " + state.subject, body_to_send, tracking_markup)
            await asyncio.to_thread(runtime.gmail.send_raw_message, raw, state.gmail_thread_id)
        elif action == "draft":
            if not state.ai_body:
                await query.answer("Premi prima 🤖 Analizza AI o usa Prompt AI.", show_alert=True)
                return
            raw = build_raw(state.sender, "Re: " + state.subject, body_to_send, tracking_markup)
            await asyncio.to_thread(runtime.gmail.create_draft, raw, state.gmail_thread_id)
        elif action == "trash":
            await asyncio.to_thread(
                runtime.gmail.modify_message,
                state.gmail_message_id,
                ["TRASH"],
                ["INBOX"],
            )
        elif action == "reject":
            pass
        else:
            await query.answer("Unsupported", show_alert=True)
            return
        await query.answer({"send": "📨", "draft": "💾", "trash": "🗑️", "reject": "❌"}[action] + " ok")
        await safe_edit(message, markup=None)
    except HttpError as exc:
        LOGGER.exception("Callback Gmail action failed.")
        await query.answer(f"Err: {exc}", show_alert=True)
    except Exception as exc:
        LOGGER.exception("Callback action failed.")
        await query.answer(f"Err: {exc}", show_alert=True)


async def process_new_email(application: Application, runtime: Runtime, gmail_message_id: str) -> None:
    if not owner_configured(runtime.config):
        return
    lang = runtime.config.lang
    payload = await asyncio.to_thread(runtime.gmail.get_full_message, gmail_message_id)
    message_payload = payload["payload"]
    headers = message_payload.get("headers", [])
    subject = decode_hdr(extract_header(headers, "subject", "(senza oggetto)")) or "(senza oggetto)"
    body = payload_text(message_payload)
    attachments = list_attachments(message_payload)
    sender = parseaddr(extract_header(headers, "from", ""))[1]
    tg_message = await application.bot.send_message(
        chat_id=runtime.config.chat_id,
        text=format_email_text(
            EmailState(
                tg_message_id=0,
                gmail_message_id=gmail_message_id,
                gmail_thread_id=payload.get("threadId", ""),
                sender=sender,
                subject=subject,
                body=body,
                header=f"📧 {subject}",
                attachments=attachments,
                starred="STARRED" in payload.get("labelIds", []),
                lang=lang,
            ),
            status_line="Premi 🤖 Analizza AI per generare una proposta di risposta.",
        ),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    state = EmailState(
        tg_message_id=tg_message.message_id,
        gmail_message_id=gmail_message_id,
        gmail_thread_id=payload.get("threadId", ""),
        sender=sender,
        subject=subject,
        body=body,
        header=f"📧 {subject}",
        attachments=attachments,
        starred="STARRED" in payload.get("labelIds", []),
        lang=lang,
    )
    runtime.store.upsert_email_state(state)
    runtime.store.purge_old_rows(runtime.config.state_retention_days)
    await safe_edit(tg_message, markup=kb_main(state.tg_message_id, state.starred, attachments))


async def watcher(runtime: Runtime, application: Application) -> None:
    if not gmail_ready_for_watch(runtime.config):
        LOGGER.info("Watcher waiting for owner/Gmail setup.")
    last_seen = runtime.store.get_bot_state(LAST_SEEN_KEY)
    if not last_seen and gmail_ready_for_watch(runtime.config):
        try:
            recent_ids = await asyncio.to_thread(
                runtime.gmail.list_recent_monitored_ids,
                runtime.config.gmail_monitor_labels,
                1,
            )
            if recent_ids:
                if gmail_initial_sync_pending(runtime):
                    await process_new_email(application, runtime, recent_ids[0])
                    clear_gmail_initial_sync_pending(runtime)
                last_seen = recent_ids[0]
                runtime.store.set_bot_state(LAST_SEEN_KEY, last_seen)
        except Exception:
            LOGGER.exception("Initial Gmail watcher bootstrap failed.")
            last_seen = None

    while not runtime.shutdown_event.is_set():
        if not gmail_ready_for_watch(runtime.config):
            try:
                interval = max(1, int(runtime.config.watch_interval))
                await asyncio.wait_for(runtime.shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
            continue
        try:
            recent_ids = await asyncio.to_thread(
                runtime.gmail.list_recent_monitored_ids,
                runtime.config.gmail_monitor_labels,
            )
            if recent_ids:
                unseen_ids, newest_seen = split_unseen_inbox_ids(recent_ids, last_seen)
                if unseen_ids:
                    for gmail_message_id in unseen_ids:
                        await process_new_email(application, runtime, gmail_message_id)
                    last_seen = newest_seen
                    if newest_seen:
                        runtime.store.set_bot_state(LAST_SEEN_KEY, newest_seen)
                elif newest_seen and newest_seen != last_seen:
                    last_seen = newest_seen
                    runtime.store.set_bot_state(LAST_SEEN_KEY, newest_seen)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Watcher loop failed.")
        try:
            interval = max(1, int(runtime.config.watch_interval))
            await asyncio.wait_for(runtime.shutdown_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def on_err(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Telegram handler error.", exc_info=context.error)
    try:
        runtime = get_runtime(context.application)
    except Exception:
        return
    if not owner_configured(runtime.config):
        return
    try:
        await context.bot.send_message(runtime.config.chat_id, f"⚠️ Errore: {context.error}")
    except Exception:
        LOGGER.exception("Failed to send Telegram error notification.")


def create_web_app(runtime: Runtime, application: Application) -> Quart:
    app = Quart(__name__)

    def dashboard_token_from_request() -> str | None:
        return request.args.get("token") or request.headers.get("X-Dashboard-Token")

    async def require_dashboard_auth() -> tuple[bool, str | None]:
        token = dashboard_token_from_request()
        if verify_dashboard_token(runtime.config, token):
            return True, token
        return False, token

    @app.get("/")
    async def index():
        return landing_page(runtime)

    @app.get("/healthz")
    async def healthz():
        return jsonify({"status": "ok", "mode": runtime.mode})

    @app.get("/config")
    @app.get("/dashboard")
    async def dashboard_get():
        authorized, token = await require_dashboard_auth()
        if not authorized:
            return (
                "<h1>Unauthorized</h1><p>Use the Telegram command <code>/setup</code> or "
                "<code>/config</code> to open a private control link.</p>",
                401,
            )
        return dashboard_page(runtime, title="GlassyReply dashboard", token=token or "")

    @app.post("/config")
    @app.post("/dashboard")
    async def dashboard_post():
        form_data = await request.form
        token = dashboard_token_from_request() or form_data.get("token")
        if not verify_dashboard_token(runtime.config, token):
            return (
                "<h1>Unauthorized</h1><p>Use the Telegram command <code>/setup</code> or "
                "<code>/config</code> to open a private control link.</p>",
                401,
            )
        current_overrides = runtime.store.get_app_settings()
        candidate_overrides = parse_dashboard_overrides(form_data, current_overrides)
        try:
            build_candidate_config(runtime, candidate_overrides)
        except ConfigError as exc:
            return (
                dashboard_page(
                    runtime,
                    title="GlassyReply dashboard",
                    token=token or "",
                    errors=[str(exc)],
                ),
                400,
            )

        sync_dashboard_overrides(runtime, candidate_overrides)
        try:
            if runtime.mode == "webhook" and runtime.config.telegram_webhook_url:
                await application.bot.set_webhook(
                    url=runtime.config.resolved_telegram_webhook_url(),
                    secret_token=runtime.config.telegram_webhook_secret,
                    drop_pending_updates=False,
                )
        except Exception:
            LOGGER.exception("Failed to refresh Telegram webhook after dashboard save.")
        return dashboard_page(
            runtime,
            title="GlassyReply dashboard",
            token=token or "",
            message="Configuration saved.",
        )

    @app.get("/oauth/google/callback")
    async def google_oauth_callback():
        state_payload_raw = runtime.store.get_bot_state(GOOGLE_OAUTH_STATE_KEY)
        if not state_payload_raw:
            return "<h1>OAuth state missing</h1><p>Restart the Gmail login from Telegram.</p>", 400
        try:
            state_payload = parse_google_oauth_state_payload(state_payload_raw)
            expected_state = state_payload["state"]
        except Exception:
            return "<h1>OAuth state invalid</h1><p>Restart the Gmail login from Telegram.</p>", 400
        if request.args.get("state") != expected_state:
            return "<h1>OAuth state mismatch</h1><p>Restart the Gmail login from Telegram.</p>", 400
        code_verifier = state_payload.get("code_verifier")
        if not code_verifier:
            return "<h1>OAuth state expired</h1><p>Restart the Gmail login from Telegram.</p>", 400
        try:
            client_config = google_web_client_config(runtime.config)
            flow = Flow.from_client_config(
                client_config,
                scopes=SCOPES,
                state=expected_state,
                redirect_uri=google_oauth_redirect_url(runtime.config),
                code_verifier=code_verifier,
            )
            flow.fetch_token(
                authorization_response=google_oauth_authorization_response(
                    runtime.config,
                    request.query_string.decode(),
                )
            )
            save_runtime_settings(runtime, {"GOOGLE_OAUTH_TOKEN_JSON": flow.credentials.to_json()})
            clear_google_oauth_state(runtime)
            mark_gmail_initial_sync_pending(runtime)
            if owner_configured(runtime.config):
                await application.bot.send_message(
                    chat_id=runtime.config.chat_id,
                    text="Gmail collegato con successo. Il token OAuth e' stato salvato.",
                    reply_markup=setup_keyboard(runtime),
                    disable_web_page_preview=True,
                )
            return (
                "<h1>Gmail connected</h1><p>You can close this page and go back to Telegram.</p>",
                200,
            )
        except Exception as exc:
            LOGGER.exception("Google OAuth callback failed.")
            return (
                f"<h1>OAuth error</h1><p>{ihtml.escape(str(exc))}</p>",
                500,
            )

    @app.post("/pixel_status")
    async def pixel_status():
        if request.headers.get("X-Pixel-Secret") != runtime.config.pixel_webhook_secret:
            return jsonify({"status": "unauthorized"}), 401

        data = await request.get_json(silent=True) or {}
        tg_message_id = data.get("tg_msg_id")
        is_user_open = data.get("is_user_open")
        email_subject = data.get("email_subject") or ""
        classification = data.get("classification") or ""
        layer = data.get("layer") or "img"
        dimensions = data.get("dimensions") or ""
        confidence = data.get("confidence")
        try:
            confidence_value = float(confidence) if confidence not in (None, "") else None
        except (TypeError, ValueError):
            confidence_value = None
        is_user_open_value: bool | None
        if is_user_open is None:
            is_user_open_value = None
        elif isinstance(is_user_open, bool):
            is_user_open_value = is_user_open
        else:
            is_user_open_value = parse_bool(str(is_user_open), default=False)

        if not tg_message_id:
            return jsonify({"status": "error", "message": "tg_msg_id missing"}), 400

        original = runtime.store.get_email_state(int(tg_message_id))
        tracked = runtime.store.record_pixel_event(
            tg_message_id=int(tg_message_id),
            classification=classification,
            layer=layer,
            dimensions=dimensions,
            confidence=confidence_value,
            is_user_open=is_user_open_value,
            email_subject=email_subject,
        )
        if original:
            if classification:
                icon = "✅" if classification == "human_browser" else "⚠️"
                confidence_text = f", conf {confidence}" if confidence is not None else ""
                status_text = f"{icon} {classification.replace('_', ' ')} via {layer}"
                if dimensions:
                    status_text += f" ({dimensions}{confidence_text})"
            else:
                status_icon = "✅" if is_user_open else "❌"
                status_text = f"{status_icon} opened by user" if is_user_open else "❌ opened by proxy"
            text = format_email_text(
                original,
                status_line=f"{status_text} {f'[{email_subject}]' if email_subject else ''}".strip(),
            )
        elif tracked:
            note = email_subject or "Evento pixel ricevuto."
            text = format_tracked_email_text(tracked, note=note)
        else:
            fallback = EmailState(
                tg_message_id=int(tg_message_id),
                gmail_message_id="",
                gmail_thread_id="",
                sender="",
                subject=email_subject or "Tracked email",
                body="Original text unavailable",
                header="",
                attachments=[],
                starred=False,
                lang="it",
            )
            text = format_email_text(fallback)

        try:
            await application.bot.edit_message_text(
                chat_id=runtime.config.chat_id,
                message_id=int(tg_message_id),
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            return jsonify({"status": "success"}), 200
        except Exception as exc:
            LOGGER.exception("Pixel webhook update failed.")
            return jsonify({"status": "error", "message": str(exc)}), 500

    @app.post("/telegram/webhook")
    async def telegram_webhook():
        if runtime.mode != "webhook":
            return jsonify({"status": "disabled"}), 404
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != runtime.config.telegram_webhook_secret:
            return jsonify({"status": "unauthorized"}), 401
        data = await request.get_json(silent=True) or {}
        update = Update.de_json(data, application.bot)
        if update is None:
            return jsonify({"status": "invalid"}), 400
        await application.update_queue.put(update)
        return jsonify({"status": "ok"}), 200

    return app


async def run_http_server(runtime: Runtime, web_app: Quart) -> None:
    server_config = HypercornConfig()
    server_config.bind = [f"{runtime.config.host}:{runtime.config.port}"]
    server_config.use_reloader = False
    await serve(web_app, server_config, shutdown_trigger=runtime.shutdown_event.wait)


def install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def trigger_shutdown() -> None:
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGABRT):
        try:
            loop.add_signal_handler(sig, trigger_shutdown)
        except NotImplementedError:
            LOGGER.warning("Signal handlers unavailable on this platform.")
            break


def build_application(runtime: Runtime) -> Application:
    application = (
        Application.builder()
        .token(runtime.config.bot_token)
        .request(HTTPXRequest(read_timeout=60, connect_timeout=20))
        .build()
    )
    application.bot_data["runtime"] = runtime
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("setup", cmd_setup))
    application.add_handler(CommandHandler("settings", cmd_settings))
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("stats", cmd_stats))
    application.add_handler(CommandHandler("tracked", cmd_tracked_email))
    application.add_handler(CommandHandler("config", cmd_dashboard))
    application.add_handler(CommandHandler("dashboard", cmd_dashboard))
    application.add_handler(CommandHandler("gmail_login", cmd_gmail_login))
    application.add_handler(CommandHandler("set", cmd_set))
    application.add_handler(CommandHandler("unset", cmd_unset))
    application.add_handler(CallbackQueryHandler(cb_btn))
    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND
            & filters.Regex(
                rf"^({re.escape(MENU_TRACKED_EMAIL)}|{re.escape(MENU_STATS)}|{re.escape(MENU_SETTINGS)})$"
            ),
            handle_menu_text,
        )
    )
    application.add_handler(
        MessageHandler((filters.TEXT & ~filters.COMMAND) | filters.Document.ALL, txt_followup)
    )
    application.add_error_handler(on_err)
    return application


async def bootstrap_telegram(application: Application, runtime: Runtime, mode: str) -> None:
    await application.initialize()
    if mode == "polling":
        await application.bot.delete_webhook(drop_pending_updates=False)
        if not application.updater:
            raise RuntimeError("Polling mode requires Telegram updater")
        await application.updater.start_polling()
    else:
        await application.bot.set_webhook(
            url=runtime.config.resolved_telegram_webhook_url(),
            secret_token=runtime.config.telegram_webhook_secret,
            drop_pending_updates=False,
        )
    await application.start()


async def shutdown_telegram(application: Application, runtime: Runtime, mode: str) -> None:
    if mode == "polling" and application.updater and application.updater.running:
        await application.updater.stop()
    if mode == "webhook":
        try:
            await application.bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            LOGGER.exception("Failed to delete Telegram webhook.")
    if application.running:
        await application.stop()
    await application.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=int, default=None)
    parser.add_argument("--lang", default=None)
    parser.add_argument("--mode", choices=["polling", "webhook"], default="polling")
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    base_config = Config.from_env()
    startup_overrides: Dict[str, str] = {}
    if args.interval is not None:
        startup_overrides["WATCH_INTERVAL"] = str(args.interval)
    if args.lang:
        startup_overrides["LANG"] = args.lang
    base_config.ensure_storage()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = StateStore(base_config.state_db_path)
    stored_overrides = store.get_app_settings()
    config = base_config.with_overrides(stored_overrides).with_overrides(startup_overrides)
    config.validate_effective(args.mode)
    config.materialize_google_credentials()
    config.materialize_gmail_token()
    store.purge_old_rows(config.state_retention_days)

    model: Any = None
    if config.google_api_key:
        genai.configure(api_key=config.google_api_key)
        model = genai.GenerativeModel(config.ai_model)

    runtime = Runtime(
        base_config=base_config,
        config=config,
        startup_overrides=startup_overrides,
        store=store,
        gmail=GmailClient(config),
        model=model,
        shutdown_event=asyncio.Event(),
        mode=args.mode,
    )
    install_signal_handlers(runtime.shutdown_event)

    application = build_application(runtime)
    web_app = create_web_app(runtime, application)
    http_task: asyncio.Task[Any] | None = None
    watcher_task: asyncio.Task[Any] | None = None
    stop_task: asyncio.Task[Any] | None = None

    if gmail_ready_for_watch(runtime.config):
        try:
            await asyncio.to_thread(runtime.gmail.refresh_labels, True)
            gmail_bootstrap_error = None
        except Exception as exc:
            LOGGER.exception("Initial Gmail label load failed.")
            gmail_bootstrap_error = str(exc)
    else:
        gmail_bootstrap_error = None

    try:
        await bootstrap_telegram(application, runtime, args.mode)
        startup_notice = startup_notice_text(runtime, gmail_bootstrap_error)
        if startup_notice:
            try:
                await application.bot.send_message(
                    chat_id=runtime.config.chat_id,
                    text=startup_notice,
                    disable_web_page_preview=True,
                )
            except Exception:
                LOGGER.exception("Failed to deliver startup notice to Telegram.")
        http_task = asyncio.create_task(run_http_server(runtime, web_app))
        watcher_task = asyncio.create_task(watcher(runtime, application))
        stop_task = asyncio.create_task(runtime.shutdown_event.wait())

        done, _ = await asyncio.wait(
            [task for task in (http_task, watcher_task, stop_task) if task is not None],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            if task is stop_task:
                continue
            exc = task.exception()
            if exc:
                raise exc
    finally:
        runtime.shutdown_event.set()
        if watcher_task:
            watcher_task.cancel()
            await asyncio.gather(watcher_task, return_exceptions=True)
        if http_task:
            await asyncio.gather(http_task, return_exceptions=True)
        if stop_task:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
        try:
            await shutdown_telegram(application, runtime, args.mode)
        finally:
            store.close()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except ConfigError as exc:
        raise SystemExit(f"Config error: {exc}") from exc
    except KeyboardInterrupt:
        LOGGER.info("Shutting down.")


if __name__ == "__main__":
    main()
