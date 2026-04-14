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
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.utils import parseaddr
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from uuid import uuid4

from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import google.generativeai as genai
from hypercorn.asyncio import serve
from hypercorn.config import Config as HypercornConfig
from quart import Quart, jsonify, request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputFile, Update
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
PREDEF_FWD = ["redazione@example.com", "boss@example.com"]
DEFAULT_PROMPT = (
    "Sei un assistente professionale. Scrivi una risposta "
    "educata, chiara e concisa all'e-mail seguente. Includi saluti "
    "e firma se opportuno."
)


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


@dataclass(slots=True)
class Config:
    bot_token: str
    chat_id: int
    google_api_key: str
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
    telegram_webhook_url: str
    telegram_webhook_secret: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Config":
        load_dotenv()
        source = dict(os.environ if env is None else env)

        bot_token = source.get("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id_raw = source.get("TELEGRAM_CHAT_ID", "").strip()
        google_api_key = source.get("GOOGLE_API_KEY", "").strip()
        try:
            chat_id = int(chat_id_raw)
        except ValueError as exc:
            raise ConfigError("TELEGRAM_CHAT_ID must be integer") from exc

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
        telegram_webhook_url = source.get("TELEGRAM_WEBHOOK_URL", "").strip()
        telegram_webhook_secret = source.get("TELEGRAM_WEBHOOK_SECRET", "").strip()

        if not bot_token:
            raise ConfigError("Missing TELEGRAM_BOT_TOKEN")
        if chat_id <= 0:
            raise ConfigError("TELEGRAM_CHAT_ID must be > 0")
        if not google_api_key:
            raise ConfigError("Missing GOOGLE_API_KEY")
        try:
            port = int(port_raw)
        except ValueError as exc:
            raise ConfigError("PORT must be integer") from exc
        if enable_pixel and not pixel_base_url:
            raise ConfigError("ENABLE_PIXEL=true requires PIXEL_BASE_URL")
        if enable_pixel and not pixel_webhook_secret:
            raise ConfigError("ENABLE_PIXEL=true requires PIXEL_WEBHOOK_SECRET")

        return cls(
            bot_token=bot_token,
            chat_id=chat_id,
            google_api_key=google_api_key,
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
        if not self.google_oauth_token_json or self.gmail_token_path.exists():
            return
        payload = json.loads(self.google_oauth_token_json)
        content = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        self.gmail_token_path.write_text(content)

    def validate_mode(self, mode: str) -> None:
        if mode == "webhook":
            if not self.telegram_webhook_secret:
                raise ConfigError("Webhook mode requires TELEGRAM_WEBHOOK_SECRET")
            if not self.telegram_webhook_url:
                raise ConfigError("Webhook mode requires TELEGRAM_WEBHOOK_URL")

    def resolved_telegram_webhook_url(self) -> str:
        if self.telegram_webhook_url.endswith("/telegram/webhook"):
            return self.telegram_webhook_url
        return self.telegram_webhook_url.rstrip("/") + "/telegram/webhook"


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

                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def purge_old_rows(self, days: int = STATE_RETENTION_DAYS) -> None:
        cutoff = (utcnow() - timedelta(days=days)).isoformat()
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM pending_actions WHERE created_at < ?", (cutoff,))
            self._conn.execute("DELETE FROM email_state WHERE updated_at < ?", (cutoff,))
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
        payload = self.call(
            lambda svc: svc.users()
            .messages()
            .list(userId="me", labelIds=["INBOX"], maxResults=1, includeSpamTrash=False)
            .execute()
        )
        messages = payload.get("messages") or []
        return messages[0]["id"] if messages else None

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

    def create_draft(self, raw: str, thread_id: str) -> None:
        message_body = {"raw": raw}
        if thread_id:
            message_body["threadId"] = thread_id
        self.call(
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
    config: Config
    store: StateStore
    gmail: GmailClient
    model: Any
    shutdown_event: asyncio.Event
    mode: str


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

    if plain:
        return plain.strip()
    if html:
        cleaned = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        return ihtml.unescape(cleaned).strip()
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


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


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


def build_tracking_markup(config: Config, state: EmailState) -> str:
    if not (config.enable_pixel and config.pixel_base_url):
        return ""
    base_url = config.pixel_base_url
    token = make_tracking_token(config, state.tg_message_id)
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


def kb_main(tg_message_id: int, starred: bool, attachments: List[dict]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📨 Invia", callback_data=f"send|{tg_message_id}"),
            InlineKeyboardButton("💾 Bozza", callback_data=f"draft|{tg_message_id}"),
        ],
        [
            InlineKeyboardButton("✏️ Riscrivi", callback_data=f"ask|{tg_message_id}"),
            InlineKeyboardButton("❌ Rifiuta", callback_data=f"reject|{tg_message_id}"),
        ],
        [InlineKeyboardButton("🗑️ Cestino", callback_data=f"trash|{tg_message_id}")],
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


def kb_fwd(tg_message_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(address, callback_data=f"fwdto|{tg_message_id}|{address}")]
        for address in PREDEF_FWD
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
    LOGGER.warning(
        "Rejected unauthorized Telegram user. user_id=%s",
        getattr(update.effective_user, "id", None),
    )
    await deny_unauthorized(update)
    return False


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
    progress = await application.bot.send_message(
        chat_id=runtime.config.chat_id,
        text="⌛ AI…",
        reply_to_message_id=state.tg_message_id,
    )
    accumulated = ""
    async for chunk in ai_stream(runtime.model, prompt or DEFAULT_PROMPT, state.body, state.lang):
        accumulated += chunk
        if len(accumulated) >= TELEGRAM_MAX:
            break
        await safe_edit(progress, text=format_email_text(state, body_override=accumulated))
        await asyncio.sleep(0.4)

    final_text = accumulated[:TELEGRAM_MAX]
    state.ai_body = final_text
    runtime.store.update_ai_body(state.tg_message_id, final_text)
    runtime.store.purge_old_rows()
    await safe_edit(progress, text=format_email_text(state, body_override=final_text))


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return
    await update.effective_message.reply_text("Bot Gmail-AI pronto.")


async def txt_followup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update, context):
        return

    message = update.effective_message
    if not message or not message.reply_to_message or not message.text:
        return

    runtime = get_runtime(context.application)
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
            text="✏️ Scrivi domanda AI (rispondi qui).",
            reply_to_message_id=message.message_id,
        )
        runtime.store.add_pending_action(prompt_message.message_id, tg_message_id, "ask")
        runtime.store.purge_old_rows()
        await query.answer()
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
        await safe_edit(message, markup=kb_fwd(tg_message_id))
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
        runtime.store.purge_old_rows()
        await query.answer()
        return

    tracking_markup = build_tracking_markup(runtime.config, state)
    body_to_send = state.ai_body or state.body

    try:
        if action == "send":
            raw = build_raw(state.sender, "Re: " + state.subject, body_to_send, tracking_markup)
            await asyncio.to_thread(runtime.gmail.send_raw_message, raw, state.gmail_thread_id)
        elif action == "draft":
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


async def process_new_email(application: Application, runtime: Runtime, gmail_message_id: str, lang: str) -> None:
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
            )
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
    runtime.store.purge_old_rows()
    await safe_edit(tg_message, markup=kb_main(state.tg_message_id, state.starred, attachments))
    await ai_reply_stream(application, runtime, state, DEFAULT_PROMPT)


async def watcher(runtime: Runtime, application: Application, interval: int, lang: str) -> None:
    last_seen = runtime.store.get_bot_state(LAST_SEEN_KEY)
    if not last_seen:
        try:
            last_seen = await asyncio.to_thread(runtime.gmail.latest_inbox_id)
            if last_seen:
                runtime.store.set_bot_state(LAST_SEEN_KEY, last_seen)
        except Exception:
            LOGGER.exception("Initial Gmail watcher bootstrap failed.")
            last_seen = None

    while not runtime.shutdown_event.is_set():
        try:
            newest = await asyncio.to_thread(runtime.gmail.latest_inbox_id)
            if newest and newest != last_seen:
                last_seen = newest
                runtime.store.set_bot_state(LAST_SEEN_KEY, newest)
                await process_new_email(application, runtime, newest, lang)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Watcher loop failed.")
        try:
            await asyncio.wait_for(runtime.shutdown_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue


async def on_err(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Telegram handler error.", exc_info=context.error)
    try:
        runtime = get_runtime(context.application)
    except Exception:
        return
    try:
        await context.bot.send_message(runtime.config.chat_id, f"⚠️ Errore: {context.error}")
    except Exception:
        LOGGER.exception("Failed to send Telegram error notification.")


def create_web_app(runtime: Runtime, application: Application) -> Quart:
    app = Quart(__name__)

    @app.get("/healthz")
    async def healthz():
        return jsonify({"status": "ok", "mode": runtime.mode})

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

        if not tg_message_id:
            return jsonify({"status": "error", "message": "tg_msg_id missing"}), 400

        original = runtime.store.get_email_state(int(tg_message_id))
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
    application.add_handler(CallbackQueryHandler(cb_btn))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, txt_followup))
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
    parser.add_argument("--interval", type=int, default=15)
    parser.add_argument("--lang", default="it")
    parser.add_argument("--mode", choices=["polling", "webhook"], default="polling")
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    config = Config.from_env()
    config.validate_mode(args.mode)
    config.ensure_storage()
    config.materialize_google_credentials()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = StateStore(config.state_db_path)
    store.purge_old_rows()

    genai.configure(api_key=config.google_api_key)
    runtime = Runtime(
        config=config,
        store=store,
        gmail=GmailClient(config),
        model=genai.GenerativeModel(AI_MODEL),
        shutdown_event=asyncio.Event(),
        mode=args.mode,
    )
    install_signal_handlers(runtime.shutdown_event)

    application = build_application(runtime)
    web_app = create_web_app(runtime, application)
    http_task: asyncio.Task[Any] | None = None
    watcher_task: asyncio.Task[Any] | None = None
    stop_task: asyncio.Task[Any] | None = None

    try:
        await asyncio.to_thread(runtime.gmail.refresh_labels, True)
    except Exception:
        LOGGER.exception("Initial Gmail label load failed.")

    try:
        await bootstrap_telegram(application, runtime, args.mode)
        http_task = asyncio.create_task(run_http_server(runtime, web_app))
        watcher_task = asyncio.create_task(watcher(runtime, application, args.interval, args.lang))
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
