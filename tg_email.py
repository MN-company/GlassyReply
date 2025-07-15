from __future__ import annotations

import os
import sys
import re
import json
import base64
import email
import argparse
import asyncio
import html as ihtml
import urllib.parse  
from io import BytesIO
from uuid import uuid4
from typing import Optional, Dict, List
from email.header import decode_header
from email.utils import parseaddr

# ───────────────  Google / Gmail  ────────────────
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ───────────────  Telegram  ──────────────────────
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest
from telegram.error import BadRequest
from telegram.constants import ParseMode

# ───────────────  Gemini  ────────────────────────
import google.generativeai as genai

# ───────────────  Quart (Webhook Server)  ───────────────
from quart import Quart, request, jsonify

# ───────────────  Env  ───────────────────────────
from dotenv import load_dotenv

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))
GEN_API = os.getenv("GOOGLE_API_KEY")
ENABLE_PIXEL = os.getenv("ENABLE_PIXEL", "false").lower() == "true"
PIXEL_BASE_URL = os.getenv("PIXEL_BASE_URL", "").rstrip("/")
PIXEL_WEBHOOK_SECRET = os.getenv("PIXEL_WEBHOOK_SECRET")  # Nuova variabile
PIXEL_WEBHOOK_URL = os.getenv("PIXEL_WEBHOOK_URL")  # Nuova variabile

if not (BOT_TOKEN and CHAT_ID and GEN_API):
    sys.exit("⚠️  Imposta TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_API_KEY")

if ENABLE_PIXEL and not (PIXEL_WEBHOOK_SECRET and PIXEL_WEBHOOK_URL):
    sys.exit("⚠️  Se ENABLE_PIXEL è true, imposta PIXEL_WEBHOOK_SECRET e PIXEL_WEBHOOK_URL")

# ───────────────  Costanti  ──────────────────────
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
LAST_FILE = "last_id.json"
PREDEF_FWD = ["redazione@example.com", "boss@example.com"]
DEFAULT_PROMPT = (
    "Sei un assistente professionale. Scrivi una risposta "
    "educata, chiara e concisa all'e‑mail seguente. Includi saluti "
    "e firma se opportuno."
)

# ───────────────  Gmail auth  ────────────────────

def gmail_service():
    creds: Optional[Credentials] = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


svc = gmail_service()

# Cache etichette
LABELS: Dict[str, str] = {}
try:
    LABELS = {
        l["id"]: l["name"]
        for l in svc.users().labels().list(userId="me").execute().get("labels", [])
    }
except Exception as e:
    print("⚠️  Etichette non caricate:", e, file=sys.stderr)

# ───────────────  MIME helpers  ───────────────────

def decode_hdr(s: str | None) -> str:
    if not s:
        return ""
    return "".join(
        (
            b.decode(enc or "utf‑8", "replace") if isinstance(b, bytes) else b
            for b, enc in decode_header(s)
        )
    )


def payload_text(payload) -> str:
    plain, html = None, None

    def walk(parts):
        nonlocal plain, html
        for p in parts:
            mime, body = p.get("mimeType", ""), p.get("body", {})
            data = body.get("data")
            if data:
                txt = base64.urlsafe_b64decode(data).decode("utf‑8", "replace")
                if mime.startswith("text/plain") and plain is None:
                    plain = txt
                elif mime.startswith("text/html") and html is None:
                    html = txt
            if "parts" in p:
                walk(p["parts"])

    walk(payload.get("parts", []))
    if plain:
        return plain.strip()
    if html:
        html = re.sub(r"<br\s*/>", "\n", html)
        html = re.sub(r"<[^>]+>", " ", html)
        return ihtml.unescape(html).strip()
    return "(corpo non disponibile)"


def list_attachments(payload) -> List[dict]:
    out: List[dict] = []

    def walk(parts):
        for p in parts:
            fn, body = p.get("filename"), p.get("body", {})
            if fn:
                out.append(
                    {
                        "id": body.get("attachmentId"),
                        "data": body.get("data"),
                        "filename": fn,
                        "size": body.get("size", 0),
                    }
                )
            if "parts" in p:
                walk(p["parts"])

    walk(payload.get("parts", []))
    return out


# ───────────────  Gemini streaming  ───────────────

genai.configure(api_key=GEN_API)
MODEL = genai.GenerativeModel(AI_MODEL)


async def ai_stream(prompt, context, lang):
    full = f"{prompt}\n\nRispondi in {lang}.\n\n{context[:MAX_CHARS]}"
    for ck in MODEL.generate_content(full, stream=True):
        txt = getattr(ck, "text", "") or "".join(p.text for p in getattr(ck, "parts", []))
        if txt.strip():
            yield txt


# ───────────────  Build email  ─────────────────────

def build_raw(to_addr, subj, plain, pixel_url=None):
    em = email.message.EmailMessage()
    em["To"], em["Subject"] = to_addr, subj
    em.set_content(plain)
    if pixel_url:
        # Properly escape plain text for HTML, then add pixel
        html = (
            ihtml.escape(plain).replace("\n", "<br>")
            + f'<img src="{pixel_url}" width="1" height="1" style="display:none">'
        )
        em.add_alternative(html, subtype="html")
    return base64.urlsafe_b64encode(em.as_bytes()).decode()


def gmail_send(to, subj, body, thread, pixel):
    raw = build_raw(to, "Re: " + subj, body, pixel)
    svc.users().messages().send(userId="me", body={"raw": raw, "threadId": thread}).execute()


def gmail_draft(to, subj, body, thread, pixel):
    raw = build_raw(to, "Re: " + subj, body, pixel)
    svc.users().drafts().create(
        userId="me", body={"message": {"raw": raw, "threadId": thread}}
    ).execute()


def _mod(mid, add=None, rem=None):
    svc.users().messages().modify(
        userId="me", id=mid, body={"addLabelIds": add or [], "removeLabelIds": rem or []}
    ).execute()


# ───────────────  Keyboards  ───────────────────────

def kb_main(mid, starred, atts):
    rows = [
        [
            InlineKeyboardButton("📨 Invia", callback_data=f"send|{mid}"),
            InlineKeyboardButton("💾 Bozza", callback_data=f"draft|{mid}"),
        ],
        [
            InlineKeyboardButton("✏️ Riscrivi", callback_data=f"ask|{mid}"),
            InlineKeyboardButton("❌ Rifiuta", callback_data=f"reject|{mid}"),
        ],
        [InlineKeyboardButton("🗑️ Cestino", callback_data=f"trash|{mid}")],
        [
            InlineKeyboardButton(
                "⭐ Unstar" if starred else "⭐ Star", callback_data=f"starT|{mid}"
            )
        ],
        [
            InlineKeyboardButton("🔁 Inoltra", callback_data=f"fwd|{mid}"),
            InlineKeyboardButton("🏷️ Tag ➜", callback_data=f"tag|{mid}|0"),
        ],
    ]
    if atts:
        rows.append(
            [InlineKeyboardButton("📎 Allegati ➜", callback_data=f"attmenu|{mid}")]
        )
    return InlineKeyboardMarkup(rows)


def kb_tag(mid, page):
    valid = [
        (i, n)
        for i, n in LABELS.items()
        if i not in {"INBOX", "SENT", "TRASH", "SPAM", "DRAFT"} and not i.startswith("CATEGORY_")
    ]
    start = page * PAGE_SIZE
    chunk = valid[start : start + PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(f"🏷️ {n[:20]}", callback_data=f"tagset|{mid}|{i}")]
        for i, n in chunk
    ]
    nav = []
    if page:
        nav.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"tag|{mid}|{page - 1}")
        )
    if start + PAGE_SIZE < len(valid):
        nav.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"tag|{mid}|{page + 1}")
        )
    nav.append(InlineKeyboardButton("⬅️ Back", callback_data=f"back|{mid}"))
    rows.append(nav)
    return InlineKeyboardMarkup(rows)


def kb_att(mid, atts):
    rows = [
        [
            InlineKeyboardButton(
                f"⬇️ {a['filename'][:25]}", callback_data=f"att|{mid}|{i}"
            )
        ]
        for i, a in enumerate(atts)
    ]
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"back|{mid}")])
    return InlineKeyboardMarkup(rows)


def kb_fwd(mid):
    rows = [
        [InlineKeyboardButton(addr, callback_data=f"fwdto|{mid}|{addr}")]
        for addr in PREDEF_FWD
    ]
    rows.append([InlineKeyboardButton("✉️ Altro…", callback_data=f"fwdother|{mid}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"back|{mid}")])
    return InlineKeyboardMarkup(rows)


# ───────────────  safe_edit()  ─────────────────────

async def safe_edit(msg, *, text=None, markup=None):
    try:
        if text == msg.text:
            text = None
        if markup == msg.reply_markup:
            markup = None
        if text is None and markup is None:
            return
        if text:
            await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        else:
            await msg.edit_reply_markup(reply_markup=markup)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


# ───────────────  State  ───────────────────────────
thread_map: Dict[str, int] = {}
state: Dict[int, dict] = {}

# ───────────────  Telegram handlers  ───────────────


async def cmd_start(u: Update, c):
    await u.message.reply_text("Bot Gmail‑AI pronto 🤖")


async def ai_reply_stream(ctx, info, prompt):
    hdr = info["header"]
    msg = await ctx.bot.send_message(CHAT_ID, "⌛ AI…", reply_to_message_id=info["tg_id"])
    acc = ""
    async for ch in ai_stream(prompt or DEFAULT_PROMPT, info["body"], info["lang"]):
        acc += ch
        if len(acc) >= TELEGRAM_MAX:
            break
        await safe_edit(msg, text=f"{hdr}\n\n-----\n{acc[:TELEGRAM_MAX]}")
        await asyncio.sleep(0.4)
    final = acc[:TELEGRAM_MAX]
    info["ai_body"] = final  # ← salva per l’invio
    await safe_edit(msg, text=f"{hdr}\n\n-----\n{final}")


async def txt_followup(u: Update, c):
    ref = u.message.reply_to_message
    if not ref or ref.message_id not in state:
        return
    inf = state[ref.message_id]
    if inf.pop("await_q", False):
        await ai_reply_stream(c, inf, u.message.text.strip())
    elif inf.pop("await_fwd", False):
        try:
            await asyncio.to_thread(
                lambda: gmail_forward(inf["mid"], u.message.text.strip())
            )
            await c.bot.send_message(
                CHAT_ID,
                f"Inoltrata a {u.message.text.strip()}",
                reply_to_message_id=inf["tg_id"],
            )
        except Exception as e:
            await c.bot.send_message(
                CHAT_ID, f"⚠️ Forward err: {e}", reply_to_message_id=inf["tg_id"]
            )


async def cb_btn(u: Update, c):
    q = u.callback_query
    parts = q.data.split("|")
    act = parts[0]
    mid = int(parts[1])
    inf = state.get(mid)
    msg = q.message
    if not inf:
        return await q.answer("Not found", show_alert=True)

    # Tag menu
    if act == "tag":
        page = int(parts[2])
        await safe_edit(msg, markup=kb_tag(mid, page))
        return await q.answer()
    if act == "tagset":
        lid = parts[2]
        await asyncio.to_thread(lambda: _mod(inf["mid"], add=[lid]))
        await q.answer(f"🏷️ {LABELS.get(lid, lid)}")
        await safe_edit(msg, markup=kb_main(mid, inf["star"], inf["atts"]))
        return
    if act == "back":
        await safe_edit(msg, markup=kb_main(mid, inf["star"], inf["atts"]))
        return await q.answer()

    # AI prompt
    if act == "ask":
        prm = await c.bot.send_message(
            CHAT_ID,
            "✏️ Scrivi la domanda AI (rispondi qui).",
            reply_to_message_id=msg.message_id,
        )
        inf["await_q"] = True
        state[prm.message_id] = inf
        return await q.answer()

    # Star toggle
    if act == "starT":
        if inf["star"]:
            await asyncio.to_thread(lambda: _mod(inf["mid"], rem=["STARRED"]))
            inf["star"] = False
            await q.answer("⭐ off")
        else:
            await asyncio.to_thread(lambda: _mod(inf["mid"], add=["STARRED"]))
            inf["star"] = True
            await q.answer("⭐ on")
        await safe_edit(msg, markup=kb_main(mid, inf["star"], inf["atts"]))
        return

    # Allegati
    if act == "attmenu":
        await safe_edit(msg, markup=kb_att(mid, inf["atts"]))
        return await q.answer()
    if act == "att":
        idx = int(parts[2])
        at = inf["atts"][idx]
        try:
            if at["data"]:
                data64 = at["data"]
            else:
                resp = await asyncio.to_thread(
                    lambda: svc.users()
                    .messages()
                    .attachments()
                    .get(userId="me", messageId=inf["mid"], id=at["id"])
                    .execute()
                )
                data64 = resp["data"]
            decoded = base64.urlsafe_b64decode(data64)
            await c.bot.send_document(
                chat_id=CHAT_ID,
                document=BytesIO(decoded),
                filename=at["filename"],
            )
            return await q.answer()
        except Exception as e:
            return await q.answer(f"Err: {e}", show_alert=True)

    # Forward
    if act == "fwd":
        await safe_edit(msg, markup=kb_fwd(mid))
        return await q.answer()
    if act == "fwdto":
        await asyncio.to_thread(lambda: gmail_forward(inf["mid"], parts[2]))
        await safe_edit(msg, markup=kb_main(mid, inf["star"], inf["atts"]))
        return await q.answer("Inoltrata")
    if act == "fwdother":
        prm = await c.bot.send_message(
            CHAT_ID,
            "✉️ Rispondi con l'indirizzo.",
            reply_to_message_id=msg.message_id,
        )
        inf["await_fwd"] = True
        state[prm.message_id] = inf
        return await q.answer()

    # Azioni base
    pixel = None
    if ENABLE_PIXEL and PIXEL_BASE_URL:
        tg_msg_id = inf["tg_id"]
        email_subj = inf["subj"]
        email_body_excerpt = inf["body"][:100]
        encoded_subj = urllib.parse.quote_plus(email_subj)
        encoded_body_excerpt = urllib.parse.quote_plus(email_body_excerpt)
        pixel = (
            f"{PIXEL_BASE_URL}?id={uuid4().hex}&tg_msg_id={tg_msg_id}&subj={encoded_subj}&body_ex={encoded_body_excerpt}"
        )
    body_to_send = inf.get("ai_body", inf["body"])

    try:
        if act == "send":
            await asyncio.to_thread(
                lambda: gmail_send(
                    inf["from"], inf["subj"], body_to_send, inf["thread"], pixel
                )
            )
        elif act == "draft":
            await asyncio.to_thread(
                lambda: gmail_draft(
                    inf["from"], inf["subj"], body_to_send, inf["thread"], pixel
                )
            )
        elif act == "trash":
            await asyncio.to_thread(lambda: _mod(inf["mid"], rem=["INBOX"], add=["TRASH"]))
        await q.answer({"send": "📨", "draft": "💾", "trash": "🗑️", "reject": "❌"}[act] + " ok")
    except HttpError as e:
        await q.answer(f"Err: {e}", show_alert=True)
    await safe_edit(msg, markup=None)


# ───────────────  Gmail forward helper  ────────────

def gmail_forward(mid, to):
    meta = svc.users().messages().get(userId="me", id=mid, format="raw").execute()
    orig = email.message_from_bytes(base64.urlsafe_b64decode(meta["raw"].encode()))
    body = "Inoltro automatico.\n\n--- Messaggio originale ---\n" + payload_text(meta["payload"])
    raw = build_raw(to, "Fwd: " + orig.get("Subject", ""), body, None)
    svc.users().messages().send(userId="me", body={"raw": raw}).execute()


# ───────────────  Watcher  ─────────────────────────

async def latest_id():
    lst = (
        svc.users()
        .messages()
        .list(userId="me", labelIds=["INBOX"], maxResults=1, includeSpamTrash=False)
        .execute()
    )
    msgs = lst.get("messages")
    return msgs[0]["id"] if msgs else None


async def watcher(intv, lang, app):
    last = None
    if os.path.exists(LAST_FILE):
        try:
            last = json.load(open(LAST_FILE))["last"]
        except Exception:
            pass
    if not last:
        last = await latest_id()
    while True:
        try:
            new = await latest_id()
            if new and new != last:
                last = new
                json.dump({"last": last}, open(LAST_FILE, "w"))
                full = await asyncio.to_thread(
                    lambda: svc.users().messages().get(userId="me", id=new, format="full").execute()
                )
                pay = full["payload"]
                subj = next(
                    (
                        decode_hdr(h["value"]) for h in pay["headers"] if h["name"].lower() == "subject"
                    ),
                    "(senza oggetto)",
                )
                body = payload_text(pay)
                atts = list_attachments(pay)
                hdr = f"📧 *{subj}*"
                first = await app.bot.send_message(
                    CHAT_ID, f"{hdr}\n\n{body[:TELEGRAM_MAX]}", parse_mode="Markdown"
                )
                tid = first.message_id
                state[tid] = {
                    "tg_id": tid,
                    "mid": new,
                    "thread": full.get("threadId"),
                    "from": parseaddr(
                        next(
                            h["value"] for h in pay["headers"] if h["name"].lower() == "from"
                        )
                    )[1],
                    "subj": subj,
                    "body": body,
                    "header": hdr,
                    "atts": atts,
                    "star": "STARRED" in full.get("labelIds", []),
                    "lang": lang,
                }
                await safe_edit(first, markup=kb_main(tid, state[tid]["star"], atts))
                # risposta AI automatica
                await ai_reply_stream(app, state[tid], DEFAULT_PROMPT)
        except Exception as e:
            print("Watcher:", e)
        await asyncio.sleep(intv)


# ───────────────  Error handler  ───────────────────

async def on_err(update, ctx):
    try:
        await ctx.bot.send_message(CHAT_ID, f"⚠️ Errore: {ctx.error}")
    except Exception:
        print("Err-handler:", ctx.error, file=sys.stderr)


# ───────────────  Webhook Server (Quart)  ───────────────

webhook_app = Quart(__name__)


@webhook_app.route("/pixel_status", methods=["POST"])
async def pixel_status():
    if request.headers.get("X-Pixel-Secret") != PIXEL_WEBHOOK_SECRET:
        return jsonify({"status": "unauthorized"}), 401

    data = await request.get_json()
    tg_msg_id = data.get("tg_msg_id")
    is_user_open = data.get("is_user_open")
    email_subject = data.get("email_subject")

    if not tg_msg_id:
        return jsonify({"status": "error", "message": "tg_msg_id missing"}), 400

    try:
        # Recupera lo stato originale (se disponibile) e aggiunge la riga pixel;
        # Se non disponibile, mostra solo lo stato pixel.
        original_state = state.get(int(tg_msg_id))
        if original_state:
            base_text = original_state["header"] + "\n\n" + original_state["body"][:TELEGRAM_MAX]
        else:
            base_text = "Messaggio (testo originale non disponibile)"
        icon = "✅" if is_user_open else "❌"
        status_text = "aperta da utente" if is_user_open else "aperta da proxy"
        new_text = f"{base_text}\n\n---\n{icon} Email {status_text} ({email_subject})"
        await webhook_app.bot.edit_message_text(
            chat_id=CHAT_ID,
            message_id=int(tg_msg_id),
            text=new_text,
            parse_mode=ParseMode.MARKDOWN,
        )
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"⚠️ Errore nel webhook pixel_status: {e}", file=sys.stderr)
        return jsonify({"status": "error", "message": str(e)}), 500


async def run_webhook_app():
    await webhook_app.run(host="0.0.0.0", port=5000, debug=False)


# ───────────────  Main  ────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=15)
    ap.add_argument("--lang", default="it")
    A = ap.parse_args()

    async def post(app):
        asyncio.create_task(watcher(A.interval, A.lang, app))
        if ENABLE_PIXEL:
            # Pass the bot instance to the webhook app
            webhook_app.bot = app.bot
            asyncio.create_task(run_webhook_app())

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(HTTPXRequest(read_timeout=60, connect_timeout=20))
        .post_init(post)
        .build()
    )
    
    # Add handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(cb_btn))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, txt_followup))
    app.add_error_handler(on_err)

    app.run_polling()


if __name__ == "__main__":
    main()
