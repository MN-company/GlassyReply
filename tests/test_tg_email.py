from __future__ import annotations

import asyncio
import base64
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from googleapiclient.errors import HttpError

from tg_email import (
    GMAIL_INITIAL_SYNC_KEY,
    GOOGLE_OAUTH_STATE_KEY,
    Runtime,
    Config,
    ConfigError,
    DEFAULT_PROMPT,
    EmailState,
    GmailClient,
    StateStore,
    TrackedEmail,
    build_candidate_config,
    build_application,
    claim_owner,
    create_web_app,
    gmail_initial_sync_pending,
    help_message_text,
    google_oauth_state_payload,
    google_web_client_config,
    localized_manual_reply_placeholder,
    mark_gmail_initial_sync_pending,
    main_menu_rows,
    make_tracking_token,
    make_dashboard_token,
    owner_configured,
    is_authorized_update,
    google_oauth_authorization_response,
    parse_google_oauth_state_payload,
    payload_text,
    pixel_asset_response,
    save_runtime_settings,
    setup_keyboard,
    setup_message_text,
    split_unseen_inbox_ids,
    start_google_oauth,
    startup_notice_text,
    reply_body_for_action,
    format_tracked_email_text,
    tracked_email_status_summary,
    tracked_stats_text,
    verify_dashboard_token,
)


class ConfigTests(unittest.TestCase):
    def test_from_env_builds_default_storage_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "TELEGRAM_CHAT_ID": "123",
                    "GOOGLE_API_KEY": "gemini",
                    "DATA_DIR": tmpdir,
                }
            )
            self.assertEqual(cfg.chat_id, 123)
            self.assertEqual(cfg.data_dir, Path(tmpdir))
            self.assertEqual(cfg.state_db_path, Path(tmpdir) / "state.db")
            self.assertEqual(cfg.gmail_token_path, Path(tmpdir) / "token.json")
            self.assertEqual(cfg.gmail_credentials_path, Path(tmpdir) / "credentials.json")

    def test_invalid_chat_id_raises(self) -> None:
        with self.assertRaises(ConfigError):
            Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "TELEGRAM_CHAT_ID": "abc",
                    "GOOGLE_API_KEY": "gemini",
                }
            )

    def test_missing_chat_id_defaults_to_zero(self) -> None:
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "GOOGLE_API_KEY": "gemini",
            }
        )
        self.assertEqual(cfg.chat_id, 0)

    def test_materialize_gmail_token_overwrites_stale_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "DATA_DIR": tmpdir,
                    "GOOGLE_OAUTH_TOKEN_JSON": json.dumps(
                        {
                            "refresh_token": "fresh-refresh-token",
                            "client_id": "client-id",
                            "client_secret": "client-secret",
                            "token_uri": "https://oauth2.googleapis.com/token",
                        }
                    ),
                }
            )
            cfg.gmail_token_path.write_text('{"refresh_token":"stale-refresh-token"}\n')

            cfg.materialize_gmail_token()

            stored = json.loads(cfg.gmail_token_path.read_text())
            self.assertEqual(stored["refresh_token"], "fresh-refresh-token")

    def test_system_prompt_defaults_and_override(self) -> None:
        cfg = Config.from_env({"TELEGRAM_BOT_TOKEN": "token"})
        self.assertEqual(cfg.system_prompt, DEFAULT_PROMPT)

        overridden = cfg.with_overrides({"SYSTEM_PROMPT": "Prompt custom"})
        self.assertEqual(overridden.system_prompt, "Prompt custom")

    def test_gmail_monitor_labels_override(self) -> None:
        cfg = Config.from_env({"TELEGRAM_BOT_TOKEN": "token"})
        overridden = cfg.with_overrides(
            {"GMAIL_MONITOR_LABELS": json.dumps(["INBOX", "CATEGORY_PROMOTIONS"])}
        )
        self.assertEqual(overridden.gmail_monitor_labels, ["INBOX", "CATEGORY_PROMOTIONS"])

    def test_invalid_timezone_raises(self) -> None:
        with self.assertRaises(ConfigError):
            Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "TIMEZONE": "Mars/Olympus",
                }
            )


class AssistantUxTests(unittest.TestCase):
    def make_runtime(self, **env_overrides) -> Runtime:
        tmpdir = tempfile.mkdtemp()
        base_env = {
            "TELEGRAM_BOT_TOKEN": "token",
            "DATA_DIR": tmpdir,
        }
        base_env.update(env_overrides)
        cfg = Config.from_env(base_env)
        store = StateStore(Path(tmpdir) / "state.db")
        runtime = Runtime(
            base_config=cfg,
            config=cfg,
            startup_overrides={},
            store=store,
            gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
            model=None,
            shutdown_event=SimpleNamespace(),
            mode="polling",
        )
        self.addCleanup(store.close)
        self.addCleanup(lambda: __import__("shutil").rmtree(tmpdir, ignore_errors=True))
        return runtime

    def test_main_menu_hides_tracked_email_until_pixel_is_enabled(self) -> None:
        runtime = self.make_runtime()
        self.assertEqual(main_menu_rows(runtime), [["Stats", "Impostazioni"]])

        pixel_runtime = self.make_runtime(ENABLE_PIXEL="1", PIXEL_WEBHOOK_SECRET="secret")
        self.assertEqual(
            main_menu_rows(pixel_runtime),
            [["Email Tracciata", "Stats"], ["Impostazioni"]],
        )

    def test_setup_message_text_is_progressive(self) -> None:
        runtime = self.make_runtime()
        first_step = setup_message_text(runtime)
        self.assertIn("Gemini key", first_step)
        self.assertNotIn("OAuth Google", first_step)

        oauth_runtime = self.make_runtime(GOOGLE_API_KEY="gemini")
        second_step = setup_message_text(oauth_runtime)
        self.assertIn("OAuth Google", second_step)
        self.assertNotIn("collega Gmail", second_step)

        gmail_runtime = self.make_runtime(
            GOOGLE_API_KEY="gemini",
            GOOGLE_OAUTH_CREDENTIALS_JSON='{"web":{"client_id":"x","project_id":"p","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","client_secret":"secret"}}',
            PUBLIC_BASE_URL="https://glassyreply-bot.fly.dev",
        )
        third_step = setup_message_text(gmail_runtime)
        self.assertIn("collega Gmail", third_step)
        self.assertIn("https://glassyreply-bot.fly.dev/oauth/google/callback", third_step)

    def test_setup_keyboard_shows_only_next_action_plus_status_and_dashboard(self) -> None:
        runtime = self.make_runtime()
        markup = setup_keyboard(runtime)
        callback_data = [button.callback_data for row in markup.inline_keyboard for button in row]
        self.assertIn("setup|google_api_key", callback_data)
        self.assertIn("setup|status", callback_data)
        self.assertIn("setup|dashboard", callback_data)
        self.assertNotIn("setup|oauth_json", callback_data)
        self.assertNotIn("setup|gmail_login", callback_data)

    def test_help_message_lists_short_commands(self) -> None:
        runtime = self.make_runtime()
        help_text = help_message_text(runtime)
        self.assertIn("/setup - mostra solo il prossimo step", help_text)
        self.assertIn("/help - mostra questo elenco", help_text)
        self.assertNotIn("/tracked -", help_text)

        pixel_runtime = self.make_runtime(ENABLE_PIXEL="1", PIXEL_WEBHOOK_SECRET="secret")
        self.assertIn("/tracked - crea una bozza email tracciata", help_message_text(pixel_runtime))

    def test_reply_body_for_action_uses_ai_then_original_then_editable_placeholder(self) -> None:
        state = EmailState(
            tg_message_id=1,
            gmail_message_id="gmail-1",
            gmail_thread_id="thread-1",
            sender="sender@example.com",
            subject="Subject",
            body="Original body",
            header="Header",
            attachments=[],
            starred=False,
            lang="it",
            ai_body="Reply pronta",
        )
        self.assertEqual(reply_body_for_action(state, "send"), "Reply pronta")
        self.assertEqual(reply_body_for_action(state, "draft"), "Reply pronta")

        state.ai_body = ""
        self.assertEqual(reply_body_for_action(state, "send"), "Original body")
        self.assertEqual(
            reply_body_for_action(state, "draft"),
            localized_manual_reply_placeholder("it"),
        )


class StateStoreTests(unittest.TestCase):
    def test_email_state_pending_actions_and_purge(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "state.db")
            state = EmailState(
                tg_message_id=101,
                gmail_message_id="gmail-1",
                gmail_thread_id="thread-1",
                sender="sender@example.com",
                subject="Subject",
                body="Body",
                header="Header",
                attachments=[{"filename": "test.txt", "id": "att-1"}],
                starred=False,
                lang="it",
            )
            store.upsert_email_state(state)
            store.update_ai_body(101, "AI reply")
            store.update_starred(101, True)
            store.add_pending_action(202, 101, "ask")
            store.set_bot_state("last_seen_gmail_message_id", "gmail-1")

            loaded = store.get_email_state(101)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.ai_body, "AI reply")
            self.assertTrue(loaded.starred)
            self.assertEqual(store.get_pending_action(202).action_kind, "ask")
            self.assertEqual(store.get_bot_state("last_seen_gmail_message_id"), "gmail-1")

            with store._conn:  # noqa: SLF001 - test only
                store._conn.execute(
                    "UPDATE email_state SET updated_at = '2000-01-01T00:00:00+00:00' WHERE tg_message_id = 101"
                )
                store._conn.execute(
                    "UPDATE pending_actions SET created_at = '2000-01-01T00:00:00+00:00' WHERE prompt_message_id = 202"
                )

            store.purge_old_rows(days=30)
            self.assertIsNone(store.get_email_state(101))
            self.assertIsNone(store.get_pending_action(202))
            store.close()

    def test_app_settings_crud(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "state.db")
            store.set_app_setting("LANG", "en")
            store.set_app_setting("WATCH_INTERVAL", "42")
            self.assertEqual(store.get_app_settings()["LANG"], "en")
            self.assertEqual(store.get_app_settings()["WATCH_INTERVAL"], "42")
            store.delete_app_setting("LANG")
            self.assertNotIn("LANG", store.get_app_settings())
            store.close()

    def test_tracked_email_pixel_event_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "state.db")
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "LANG": "it",
                    "TIMEZONE": "Europe/Rome",
                    "DATA_DIR": tmpdir,
                }
            )
            tracked = TrackedEmail(
                tg_message_id=301,
                draft_id="draft-1",
                recipient="lead@example.com",
                subject="Hello",
                open_count=0,
                first_opened_at="",
                last_opened_at="",
                last_classification="",
                last_layer="",
                last_dimensions="",
                last_confidence=None,
            )
            store.upsert_tracked_email(tracked)
            updated = store.record_pixel_event(
                tg_message_id=301,
                classification="human_browser",
                layer="font",
                dimensions="2x1",
                confidence=0.91,
                is_user_open=True,
                email_subject="Hello",
            )

            self.assertIsNotNone(updated)
            assert updated is not None
            self.assertEqual(updated.open_count, 1)
            self.assertEqual(updated.proxy_count, 0)
            self.assertEqual(updated.last_layer, "font")
            self.assertAlmostEqual(updated.last_confidence or 0, 0.91, places=2)
            self.assertIn("apertura utente probabile 1 volta", tracked_email_status_summary(updated))
            self.assertIn("confidenza alta", tracked_email_status_summary(updated))
            self.assertIn("Hello", tracked_stats_text([updated], cfg))
            with patch("tg_email.utcnow", return_value=datetime(2026, 4, 15, 16, 0, tzinfo=timezone.utc)):
                rendered = format_tracked_email_text(updated, cfg)
            self.assertIn("Ultima apertura utente:", rendered)
            self.assertIn("15 apr 2026", rendered)
            store.close()

    def test_proxy_fetch_does_not_count_as_user_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "state.db")
            tracked = TrackedEmail(
                tg_message_id=302,
                draft_id="draft-2",
                recipient="lead@example.com",
                subject="Proxy only",
                open_count=0,
                first_opened_at="",
                last_opened_at="",
                last_classification="",
                last_layer="",
                last_dimensions="",
                last_confidence=None,
            )
            store.upsert_tracked_email(tracked)
            updated = store.record_pixel_event(
                tg_message_id=302,
                classification="gmail_proxy",
                layer="img",
                dimensions="2x1",
                confidence=0.98,
                is_user_open=False,
                email_subject="Proxy only",
            )

            self.assertIsNotNone(updated)
            assert updated is not None
            self.assertEqual(updated.open_count, 0)
            self.assertEqual(updated.proxy_count, 1)
            self.assertEqual(updated.last_opened_at, "")
            self.assertIn("nessuna apertura utente confermata", tracked_email_status_summary(updated))
            self.assertIn("fetch proxy 1 volta", tracked_email_status_summary(updated))
            store.close()

    def test_multiple_pixel_events_same_session_are_deduped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = StateStore(Path(tmpdir) / "state.db")
            tracked = TrackedEmail(
                tg_message_id=303,
                draft_id="draft-3",
                recipient="lead@example.com",
                subject="Deduped",
                open_count=0,
                first_opened_at="",
                last_opened_at="",
                last_classification="",
                last_layer="",
                last_dimensions="",
                last_confidence=None,
            )
            store.upsert_tracked_email(tracked)
            with patch(
                "tg_email.utcnow_iso",
                side_effect=[
                    "2026-04-15T15:00:00+00:00",
                    "2026-04-15T15:00:10+00:00",
                    "2026-04-15T15:02:05+00:00",
                ],
            ):
                store.record_pixel_event(
                    tg_message_id=303,
                    classification="human_browser",
                    layer="img",
                    dimensions="2x1",
                    confidence=0.82,
                    is_user_open=True,
                    email_subject="Deduped",
                )
                store.record_pixel_event(
                    tg_message_id=303,
                    classification="font_loader",
                    layer="font",
                    dimensions="font",
                    confidence=0.76,
                    is_user_open=True,
                    email_subject="Deduped",
                )
                updated = store.record_pixel_event(
                    tg_message_id=303,
                    classification="human_browser",
                    layer="bg",
                    dimensions="2x1",
                    confidence=0.82,
                    is_user_open=True,
                    email_subject="Deduped",
                )

            self.assertIsNotNone(updated)
            assert updated is not None
            self.assertEqual(updated.open_count, 2)
            self.assertIn("riapertura probabile 1 volta", tracked_email_status_summary(updated))
            store.close()


class SelfHostedSetupTests(unittest.TestCase):
    def test_claim_owner_persists_in_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "GOOGLE_API_KEY": "",
                    "DATA_DIR": tmpdir,
                }
            )
            store = StateStore(Path(tmpdir) / "state.db")
            runtime = Runtime(
                base_config=cfg,
                config=cfg,
                startup_overrides={},
                store=store,
                gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                model=None,
                shutdown_event=SimpleNamespace(),
                mode="polling",
            )
            claim_owner(runtime, 555)
            self.assertTrue(owner_configured(runtime.config))
            self.assertEqual(runtime.config.chat_id, 555)
            self.assertEqual(store.get_app_settings()["TELEGRAM_CHAT_ID"], "555")
            store.close()

    def test_google_web_client_config_requires_web_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "DATA_DIR": tmpdir,
                }
            )
            runtime = Runtime(
                base_config=cfg,
                config=cfg,
                startup_overrides={},
                store=StateStore(Path(tmpdir) / "state.db"),
                gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                model=None,
                shutdown_event=SimpleNamespace(),
                mode="polling",
            )
            try:
                with self.assertRaises(ConfigError):
                    save_runtime_settings(
                        runtime,
                        {
                            "GOOGLE_OAUTH_CREDENTIALS_JSON": '{"installed":{"client_id":"x","project_id":"p"}}'
                        },
                    )
                save_runtime_settings(
                    runtime,
                    {
                        "GOOGLE_OAUTH_CREDENTIALS_JSON": '{"web":{"client_id":"x","project_id":"p","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","client_secret":"secret"}}'
                    },
                )
                loaded = google_web_client_config(runtime.config)
                self.assertIn("web", loaded)
            finally:
                runtime.store.close()

    def test_google_web_client_config_does_not_require_redirect_uri_in_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "PUBLIC_BASE_URL": "https://example.com",
                    "DATA_DIR": tmpdir,
                }
            )
            store = StateStore(Path(tmpdir) / "state.db")
            runtime = Runtime(
                base_config=cfg,
                config=cfg,
                startup_overrides={},
                store=store,
                gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                model=None,
                shutdown_event=SimpleNamespace(),
                mode="polling",
            )
            try:
                save_runtime_settings(
                    runtime,
                    {
                        "GOOGLE_OAUTH_CREDENTIALS_JSON": '{"web":{"client_id":"x","project_id":"p","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","client_secret":"secret"}}'
                    },
                )
                loaded = google_web_client_config(runtime.config)
                self.assertEqual(loaded["web"]["client_id"], "x")
            finally:
                runtime.store.close()

    def test_google_oauth_authorization_response_uses_public_base_url(self) -> None:
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "PUBLIC_BASE_URL": "https://glassyreply-bot.fly.dev",
                "DATA_DIR": tempfile.mkdtemp(),
            }
        )
        try:
            response_url = google_oauth_authorization_response(cfg, "code=abc&state=xyz")
            self.assertTrue(response_url.startswith("https://glassyreply-bot.fly.dev/"))
            self.assertIn("code=abc", response_url)
            self.assertIn("state=xyz", response_url)
        finally:
            import shutil

            shutil.rmtree(cfg.data_dir, ignore_errors=True)

    def test_start_google_oauth_persists_code_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "PUBLIC_BASE_URL": "https://glassyreply-bot.fly.dev",
                    "GOOGLE_OAUTH_CREDENTIALS_JSON": '{"web":{"client_id":"x","project_id":"p","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","client_secret":"secret"}}',
                    "DATA_DIR": tmpdir,
                }
            )
            store = StateStore(Path(tmpdir) / "state.db")
            runtime = Runtime(
                base_config=cfg,
                config=cfg,
                startup_overrides={},
                store=store,
                gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                model=None,
                shutdown_event=SimpleNamespace(),
                mode="polling",
            )

            class FakeFlow:
                code_verifier = "pkce-verifier"

                def authorization_url(self, **kwargs):
                    return "https://accounts.google.com/auth?state=oauth-state", "oauth-state"

            try:
                with patch("tg_email.Flow.from_client_config", return_value=FakeFlow()) as mocked:
                    auth_url = start_google_oauth(runtime)
                self.assertIn("oauth-state", auth_url)
                state_payload = parse_google_oauth_state_payload(
                    store.get_bot_state(GOOGLE_OAUTH_STATE_KEY)
                )
                self.assertEqual(state_payload["state"], "oauth-state")
                self.assertEqual(state_payload["code_verifier"], "pkce-verifier")
                self.assertIn("redirect_uri", mocked.call_args.kwargs)
            finally:
                store.close()

    def test_mark_gmail_initial_sync_pending_sets_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "DATA_DIR": tmpdir,
                }
            )
            store = StateStore(Path(tmpdir) / "state.db")
            runtime = Runtime(
                base_config=cfg,
                config=cfg,
                startup_overrides={},
                store=store,
                gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                model=None,
                shutdown_event=SimpleNamespace(),
                mode="polling",
            )
            try:
                runtime.store.set_bot_state("last_seen_gmail_message_id", "old-id")
                mark_gmail_initial_sync_pending(runtime)
                self.assertTrue(gmail_initial_sync_pending(runtime))
                self.assertEqual(runtime.store.get_bot_state(GMAIL_INITIAL_SYNC_KEY), "1")
                self.assertEqual(runtime.store.get_bot_state("last_seen_gmail_message_id"), "")
            finally:
                store.close()

    def test_startup_notice_text_reports_missing_setup(self) -> None:
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "123",
                "DATA_DIR": tempfile.mkdtemp(),
            }
        )
        store = StateStore(Path(cfg.data_dir) / "state.db")
        runtime = Runtime(
            base_config=cfg,
            config=cfg,
            startup_overrides={},
            store=store,
            gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
            model=None,
            shutdown_event=SimpleNamespace(),
            mode="polling",
        )
        try:
            notice = startup_notice_text(runtime, gmail_error="Gmail auth failed at startup")
            self.assertIsNotNone(notice)
            self.assertIn("setup is still incomplete", notice)
            self.assertIn("Gmail auth failed at startup", notice)
        finally:
            store.close()
            import shutil

            shutil.rmtree(cfg.data_dir, ignore_errors=True)


class AuthGuardTests(unittest.TestCase):
    def test_authorized_user_matches_chat_id(self) -> None:
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "123",
                "GOOGLE_API_KEY": "gemini",
            }
        )
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123))
        self.assertTrue(is_authorized_update(update, cfg))

    def test_unauthorized_user_rejected(self) -> None:
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "123",
                "GOOGLE_API_KEY": "gemini",
            }
        )
        update = SimpleNamespace(effective_user=SimpleNamespace(id=999))
        self.assertFalse(is_authorized_update(update, cfg))


class DashboardAuthTests(unittest.TestCase):
    def test_dashboard_token_roundtrip(self) -> None:
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "123",
                "GOOGLE_API_KEY": "gemini",
            }
        )
        token = make_dashboard_token(cfg)
        self.assertTrue(verify_dashboard_token(cfg, token))
        self.assertFalse(verify_dashboard_token(cfg, token + "x"))


class DashboardConfigTests(unittest.TestCase):
    def test_candidate_config_rejects_invalid_pixel_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config.from_env(
                {
                    "TELEGRAM_BOT_TOKEN": "token",
                    "TELEGRAM_CHAT_ID": "123",
                    "GOOGLE_API_KEY": "gemini",
                    "DATA_DIR": tmpdir,
                }
            )
            runtime = Runtime(
                base_config=cfg,
                config=cfg,
                startup_overrides={},
                store=StateStore(Path(tmpdir) / "state.db"),
                gmail=SimpleNamespace(),
                model=SimpleNamespace(),
                shutdown_event=SimpleNamespace(),
                mode="polling",
            )
            try:
                with self.assertRaises(ConfigError):
                    build_candidate_config(runtime, {"ENABLE_PIXEL": "1", "PIXEL_WEBHOOK_SECRET": ""})
                candidate = build_candidate_config(
                    runtime,
                    {"ENABLE_PIXEL": "1", "PIXEL_WEBHOOK_SECRET": "secret"},
                )
                self.assertEqual(candidate.resolved_pixel_base_url(), candidate.resolved_public_base_url())
            finally:
                runtime.store.close()


class GmailWatchHelpersTests(unittest.TestCase):
    def test_split_unseen_inbox_ids_returns_chronological_new_messages(self) -> None:
        unseen, newest = split_unseen_inbox_ids(["m5", "m4", "m3", "m2"], "m3")
        self.assertEqual(unseen, ["m4", "m5"])
        self.assertEqual(newest, "m5")

    def test_split_unseen_inbox_ids_handles_missing_last_seen(self) -> None:
        unseen, newest = split_unseen_inbox_ids(["m5", "m4"], None)
        self.assertEqual(unseen, [])
        self.assertEqual(newest, "m5")


class EmailRenderingTests(unittest.TestCase):
    def test_payload_text_prefers_clean_html_when_plain_missing(self) -> None:
        html = """
        <html>
          <head><style>.hero { display:none; }</style></head>
          <body>
            <div>Hi Paolo,</div>
            <p>check the update below</p>
            <img src="cid:test" alt="promo banner">
            <script>alert('x')</script>
            <div>Thanks</div>
          </body>
        </html>
        """
        payload = {
            "mimeType": "text/html",
            "body": {"data": base64.urlsafe_b64encode(html.encode()).decode()},
            "parts": [],
        }

        rendered = payload_text(payload)

        self.assertIn("Hi Paolo,", rendered)
        self.assertIn("check the update below", rendered)
        self.assertIn("[immagine: promo banner]", rendered)
        self.assertNotIn("alert", rendered)

    def test_pixel_asset_response_sets_aggressive_no_cache_headers(self) -> None:
        response = pixel_asset_response("image")
        self.assertIn("no-store", response.headers["cache-control"])
        self.assertIn("proxy-revalidate", response.headers["cache-control"])
        self.assertEqual(response.headers["pragma"], "no-cache")
        self.assertEqual(response.headers["surrogate-control"], "no-store")


class WebAppSmokeTests(unittest.TestCase):
    def test_root_and_dashboard_auth(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = Config.from_env(
                    {
                        "TELEGRAM_BOT_TOKEN": "token",
                        "TELEGRAM_CHAT_ID": "123",
                        "GOOGLE_API_KEY": "gemini",
                        "DATA_DIR": tmpdir,
                        "STATE_DB_PATH": str(Path(tmpdir) / "state.db"),
                        "GMAIL_TOKEN_PATH": str(Path(tmpdir) / "token.json"),
                        "GMAIL_CREDENTIALS_PATH": str(Path(tmpdir) / "credentials.json"),
                    }
                )
                store = StateStore(cfg.state_db_path)
                runtime = Runtime(
                    base_config=cfg,
                    config=cfg,
                    startup_overrides={},
                    store=store,
                    gmail=GmailClient(cfg, service_factory=lambda: SimpleNamespace()),
                    model=SimpleNamespace(),
                    shutdown_event=asyncio.Event(),
                    mode="polling",
                )
                app = build_application(runtime)
                web = create_web_app(runtime, app)
                client = web.test_client()

                root = await client.get("/")
                health = await client.get("/healthz")
                unauth = await client.get("/dashboard")
                token = make_dashboard_token(cfg)
                auth = await client.get(f"/dashboard?token={token}")

                self.assertEqual(root.status_code, 200)
                self.assertEqual(health.status_code, 200)
                self.assertEqual(unauth.status_code, 401)
                self.assertEqual(auth.status_code, 200)
                store.close()

        asyncio.run(run())

    def test_google_oauth_callback_uses_saved_code_verifier(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = Config.from_env(
                    {
                        "TELEGRAM_BOT_TOKEN": "token",
                        "PUBLIC_BASE_URL": "https://glassyreply-bot.fly.dev",
                        "GOOGLE_OAUTH_CREDENTIALS_JSON": '{"web":{"client_id":"x","project_id":"p","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","client_secret":"secret"}}',
                        "DATA_DIR": tmpdir,
                    }
                )
                store = StateStore(Path(tmpdir) / "state.db")
                runtime = Runtime(
                    base_config=cfg,
                    config=cfg,
                    startup_overrides={},
                    store=store,
                    gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                    model=None,
                    shutdown_event=asyncio.Event(),
                    mode="polling",
                )
                runtime.store.set_bot_state(
                    GOOGLE_OAUTH_STATE_KEY,
                    google_oauth_state_payload("oauth-state", "pkce-verifier"),
                )
                app = build_application(runtime)
                web = create_web_app(runtime, app)
                client = web.test_client()

                class FakeCredentials:
                    def to_json(self):
                        return json.dumps({"refresh_token": "token-value"})

                class FakeFlow:
                    def __init__(self):
                        self.credentials = FakeCredentials()
                        self.authorization_response = None

                    def fetch_token(self, authorization_response):
                        self.authorization_response = authorization_response

                fake_flow = FakeFlow()
                try:
                    with patch("tg_email.Flow.from_client_config", return_value=fake_flow) as mocked:
                        response = await client.get("/oauth/google/callback?state=oauth-state&code=abc")
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(
                        mocked.call_args.kwargs["code_verifier"],
                        "pkce-verifier",
                    )
                    self.assertEqual(
                        fake_flow.authorization_response,
                        "https://glassyreply-bot.fly.dev/oauth/google/callback?state=oauth-state&code=abc",
                    )
                    self.assertEqual(
                        store.get_app_settings()["GOOGLE_OAUTH_TOKEN_JSON"],
                        json.dumps({"refresh_token": "token-value"}),
                    )
                    self.assertEqual(store.get_bot_state(GMAIL_INITIAL_SYNC_KEY), "1")
                    self.assertEqual(store.get_bot_state("last_seen_gmail_message_id"), "")
                finally:
                    store.close()

        asyncio.run(run())

    def test_google_oauth_callback_rejects_missing_code_verifier(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = Config.from_env(
                    {
                        "TELEGRAM_BOT_TOKEN": "token",
                        "PUBLIC_BASE_URL": "https://glassyreply-bot.fly.dev",
                        "GOOGLE_OAUTH_CREDENTIALS_JSON": '{"web":{"client_id":"x","project_id":"p","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","client_secret":"secret"}}',
                        "DATA_DIR": tmpdir,
                    }
                )
                store = StateStore(Path(tmpdir) / "state.db")
                runtime = Runtime(
                    base_config=cfg,
                    config=cfg,
                    startup_overrides={},
                    store=store,
                    gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                    model=None,
                    shutdown_event=asyncio.Event(),
                    mode="polling",
                )
                runtime.store.set_bot_state(
                    GOOGLE_OAUTH_STATE_KEY,
                    json.dumps({"state": "oauth-state", "created_at": "2026-04-15T00:00:00+00:00"}),
                )
                app = build_application(runtime)
                web = create_web_app(runtime, app)
                client = web.test_client()
                try:
                    response = await client.get("/oauth/google/callback?state=oauth-state&code=abc")
                    self.assertEqual(response.status_code, 400)
                    body = (await response.get_data()).decode()
                    self.assertIn("OAuth state expired", body)
                finally:
                    store.close()

        asyncio.run(run())

    def test_pixel_status_updates_tracked_email(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = Config.from_env(
                    {
                        "TELEGRAM_BOT_TOKEN": "token",
                        "TELEGRAM_CHAT_ID": "123",
                        "PIXEL_BASE_URL": "https://pixel.example.com",
                        "PIXEL_WEBHOOK_SECRET": "secret",
                        "ENABLE_PIXEL": "1",
                        "DATA_DIR": tmpdir,
                    }
                )
                store = StateStore(Path(tmpdir) / "state.db")
                runtime = Runtime(
                    base_config=cfg,
                    config=cfg,
                    startup_overrides={},
                    store=store,
                    gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                    model=None,
                    shutdown_event=asyncio.Event(),
                    mode="polling",
                )
                store.upsert_tracked_email(
                    TrackedEmail(
                        tg_message_id=444,
                        draft_id="draft-1",
                        recipient="lead@example.com",
                        subject="Tracked subject",
                        open_count=0,
                        first_opened_at="",
                        last_opened_at="",
                        last_classification="",
                        last_layer="",
                        last_dimensions="",
                        last_confidence=None,
                    )
                )
                app = build_application(runtime)
                web = create_web_app(runtime, app)
                client = web.test_client()
                with patch.object(type(app.bot), "edit_message_text", new_callable=AsyncMock) as mocked:
                    response = await client.post(
                        "/pixel_status",
                        headers={"X-Pixel-Secret": "secret"},
                        json={
                            "tg_msg_id": 444,
                            "classification": "human_browser",
                            "layer": "font",
                            "dimensions": "2x1",
                            "confidence": 0.9,
                            "email_subject": "Tracked subject",
                        },
                    )

                    self.assertEqual(response.status_code, 200)
                    tracked = store.get_tracked_email(444)
                    self.assertIsNotNone(tracked)
                    assert tracked is not None
                    self.assertEqual(tracked.open_count, 1)
                    mocked.assert_awaited_once()
                store.close()

        asyncio.run(run())

    def test_self_hosted_track_route_updates_tracked_email(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                cfg = Config.from_env(
                    {
                        "TELEGRAM_BOT_TOKEN": "token",
                        "TELEGRAM_CHAT_ID": "123",
                        "PUBLIC_BASE_URL": "https://glassyreply-bot.fly.dev",
                        "PIXEL_WEBHOOK_SECRET": "secret",
                        "ENABLE_PIXEL": "1",
                        "DATA_DIR": tmpdir,
                    }
                )
                store = StateStore(Path(tmpdir) / "state.db")
                runtime = Runtime(
                    base_config=cfg,
                    config=cfg,
                    startup_overrides={},
                    store=store,
                    gmail=SimpleNamespace(config=cfg, invalidate=lambda: None),
                    model=None,
                    shutdown_event=asyncio.Event(),
                    mode="polling",
                )
                store.upsert_tracked_email(
                    TrackedEmail(
                        tg_message_id=555,
                        draft_id="draft-2",
                        recipient="lead@example.com",
                        subject="Self hosted pixel",
                        open_count=0,
                        first_opened_at="",
                        last_opened_at="",
                        last_classification="",
                        last_layer="",
                        last_dimensions="",
                        last_confidence=None,
                    )
                )
                app = build_application(runtime)
                web = create_web_app(runtime, app)
                client = web.test_client()
                token = make_tracking_token(cfg, 555)

                with patch.object(type(app.bot), "edit_message_text", new_callable=AsyncMock) as mocked:
                    response = await client.get(
                        f"/track/img/2x1/{token}.png",
                        headers={"user-agent": "Mozilla/5.0 Chrome/123.0", "sec-fetch-mode": "no-cors"},
                    )

                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.headers["content-type"], "image/png")
                    tracked = store.get_tracked_email(555)
                    self.assertIsNotNone(tracked)
                    assert tracked is not None
                    self.assertEqual(tracked.open_count, 1)
                    self.assertEqual(tracked.last_classification, "human_browser")
                    mocked.assert_awaited_once()
                store.close()

        asyncio.run(run())


class GmailClientTests(unittest.TestCase):
    class _Response:
        def __init__(self, status: int):
            self.status = status
            self.reason = "reason"

    class _LabelsService:
        def __init__(self, labels_result: dict):
            self._labels_result = labels_result

        def list(self, userId: str):  # noqa: ARG002
            return GmailClientTests._Execute(result=self._labels_result)

    class _UsersService:
        def __init__(self, labels_result: dict):
            self._labels_result = labels_result

        def labels(self):
            return GmailClientTests._LabelsService(self._labels_result)

    class _Execute:
        def __init__(self, *, result=None, error=None):
            self._result = result
            self._error = error

        def execute(self):
            if self._error:
                raise self._error
            return self._result

    class _Service:
        def __init__(self, *, run_error=None, run_result=None, labels_result=None):
            self.run_error = run_error
            self.run_result = run_result
            self.labels_result = labels_result or {"labels": [{"id": "LBL", "name": "Label"}]}

        def users(self):
            return GmailClientTests._UsersService(self.labels_result)

        def run(self):
            if self.run_error:
                raise self.run_error
            return self.run_result

    def _http_error(self, status: int) -> HttpError:
        return HttpError(self._Response(status), b"{}")

    def test_retries_once_after_401(self) -> None:
        services = [
            self._Service(run_error=self._http_error(401)),
            self._Service(run_result="ok"),
        ]
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "123",
                "GOOGLE_API_KEY": "gemini",
            }
        )
        client = GmailClient(cfg, service_factory=lambda: services.pop(0))

        result = client.call(lambda svc: svc.run())

        self.assertEqual(result, "ok")
        self.assertEqual(client.refresh_labels(), {"LBL": "Label"})
        self.assertEqual(services, [])

    def test_raises_after_second_401(self) -> None:
        services = [
            self._Service(run_error=self._http_error(401)),
            self._Service(run_error=self._http_error(401)),
        ]
        cfg = Config.from_env(
            {
                "TELEGRAM_BOT_TOKEN": "token",
                "TELEGRAM_CHAT_ID": "123",
                "GOOGLE_API_KEY": "gemini",
            }
        )
        client = GmailClient(cfg, service_factory=lambda: services.pop(0))

        with self.assertRaises(HttpError):
            client.call(lambda svc: svc.run())

        self.assertEqual(services, [])

    def test_list_recent_monitored_ids_merges_multiple_labels(self) -> None:
        cfg = Config.from_env({"TELEGRAM_BOT_TOKEN": "token"})
        client = GmailClient(cfg, service_factory=lambda: SimpleNamespace())
        label_payloads = {
            "INBOX": ["m3", "m1"],
            "CATEGORY_PROMOTIONS": ["m2", "m1"],
        }
        internal_dates = {"m1": 10, "m2": 20, "m3": 30}

        with patch.object(
            client,
            "list_recent_label_ids",
            side_effect=lambda label_id, limit=100: label_payloads[label_id],
        ), patch.object(
            client,
            "get_internal_date",
            side_effect=lambda message_id: internal_dates[message_id],
        ):
            merged = client.list_recent_monitored_ids(["INBOX", "CATEGORY_PROMOTIONS"], limit=10)

        self.assertEqual(merged, ["m3", "m2", "m1"])


if __name__ == "__main__":
    unittest.main()
