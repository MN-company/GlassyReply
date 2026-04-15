from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from googleapiclient.errors import HttpError

from tg_email import (
    Runtime,
    Config,
    ConfigError,
    EmailState,
    GmailClient,
    StateStore,
    build_candidate_config,
    build_application,
    claim_owner,
    create_web_app,
    google_web_client_config,
    make_dashboard_token,
    owner_configured,
    is_authorized_update,
    save_runtime_settings,
    split_unseen_inbox_ids,
    startup_notice_text,
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
                    build_candidate_config(runtime, {"ENABLE_PIXEL": "1"})
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


if __name__ == "__main__":
    unittest.main()
