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
    create_web_app,
    make_dashboard_token,
    is_authorized_update,
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
