"""Unit & smoke coverage for AIStudio2API-style Camoufox + WebDriver BiDi runtime."""

import asyncio
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, patch

from src.core.config import config
from src.core.database import Database
from src.core.models import Token
from src.services.browser_captcha import BrowserCaptchaService, TokenBrowser
from src.services.camoufox_bidi import (
    BiDiClient,
    CAMOUFOX_FIREFOX_MAJOR,
    CamoufoxLoginManager,
    CamoufoxLoginSession,
    CamoufoxWorker,
    LocalSocks5Relay,
    _from_bidi_remote_value,
    _to_bidi_local_value,
    acquire_profile_login_lease,
    build_camoufox_env,
    build_camoufox_fingerprint_config,
    find_camoufox_executable,
    get_profile_login_holder,
    is_token_in_login_session,
    list_browser_profile_dirs,
    load_or_create_account_camoufox_config,
    normalize_browser_profile_dir,
    persist_account_login_artifacts,
    release_profile_login_lease,
    resolve_account_profile_dir,
    restore_startup_cache,
    save_startup_cache,
    write_camoufox_user_js,
)
from src.services.flow_client import FlowClient


class CamoufoxBiDiUnitTests(unittest.IsolatedAsyncioTestCase):
    def test_windows_fingerprint_persistence_and_env_chunking(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg1 = load_or_create_account_camoufox_config(
                tmp_dir, CAMOUFOX_FIREFOX_MAJOR, "en-US", "America/New_York"
            )
            self.assertEqual(cfg1["navigator.platform"], "Win32")
            self.assertIn(f"Firefox/{CAMOUFOX_FIREFOX_MAJOR}.0", cfg1["navigator.userAgent"])
            self.assertTrue(cfg1.get("fonts"))
            self.assertIn("Segoe UI", cfg1["fonts"])
            self.assertTrue(cfg1.get("webGl:renderer"))

            # Reloading from the same directory preserves the exact fingerprint seeds
            cfg2 = load_or_create_account_camoufox_config(
                tmp_dir, CAMOUFOX_FIREFOX_MAJOR, "en-US", "America/New_York"
            )
            self.assertEqual(cfg1["canvas:seed"], cfg2["canvas:seed"])
            self.assertEqual(cfg1["audio:seed"], cfg2["audio:seed"])
            self.assertEqual(cfg1["fonts:spacing_seed"], cfg2["fonts:spacing_seed"])

            env = build_camoufox_env(cfg1, base_env={"PATH": "/usr/bin", "CAMOU_CONFIG_99": "stale"})
            self.assertNotIn("CAMOU_CONFIG_99", env)
            self.assertIn("CAMOU_CONFIG_1", env)
            reassembled = "".join(
                env[f"CAMOU_CONFIG_{i}"]
                for i in range(1, 50)
                if f"CAMOU_CONFIG_{i}" in env
            )
            self.assertEqual(json.loads(reassembled)["canvas:seed"], cfg1["canvas:seed"])

    def test_user_js_writes_1fps_divisor_and_proxy_prefs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            profile_dir = Path(tmp_dir)
            fp_cfg = build_camoufox_fingerprint_config()
            write_camoufox_user_js(
                profile_dir,
                fp_cfg,
                relay_addr=("127.0.0.1", 19080),
                headless=True,
                cache_dir=profile_dir / "cache",
            )
            user_js = (profile_dir / "user.js").read_text(encoding="utf-8")
            self.assertNotIn('user_pref("gfx.display.frame-rate-divisor", 60);', user_js)
            self.assertIn('user_pref("devtools.chrome.enabled", true);', user_js)
            self.assertIn('user_pref("network.proxy.socks", "127.0.0.1");', user_js)
            self.assertIn('user_pref("network.proxy.socks_port", 19080);', user_js)
            self.assertIn('user_pref("network.proxy.socks_remote_dns", true);', user_js)

            self.assertNotIn('font.system.whitelist', user_js)

            headed_profile_dir = profile_dir / "headed"
            headed_profile_dir.mkdir(parents=True, exist_ok=True)
            write_camoufox_user_js(
                headed_profile_dir,
                fp_cfg,
                headless=False,
                live_view=True,
            )
            headed_user_js = (headed_profile_dir / "user.js").read_text(encoding="utf-8")
            self.assertNotIn('user_pref("gfx.display.frame-rate-divisor", 60);', headed_user_js)
            if os.uname().sysname.lower() == "darwin":
                self.assertIn('user_pref("gfx.font_loader.delay", 0);', headed_user_js)
                self.assertIn('user_pref("layers.gpu-process.enabled", false);', headed_user_js)
                self.assertIn('user_pref("webgl.allow-in-parent", true);', headed_user_js)
    def test_bidi_value_codec_roundtrip(self):
        original = {
            "ok": True,
            "count": 42,
            "token": "03AFcWeA",
            "items": ["a", 1, None],
        }
        encoded = _to_bidi_local_value(original)
        decoded = _from_bidi_remote_value(encoded)
        self.assertEqual(decoded, original)

    def test_flow_client_firefox_fingerprint_omits_sec_ch_ua_and_uses_firefox_impersonate(self):
        client = FlowClient(None)
        ff_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0"
        fp = client._build_fingerprint_from_user_agent(
            ff_ua, accept_language="en-US,en;q=0.9"
        )
        self.assertEqual(fp["user_agent"], ff_ua)
        self.assertEqual(fp["sec_ch_ua"], "")
        self.assertEqual(fp["sec_ch_ua_platform"], "")
        self.assertEqual(fp["sec_ch_ua_mobile"], "")

        client._set_request_fingerprint(fp)
        try:
            self.assertEqual(client._resolve_runtime_impersonate(), "firefox")
        finally:
            client.clear_request_fingerprint()

    async def test_browser_captcha_service_creates_camoufox_worker_by_default(self):
        service = BrowserCaptchaService(db=None)
        with patch.object(config, "_config", {"captcha": {"browser_engine": "camoufox"}}):
            browser = await service._get_or_create_browser(0)
            self.assertIsInstance(browser, CamoufoxWorker)

    async def test_current_rpc_uses_in_page_camoufox_executor_when_lease_active(self):
        client = FlowClient(None)
        client._set_captcha_request_context(
            {"method": "browser", "token_id": 7, "browser_ref": "0:lease-abc/"}
        )
        wrb_frame = json.dumps(
            [["wrb.fr", "ogiZ0b", json.dumps([["https://flow-content.google/image/11111111-2222-3333-4444-555555555555"]]), None, None, None, "generic"]]
        )
        fake_service = types.SimpleNamespace(
            execute_flow_frontend_rpc=AsyncMock(
                return_value={"status": 200, "text": f")}}'\n\n{len(wrb_frame)}\n{wrb_frame}\n"}
            )
        )
        try:
            with patch(
                "src.services.browser_captcha.BrowserCaptchaService.get_instance",
                AsyncMock(return_value=fake_service),
            ):
                payload = await client._current_rpc(
                    rpc_id="ogiZ0b",
                    argument=["arg1"],
                    token_id=7,
                    project_id="proj-1",
                )
            self.assertEqual(
                payload,
                [["https://flow-content.google/image/11111111-2222-3333-4444-555555555555"]],
            )
            fake_service.execute_flow_frontend_rpc.assert_awaited_once()
        finally:
            client._set_captcha_request_context(None)

    async def test_live_camoufox_launch_and_1fps_bidi_control(self):
        exe = find_camoufox_executable()
        if not exe:
            self.skipTest("Camoufox executable not installed locally")

        with tempfile.TemporaryDirectory() as tmp_dir:
            worker = CamoufoxWorker(0, tmp_dir, db=None)
            try:
                proc, bidi, context_id = await worker._get_or_create_shared_browser()
                self.assertIsNotNone(proc.pid)
                self.assertTrue(context_id)

                # Verify Windows Firefox fingerprint injected via CAMOU_CONFIG_*
                nav_info = await bidi.evaluate(
                    context_id,
                    "({ ua: navigator.userAgent, platform: navigator.platform, webdriver: navigator.webdriver })",
                    await_promise=False,
                )
                self.assertEqual(nav_info["platform"], "Win32")
                self.assertIn("Windows NT 10.0", nav_info["ua"])
                self.assertFalse(nav_info["webdriver"])

                # Verify 60fps boost during _boost_frames()
                async with worker._boost_frames():
                    active_divisor = await worker._chrome_evaluate(
                        "Services.prefs.getIntPref('gfx.display.frame-rate-divisor')"
                    )
                    self.assertEqual(active_divisor, 1)
            finally:
                await worker.recycle_browser(reason="unit_test_done")


    def test_startup_cache_and_login_artifacts_persistence(self):
        with tempfile.TemporaryDirectory() as tmp_root:
            root = Path(tmp_root)
            profile_dir = root / "profile_1"
            account_dir = root / "login_account"
            final_token_dir = root / "token_42"

            (profile_dir / "startupCache").mkdir(parents=True)
            (profile_dir / "startupCache" / "startupCache.8.little").write_bytes(b"CACHE_DATA")
            (profile_dir / "compatibility.ini").write_text("[Compatibility]\nLastVersion=152.0\n", encoding="utf-8")
            account_dir.mkdir(parents=True)
            (account_dir / "camoufox-fingerprint.json").write_text('{"firefox_version":152}', encoding="utf-8")
            (account_dir / "storage-state.json").write_text('{"cookies":[],"origins":[]}', encoding="utf-8")

            save_startup_cache(profile_dir, account_dir)
            self.assertTrue((account_dir / "profile-startup" / "startupCache" / "startupCache.8.little").is_file())
            self.assertTrue((account_dir / "profile-startup" / "compatibility.ini").is_file())

            persist_account_login_artifacts(account_dir, final_token_dir)
            self.assertTrue((final_token_dir / "camoufox-fingerprint.json").is_file())
            self.assertTrue((final_token_dir / "storage-state.json").is_file())

            fresh_profile = root / "profile_2"
            fresh_profile.mkdir(parents=True)
            restore_startup_cache(final_token_dir, fresh_profile)
            self.assertEqual(
                (fresh_profile / "startupCache" / "startupCache.8.little").read_bytes(),
                b"CACHE_DATA",
            )

    async def test_camoufox_login_session_liveview_and_cookie_harvest(self):
        exe = find_camoufox_executable()
        if not exe:
            self.skipTest("Camoufox executable not installed locally")

        with tempfile.TemporaryDirectory() as tmp_root:
            session = CamoufoxLoginSession(
                "test-login-sess",
                mode="liveview",
                auto_save_token=True,
                timeout_seconds=45.0,
                user_data_root=Path(tmp_root),
            )
            completed_event = asyncio.Event()

            async def _fake_on_complete(sess, exported):
                completed_event.set()
                return {"token_id": 99, "action": "added", "credits": 1000}

            # Stub _wait_for_login to verify LiveView frame capture and then return simulated Flow auth
            original_wait = session._wait_for_login

            async def _stubbed_wait_for_login():
                # Wait up to 8s for LiveView to capture at least one JPEG screenshot frame
                for _ in range(30):
                    if session.live_view.frame_b64:
                        break
                    await asyncio.sleep(0.2)
                self.assertTrue(session.live_view.frame_b64)
                # Seed Google/Flow session cookies via BiDi so _export_login_state harvests them
                assert session._bidi is not None
                for cname, cval, cdom in [
                    ("SID", "sid-val-123", ".google.com"),
                    ("__Secure-1PSID", "psid-val-456", ".google.com"),
                    ("SAPISID", "sapisid-val-789", ".google.com"),
                ]:
                    await session._bidi.command(
                        "storage.setCookie",
                        {
                            "cookie": {
                                "name": cname,
                                "value": {"type": "string", "value": cval},
                                "domain": cdom,
                                "path": "/",
                                "secure": True,
                                "httpOnly": False,
                                "sameSite": "lax",
                            }
                        },
                    )
                return {
                    "email": "flowuser@gmail.com",
                    "xsrf_token": "xsrf-abc:123",
                    "project_id": "11111111-2222-3333-4444-555555555555",
                    "page_url": "https://flow.google.com/project/11111111-2222-3333-4444-555555555555",
                    "origins": {
                        "https://flow.google.com": {
                            "origin": "https://flow.google.com",
                            "localStorage": [{"name": "flow_theme", "value": "dark"}],
                        }
                    },
                }

            session._wait_for_login = _stubbed_wait_for_login
            await session.start(on_complete=_fake_on_complete)
            await asyncio.wait_for(completed_event.wait(), timeout=35.0)
            # Wait for session task to finish persisting artifacts
            if session._task:
                await asyncio.wait_for(session._task, timeout=15.0)

            self.assertEqual(session.status, "completed")
            self.assertIsNotNone(session.result)
            self.assertEqual(session.result["email"], "flowuser@gmail.com")
            self.assertEqual(session.result["token_id"], 99)
            cookies = json.loads(session.result["google_cookies"])
            cookie_names = {c["name"] for c in cookies}
            self.assertIn("SID", cookie_names)
            self.assertIn("__Secure-1PSID", cookie_names)
            # Verify fingerprint and storage-state were persisted to token_99 directory
            token_dir = Path(tmp_root) / "token_99"
            self.assertTrue((token_dir / "camoufox-fingerprint.json").is_file())
            self.assertTrue((token_dir / "storage-state.json").is_file())

    def test_normalize_and_resolve_account_profile_dir(self):
        with tempfile.TemporaryDirectory() as tmp_root:
            root = Path(tmp_root)
            self.assertEqual(normalize_browser_profile_dir(None), "")
            self.assertEqual(normalize_browser_profile_dir("alice"), "profiles/alice")
            self.assertEqual(
                normalize_browser_profile_dir("browser_data_rt/profiles/team_a/"),
                "profiles/team_a",
            )
            self.assertEqual(normalize_browser_profile_dir("token_9"), "token_9")

            for bad in ("../escape", "profiles/../x", "shared", "pending", "browser_0", "bad name!"):
                with self.assertRaises(ValueError):
                    normalize_browser_profile_dir(bad)

            dir1, key1 = resolve_account_profile_dir(root, token_id=7, profile_dir_name=None)
            self.assertEqual((dir1, key1), (root / "token_7", "token_7"))
            dir2, key2 = resolve_account_profile_dir(root, token_id=7, profile_dir_name="alice")
            self.assertEqual((dir2, key2), (root / "profiles" / "alice", "profiles/alice"))
            dir3, key3 = resolve_account_profile_dir(root, token_id=None, profile_dir_name=None)
            self.assertEqual((dir3, key3), (root / "pending", ""))

    def test_profile_login_lease_and_directory_discovery(self):
        with tempfile.TemporaryDirectory() as tmp_root:
            root = Path(tmp_root)
            t1 = root / "token_1"
            (t1 / "firefox-profile").mkdir(parents=True)
            (t1 / "firefox-profile" / "cookies.sqlite").write_bytes(b"db")
            (t1 / "camoufox-fingerprint.json").write_text("{}", encoding="utf-8")
            alice = root / "profiles" / "alice"
            (alice / "firefox-profile").mkdir(parents=True)
            (alice / "storage-state.json").write_text("{}", encoding="utf-8")
            # Ignored internal directories
            (root / "shared").mkdir()
            (root / "pending").mkdir()
            (root / "browser_0").mkdir()

            acquire_profile_login_lease(alice, "sess-1", token_id=42)
            try:
                self.assertEqual(get_profile_login_holder(alice), "sess-1")
                self.assertTrue(is_token_in_login_session(42))
                with self.assertRaises(RuntimeError):
                    acquire_profile_login_lease(alice, "sess-2", token_id=42)

                entries = list_browser_profile_dirs(root)
                self.assertEqual(
                    [e["profile_dir"] for e in entries], ["token_1", "profiles/alice"]
                )
                by_dir = {e["profile_dir"]: e for e in entries}
                self.assertTrue(by_dir["profiles/alice"]["active_login"])
                self.assertTrue(by_dir["profiles/alice"]["has_storage_state"])
                self.assertFalse(by_dir["token_1"]["active_login"])
                self.assertTrue(by_dir["token_1"]["has_cookies_db"])
                self.assertTrue(by_dir["token_1"]["has_fingerprint"])
            finally:
                release_profile_login_lease(alice, "sess-1", token_id=42)
            self.assertIsNone(get_profile_login_holder(alice))
            self.assertFalse(is_token_in_login_session(42))

    async def test_database_browser_profile_dir_backfill_and_uniqueness(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = Database(db_path=str(Path(tmp_dir) / "test.db"))
            await db.init_db()
            try:
                t1_id = await db.add_token(
                    Token(
                        st="frontend-1",
                        at="at-1",
                        email="one@example.com",
                        google_cookies="SID=1",
                    )
                )
                t2_id = await db.add_token(
                    Token(
                        st="frontend-2",
                        at="at-2",
                        email="two@example.com",
                        google_cookies="SID=2",
                        browser_profile_dir="profiles/alice",
                    )
                )
                t1 = await db.get_token(t1_id)
                t2 = await db.get_token(t2_id)
                self.assertEqual(t1.browser_profile_dir, f"token_{t1_id}")
                self.assertEqual(t2.browser_profile_dir, "profiles/alice")

                with self.assertRaises(ValueError):
                    await db.add_token(
                        Token(
                            st="frontend-3",
                            at="at-3",
                            email="three@example.com",
                            google_cookies="SID=3",
                            browser_profile_dir="PROFILES/ALICE",
                        )
                    )
                with self.assertRaises(ValueError):
                    await db.update_token(t1_id, browser_profile_dir="profiles/alice")
            finally:
                await db.close()

    async def test_warmup_skips_tokens_in_active_profile_login_lease(self):
        with tempfile.TemporaryDirectory() as tmp_root:
            root = Path(tmp_root)
            db_stub = types.SimpleNamespace(
                get_active_tokens=AsyncMock(
                    return_value=[
                        Token(
                            id=1,
                            st="frontend-1",
                            email="a@example.com",
                            google_cookies="SID=1",
                            browser_profile_dir="profiles/alice",
                            captcha_proxy_url="http://127.0.0.1:8080",
                        ),
                        Token(
                            id=2,
                            st="frontend-2",
                            email="b@example.com",
                            google_cookies="SID=2",
                            browser_profile_dir="token_2",
                        ),
                    ]
                )
            )
            svc = BrowserCaptchaService(db=db_stub)
            svc._browser_count = 2
            svc.base_user_data_dir = str(root)
            alice_dir = root / "profiles" / "alice"
            acquire_profile_login_lease(alice_dir, "sess-busy", token_id=1)
            try:
                specs = await svc._get_warmup_token_specs()
                self.assertEqual(specs, [(2, None)])
            finally:
                release_profile_login_lease(alice_dir, "sess-busy", token_id=1)

    async def test_admin_browser_profile_list_and_login_start_binding(self):
        from src.api import admin as admin_api
        from fastapi import HTTPException

        with tempfile.TemporaryDirectory() as tmp_root:
            root = Path(tmp_root)
            tok1 = Token(
                id=1,
                st="frontend-1",
                email="owner@example.com",
                google_cookies="SID=1",
                browser_profile_dir="profiles/alice",
                captcha_proxy_url="http://127.0.0.1:9000",
                is_active=True,
            )
            tok2 = Token(
                id=2,
                st="frontend-2",
                email="second@example.com",
                google_cookies="SID=2",
                browser_profile_dir="token_2",
                is_active=True,
            )
            fake_tm = types.SimpleNamespace(
                get_all_tokens=AsyncMock(return_value=[tok1, tok2]),
                get_token=AsyncMock(side_effect=lambda tid: {1: tok1, 2: tok2}.get(tid)),
            )
            fake_db = types.SimpleNamespace(get_captcha_config=AsyncMock(return_value=None))
            captured_calls = []

            class FakeMgr:
                async def start_session(self, **kwargs):
                    captured_calls.append(kwargs)
                    return types.SimpleNamespace(
                        to_dict=lambda: {
                            "session_id": "s-1",
                            "status": "starting",
                            "mode": kwargs["mode"],
                            "headless": True,
                            "token_id": kwargs["token_id"],
                            "browser_profile_dir": kwargs["browser_profile_dir"] or "",
                            "clean_login": kwargs["clean_login"],
                            "reused_existing_profile": False,
                            "message": "ok",
                            "error": None,
                            "page_url": "",
                            "result": None,
                        }
                    )

            with (
                patch.object(admin_api, "token_manager", fake_tm),
                patch.object(admin_api, "db", fake_db),
                patch.object(
                    CamoufoxLoginManager, "get_instance", return_value=FakeMgr()
                ),
            ):
                res = await admin_api.start_browser_login(
                    admin_api.BrowserLoginStartRequest(
                        mode="liveview",
                        browser_profile_dir="alice",
                        clean_login=True,
                    ),
                    _admin_token="ok",
                )
                self.assertEqual(res["token_id"], 1)
                self.assertEqual(res["browser_profile_dir"], "profiles/alice")
                self.assertTrue(captured_calls[-1]["clean_login"])
                self.assertEqual(captured_calls[-1]["proxy_url"], "http://127.0.0.1:9000")

                with self.assertRaises(HTTPException) as ctx:
                    await admin_api.start_browser_login(
                        admin_api.BrowserLoginStartRequest(
                            token_id=2,
                            browser_profile_dir="profiles/alice",
                        ),
                        _admin_token="ok",
                    )
                self.assertEqual(ctx.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
