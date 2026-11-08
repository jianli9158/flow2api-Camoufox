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
from src.services.browser_captcha import BrowserCaptchaService, TokenBrowser
from src.services.camoufox_bidi import (
    BiDiClient,
    CAMOUFOX_FIREFOX_MAJOR,
    CamoufoxLoginSession,
    CamoufoxWorker,
    LocalSocks5Relay,
    _from_bidi_remote_value,
    _to_bidi_local_value,
    build_camoufox_env,
    build_camoufox_fingerprint_config,
    find_camoufox_executable,
    load_or_create_account_camoufox_config,
    persist_account_login_artifacts,
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

if __name__ == "__main__":
    unittest.main()
