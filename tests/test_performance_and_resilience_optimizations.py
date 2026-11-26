"""Focused regression tests for Batch 1-3 performance, resilience, storage, and security optimizations."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from src.api import admin as admin_api
from src.api import routes as routes_api
from src.core.config import config
from src.core.database import Database
from src.core.models import RequestLog, Token
from src.services.browser_captcha import BrowserCaptchaService
from src.services.camoufox_bidi import (
    build_camoufox_fingerprint_config,
    prune_camoufox_profile_bloat,
    write_camoufox_user_js,
)
from src.services.file_cache import FileCache
from src.services.flow_client import FlowClient
from src.services.flow_features import FlowFeatureService
from src.services.generation_handler import GenerationHandler
from src.services.load_balancer import LoadBalancer


class PerformanceAndResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def test_warm_slot_affinity_in_load_aware_and_polling_modes(self):
        tokens = [
            Token(id=i, st=f"st-{i}", at="flow-frontend-cookie", email=f"u{i}@example.com", is_active=True, image_enabled=True, video_enabled=True)
            for i in (1, 2, 3, 4)
        ]
        tm = types.SimpleNamespace(
            get_active_tokens=AsyncMock(return_value=tokens),
            needs_at_refresh=lambda _t: False,
            ensure_valid_token=AsyncMock(side_effect=lambda t: t),
        )
        svc = BrowserCaptchaService(db=None)
        svc._browser_count = 2
        svc._browsers = {
            0: types.SimpleNamespace(
                has_shared_browser=lambda: True,
                is_busy=lambda: False,
                _shared_bound_token_id=2,
            ),
            1: types.SimpleNamespace(
                has_shared_browser=lambda: True,
                is_busy=lambda: False,
                _shared_bound_token_id=4,
            ),
        }
        self.assertEqual(svc.get_warm_bound_token_ids(idle_only=True), {2, 4})
        self.assertFalse(svc.has_empty_or_unbound_slot())

        lb = LoadBalancer(tm)
        lb.WARM_AFFINITY_EPOCH_MULTIPLIER = 2  # epoch_limit = 2 * 2 = 4 warm picks before 1 cold rotation
        with (
            patch.object(BrowserCaptchaService, "_instance", svc),
            patch("src.services.load_balancer.config") as mock_cfg,
        ):
            mock_cfg.captcha_method = "camoufox"
            mock_cfg.call_logic_mode = "default"
            for _ in range(5):
                picked = await lb.select_token(for_image_generation=True)
                self.assertIn(picked.id, {2, 4})

            mock_cfg.call_logic_mode = "polling"
            seq = [
                (await lb.select_token(for_image_generation=True)).id
                for _ in range(5)
            ]
            # First 4 picks stay strictly inside warm pool {2, 4}; 5th pick rotates in a cold account {1, 3}
            self.assertTrue(all(tid in {2, 4} for tid in seq[:4]))
            self.assertIn(seq[4], {1, 3})

    async def test_tiered_rpc_http_first_with_camoufox_fallback_and_project_singleflight_cache(self):
        client = FlowClient(proxy_manager=None, db=None)
        client._get_runtime_config = lambda: types.SimpleNamespace(
            captcha_method="camoufox", browser_in_page_rpc=True
        )
        project_payload = [
            [],
            [["wf-1", None, None, ["CustomHero", None, None, None, "med-1"]]],
            [["med-1", None, "wf-1", None, None, [None, None, None, None, None, None, None, None, None, 1]]],
            [["Achernar", "Female", ["soft"], "https://gstatic.com/Achernar.wav", True]],
            None,
            [],
        ]
        wrb_frame = json.dumps(
            [["wrb.fr", "as29s", json.dumps(["med-fb", "proj-1", "wf-1", "https://flow-content.google/image/med-fb"]), None, None, None, "generic"]]
        )
        fake_svc = types.SimpleNamespace(
            execute_flow_frontend_rpc=AsyncMock(
                return_value={"status": 200, "text": f")}}'\n\n{len(wrb_frame)}\n{wrb_frame}\n"}
            ),
            get_warm_bound_token_ids=lambda idle_only=False: {7},
        )
        http_calls = []

        async def fake_http_rpc(**kwargs):
            http_calls.append(kwargs["rpc_id"])
            if kwargs["rpc_id"] == "Zzl0ze":
                await asyncio.sleep(0.03)
                return project_payload
            if kwargs["rpc_id"] == "as29s":
                raise RuntimeError("Flow frontend RPC rejected: rpc=as29s, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY")
            return []

        client._resolve_flow_frontend_cookie_storage = AsyncMock(return_value="SID=ok")
        client._call_flow_frontend_rpc = AsyncMock(side_effect=fake_http_rpc)

        with (
            patch.object(BrowserCaptchaService, "_instance", fake_svc),
            patch.object(BrowserCaptchaService, "get_instance", AsyncMock(return_value=fake_svc)),
        ):
            # Concurrent list_voices + list_characters coalesce into 1 HTTP Zzl0ze call and 0 browser borrows
            v_res, c_res = await asyncio.gather(
                client.list_voices("proj-1", token_id=7),
                client.list_characters("proj-1", token_id=7),
            )
            self.assertEqual(http_calls, ["Zzl0ze"])
            fake_svc.execute_flow_frontend_rpc.assert_not_awaited()
            self.assertEqual(len(v_res["presetVoices"]), 1)
            self.assertEqual(c_res["projectId"], "proj-1")

            # Subsequent cached read hits memory cache without another RPC
            _ = await client.get_project_settings("proj-1", token_id=7)
            self.assertEqual(http_calls, ["Zzl0ze"])

            # Mutation invalidates project cache
            await client.delete_voice(project_id="proj-1", workflow_id="wf-1", token_id=7)
            _ = await client.list_voices("proj-1", token_id=7)
            self.assertEqual(http_calls.count("Zzl0ze"), 2)

            # HTTP risk challenge on low-risk RPC automatically falls back to Camoufox in-page execution
            media = await client.get_media("at", "med-fb", token_id=7, project_id="proj-1")
            self.assertIn("med-fb", media["url"])
            fake_svc.execute_flow_frontend_rpc.assert_awaited_once()

    def test_profile_disk_quota_prefs_and_bloat_pruner_respects_locks(self):
        with tempfile.TemporaryDirectory() as tmp_root:
            root = Path(tmp_root)
            idle_ff = root / "token_1" / "firefox-profile"
            busy_ff = root / "token_2" / "firefox-profile"
            (idle_ff / "gmp-widevinecdm" / "4.10").mkdir(parents=True)
            (idle_ff / "gmp-widevinecdm" / "4.10" / "libwidevine.dylib").write_bytes(b"W" * 4096)
            (idle_ff / "cache2" / "entries").mkdir(parents=True)
            for idx in range(4):
                p = idle_ff / "cache2" / "entries" / f"entry_{idx}"
                p.write_bytes(b"C" * 2048)

            (busy_ff / "gmp-widevinecdm").mkdir(parents=True)
            (busy_ff / "gmp-widevinecdm" / "keep.bin").write_bytes(b"K" * 2048)
            (busy_ff / ".parentlock").write_bytes(b"")

            write_camoufox_user_js(idle_ff, build_camoufox_fingerprint_config())
            user_js = (idle_ff / "user.js").read_text(encoding="utf-8")
            self.assertIn('user_pref("browser.cache.disk.capacity", 30720);', user_js)
            self.assertIn('user_pref("media.gmp-widevinecdm.enabled", false);', user_js)

            stats = prune_camoufox_profile_bloat(root, max_cache_bytes=3000)
            self.assertEqual(stats["profiles_scanned"], 2)
            self.assertEqual(stats["profiles_skipped_busy"], 1)
            self.assertGreater(stats["bytes_reclaimed"], 8000)
            self.assertFalse((idle_ff / "gmp-widevinecdm").exists())
            self.assertTrue((busy_ff / "gmp-widevinecdm" / "keep.bin").is_file())
            remaining_cache = sum(
                f.stat().st_size for f in (idle_ff / "cache2" / "entries").iterdir()
            )
            self.assertLessEqual(remaining_cache, 3000)

    async def test_client_disconnect_cancels_non_stream_and_converges_102_stream_log_on_generator_exit(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = Database(db_path=str(Path(tmp_dir) / "disc.db"))
            await db.init_db()
            try:
                tok_id = await db.add_token(
                    Token(st="st", at="flow-frontend-cookie", email="a@b.com", is_active=True, image_enabled=True, video_enabled=True)
                )
                tok = await db.get_token(tok_id)
                # Seed an orphaned 102 row and re-run init_db to verify startup sweep -> 499
                orphan_id = await db.add_request_log(
                    RequestLog(
                        token_id=tok_id,
                        operation="generate_video",
                        request_body="{}",
                        response_body="{}",
                        status_code=102,
                        duration=0.0,
                        status_text="preparing_video",
                        progress=24,
                    )
                )
                await db.init_db()
                logs = await db.get_logs(limit=5)
                by_id = {row["id"]: row for row in logs}
                self.assertEqual(by_id[orphan_id]["status_code"], 499)

                # Verify GeneratorExit (client closing SSE stream mid-flight) updates 102 -> 499
                handler = GenerationHandler.__new__(GenerationHandler)
                handler.proxy_manager = None
                handler.file_cache = types.SimpleNamespace()
                handler.token_manager = types.SimpleNamespace(
                    get_active_tokens=AsyncMock(return_value=[tok]),
                    needs_at_refresh=lambda _t: False,
                    ensure_valid_token=AsyncMock(return_value=tok),
                    ensure_project_exists=AsyncMock(return_value="proj-1"),
                )
                handler.load_balancer = LoadBalancer(handler.token_manager)

                cancelled_inside_gen = asyncio.Event()

                async def slow_generate_image(**_kwargs):
                    try:
                        await asyncio.sleep(30)
                    except asyncio.CancelledError:
                        cancelled_inside_gen.set()
                        raise

                handler.flow_client = types.SimpleNamespace(
                    clear_request_fingerprint=lambda: None,
                    generate_image=slow_generate_image,
                    get_request_fingerprint=lambda: None,
                )

                gen = handler.handle_generation(
                    "gemini-3.1-flash-image-landscape", "slow prompt", stream=True
                )
                first_chunk = await gen.__anext__()
                self.assertIn("已启动", first_chunk)
                await gen.aclose()  # Throws GeneratorExit inside handle_generation

                latest_logs = await db.get_logs(limit=5)
                self.assertEqual(latest_logs[0]["status_code"], 499)

                # Verify _collect_non_stream_result cancels slow generation when raw_request disconnects
                orig_handler = routes_api.generation_handler
                routes_api.set_generation_handler(handler)
                try:
                    disconnect_flag = False

                    class FakeReq:
                        async def is_disconnected(self):
                            return disconnect_flag

                    async def trigger_disconnect():
                        nonlocal disconnect_flag
                        await asyncio.sleep(0.1)
                        disconnect_flag = True

                    asyncio.create_task(trigger_disconnect())
                    with self.assertRaises(HTTPException) as ctx:
                        await routes_api._collect_non_stream_result(
                            "gemini-3.1-flash-image-landscape",
                            "cancel me",
                            [],
                            raw_request=FakeReq(),
                        )
                    self.assertEqual(ctx.exception.status_code, 499)
                    self.assertTrue(cancelled_inside_gen.is_set())
                finally:
                    routes_api.set_generation_handler(orig_handler)
            finally:
                await db.close()

    async def test_flow_features_failover_asset_owner_routing_and_email_index_lookup(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = Database(db_path=str(Path(tmp_dir) / "feat.db"))
            await db.init_db()
            try:
                t1_id = await db.add_token(
                    Token(st="s1", at="flow-frontend-cookie", email="bad@example.com", google_cookies="SID=1", is_active=True, current_project_id="p1")
                )
                t2_id = await db.add_token(
                    Token(st="s2", at="flow-frontend-cookie", email="owner@example.com", google_cookies="SID=2", is_active=True, current_project_id="p2")
                )
                t1 = await db.get_token(t1_id)
                t2 = await db.get_token(t2_id)

                tm = types.SimpleNamespace(
                    get_active_tokens=AsyncMock(return_value=[t1, t2]),
                    needs_at_refresh=lambda _t: False,
                    ensure_valid_token=AsyncMock(side_effect=lambda t: t),
                    ensure_project_exists=AsyncMock(side_effect=lambda tid: f"p{tid}"),
                    _clear_at_validation_cache=MagicMock(),
                )
                lb = LoadBalancer(tm)
                lb._round_robin_state["default"] = t2_id
                media_calls = []

                async def fake_get_media(_at, media_id, *, token_id=None, **_kw):
                    media_calls.append(token_id)
                    if token_id == t1_id:
                        raise RuntimeError("Flow 登录认证失败：Google Cookie 已失效 (rpc=as29s, code=401)")
                    return {
                        "name": media_id,
                        "projectId": "p2",
                        "workflowId": "wf-2",
                        "mediaType": "image",
                        "url": f"https://flow-content.google/image/{media_id}",
                        "frontendRpc": "as29s",
                    }

                fc = types.SimpleNamespace(
                    FRONTEND_ACCESS_TOKEN="flow-frontend-cookie",
                    get_media=AsyncMock(side_effect=fake_get_media),
                )
                svc = FlowFeatureService(fc, tm, lb, db)

                # 1. Auto-failover from t1 (401 -> marked in cooldown) to t2 (200 OK)
                with patch("src.services.load_balancer.config") as mock_lb_cfg:
                    mock_lb_cfg.call_logic_mode = "polling"
                    mock_lb_cfg.captcha_method = "http"
                    res1 = await svc.get_asset("asset-aaa")
                self.assertEqual(res1["tokenId"], t2_id)
                self.assertTrue(lb.is_token_in_cooldown(t1_id))

                # 2. Asset ownership remembered: next lookup routes straight to owner t2 on first try
                media_calls.clear()
                res2 = await svc.get_asset("asset-aaa")
                self.assertEqual(res2["tokenId"], t2_id)
                self.assertEqual(media_calls, [t2_id])

                # 3. Email lookup uses get_token_by_email directly without scanning get_all_tokens
                with patch.object(db, "get_all_tokens", side_effect=AssertionError("Should not scan all tokens")):
                    resolved_tok, resolved_pid = await svc.resolve_account_context(email="OWNER@example.com")
                    self.assertEqual(resolved_tok.id, t2_id)
                    self.assertEqual(resolved_pid, "p2")

                # 4. Concurrent read pool & maintenance pragmas
                read_results = await asyncio.gather(*(db.get_token(t2_id) for _ in range(8)))
                self.assertTrue(all(r.id == t2_id for r in read_results))
                maint = await db.run_maintenance_pragmas()
                self.assertTrue(maint["checkpointed"])
            finally:
                await db.close()

    async def test_file_cache_http_session_pool_reuse_and_admin_security_guards(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache = FileCache(cache_dir=tmp_dir, default_timeout=60)
            created_sessions = []

            def fake_session_factory(**kwargs):
                s = MagicMock()
                s.__aenter__ = AsyncMock(return_value=s)
                s.__aexit__ = AsyncMock(return_value=None)
                s.kwargs = kwargs
                created_sessions.append(s)
                return s

            with patch("src.services.file_cache.AsyncSession", side_effect=fake_session_factory):
                k1, s1 = await cache.get_http_session(impersonate="chrome120", proxy_url="http://127.0.0.1:7890")
                k2, s2 = await cache.get_http_session(impersonate="chrome120", proxy_url="http://127.0.0.1:7890")
                self.assertIs(s1, s2)
                self.assertEqual(len(created_sessions), 1)
                await cache.evict_http_session(k1, s1)
                s1.__aexit__.assert_awaited_once()
                await cache.close_http_sessions()

        # Admin login rate limit + cross-origin cookie CSRF guard
        app = FastAPI()
        app.include_router(admin_api.router)
        admin_api._login_failure_tracker.clear()
        fake_db = types.SimpleNamespace(
            get_admin_config=AsyncMock(
                return_value=types.SimpleNamespace(username="admin", password="correct-password")
            )
        )
        with (
            patch.object(admin_api, "db", fake_db),
            patch.object(config, "_admin_password", None),
        ):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://localhost:8001") as client:
                for _ in range(admin_api._LOGIN_MAX_FAILURES):
                    r = await client.post(
                        "/api/admin/login",
                        json={"username": "admin", "password": "wrong"},
                        headers={"X-Forwarded-For": "203.0.113.9"},
                    )
                    self.assertEqual(r.status_code, 401)

                blocked = await client.post(
                    "/api/admin/login",
                    json={"username": "admin", "password": "correct-password"},
                    headers={"X-Forwarded-For": "203.0.113.9"},
                )
                self.assertEqual(blocked.status_code, 429)
                self.assertIn("Retry-After", blocked.headers)

                # Different IP succeeds and gets session cookie
                ok = await client.post(
                    "/api/admin/login",
                    json={"username": "admin", "password": "correct-password"},
                    headers={"X-Forwarded-For": "198.51.100.2"},
                )
                self.assertEqual(ok.status_code, 200)
                sess_tok = ok.json()["token"]

                # Cross-origin POST relying solely on ambient cookie is rejected with 403
                csrf_resp = await client.post(
                    "/api/admin/logout",
                    cookies={admin_api.ADMIN_SESSION_COOKIE_NAME: sess_tok},
                    headers={"Origin": "https://evil.example"},
                )
                self.assertEqual(csrf_resp.status_code, 403)

                # Same-origin POST with cookie succeeds
                clean_logout = await client.post(
                    "/api/admin/logout",
                    cookies={admin_api.ADMIN_SESSION_COOKIE_NAME: sess_tok},
                    headers={"Origin": "http://localhost:8001"},
                )
                self.assertEqual(clean_logout.status_code, 200)
        admin_api._login_failure_tracker.clear()

    async def test_background_cookie_refresher_staggered_camoufox_and_anti_instaban(self):
        from datetime import datetime, timedelta, timezone
        import inspect
        import src.main as main_mod
        from src.services.browser_captcha import BrowserCaptchaQueueTimeout
        from src.services.token_manager import TokenManager

        # 1. Ensure lifespan starts and stops protocol refresher
        lifespan_src = inspect.getsource(main_mod.lifespan)
        self.assertIn("token_manager.start_protocol_refresher()", lifespan_src)
        self.assertIn("token_manager.stop_protocol_refresher()", lifespan_src)

        # 2. Verify BrowserCaptchaService NEVER launches a 2nd slot on an account's busy profile
        svc = BrowserCaptchaService(db=None)
        svc._browser_count = 2
        svc._browsers = {
            0: types.SimpleNamespace(
                has_shared_browser=lambda: True,
                is_busy=lambda: True,  # Slot 0 is currently busy with token 3
                _shared_bound_token_id=3,
            ),
            1: types.SimpleNamespace(
                has_shared_browser=lambda: True,
                is_busy=lambda: False,  # Slot 1 is idle (bound to token 4)
                _shared_bound_token_id=4,
            ),
        }
        with self.assertRaises(BrowserCaptchaQueueTimeout):
            await svc._select_browser_id("proj-3", token_id=3, timeout_seconds=0.05)

        # 3. Verify Camoufox refresher never calls curl_cffi protocol_loginer and staggers cold tokens (max 1/tick)
        raw_cookies = json.dumps(
            [
                {"name": "OSID", "value": "osid-1", "domain": "flow.google.com", "hostOnly": True, "path": "/", "secure": True, "httpOnly": True},
                {"name": "SID", "value": "sid-1", "domain": ".google.com", "path": "/", "secure": True},
            ]
        )
        rotated_browser = raw_cookies.replace("osid-1", "osid-browser")

        with tempfile.TemporaryDirectory() as tmp_dir:
            db = Database(db_path=str(Path(tmp_dir) / "refresh.db"))
            await db.init_db()
            try:
                now = datetime.now(timezone.utc)
                warm_tid = await db.add_token(
                    Token(
                        st="frontend-warm",
                        at="flow-frontend-cookie",
                        at_expires=now - timedelta(minutes=10),
                        email="warm@example.com",
                        login_account="warm@example.com",
                        google_cookies=raw_cookies,
                        refresh_interval_minutes=90,
                        last_st_refresh_at=now - timedelta(minutes=95),
                        last_st_refresh_result="success",
                    )
                )
                cold1_tid = await db.add_token(
                    Token(
                        st="frontend-cold1",
                        at="flow-frontend-cookie",
                        at_expires=now - timedelta(minutes=10),
                        email="cold1@example.com",
                        login_account="cold1@example.com",
                        google_cookies=raw_cookies,
                        refresh_interval_minutes=90,
                        last_st_refresh_at=now - timedelta(minutes=5),
                        last_st_refresh_result="success",
                    )
                )
                cold2_tid = await db.add_token(
                    Token(
                        st="frontend-cold2",
                        at="flow-frontend-cookie",
                        at_expires=now - timedelta(minutes=10),
                        email="cold2@example.com",
                        login_account="cold2@example.com",
                        google_cookies=raw_cookies,
                        refresh_interval_minutes=90,
                        last_st_refresh_at=now - timedelta(minutes=95),
                        last_st_refresh_result="success",
                    )
                )

                flow = MagicMock()
                flow.FRONTEND_ACCESS_TOKEN = "flow-frontend-cookie"
                flow.proxy_manager = None
                flow.get_request_fingerprint.return_value = None
                flow.st_to_at = AsyncMock(
                    return_value={
                        "access_token": "flow-frontend-cookie",
                        "expires": (now + timedelta(minutes=55)).isoformat(),
                        "credits": 88,
                        "userPaygateTier": "PAYGATE_TIER_ONE",
                    }
                )
                manager = TokenManager(db, flow)

                async def fake_camoufox_refresh(_pid, *, token_id):
                    tok = await db.get_token(token_id)
                    return {
                        "google_cookies": raw_cookies.replace("osid-1", f"osid-browser-{token_id}"),
                        "email": tok.email,
                    }
                fake_camoufox = MagicMock()
                fake_camoufox.get_warm_bound_token_ids = lambda idle_only=False: {warm_tid}
                fake_camoufox.has_any_idle_slot = lambda: True
                fake_camoufox.refresh_google_cookies = AsyncMock(side_effect=fake_camoufox_refresh)

                with (
                    patch("src.services.token_manager.config") as mock_cfg,
                    patch.object(BrowserCaptchaService, "_instance", fake_camoufox),
                    patch.object(BrowserCaptchaService, "get_instance", AsyncMock(return_value=fake_camoufox)),
                    patch(
                        "src.services.protocol_login.protocol_loginer.login",
                        AsyncMock(side_effect=AssertionError("Must never mix curl_cffi ProtocolLogin with Camoufox")),
                    ),
                ):
                    mock_cfg.captcha_method = "camoufox"
                    mock_cfg.browser_captcha_engine = "camoufox"
                    await manager.run_protocol_refresh_once()

                # Warm-idle token + at most 1 cold token (stalest last_st_refresh_at first = cold2) refreshed in tick 1; cold1 deferred
                refreshed_tids = [c.kwargs["token_id"] for c in fake_camoufox.refresh_google_cookies.call_args_list]
                self.assertEqual(refreshed_tids, [warm_tid, cold2_tid])

                warm_after = await db.get_token(warm_tid)
                cold1_after = await db.get_token(cold1_tid)
                cold2_after = await db.get_token(cold2_tid)
                self.assertEqual(warm_after.google_cookies, raw_cookies.replace("osid-1", f"osid-browser-{warm_tid}"))
                self.assertEqual(cold2_after.google_cookies, raw_cookies.replace("osid-1", f"osid-browser-{cold2_tid}"))
                self.assertEqual(cold1_after.google_cookies, raw_cookies)
                self.assertGreater(
                    (warm_after.at_expires.replace(tzinfo=timezone.utc) if warm_after.at_expires.tzinfo is None else warm_after.at_expires) - now,
                    timedelta(minutes=100),
                )

                # 4. Verify _should_count_token_error never counts 401/auth jitter or client validation errors toward error_ban_threshold
                gh = GenerationHandler.__new__(GenerationHandler)
                self.assertFalse(gh._should_count_token_error(RuntimeError("首尾帧模型需要 1-2 张图片，当前提供了 0 张")))
                self.assertFalse(gh._should_count_token_error(RuntimeError("Flow 登录认证失败 (rpc=maseQ, code=401)")))
                self.assertFalse(gh._should_count_token_error(RuntimeError("Flow 账号登录态已失效")))
                self.assertFalse(gh._should_count_token_error(RuntimeError("PUBLIC_ERROR_MODEL_ACCESS_DENIED")))
            finally:
                await db.close()

    async def test_dual_tab_captcha_api_isolation_and_risk_reset_retry(self):
        from src.services.camoufox_bidi import CamoufoxWorker
        from src.services.flow_client import FlowClient

        with tempfile.TemporaryDirectory() as tmp_dir:
            worker = CamoufoxWorker(1, tmp_dir, db=None)
            worker._process = types.SimpleNamespace(returncode=None)
            worker._context_id = "api-tab-1"
            worker._cached_wiz = {"bl": "bl-1", "fSid": "fsid-1", "at": "at-1"}
            worker._shared_bound_token_id = 5
            created_counter = 0
            commands = []
            evaluated_contexts = []

            class FakeBidi:
                _closed = False

                async def command(self, method, params=None, timeout=None):
                    nonlocal created_counter
                    commands.append((method, params))
                    if method == "browsingContext.create":
                        created_counter += 1
                        return {"context": f"captcha-tab-{created_counter}"}
                    return {}

                async def evaluate(self, ctx, expr, await_promise=False, user_activation=False, timeout=None):
                    evaluated_contexts.append((ctx, expr[:40]))
                    if expr.strip() == "1":
                        return 1
                    if user_activation and "grecaptcha" in expr:
                        return {"ok": True, "token": "tok-fresh"}
                    if "grecaptcha" in expr:
                        return True
                    if "location.href" in expr:
                        return "https://flow.google.com/project/p1"
                    return "complete"

                async def call_function(self, ctx, fn_decl, arguments=None, await_promise=True, user_activation=True, timeout=None):
                    evaluated_contexts.append((ctx, "call_function"))
                    return {"ok": True, "status": 200, "text": ")]}'\n\n10\n[[\"wrb.fr\"]]", "bl": "bl-1", "fSid": "fsid-1", "at": "at-1"}

            worker._bidi = FakeBidi()
            worker._get_or_create_shared_browser = AsyncMock(return_value=(worker._process, worker._bidi, "api-tab-1"))
            worker._ensure_flow_page_ready = AsyncMock(return_value=True)
            worker._perform_human_warmup = AsyncMock()
            worker._sync_live_cookies_to_db = AsyncMock()

            # 1. Captcha solve uses separate captcha-tab-1, leaving api-tab-1 untouched
            tok1, _ = await worker.get_token("p1", action="UPLOAD_IMAGE", token_id=5)
            self.assertEqual(tok1, "tok-fresh")
            self.assertEqual(worker._captcha_context_id, "captcha-tab-1")
            self.assertEqual(worker._context_id, "api-tab-1")

            # 2. Frontend RPC executes inside api-tab-1 (NOT captcha-tab-1)
            rpc_res = await worker.execute_flow_frontend_rpc(rpc_id="maseQ", argument=[], project_id="p1", token_id=5)
            self.assertEqual(rpc_res["status"], 200)
            self.assertEqual(evaluated_contexts[-1][0], "api-tab-1")

            # 3. Risk rejection marks captcha tab dirty, clears _GRECAPTCHA cookies & recreates captcha-tab-2, preserving api-tab-1 and _cached_wiz
            worker.note_recaptcha_rejected()
            self.assertTrue(worker._captcha_context_dirty)
            tok2, _ = await worker.get_token("p1", action="VIDEO_GENERATION", token_id=5)
            self.assertEqual(tok2, "tok-fresh")
            self.assertEqual(worker._captcha_context_id, "captcha-tab-2")
            self.assertEqual(worker._context_id, "api-tab-1")
            self.assertEqual(worker._cached_wiz["at"], "at-1")
            self.assertIn(("storage.deleteCookies", {"filter": {"name": "_GRECAPTCHA"}}), commands)
            self.assertIn(("browsingContext.close", {"context": "captcha-tab-1"}), commands)

            # 4. BrowserCaptchaService flags HTTP 200 batchexecute PUBLIC_ERROR_UNUSUAL_ACTIVITY as recaptcha rejection
            svc = BrowserCaptchaService(db=None)
            svc._browser_count = 1
            svc._browsers = {0: worker}
            self.assertTrue(svc._is_recaptcha_rejection('Flow frontend RPC rejected: rpc=MZZa6b, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY'))
            svc._select_browser_id = AsyncMock(return_value=0)
            svc._get_or_create_browser = AsyncMock(return_value=worker)
            worker.execute_flow_frontend_rpc = AsyncMock(
                return_value={"status": 200, "text": '[["er","MZZa6b",null,null,null,null,null,["PUBLIC_ERROR_UNUSUAL_ACTIVITY"]]]'}
            )
            svc.report_error = AsyncMock()
            await svc.execute_flow_frontend_rpc(rpc_id="MZZa6b", argument=[], project_id="p1", token_id=5)
            svc.report_error.assert_awaited_once()

            # 5. jwpduf is HTTP-first control plane RPC, and _current_generate_video retries once after PUBLIC_ERROR_UNUSUAL_ACTIVITY
            client = FlowClient(None)
            self.assertIn("jwpduf", client.HTTP_FIRST_CONTROL_PLANE_RPCS)
            client._frontend_cookie = AsyncMock(return_value="SID=s")
            client._get_recaptcha_token = AsyncMock(side_effect=[("tok-bad", "0:lease-1/"), ("tok-good", "0:lease-2/")])
            client._notify_browser_captcha_error = AsyncMock()
            client._notify_browser_captcha_request_finished = AsyncMock()
            op_uuid = "11111111-1111-1111-1111-111111111111"
            proj_uuid = "22222222-2222-2222-2222-222222222222"
            media_uuid = "33333333-3333-3333-3333-333333333333"
            client._current_rpc = AsyncMock(
                side_effect=[
                    RuntimeError("Flow frontend RPC rejected: rpc=MZZa6b, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY"),
                    [[[op_uuid, proj_uuid, media_uuid]]],
                ]
            )
            with patch("src.services.flow_current.asyncio.sleep", AsyncMock()):
                out = await client._current_generate_video(
                    project_id=proj_uuid,
                    prompt="hello",
                    model_key="macaron_cookie_r2v_4s",
                    aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
                    mode="references",
                    reference_media_ids=["img-1"],
                    token_id=5,
                    token_video_concurrency=None,
                    google_cookies="SID=s",
                )
            self.assertEqual(client._current_rpc.await_count, 2)
            client._notify_browser_captcha_error.assert_awaited_once()
            self.assertEqual(out["operations"][0]["name"], op_uuid)

if __name__ == "__main__":
    unittest.main()
