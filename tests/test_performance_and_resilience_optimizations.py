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


if __name__ == "__main__":
    unittest.main()
