"""Regression tests for P0-P2 audit bug fixes and resilience improvements."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel

from src.api import admin as admin_api
from src.api import flow_routes
from src.core.auth import AuthManager
from src.core.database import Database
from src.core.model_resolver import resolve_model_name
from src.core.model_catalog import MODEL_CONFIG
from src.core.models import APIKeyRecord, Task, Token
from src.main import safe_request_validation_exception_handler
from src.services.browser_captcha import BrowserCaptchaService, _BrowserRequest
from src.services.concurrency_manager import ConcurrencyManager
from src.services.load_balancer import LoadBalancer
from src.services.token_manager import TokenManager


class _JsonOnlyBody(BaseModel):
    name: str


class AuditRemediationRegressionTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        AuthManager.clear_api_keys()

    async def test_binary_multipart_to_json_endpoint_returns_422_not_500(self):
        test_app = FastAPI()
        test_app.add_exception_handler(
            RequestValidationError, safe_request_validation_exception_handler
        )

        @test_app.post("/json-only")
        async def json_only_endpoint(payload: _JsonOnlyBody):
            return {"name": payload.name}

        raw_jpeg_multipart = (
            b"------boundary\r\n"
            b'Content-Disposition: form-data; name="file"; filename="a.jpg"\r\n'
            b"Content-Type: image/jpeg\r\n\r\n"
            b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\xff\xd9\r\n"
            b"------boundary--\r\n"
        )
        transport = ASGITransport(app=test_app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/json-only",
                content=raw_jpeg_multipart,
                headers={"Content-Type": "multipart/form-data; boundary=----boundary"},
            )
        self.assertEqual(resp.status_code, 422)
        body = resp.json()
        self.assertIn("detail", body)

    def test_managed_api_key_allows_4s_6s_veo_aliases_and_imagen3_resolver(self):
        AuthManager.set_api_keys(
            [
                APIKeyRecord(
                    id=1,
                    name="lite-video-only",
                    key_value="sk-lite-only",
                    is_active=True,
                    allow_all_models=False,
                    allowed_models=["veo-3.1-lite"],
                )
            ]
        )
        self.assertTrue(
            flow_routes._flow_video_model_key_allowed(
                "sk-lite-only", "veo_3_1_t2v_lite_4s"
            )
        )
        self.assertTrue(
            flow_routes._flow_video_model_key_allowed(
                "sk-lite-only", "veo_3_1_t2v_lite_6s"
            )
        )
        self.assertFalse(
            flow_routes._flow_video_model_key_allowed(
                "sk-lite-only", "veo_3_1_i2v_s_lite_6s"
            )
        )
        self.assertFalse(
            flow_routes._flow_video_model_key_allowed(
                "sk-lite-only", "veo_3_1_t2v_portrait"
            )
        )

        filtered = flow_routes._filter_flow_models_payload(
            {
                "nativeVideoModels": [
                    "veo_3_1_t2v_lite_4s",
                    "veo_3_1_t2v_lite_6s",
                    "veo_3_1_t2v_portrait",
                ]
            },
            "sk-lite-only",
        )
        self.assertEqual(
            filtered["nativeVideoModels"],
            ["veo_3_1_t2v_lite_4s", "veo_3_1_t2v_lite_6s"],
        )

        # Imagen-3 compatibility aliases for :predict
        resolved_pro = resolve_model_name(
            model="imagen-3.0-generate-002",
            request=None,
            model_config=MODEL_CONFIG,
            images=None,
        )
        resolved_fast = resolve_model_name(
            model="imagen-3.0-fast-generate-001",
            request=None,
            model_config=MODEL_CONFIG,
            images=None,
        )
        self.assertIn(resolved_pro, MODEL_CONFIG)
        self.assertIn(resolved_fast, MODEL_CONFIG)

    async def test_use_count_increment_success_stats_429_unban_and_db_task_recovery(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db = Database(db_path=str(Path(tmp_dir) / "audit.db"))
            await db.init_db()
            try:
                now = datetime.now(timezone.utc)
                tid = await db.add_token(
                    Token(
                        st="st-1",
                        at="at-1",
                        at_expires=now - timedelta(hours=12),
                        email="u1@example.com",
                        is_active=False,
                        ban_reason="429_rate_limit",
                        banned_at=now - timedelta(hours=13),
                    )
                )
                flow = MagicMock()
                manager = TokenManager(db, flow)

                # 1. 429 auto-unban restores active state even when short-lived AT is expired
                await manager.ban_token_for_429(tid)
                await db.update_token(tid, banned_at=now - timedelta(hours=13))
                await manager.auto_unban_429_tokens()
                unbanned = await db.get_token(tid)
                self.assertTrue(unbanned.is_active)
                self.assertIsNone(unbanned.ban_reason)

                # 2. record_usage increments use_count atomically instead of resetting to 1
                await manager.record_usage(tid, is_video=False, count_media=True)
                await manager.record_usage(tid, is_video=True, count_media=True)
                await manager.record_usage(tid, is_video=False, count_media=False)
                await manager.record_success(tid)
                await manager.record_success(tid)

                tok_after = await db.get_token(tid)
                stats_after = await db.get_token_stats(tid)
                self.assertEqual(tok_after.use_count, 3)
                self.assertIsNotNone(tok_after.last_used_at)
                self.assertEqual(stats_after.image_count, 1)
                self.assertEqual(stats_after.video_count, 1)
                self.assertEqual(stats_after.success_count, 2)
                self.assertIsNotNone(stats_after.last_success_at)

                # 3. Column allowlists block invalid/injected column names
                with self.assertRaises(ValueError):
                    await db.update_token(tid, **{"is_active = 1; DROP TABLE tokens; --": 1})
                with self.assertRaises(ValueError):
                    await db.update_task("t-1", **{"status = 'completed' --": "x"})

                # 4. Stale processing tasks recover on startup & legacy float completed_at loads cleanly
                await db.create_task(
                    Task(
                        task_id="op-stale-1",
                        token_id=tid,
                        model="veo-3.1-lite",
                        prompt="test",
                        status="processing",
                    )
                )
                recovered = await db.recover_stale_processing_tasks()
                self.assertEqual(recovered, 1)
                stale_task = await db.get_task("op-stale-1")
                self.assertEqual(stale_task.status, "failed")
                self.assertIsNotNone(stale_task.completed_at)
            finally:
                await db.close()

    async def test_same_token_held_browser_waits_for_busy_slot_instead_of_502(self):
        svc = BrowserCaptchaService.__new__(BrowserCaptchaService)
        svc.db = None
        svc.base_user_data_dir = tempfile.gettempdir()
        svc._browser_count = 1
        svc._browsers = {}
        svc._browsers_lock = asyncio.Lock()
        svc._slot_condition = asyncio.Condition()
        svc._slot_reservations = {}
        svc._slot_recovery = {}
        svc._requests = {}
        svc._cleanup_tasks = set()
        svc._round_robin_index = 0
        svc._admitted_requests = 0
        svc._queue_wait_total_seconds = 0.0
        svc._queue_timeouts = 0
        svc._closed = False

        fake_worker = types.SimpleNamespace(
            _runtime_generation=1,
            _shared_bound_token_id=13,
            is_busy=lambda: False,
            has_shared_browser=lambda: True,
        )
        svc._browsers[0] = fake_worker

        owner_task = asyncio.current_task()
        held_req = _BrowserRequest(
            ref="0:lease-test/",
            slot_id=0,
            browser=fake_worker,
            token_id=13,
            generation=1,
            owner=owner_task,
            borrower=None,
            hold=True,
            in_browser_op=False,
        )
        svc._requests[held_req.ref] = held_req

        # Simulate another concurrent request of token 13 holding slot 0 briefly
        svc._slot_reservations[0] = 1

        async def _release_other_op():
            await asyncio.sleep(0.05)
            await svc._release_slot_reservation(0)

        releaser = asyncio.create_task(_release_other_op())
        try:
            async with svc._borrow_browser(
                token_id=13, browser_ref=held_req.ref, wait_timeout=2.0
            ) as borrowed:
                self.assertIs(borrowed, held_req)
                self.assertTrue(borrowed.in_browser_op)

                # Truly re-entrant submission on the SAME lease MUST still fail fast
                with self.assertRaisesRegex(RuntimeError, "already submitting"):
                    async with svc._borrow_browser(
                        token_id=13, browser_ref=held_req.ref, wait_timeout=0.5
                    ):
                        pass
        finally:
            await releaser

    async def test_load_balancer_speculative_pending_prevents_concurrent_stampede(self):
        t1 = Token(id=1, st="s1", at="a1", email="u1@example.com", is_active=True, image_enabled=True, image_concurrency=1, credits=100)
        t2 = Token(id=2, st="s2", at="a2", email="u2@example.com", is_active=True, image_enabled=True, image_concurrency=1, credits=100)

        tm = MagicMock()
        tm.get_active_tokens = AsyncMock(return_value=[t1, t2])

        async def slow_validate(tok):
            await asyncio.sleep(0.05)
            return tok

        tm.ensure_valid_token = AsyncMock(side_effect=slow_validate)
        cm = ConcurrencyManager()
        await cm.initialize([t1, t2])
        lb = LoadBalancer(tm, cm)

        # Under polling mode with concurrency=1 per token, two concurrent select_token calls
        # must spread across t1 and t2 instead of both picking t1 during slow ensure_valid_token!
        from unittest.mock import patch

        with patch("src.services.load_balancer.config") as mock_cfg:
            mock_cfg.captcha_method = "none"
            mock_cfg.call_logic_mode = "polling"
            picked = await asyncio.gather(
                lb.select_token(for_image_generation=True, track_pending=True),
                lb.select_token(for_image_generation=True, track_pending=True),
            )
        picked_ids = {tok.id for tok in picked if tok is not None}
        self.assertEqual(picked_ids, {1, 2})


if __name__ == "__main__":
    unittest.main()
