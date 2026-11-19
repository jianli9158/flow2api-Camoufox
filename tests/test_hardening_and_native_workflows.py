"""Tests for OS profile leases, error taxonomy, redaction, prompt tools, video chaining, and MCP."""

import asyncio
import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api import flow_routes, mcp_routes
from src.core.auth import verify_api_key_flexible
from src.core.flow_errors import FlowErrorCategory, classify_flow_error
from src.core.models import Token
from src.core.prompt_tools import (
    jitter_seconds,
    resolve_prompt_mentions,
    strip_sd_banned_keywords,
    validate_base64_media_payload,
)
from src.core.redaction import redact_sensitive_text, redact_structure
from src.services.camoufox_profile import (
    PROFILE_KERNEL_LOCK_FILENAME,
    ProfileLeaseBusyError,
    acquire_profile_lease,
)
from src.services.file_cache import FileCache
from src.services.flow_features import FlowFeatureService
from src.services.flow_frontend import FlowFrontendMixin
from src.services.generation_handler import GenerationHandler


class _DummyFrontend(FlowFrontendMixin):
    pass


class HardeningAndWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def test_profile_lease_blocks_in_process_and_cross_process_and_keeps_inode(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            acc_dir = Path(tmp_dir) / "token_42"
            lease = acquire_profile_lease(acc_dir, holder="parent_worker")
            try:
                lock_file = acc_dir / PROFILE_KERNEL_LOCK_FILENAME
                self.assertTrue(lock_file.is_file())
                inode_before = lock_file.stat().st_ino
                with self.assertRaises(ProfileLeaseBusyError):
                    acquire_profile_lease(acc_dir, holder="duplicate_worker")
            finally:
                lease.release()

            self.assertTrue(lock_file.is_file())
            self.assertEqual(lock_file.stat().st_ino, inode_before)

            # Cross-process flock test: child process acquires lease and holds it until stdin closes
            child_script = (
                "import sys; "
                "from src.services.camoufox_profile import acquire_profile_lease; "
                "l = acquire_profile_lease(sys.argv[1], holder='child'); "
                "print('LOCKED', flush=True); "
                "sys.stdin.readline(); "
                "l.release()"
            )
            proc = subprocess.Popen(
                [sys.executable, "-c", child_script, str(acc_dir)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                line = proc.stdout.readline().strip()
                self.assertEqual(line, "LOCKED")
                with self.assertRaises(ProfileLeaseBusyError):
                    acquire_profile_lease(acc_dir, holder="parent_while_child_holds")
            finally:
                if proc.stdin:
                    proc.stdin.write("\n")
                    proc.stdin.flush()
                proc.wait(timeout=10)

            reacquired = acquire_profile_lease(acc_dir, holder="parent_after_child")
            reacquired.release()

    def test_error_classification_and_redaction_and_prompt_tools(self):
        safety = classify_flow_error("Flow RPC rejected: PUBLIC_ERROR_PROMINENT_PEOPLE_FILTER_FAILED")
        self.assertEqual(safety.category, FlowErrorCategory.CONTENT_POLICY)
        self.assertEqual(safety.http_status, 400)
        self.assertFalse(safety.retryable_same_token)
        self.assertFalse(safety.failover_other_token)
        self.assertFalse(safety.counts_toward_ban)

        upsample_4k = classify_flow_error(
            "HTTP Error 403: forbidden",
            target_resolution="UPSAMPLE_IMAGE_RESOLUTION_4K",
            operation="upsample",
        )
        self.assertEqual(upsample_4k.category, FlowErrorCategory.TIER_PERMISSION)
        self.assertEqual(upsample_4k.http_status, 403)
        self.assertFalse(upsample_4k.retryable_same_token)
        self.assertFalse(upsample_4k.counts_toward_ban)

        quota = classify_flow_error("Daily limit for Veo 3.1 Fast reached (429)")
        self.assertEqual(quota.category, FlowErrorCategory.RATE_LIMIT)
        self.assertEqual(quota.http_status, 429)
        self.assertEqual(quota.quota_model, "Veo 3.1 Fast")
        self.assertFalse(quota.counts_toward_ban)
        self.assertTrue(quota.failover_other_token)

        raw_err = (
            "Auth failed Bearer secret-jwt-token-value; "
            "Cookie: __Secure-1PSIDTS=abc123xyz; SID=sidval; __Secure-OSID=osidval; "
            "url=https://storage.googleapis.com/x.mp4?Signature=sigsecret&Expires=999"
        )
        scrubbed = redact_sensitive_text(raw_err)
        self.assertNotIn("secret-jwt-token-value", scrubbed)
        self.assertNotIn("abc123xyz", scrubbed)
        self.assertNotIn("sidval", scrubbed)
        self.assertNotIn("osidval", scrubbed)
        self.assertNotIn("sigsecret", scrubbed)

        struct = redact_structure(
            {"token": "top-secret", "nested": {"fifeUrl": "https://a/b?Signature=xyz"}},
            redact_urls=True,
        )
        self.assertEqual(struct["token"], "<redacted>")
        self.assertEqual(struct["nested"]["fifeUrl"], "<redacted:url>")

        cleaned, removed = strip_sd_banned_keywords(
            "A serene lake at sunrise, masterpiece, 8k, ultra hd, cinematic lighting"
        )
        self.assertIn("serene lake at sunrise", cleaned)
        self.assertNotIn("masterpiece", cleaned.lower())
        self.assertNotIn("8k", cleaned.lower())
        self.assertTrue(len(removed) >= 3)

        # @mention resolution ignores emails and guards characters without imageRefs
        chars = [
            {"id": "char-hero", "displayName": "Hero", "imageRefs": ["ref-1"]},
            {"id": "char-empty", "displayName": "Ghost", "imageRefs": []},
        ]
        wfs = [{"displayName": "SwordProp", "primaryMediaId": "media-sword"}]
        res = resolve_prompt_mentions(
            'Email admin@example.com while @Hero swings @"SwordProp"',
            characters=chars,
            workflows=wfs,
        )
        self.assertEqual(res.character_ids, ["char-hero"])
        self.assertEqual(res.image_media_ids, ["media-sword"])
        with self.assertRaises(ValueError):
            resolve_prompt_mentions("@Ghost appears", characters=chars, workflows=wfs)

        # Jitter sanity
        samples = [jitter_seconds(1.0) for _ in range(10)]
        self.assertTrue(all(0.75 <= s <= 1.25 for s in samples))

    def test_base64_media_guard_and_file_cache_rejection(self):
        valid_png = base64.b64encode(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32).decode("ascii")
        decoded = validate_base64_media_payload(valid_png, expected_kind="image")
        self.assertTrue(decoded.startswith(b"\x89PNG"))

        with self.assertRaises(ValueError):
            validate_base64_media_payload(
                base64.b64encode(b"<html>not an image</html>").decode("ascii"),
                expected_kind="image",
            )
        with self.assertRaises(ValueError):
            validate_base64_media_payload("AAAA", expected_kind="image", max_b64_bytes=None) if False else validate_base64_media_payload("A" * 100, expected_kind="image", max_b64_chars=10)

        with tempfile.TemporaryDirectory() as tmp_dir:
            fc = FileCache(cache_dir=tmp_dir, default_timeout=0)
            target = Path(tmp_dir) / "out.jpg"
            written = fc._decode_and_write_base64(target, valid_png)
            self.assertGreater(written, 8)
            with self.assertRaises(ValueError):
                fc._decode_and_write_base64(
                    target, base64.b64encode(b"bad-payload").decode("ascii")
                )

    async def test_upsample_4k_tier_403_skips_retries_and_model_provenance(self):
        norm_img = _DummyFrontend._normalize_frontend_image_generation_response(
            [[["media-img-1", ["https://flow.google.com/image/img1.jpg"], "BELUGA"]]]
        )
        self.assertEqual(norm_img["observedModel"], "BELUGA")
        self.assertEqual(norm_img["media"][0]["observedModel"], "BELUGA")

        flow_client = MagicMock()
        flow_client.generate_image = AsyncMock(
            return_value=(norm_img, "sess-1", {"generation_attempts": [{"attempt": 1}]})
        )
        flow_client.upsample_image = AsyncMock(
            side_effect=RuntimeError("Image upsample permission denied (4K): HTTP 403")
        )
        flow_client._get_retry_reason = MagicMock(return_value="403错误")

        handler = GenerationHandler(
            flow_client=flow_client,
            token_manager=MagicMock(),
            load_balancer=MagicMock(),
            db=MagicMock(),
            concurrency_manager=MagicMock(),
            proxy_manager=MagicMock(),
        )
        handler._update_request_log_progress = AsyncMock()
        handler.file_cache.download_and_cache = AsyncMock(return_value="cached.jpg")

        token = Token(
            id=7,
            st="st",
            at="at",
            email="u@test.com",
            user_paygate_tier="PAYGATE_TIER_ONE",
            current_project_id="proj-1",
        )
        gen_result = handler._create_generation_result()
        resp_state = handler._create_response_state()

        chunks = []
        async for chunk in handler._handle_image_generation(
            token=token,
            project_id="proj-1",
            model_config={
                "type": "image",
                "model_name": "GEM_PIX_2",
                "aspect_ratio": "IMAGE_ASPECT_RATIO_LANDSCAPE",
                "upsample": "UPSAMPLE_IMAGE_RESOLUTION_4K",
            },
            prompt="test",
            images=None,
            stream=False,
            generation_result=gen_result,
            response_state=resp_state,
        ):
            chunks.append(chunk)

        self.assertTrue(gen_result["success"])
        flow_client.upsample_image.assert_awaited_once()
        self.assertEqual(resp_state["generated_assets"]["requested_model"], "GEM_PIX_2")
        self.assertEqual(resp_state["generated_assets"]["observed_model"], "BELUGA")
        self.assertTrue(resp_state["generated_assets"]["upscaled_image"]["failed"])

    async def test_character_saga_video_chain_models_and_mcp_routes(self):
        flow_client = MagicMock()
        flow_client.FRONTEND_ACCESS_TOKEN = "frontend"
        flow_client.get_project = AsyncMock(
            return_value={
                "projectId": "proj-1",
                "characters": [
                    {"id": "char-1", "displayName": "Aria", "imageRefs": ["ref-aria"]}
                ],
                "workflows": [],
            }
        )
        flow_client.create_character = AsyncMock(
            return_value={"id": "char-new", "displayName": "Nova", "imageRefs": []}
        )
        flow_client.generate_image = AsyncMock(
            side_effect=[
                ({"media": [{"name": "face-mid"}]}, "s1", {}),
                ({"media": [{"name": "body-mid"}]}, "s2", {}),
            ]
        )
        flow_client.update_character = AsyncMock(
            return_value={
                "id": "char-new",
                "displayName": "Nova",
                "imageRefs": ["face-mid", "body-mid"],
            }
        )
        flow_client._current_generate_video = AsyncMock(
            side_effect=[
                {"operations": [{"operation": {"name": "op-1"}, "name": "op-1"}]},
                {"operations": [{"operation": {"name": "op-2"}, "name": "op-2"}]},
            ]
        )
        flow_client.check_video_status = AsyncMock(
            side_effect=[
                {
                    "operations": [
                        {
                            "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
                            "operation": {
                                "name": "op-1",
                                "metadata": {
                                    "video": {
                                        "fifeUrl": "https://flow.example/v1.mp4",
                                        "mediaGenerationId": "vid-1",
                                    }
                                },
                            },
                        }
                    ]
                },
                {
                    "operations": [
                        {
                            "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
                            "operation": {
                                "name": "op-2",
                                "metadata": {
                                    "video": {
                                        "fifeUrl": "https://flow.example/v2.mp4",
                                        "mediaGenerationId": "vid-2",
                                    }
                                },
                            },
                        }
                    ]
                },
            ]
        )
        flow_client.upload_image = AsyncMock(return_value="tail-frame-mid")

        token = Token(
            id=5,
            st="st",
            at="at",
            email="creator@test.com",
            google_cookies="SID=s; OSID=o",
            current_project_id="proj-1",
            user_paygate_tier="PAYGATE_TIER_ONE",
            credits=500,
        )
        token_manager = MagicMock()
        token_manager.get_all_tokens = AsyncMock(return_value=[token])
        token_manager.get_token = AsyncMock(return_value=token)
        token_manager.ensure_project_exists = AsyncMock(return_value="proj-1")
        token_manager.ensure_valid_token = AsyncMock(return_value=token)
        token_manager.is_at_valid = AsyncMock(return_value=True)
        db_mock = MagicMock()
        db_mock.get_token = AsyncMock(return_value=token)
        load_balancer = MagicMock()
        load_balancer.select_token = AsyncMock(return_value=token)

        with tempfile.TemporaryDirectory() as cache_dir:
            fc = FileCache(cache_dir=cache_dir, default_timeout=0)
            svc = FlowFeatureService(
                flow_client=flow_client,
                token_manager=token_manager,
                load_balancer=load_balancer,
                db=db_mock,
                file_cache=fc,
            )
            svc.log_api_request = AsyncMock(return_value=1)
            svc._download_binary = AsyncMock(return_value=b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 64)

            app = FastAPI()
            app.include_router(flow_routes.router)
            app.include_router(mcp_routes.router)
            app.dependency_overrides[verify_api_key_flexible] = lambda: "test-api-key"
            flow_routes.set_flow_feature_service(svc)
            mcp_routes.set_mcp_dependencies(svc, None)
            mcp_routes.spend_limiter.reset()

            client = TestClient(app)

            # 1. Character Saga
            char_resp = client.post(
                "/v1/flow/characters",
                json={
                    "display_name": "Nova",
                    "face_prompt": "close up portrait of Nova",
                    "body_prompt": "full body standing shot of Nova",
                },
            )
            self.assertEqual(char_resp.status_code, 200)
            char_json = char_resp.json()
            self.assertEqual(char_json["faceMediaId"], "face-mid")
            self.assertEqual(char_json["bodyMediaId"], "body-mid")
            self.assertEqual(char_json["generatedMediaIds"], ["face-mid", "body-mid"])
            # Body prompt received face-mid anchor
            second_call_kwargs = flow_client.generate_image.call_args_list[1].kwargs
            self.assertEqual(
                second_call_kwargs["image_inputs"],
                [{"imageUsageType": "IMAGE_USAGE_TYPE_ASSET", "mediaId": "face-mid"}],
            )

            # 2. /v1/flow/models catalog
            models_resp = client.get("/v1/flow/models")
            self.assertEqual(models_resp.status_code, 200)
            models_data = models_resp.json()
            self.assertEqual(models_data["accountTier"], "PAYGATE_TIER_ONE")
            self.assertEqual(models_data["credits"], 500)
            self.assertTrue(len(models_data["imageModels"]) > 0)
            self.assertTrue(len(models_data["videoModels"]) > 0)

            # 3. /v1/flow/videos/chain with mocked ffmpeg/ffprobe
            async def _fake_concat(_bin, _paths, out_path: Path):
                out_path.write_bytes(b"\x00\x00\x00\x18ftypmp42merged")
                return out_path

            with patch("src.services.flow_features.check_ffmpeg_available", return_value=("ffmpeg", "ffprobe")), \
                 patch("src.services.flow_features.extract_tail_frame", AsyncMock(return_value=b"\xff\xd8\xff\xe0frame")), \
                 patch("src.services.flow_features.concat_videos", side_effect=_fake_concat):
                chain_resp = client.post(
                    "/v1/flow/videos/chain",
                    json={
                        "segments": [
                            {"prompt": "@Aria enters the room, masterpiece, 8k"},
                            {"prompt": "Aria picks up the lantern"},
                        ],
                        "clean_prompt": True,
                        "poll_interval": 1.0,
                    },
                )
            self.assertEqual(chain_resp.status_code, 200, chain_resp.text)
            chain_data = chain_resp.json()
            self.assertEqual(chain_data["segmentCount"], 2)
            self.assertEqual(chain_data["clipMediaIds"], ["op-1", "op-2"])
            self.assertEqual(chain_data["segments"][1]["bridgedFromMediaId"], "tail-frame-mid")
            self.assertIn("/tmp/chain_", chain_data["finalVideoUrl"])
            # Segment 0 resolved @Aria and cleaned SD tags
            seg0_kwargs = flow_client._current_generate_video.call_args_list[0].kwargs
            self.assertEqual(seg0_kwargs["reference_entity_ids"], ["char-1"])
            self.assertNotIn("masterpiece", seg0_kwargs["prompt"].lower())

            # 4. MCP Streamable HTTP endpoint + TokenBucket spend limiter
            init_resp = client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            )
            self.assertEqual(init_resp.status_code, 200)
            self.assertEqual(init_resp.json()["result"]["serverInfo"]["name"], "flow2api-mcp")

            list_resp = client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            )
            self.assertEqual(list_resp.status_code, 200)
            tool_names = [t["name"] for t in list_resp.json()["result"]["tools"]]
            self.assertIn("flow_list_models", tool_names)
            self.assertIn("flow_generate_video", tool_names)

            call_resp = client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "flow_list_models", "arguments": {}},
                },
            )
            self.assertEqual(call_resp.status_code, 200)
            self.assertFalse(call_resp.json()["result"]["isError"])

            # Exhaust spend bucket (capacity=8) on flow_generate_video
            with patch.object(svc, "generate_reference_video", AsyncMock(return_value={"status": "ok"})):
                for i in range(8):
                    ok = client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 10 + i,
                            "method": "tools/call",
                            "params": {"name": "flow_generate_video", "arguments": {"prompt": "hi"}},
                        },
                    )
                    self.assertEqual(ok.status_code, 200)
                blocked = client.post(
                    "/mcp",
                    json={
                        "jsonrpc": "2.0",
                        "id": 99,
                        "method": "tools/call",
                        "params": {"name": "flow_generate_video", "arguments": {"prompt": "hi"}},
                    },
                )
                self.assertEqual(blocked.status_code, 429)
                self.assertEqual(blocked.json()["error"]["code"], -32029)

    async def test_p0_p1_caching_bucket_cooldown_tier_routing_billing_safety_and_capabilities(self):
        import threading
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from src.api import admin as admin_api, flow_routes, mcp_routes
        from src.core.browser_runtime_status import get_runtime_status
        from src.core.database import Database
        from src.core.flow_errors import (
            FlowErrorCategory,
            classify_flow_error,
            extract_retry_after_seconds,
            resolve_cooldown_bucket,
        )
        from src.core.models import Token
        from src.services.camoufox_profile import acquire_profile_lease
        from src.services.flow_client import FlowClient
        from src.services.load_balancer import LoadBalancer
        from src.services.proxy_manager import ProxyManager

        # 1. _normalize_runtime_method & browser_runtime_status accept "camoufox"
        self.assertEqual(admin_api._normalize_runtime_method("camoufox"), "browser")
        self.assertEqual(admin_api._normalize_runtime_method("unknown"), "browser")
        self.assertIn("state", get_runtime_status("camoufox"))

        # 2. Database write-through cache for active tokens & proxy config + ProxyManager single read
        with tempfile.TemporaryDirectory() as tmpdir:
            db = Database(str(Path(tmpdir) / "cache_test.db"))
            await db.init_db()
            try:
                tid = await db.add_token(
                    Token(st="st1", email="cache@test.com", is_active=True)
                )
                first = await db.get_active_tokens()
                self.assertEqual([t.id for t in first], [tid])
                # Second call hits cache even if underlying DB connection would fail
                with patch.object(db, "_connect", side_effect=AssertionError("should hit cache")):
                    second = await db.get_active_tokens()
                self.assertEqual([t.id for t in second], [tid])
                # Mutation invalidates immediately
                await db.update_token(tid, is_active=False)
                third = await db.get_active_tokens()
                self.assertEqual(third, [])

                pm = ProxyManager(db)
                await pm.update_proxy_config(True, "http://127.0.0.1:7890")
                self.assertEqual(await pm.get_media_proxy_url(), "http://127.0.0.1:7890")
                with patch.object(db, "_connect", side_effect=AssertionError("proxy should hit cache")):
                    self.assertEqual(await pm.get_media_proxy_url(), "http://127.0.0.1:7890")
            finally:
                await db.close()

        # 3. Per-model-family cooldown & tier-aware routing in LoadBalancer
        t_free = Token(
            id=1, st="s1", at="a1", email="free@test.com",
            user_paygate_tier="PAYGATE_TIER_NOT_PAID", is_active=True,
        )
        t_ultra = Token(
            id=2, st="s2", at="a2", email="ultra@test.com",
            user_paygate_tier="PAYGATE_TIER_TWO", is_active=True,
        )
        tm = MagicMock()
        tm.get_active_tokens = AsyncMock(return_value=[t_ultra, t_free])
        tm.ensure_valid_token = AsyncMock(side_effect=lambda t: t)
        tm.needs_at_refresh = MagicMock(return_value=False)
        lb = LoadBalancer(tm)
        # Cheap model prefers Free over Ultra when both idle
        picked_free = await lb.select_token(
            for_image_generation=True,
            model="gemini-3.1-flash-image-landscape",
            prefer_warm_browser=False,
        )
        self.assertEqual(picked_free.id, 1)
        # 4K model requires Ultra
        picked_4k = await lb.select_token(
            for_image_generation=True,
            model="gemini-3.1-flash-image-landscape-4k",
            prefer_warm_browser=False,
        )
        self.assertEqual(picked_4k.id, 2)
        # Cooling Token 1 only on image:GEM_PIX_2 does NOT cool BELUGA or video
        bucket = resolve_cooldown_bucket("gemini-3.0-pro-image-landscape", for_image=True)
        self.assertEqual(bucket, "image:GEM_PIX_2")
        lb.mark_token_cooldown(1, seconds=120.0, reason="daily quota", bucket=bucket)
        self.assertTrue(lb.is_token_in_cooldown(1, bucket="image:GEM_PIX_2"))
        self.assertFalse(lb.is_token_in_cooldown(1, bucket="image:BELUGA"))
        self.assertFalse(lb.is_token_in_cooldown(1, bucket="video"))
        picked_pro_img = await lb.select_token(
            for_image_generation=True,
            model="gemini-3.0-pro-image-landscape",
            prefer_warm_browser=False,
        )
        self.assertEqual(picked_pro_img.id, 2)
        picked_beluga = await lb.select_token(
            for_image_generation=True,
            model="gemini-3.1-flash-image-landscape",
            prefer_warm_browser=False,
        )
        self.assertEqual(picked_beluga.id, 1)

        # 4. Retry-After parsing, /about identity_recheck_required, and post-submit paid video guard
        self.assertEqual(extract_retry_after_seconds("429 Too Many Requests; Retry-After: 45s"), 45.0)
        recheck = classify_flow_error("identity_recheck_required: redirected to /about")
        self.assertEqual(recheck.category, FlowErrorCategory.AUTH_EXPIRED)
        self.assertEqual(recheck.next_action, "reauth")
        post_err = classify_flow_error(
            "HTTP Error 504: Gateway Timeout", post_submit=True, billed_operation=True
        )
        self.assertEqual(post_err.charged, "maybe")
        self.assertEqual(post_err.next_action, "reconcile")
        self.assertFalse(post_err.retryable_same_token)
        self.assertFalse(post_err.failover_other_token)

        # Post-submit timeout on _current_generate_video must NEVER blindly retry _current_rpc
        client = FlowClient(proxy_manager=MagicMock())
        client._frontend_cookie = AsyncMock(return_value="SID=1")
        client._get_recaptcha_token = AsyncMock(return_value=("rc-tok", "1:req"))
        client._notify_browser_captcha_request_finished = AsyncMock()
        client._current_rpc = AsyncMock(side_effect=RuntimeError("XHR timeout after submit"))
        with self.assertRaises(RuntimeError) as ctx:
            await client._current_generate_video(
                project_id="proj-1",
                prompt="test",
                model_key="veo_3_1_t2v_lite",
                aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
                mode="t2v",
                token_id=1,
                token_video_concurrency=None,
                google_cookies="SID=1",
            )
        self.assertTrue(getattr(ctx.exception, "submission_uncertain", False))
        self.assertEqual(client._current_rpc.await_count, 1)

        # 5. ProfileLease wait_seconds waits briefly for release
        with tempfile.TemporaryDirectory() as tmpdir:
            acc = Path(tmpdir) / "token_wait"
            l1 = acquire_profile_lease(acc, holder="first")
            def _release_soon():
                import time as _t
                _t.sleep(0.12)
                l1.release()
            th = threading.Thread(target=_release_soon, daemon=True)
            th.start()
            l2 = acquire_profile_lease(acc, holder="second", wait_seconds=1.5, poll_interval=0.04)
            try:
                self.assertTrue(l2.active)
            finally:
                l2.release()
                th.join(timeout=2.0)

        # 6. GET /v1/flow/capabilities + MCP capabilities://self + no_spend mode
        app = FastAPI()
        app.include_router(flow_routes.router)
        app.include_router(mcp_routes.router)
        app.dependency_overrides[flow_routes.verify_api_key_flexible] = lambda: "k"
        app.dependency_overrides[mcp_routes.verify_api_key_flexible] = lambda: "k"
        mcp_routes.set_mcp_dependencies(MagicMock(), MagicMock())
        mcp_routes.spend_limiter.reset()
        tc = TestClient(app)

        cap_normal = tc.get("/v1/flow/capabilities")
        self.assertEqual(cap_normal.status_code, 200)
        self.assertEqual(cap_normal.json()["capabilities"]["generate.video"], "yes")
        self.assertEqual(cap_normal.json()["capabilities"]["idempotent_submit"], "no")

        cap_ns = tc.get("/v1/flow/capabilities", headers={"X-Flow-No-Spend": "1"})
        self.assertEqual(cap_ns.status_code, 200)
        self.assertTrue(cap_ns.json()["noSpend"])
        self.assertEqual(cap_ns.json()["capabilities"]["generate.video"], "no")

        res_list = tc.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "resources/list", "params": {}},
        )
        self.assertEqual(res_list.status_code, 200)
        self.assertEqual(res_list.json()["result"]["resources"][0]["uri"], "capabilities://self")

        res_read = tc.post(
            "/mcp",
            headers={"X-Flow-No-Spend": "true"},
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "resources/read",
                "params": {"uri": "capabilities://self"},
            },
        )
        self.assertEqual(res_read.status_code, 200)
        res_caps = json.loads(res_read.json()["result"]["contents"][0]["text"])
        self.assertTrue(res_caps["noSpend"])

        ns_tools = tc.post(
            "/mcp",
            headers={"X-Flow-No-Spend": "true"},
            json={"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}},
        )
        self.assertNotIn(
            "flow_generate_video",
            [t["name"] for t in ns_tools.json()["result"]["tools"]],
        )

        ns_blocked = tc.post(
            "/mcp",
            headers={"X-Flow-No-Spend": "true"},
            json={
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {"name": "flow_generate_video", "arguments": {"prompt": "hi"}},
            },
        )
        self.assertEqual(ns_blocked.status_code, 403)
        self.assertEqual(ns_blocked.json()["error"]["code"], -32030)
        self.assertEqual(ns_blocked.json()["error"]["data"]["charged"], "no")


if __name__ == "__main__":
    unittest.main()
