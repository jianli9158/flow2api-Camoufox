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


if __name__ == "__main__":
    unittest.main()
