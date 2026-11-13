"""Contracts for current Flow image models and authenticated RPC submission."""

import json
import types
import unittest
from unittest.mock import AsyncMock, Mock, PropertyMock, patch
from urllib.parse import parse_qs, urlparse

from src.api.routes import _get_gemini_model_catalog, _get_openai_model_catalog
from src.core.config import config
from src.core.model_resolver import resolve_model_name
from src.services.flow_client import FlowClient
from src.services.generation_handler import MODEL_CONFIG


PROJECT_ID = "01234567-89ab-cdef-0123-456789abcdef"


def _rpc_response(payload):
    return json.dumps(
        [["wrb.fr", "ogiZ0b", json.dumps(payload)]], separators=(",", ":")
    )


class ImageRpcAuthenticationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = FlowClient(None)
        self.bootstrap = (
            "boq_labs-ai-sandbox-frontend_20261007.14_p0 "
            '{"FdrFJe":"123", "SNlM0e" : "TOKEN\\u003a/+="}'
        )
        self.client._load_flow_frontend_bootstrap = AsyncMock(
            return_value=self.bootstrap
        )
        self.client._make_text_request = AsyncMock(
            return_value=_rpc_response({"ok": True})
        )

    async def call_rpc(self):
        return await self.client._call_flow_frontend_rpc(
            rpc_id="ogiZ0b",
            argument=["argument"],
            cookie_header="OSID=dummy",
            timeout=10,
            project_id=PROJECT_ID,
        )

    async def test_first_submission_includes_bootstrap_xsrf(self):
        await self.call_rpc()
        self.client._make_text_request.assert_awaited_once()
        request = self.client._make_text_request.call_args.kwargs
        self.assertEqual(parse_qs(request["raw_body"])["at"], ["TOKEN:/+="])
        self.assertEqual(request["headers"]["Referer"], "https://flow.google.com/")
        self.assertEqual(
            parse_qs(urlparse(request["url"]).query)["source-path"],
            [f"/project/{PROJECT_ID}"],
        )
        self.assertTrue(request["redact_sensitive_logs"])

    async def test_expired_xsrf_replacement_preserves_body_without_duplicate_at(self):
        self.client._make_text_request.side_effect = [
            '[["xsrf","REFRESH:/+="]]',
            _rpc_response({"ok": True}),
        ]
        await self.call_rpc()
        requests = self.client._make_text_request.call_args_list
        self.assertEqual(len(requests), 2)
        first = parse_qs(requests[0].kwargs["raw_body"])
        second = parse_qs(requests[1].kwargs["raw_body"])
        self.assertEqual(first["at"], ["TOKEN:/+="])
        self.assertEqual(second["at"], ["REFRESH:/+="])
        self.assertEqual(first["f.req"], second["f.req"])

    async def test_missing_bootstrap_xsrf_retains_challenge_compatibility(self):
        self.client._load_flow_frontend_bootstrap.return_value = (
            'boq_labs-ai-sandbox-frontend_20261007.14_p0 {"FdrFJe":"123"}'
        )
        self.client._make_text_request.side_effect = [
            '[["xsrf","CHALLENGE"]]',
            _rpc_response({"ok": True}),
        ]
        await self.call_rpc()
        requests = self.client._make_text_request.call_args_list
        self.assertNotIn("at", parse_qs(requests[0].kwargs["raw_body"]))
        self.assertEqual(parse_qs(requests[1].kwargs["raw_body"])["at"], ["CHALLENGE"])

    async def test_repeated_xsrf_challenge_does_not_loop(self):
        self.client._make_text_request.side_effect = [
            '[["xsrf","A"]]',
            '[["xsrf","B"]]',
        ]
        with self.assertRaises(RuntimeError):
            await self.call_rpc()
        self.assertEqual(self.client._make_text_request.await_count, 2)


class ImageModelCatalogTests(unittest.TestCase):
    def test_current_default_and_lite_families_have_matching_variants(self):
        primary_prefix = "gemini-3.1-flash-image-"
        lite_prefix = "gemini-3.1-flash-lite-image-"
        primary = {
            name: entry
            for name, entry in MODEL_CONFIG.items()
            if name.startswith(primary_prefix)
        }
        lite = {
            name: entry
            for name, entry in MODEL_CONFIG.items()
            if name.startswith(lite_prefix)
        }
        self.assertEqual(len(primary), 15)
        self.assertEqual(len(lite), 15)
        for name, entry in primary.items():
            counterpart = lite[name.replace(primary_prefix, lite_prefix, 1)]
            self.assertEqual(entry["model_name"], "BELUGA")
            self.assertEqual(counterpart["model_name"], "HARBOR_SEAL")
            self.assertEqual(entry["aspect_ratio"], counterpart["aspect_ratio"])
            self.assertEqual(entry.get("upsample"), counterpart.get("upsample"))
            self.assertTrue(entry["listed"])
            self.assertTrue(counterpart["listed"])
        openai = {entry["id"] for entry in _get_openai_model_catalog()}
        gemini = _get_gemini_model_catalog()
        self.assertTrue(set(primary) | set(lite) <= openai)
        self.assertTrue(set(primary) | set(lite) <= set(gemini))
        self.assertEqual(
            MODEL_CONFIG["gemini-3.0-pro-image-landscape"]["model_name"], "GEM_PIX_2"
        )
        pro = {
            name: entry
            for name, entry in MODEL_CONFIG.items()
            if name.startswith("gemini-3.0-pro-image-")
        }
        self.assertEqual(len(pro), 15)
        for name, entry in pro.items():
            self.assertEqual(entry["model_name"], "GEM_PIX_2")
            self.assertEqual(entry["display_name"], "Nano Banana Pro")
            self.assertTrue(entry["listed"])
        self.assertTrue(set(pro) <= openai)
        self.assertTrue(set(pro) <= set(gemini))

    def test_nano_banana_friendly_aliases_resolve_to_current_catalog(self):
        self.assertEqual(
            resolve_model_name("nano-banana-2.1", model_config=MODEL_CONFIG),
            "gemini-3.1-flash-image-landscape",
        )
        self.assertEqual(
            resolve_model_name("nano-banana-2-lite", model_config=MODEL_CONFIG),
            "gemini-3.1-flash-lite-image-landscape",
        )
        self.assertEqual(
            resolve_model_name("nano-banana-pro", model_config=MODEL_CONFIG),
            "gemini-3.0-pro-image-landscape",
        )

    def test_catalog_only_references_current_image_fields(self):
        self.assertEqual(
            {
                entry["model_name"]
                for entry in MODEL_CONFIG.values()
                if entry["type"] == "image"
            },
            {"BELUGA", "HARBOR_SEAL", "GEM_PIX_2"},
        )
        for name in (
            "imagen-4.0-generate-preview-landscape",
            "imagen-4.0-generate-preview-portrait",
        ):
            self.assertNotIn(name, MODEL_CONFIG)

    def test_lite_base_alias_resolves_ratio_and_upsample(self):
        request = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(
                aspectRatio="portrait",
                imageSize="2k",
            )
        )
        resolved = resolve_model_name(
            "gemini-3.1-flash-lite-image",
            request=request,
            model_config=MODEL_CONFIG,
        )
        self.assertEqual(resolved, "gemini-3.1-flash-lite-image-portrait-2k")


class ImageModelRolloutTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = FlowClient(None)
        self.client._get_runtime_config = lambda: types.SimpleNamespace(
            flow_max_retries=1,
            flow_image_request_timeout=10,
        )
        self.client._frontend_cookie = AsyncMock(return_value="cookie")
        self.client._resolve_flow_frontend_cookie_storage = AsyncMock(
            return_value="OSID=dummy"
        )
        self.client._get_recaptcha_token = AsyncMock(
            side_effect=[("CAPTCHA-1", None), ("CAPTCHA-2", None)]
        )
        self.client._current_rpc = AsyncMock(return_value={"ok": True})
        self.client._normalize_frontend_image_generation_response = Mock(
            return_value={"media": ["media"]}
        )
        self.client._notify_browser_captcha_request_finished = AsyncMock()
        self.client._handle_retryable_generation_error = AsyncMock(return_value=False)
        self.client._call_flow_stream_chat = AsyncMock(
            return_value={"mediaIds": ["media"]}
        )

    async def generate(self, model_name="BELUGA", image_inputs=None):
        with patch.object(
            type(config),
            "captcha_method",
            new_callable=PropertyMock,
            return_value="yescaptcha",
        ):
            return await self.client.generate_image(
                "access",
                PROJECT_ID,
                "an apple",
                model_name,
                "IMAGE_ASPECT_RATIO_LANDSCAPE",
                image_inputs=image_inputs,
                token_id=7,
            )

    async def test_not_found_does_not_submit_a_retired_model(self):
        for code in ("5", "[5]"):
            with self.subTest(code=code):
                self.setUp()
                self.client._current_rpc.side_effect = RuntimeError(
                    f"Flow frontend RPC rejected: rpc=ogiZ0b, code={code}"
                )
                with self.assertRaises(RuntimeError):
                    await self.generate(image_inputs=[{"name": "reference"}])
                self.client._current_rpc.assert_awaited_once()
                self.client._get_recaptcha_token.assert_awaited_once()
                self.assertEqual(
                    self.client._current_rpc.call_args.kwargs["argument"][1][0][5],
                    "BELUGA",
                )

    async def test_legacy_internal_key_is_updated_before_submission(self):
        await self.generate("NARWHAL")
        self.client._current_rpc.assert_awaited_once()
        self.assertEqual(
            self.client._current_rpc.call_args.kwargs["argument"][1][0][5],
            "BELUGA",
        )

    async def test_retired_imagen_key_is_rejected_before_captcha(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Flow image model"):
            await self.generate("IMAGEN_3_5")
        self.client._frontend_cookie.assert_not_awaited()
        self.client._get_recaptcha_token.assert_not_awaited()
        self.client._current_rpc.assert_not_awaited()

    async def test_permission_policy_and_unrelated_not_found_do_not_change_model(self):
        for message in (
            "Flow frontend RPC rejected: rpc=ogiZ0b, error=MODEL_ACCESS_DENIED",
            "Flow frontend RPC rejected: rpc=ogiZ0b, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY",
            "Flow frontend RPC rejected: rpc=as29s, code=[5]",
        ):
            with self.subTest(error=message):
                self.setUp()
                self.client._current_rpc.side_effect = RuntimeError(message)
                with self.assertRaises(RuntimeError):
                    await self.generate()
                self.client._current_rpc.assert_awaited_once()

    async def test_lite_and_pro_not_found_never_switch_model_family(self):
        for model in ("HARBOR_SEAL", "GEM_PIX_2"):
            with self.subTest(model=model):
                self.setUp()
                self.client._current_rpc.side_effect = RuntimeError(
                    "Flow frontend RPC rejected: rpc=ogiZ0b, code=[5]"
                )
                with self.assertRaises(RuntimeError):
                    await self.generate(model)
                self.client._current_rpc.assert_awaited_once()

    async def test_browser_modes_submit_explicit_models_using_local_captcha(self):
        aspects = (
            ("IMAGE_ASPECT_RATIO_LANDSCAPE", 3),
            ("IMAGE_ASPECT_RATIO_PORTRAIT", 2),
            ("IMAGE_ASPECT_RATIO_SQUARE", 1),
            ("IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE", 4),
            ("IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR", 5),
        )
        for mode in ("browser", "camoufox"):
            for model in ("BELUGA", "HARBOR_SEAL", "GEM_PIX_2"):
                for aspect, aspect_enum in aspects:
                    for references in (None, [{"name": "reference"}]):
                        with self.subTest(
                            mode=mode,
                            model=model,
                            aspect=aspect,
                            references=bool(references),
                        ):
                            self.setUp()
                            with patch.object(
                                type(config),
                                "captcha_method",
                                new_callable=PropertyMock,
                                return_value=mode,
                            ):
                                result, _, trace = await self.client.generate_image(
                                    "access",
                                    PROJECT_ID,
                                    "an apple",
                                    model,
                                    aspect,
                                    image_inputs=references,
                                )
                            self.assertEqual(result["media"], ["media"])
                            pass
                            self.client._call_flow_stream_chat.assert_not_awaited()
                            self.client._current_rpc.assert_awaited_once()
                            self.assertEqual(
                                self.client._current_rpc.call_args.kwargs["rpc_id"],
                                "ogiZ0b",
                            )
                            request = self.client._current_rpc.call_args.kwargs[
                                "argument"
                            ][1][0]
                            self.assertEqual(request[5], model)
                            self.assertEqual(request[4], aspect_enum)
                            self.assertEqual(
                                request[2],
                                [["reference", None, None, None, 1]]
                                if references
                                else None,
                            )
                            self.assertEqual(
                                trace["generation_attempts"][0]["image_model_name"],
                                model,
                            )
                            captcha = self.client._get_recaptcha_token.call_args.kwargs
                            self.assertEqual(captcha["action"], "IMAGE_GENERATION")
                            self.assertIsNone(captcha.get("method_override"))
                            self.client._notify_browser_captcha_request_finished.assert_awaited_once_with(
                                None
                            )

    async def test_image_auxiliary_rpcs_preserve_selected_captcha_provider(self):
        for mode in ("browser", "camoufox"):
            with self.subTest(mode=mode):
                self.setUp()
                self.client._get_recaptcha_token.side_effect = None
                self.client._get_recaptcha_token.return_value = ("CAPTCHA", None)
                self.client._get_runtime_config = lambda: types.SimpleNamespace(
                    flow_max_retries=1, upsample_timeout=30
                )
                self.client._get_control_plane_timeout = Mock(return_value=30)
                self.client._get_video_submit_timeout = Mock(return_value=30)
                self.client._find_uuid_value = Mock(return_value="media")
                self.client._find_flow_content_url = Mock(return_value="")
                self.client._find_media_url = Mock(
                    return_value="https://example.com/image"
                )
                self.client._build_frontend_video_generation_argument = Mock(
                    return_value=("YhhmEf", [])
                )
                self.client._normalize_frontend_video_submission = Mock(
                    return_value={"operations": []}
                )
                with patch.object(
                    type(config),
                    "captcha_method",
                    new_callable=PropertyMock,
                    return_value=mode,
                ):
                    await self.client.upload_image(
                        "access", b"image", project_id=PROJECT_ID
                    )
                    await self.client.upsample_image("access", PROJECT_ID, "media")
                    await self.client._current_generate_video(
                        project_id=PROJECT_ID,
                        prompt="an apple",
                        model_key="veo_3_1_t2v_fast",
                        aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
                        mode="t2v",
                        token_id=7,
                        token_video_concurrency=None,
                        google_cookies=None,
                    )
                calls = self.client._get_recaptcha_token.call_args_list
                self.assertEqual(
                    [call.kwargs["action"] for call in calls],
                    ["UPLOAD_IMAGE", "IMAGE_GENERATION", "VIDEO_GENERATION"],
                )
                self.assertTrue(
                    all(call.kwargs.get("method_override") is None for call in calls)
                )

    async def test_accepted_rpc_is_not_resubmitted_on_response_normalization_failure(
        self,
    ):
        self.client._get_runtime_config = lambda: types.SimpleNamespace(
            flow_max_retries=2, flow_image_request_timeout=10
        )
        self.client._normalize_frontend_image_generation_response.side_effect = (
            RuntimeError("HTTP Error 500: response could not be normalized")
        )
        self.client._handle_retryable_generation_error.return_value = True
        with self.assertRaisesRegex(RuntimeError, "normalized"):
            await self.generate()
        self.client._current_rpc.assert_awaited_once()
        self.client._get_recaptcha_token.assert_awaited_once()
        self.client._handle_retryable_generation_error.assert_not_awaited()

    async def test_uncertain_submission_never_triggers_another_request(self):
        for flag in ("outcome_unknown", "submission_uncertain"):
            with self.subTest(flag=flag):
                self.setUp()
                error = RuntimeError("Flow frontend RPC rejected: rpc=ogiZ0b, code=[5]")
                setattr(error, flag, True)
                self.client._current_rpc.side_effect = error
                with self.assertRaises(RuntimeError):
                    await self.generate()
                self.client._current_rpc.assert_awaited_once()

    async def test_captcha_rejections_respect_configured_retry_limit(self):
        self.client._get_runtime_config = lambda: types.SimpleNamespace(
            flow_max_retries=2,
            flow_image_request_timeout=10,
        )
        self.client._current_rpc.side_effect = RuntimeError(
            "Flow frontend RPC rejected: rpc=ogiZ0b, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY"
        )
        self.client._handle_retryable_generation_error.return_value = True
        with self.assertRaises(RuntimeError):
            await self.generate()
        self.assertEqual(self.client._current_rpc.await_count, 2)
        models = [
            call.kwargs["argument"][1][0][5]
            for call in self.client._current_rpc.call_args_list
        ]
        self.assertEqual(models, ["BELUGA", "BELUGA"])

    async def test_camoufox_mode_submits_direct_rpc_without_streamchat_ui_hijack(self):
        import asyncio
        from contextlib import asynccontextmanager
        from src.services.browser_captcha import BrowserCaptchaService

        fake_service = types.SimpleNamespace(
            generate_native_image=AsyncMock(side_effect=AssertionError("should not call StreamChat UI")),
            generate_native_video=AsyncMock(side_effect=AssertionError("should not call StreamChat UI")),
        )
        with patch.object(
            type(config),
            "captcha_method",
            new_callable=PropertyMock,
            return_value="camoufox",
        ), patch.object(
            BrowserCaptchaService,
            "get_instance",
            AsyncMock(return_value=fake_service),
        ):
            await self.client.generate_image(
                "access",
                PROJECT_ID,
                "an apple",
                "BELUGA",
                "IMAGE_ASPECT_RATIO_LANDSCAPE",
                token_id=7,
            )
            self.client._current_rpc.assert_awaited_once()
            self.assertEqual(self.client._current_rpc.call_args.kwargs["rpc_id"], "ogiZ0b")

            self.client._current_rpc.reset_mock()
            self.client._get_recaptcha_token = AsyncMock(return_value=("CAPTCHA-V", None))
            self.client._current_rpc = AsyncMock(
                return_value=[["11111111-1111-1111-1111-111111111111", PROJECT_ID, "22222222-2222-2222-2222-222222222222"]]
            )
            await self.client.generate_video_text(
                "access",
                PROJECT_ID,
                "ocean waves",
                "veo_3_1_t2v_fast",
                "VIDEO_ASPECT_RATIO_LANDSCAPE",
                use_v2_model_config=True,
                token_id=7,
            )
            self.client._current_rpc.assert_awaited_once()
            self.assertEqual(self.client._current_rpc.call_args.kwargs["rpc_id"], "YhhmEf")
        fake_service.generate_native_image.assert_not_called()
        fake_service.generate_native_video.assert_not_called()

        service = BrowserCaptchaService(db=None)
        fake_browser = types.SimpleNamespace(
            execute_flow_frontend_rpc=AsyncMock(side_effect=asyncio.TimeoutError()),
            get_last_fingerprint=lambda: {},
            _runtime_generation=0,
        )
        fake_request = types.SimpleNamespace(
            ref="0:lease-1/",
            browser=fake_browser,
            generation=0,
            fingerprint=None,
            failed=False,
        )

        @asynccontextmanager
        async def _borrow(*args, **kwargs):
            yield fake_request

        service._borrow_browser = _borrow
        service._resolve_token_proxy_url = AsyncMock(return_value=None)
        with self.assertRaises(RuntimeError) as ctx:
            await service.execute_flow_frontend_rpc(
                rpc_id="ogiZ0b",
                argument=["arg"],
                project_id=PROJECT_ID,
                timeout=10,
                token_id=7,
            )
        self.assertIn("ogiZ0b", str(ctx.exception))
        self.assertTrue(getattr(ctx.exception, "submission_uncertain", False))


    async def test_expired_login_from_camoufox_is_propagated_not_swallowed(self):
        from src.services.browser_captcha import BrowserCaptchaService
        from src.services.camoufox_bidi import CamoufoxWorker
        from src.services.flow_client import FlowClient

        client = FlowClient(proxy_manager=None, db=None)
        fake_service = types.SimpleNamespace(
            get_token=AsyncMock(
                side_effect=RuntimeError(
                    "Flow 账号登录态已失效（页面已跳转至未登录页），请在后台点击该账号的「重登」重新登录"
                )
            )
        )
        with patch.object(
            BrowserCaptchaService, "get_instance", AsyncMock(return_value=fake_service)
        ):
            with self.assertRaisesRegex(RuntimeError, "登录态已失效"):
                await client._get_recaptcha_token(PROJECT_ID, action="IMAGE_GENERATION", token_id=1)

        worker = CamoufoxWorker(0, "/tmp/nonexistent-camoufox-test")
        worker._get_or_create_shared_browser = AsyncMock(return_value=(None, None, "ctx"))
        worker._ensure_flow_page_ready = AsyncMock(
            side_effect=RuntimeError("Flow 账号登录态已失效（页面已跳转至未登录页）")
        )
        with self.assertRaisesRegex(RuntimeError, "登录态已失效"):
            await worker.get_token(PROJECT_ID, token_id=1)

    async def test_load_balancer_cooldown_and_generation_handler_failover(self):
        from src.core.models import Token
        from src.services.generation_handler import GenerationHandler
        from src.services.load_balancer import LoadBalancer

        tok3 = Token(id=3, st="st3", at="at3", email="risk@example.com", is_active=True, image_enabled=True, video_enabled=True)
        tok2 = Token(id=2, st="st2", at="at2", email="good@example.com", is_active=True, image_enabled=True, video_enabled=True)

        tm = types.SimpleNamespace(
            get_active_tokens=AsyncMock(return_value=[tok3, tok2]),
            needs_at_refresh=lambda t: False,
            ensure_valid_token=AsyncMock(side_effect=lambda t: t),
            ensure_project_exists=AsyncMock(return_value="proj-1"),
            record_usage=AsyncMock(),
            record_success=AsyncMock(),
            record_error=AsyncMock(),
            disable_token=AsyncMock(),
        )
        lb = LoadBalancer(tm)
        lb.mark_token_cooldown(3, seconds=60.0, reason="PUBLIC_ERROR_UNUSUAL_ACTIVITY")
        chosen = await lb.select_token(for_image_generation=True, model="gemini-3.1-flash-image-landscape")
        self.assertEqual(chosen.id, 2)
        chosen2 = await lb.select_token(
            for_image_generation=True,
            model="gemini-3.1-flash-image-landscape",
            excluded_token_ids={2},
        )
        self.assertEqual(chosen2.id, 3)

        # Verify GenerationHandler fails over cleanly on PUBLIC_ERROR_UNUSUAL_ACTIVITY,
        # but NEVER fails over when submission_uncertain=True.
        lb2 = LoadBalancer(tm)
        handler = GenerationHandler.__new__(GenerationHandler)
        lb2._round_robin_state["image"] = 2
        handler.token_manager = tm
        handler.load_balancer = lb2
        handler.proxy_manager = None
        handler.db = types.SimpleNamespace()
        handler.file_cache = types.SimpleNamespace()
        handler._log_request = AsyncMock(return_value=None)
        handler._update_request_log_progress = AsyncMock()

        async def fake_generate_image(*, token_id, **kwargs):
            if token_id == 3:
                raise RuntimeError("Flow frontend RPC rejected: rpc=ogiZ0b, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY")
            return (
                {"media": [{"name": "m1", "image": {"generatedImage": {"fifeUrl": "https://flow-content.google/image/ok"}}}]},
                "sess-1",
                {},
            )

        handler.flow_client = types.SimpleNamespace(
            clear_request_fingerprint=lambda: None,
            generate_image=AsyncMock(side_effect=fake_generate_image),
            get_request_fingerprint=lambda: None,
        )
        with (
            patch("src.services.generation_handler.config") as mock_cfg,
            patch("src.services.load_balancer.config") as mock_lb_cfg,
        ):
            mock_cfg.cache_enabled = False
            mock_cfg.call_logic_mode = "polling"
            mock_lb_cfg.call_logic_mode = "polling"
            chunks = [
                c
                async for c in handler.handle_generation(
                    "gemini-3.1-flash-image-landscape",
                    "a small cup",
                    stream=False,
                )
            ]
        joined = "".join(str(c) for c in chunks)
        self.assertIn("https://flow-content.google/image/ok", joined)
        self.assertTrue(lb2.is_token_in_cooldown(3))
        self.assertEqual(handler.flow_client.generate_image.await_count, 2)

        # Uncertain submission must NOT fail over to another account.
        uncertain_err = RuntimeError("Flow frontend RPC timeout: rpc=ogiZ0b")
        uncertain_err.submission_uncertain = True
        self.assertFalse(handler._should_failover_token(uncertain_err, {}))

        # Verify _get_retry_reason does not retry PUBLIC_ERROR_UNUSUAL_ACTIVITY on the same token.
        from src.services.flow_client import FlowClient
        real_client = FlowClient(proxy_manager=None, db=None)
        self.assertIsNone(
            real_client._get_retry_reason(
                "Flow frontend RPC rejected: rpc=ogiZ0b, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY"
            )
        )
        self.assertIsNone(
            real_client._get_retry_reason(
                "Flow frontend RPC rejected: rpc=ogiZ0b, error=PUBLIC_ERROR_UNSAFE_GENERATION"
            )
        )
        self.assertEqual(
            real_client._get_retry_reason("HTTP Error 500: upstream error"),
            "500/内部错误",
        )

if __name__ == "__main__":
    unittest.main()
