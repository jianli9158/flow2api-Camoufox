import types
import unittest
from io import BytesIO
from unittest.mock import AsyncMock, patch

from PIL import Image

from src.api.routes import _get_openai_model_catalog, _normalize_openai_request
from src.core.models import ChatCompletionRequest, ChatMessage
from src.core.config import config
from src.core.model_resolver import resolve_model_name
from src.services.flow_client import FlowClient
from src.services.generation_handler import MODEL_CONFIG, GenerationHandler


def _make_image_bytes(size: tuple[int, int], color: str = "white") -> bytes:
    image = Image.new("RGB", size, color=color)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


class VeoLiteModelResolverTests(unittest.TestCase):
    def test_public_catalog_exposes_only_active_flow_families(self):
        listed = {item["id"] for item in _get_openai_model_catalog()}

        self.assertEqual(
            listed,
            {
                "gemini-3.1-flash-image",
                "gemini-3.1-flash-lite-image",
                "gemini-3.0-pro-image",
                "veo-3.1-lite",
                "veo-3.1-lite-i2v",
                "veo-3.1-lite-r2v",
                "veo-3.1-lite-extend",
                "omni-1.2-flash",
                "omni-1.2-flash-i2v",
                "omni-1.2-flash-extend",
            },
        )
        # Legacy suffixed variants stay hidden in MODEL_CONFIG for backward compatibility
        for legacy in (
            "veo-3.1-lite-4s-landscape",
            "veo-3.1-lite-i2v-8s-landscape",
            "omni-1.2-flash-360p",
            "omni-1.2-flash-4s-landscape",
            "omni-1.2-flash-15s-portrait",
            "veo_3_1_t2v_lite_landscape_4s",
        ):
            self.assertNotIn(legacy, listed)
            self.assertIn(legacy, MODEL_CONFIG)
        for retired in (
            "veo-3.1-fast",
            "veo-3.1-fast-4s-landscape",
            "veo-3.1-quality-4s-landscape",
            "veo-3.1-fast-i2v-8s-landscape",
            "veo-3.1-quality-i2v-8s-landscape",
            "omni-1.1-flash",
            "omni-1.1-flash-4s-landscape",
            "veo_3_1_t2v_fast",
            "veo_3_1_t2v_fast_landscape_4s",
            "veo_3_1_t2v_quality_4s",
            "abra_t2v_8s",
            "abra_r2v_8s",
        ):
            self.assertNotIn(retired, listed)
            self.assertNotIn(retired, MODEL_CONFIG)

    def test_current_aliases_use_live_upstream_keys(self):
        self.assertEqual(
            MODEL_CONFIG["veo-3.1-lite-4s-landscape"]["model_key"],
            "veo_3_1_t2v_lite_4s",
        )
        self.assertEqual(
            MODEL_CONFIG["omni-1.2-flash-4s-landscape"]["model_key"],
            "macaron_cookie_t2v_4s",
        )
        self.assertEqual(
            MODEL_CONFIG["veo-3.1-lite-r2v-8s-landscape"]["model_key"],
            "veo_3_1_r2v_lite",
        )
        self.assertEqual(
            MODEL_CONFIG["veo-3.1-lite-r2v-8s-portrait"]["model_key"],
            "veo_3_1_r2v_lite",
        )
        self.assertEqual(MODEL_CONFIG["omni"]["model_key"], "macaron_cookie_t2v_8s")
        self.assertEqual(
            MODEL_CONFIG["omni"]["reference_model_key"], "macaron_cookie_r2v_8s"
        )

    def test_resolve_t2v_lite_alias_to_portrait_variant(self):
        request = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(aspectRatio="portrait")
        )

        resolved = resolve_model_name(
            "veo_3_1_t2v_lite",
            request=request,
            model_config=MODEL_CONFIG,
        )

        self.assertEqual(resolved, "veo_3_1_t2v_lite_portrait")

    def test_resolve_lite_4s_upsample_alias_to_portrait_variant(self):
        request = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(aspectRatio="portrait")
        )

        resolved = resolve_model_name(
            "veo-3.1-lite-4s-4k",
            request=request,
            model_config=MODEL_CONFIG,
        )

        self.assertEqual(resolved, "veo-3.1-lite-4s-portrait-4k")

    def test_resolve_video_image_size_to_upsample_variant(self):
        request = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(
                aspectRatio="landscape", imageSize="1080p"
            )
        )

        resolved = resolve_model_name(
            "veo-3.1-lite-i2v-6s",
            request=request,
            model_config=MODEL_CONFIG,
        )

        self.assertEqual(resolved, "veo-3.1-lite-i2v-6s-landscape-1080p")

    def test_resolve_omni_extend_8s_alias_to_portrait_variant(self):
        request = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(aspectRatio="portrait")
        )

        resolved = resolve_model_name(
            "omni-1.2-flash-extend-8s",
            request=request,
            model_config=MODEL_CONFIG,
        )

        self.assertEqual(resolved, "omni-1.2-flash-extend-8s-portrait")

    def test_resolve_omni_8s_upsample_alias_to_portrait_variant(self):
        request = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(
                aspectRatio="portrait", imageSize="4k"
            )
        )

        resolved = resolve_model_name(
            "omni-1.2-flash-i2v",
            request=request,
            model_config=MODEL_CONFIG,
        )

        self.assertEqual(resolved, "omni-1.2-flash-i2v-8s-portrait-4k")
    def test_image_model_follows_reference_image_aspect_ratio(self):
        request = types.SimpleNamespace(generationConfig=None)
        portrait_image = _make_image_bytes((900, 1600))

        resolved = resolve_model_name(
            "gemini-3.0-pro-image",
            request=request,
            model_config=MODEL_CONFIG,
            images=[portrait_image],
        )

        self.assertEqual(resolved, "gemini-3.0-pro-image-portrait-2k")

        req_1k = types.SimpleNamespace(
            generationConfig=types.SimpleNamespace(imageSize="1K")
        )
        self.assertEqual(
            resolve_model_name(
                "gemini-3.0-pro-image",
                request=req_1k,
                model_config=MODEL_CONFIG,
                images=[portrait_image],
            ),
            "gemini-3.0-pro-image-portrait",
        )

class VeoLiteGenerationHandlerTests(unittest.TestCase):
    def test_tier_two_does_not_upgrade_lite_model_to_fake_ultra(self):
        handler = GenerationHandler.__new__(GenerationHandler)

        model_key, message = handler._resolve_video_model_key_for_tier(
            {
                "model_key": "veo_3_1_t2v_lite",
                "allow_tier_upgrade": False,
            },
            "PAYGATE_TIER_TWO",
        )

        self.assertEqual(model_key, "veo_3_1_t2v_lite")
        self.assertIsNone(message)

    def test_tier_two_does_not_upgrade_omni_or_lite_models_to_fake_ultra(self):
        handler = GenerationHandler.__new__(GenerationHandler)

        for key in ("veo_3_1_t2v_lite", "macaron_cookie_t2v_8s", "veo_3_1_r2v_lite"):
            model_key, message = handler._resolve_video_model_key_for_tier(
                {
                    "model_key": key,
                    "allow_tier_upgrade": False,
                },
                "PAYGATE_TIER_TWO",
            )
            self.assertEqual(model_key, key)
            self.assertIsNone(message)

    def test_lite_4s_upsample_model_generates_then_upsamples(self):
        cfg = MODEL_CONFIG["veo-3.1-lite-4s-landscape-4k"]

        self.assertEqual(cfg["model_key"], "veo_3_1_t2v_lite_4s")
        self.assertEqual(cfg["video_type"], "t2v")
        self.assertEqual(cfg["upsample"]["model_key"], "veo_3_1_upsampler_4k")
        self.assertEqual(cfg["upsample"]["resolution"], "VIDEO_RESOLUTION_4K")

    def test_omni_6s_i2v_1080p_model_generates_then_upsamples(self):
        cfg = MODEL_CONFIG["omni-1.2-flash-i2v-6s-landscape-1080p"]

        self.assertEqual(cfg["model_key"], "macaron_cookie_i2v_fl_6s")
        self.assertEqual(cfg["start_frame_model_key"], "macaron_cookie_i2v_6s")
        self.assertEqual(cfg["video_type"], "i2v")
        self.assertEqual(cfg["upsample"]["model_key"], "veo_3_1_upsampler_1080p")
        self.assertEqual(cfg["upsample"]["resolution"], "VIDEO_RESOLUTION_1080P")

    def test_explicit_8s_aliases_reuse_default_upstream_keys(self):
        self.assertEqual(
            MODEL_CONFIG["veo_3_1_t2v_lite_8s_landscape"]["model_key"],
            "veo_3_1_t2v_lite",
        )
        self.assertEqual(
            MODEL_CONFIG["veo_3_1_interpolation_lite_8s_landscape"]["model_key"],
            "veo_3_1_interpolation_lite",
        )
        self.assertEqual(
            MODEL_CONFIG["omni-1.2-flash-landscape"]["model_key"],
            "macaron_cookie_t2v_8s",
        )

    def test_short_duration_lite_models_include_explicit_landscape_aliases(self):
        expected_aliases = {
            "veo_3_1_t2v_lite_landscape_4s": "veo_3_1_t2v_lite_4s_landscape",
            "veo_3_1_t2v_lite_landscape_6s": "veo_3_1_t2v_lite_6s_landscape",
            "veo_3_1_i2v_lite_landscape_4s": "veo_3_1_i2v_lite_4s_landscape",
            "veo_3_1_i2v_lite_landscape_6s": "veo_3_1_i2v_lite_6s_landscape",
            "veo_3_1_interpolation_lite_landscape_4s": "veo_3_1_interpolation_lite_4s_landscape",
            "veo_3_1_interpolation_lite_landscape_6s": "veo_3_1_interpolation_lite_6s_landscape",
        }

        for alias, target in expected_aliases.items():
            self.assertIn(alias, MODEL_CONFIG)
            self.assertEqual(MODEL_CONFIG[alias], MODEL_CONFIG[target])

    def test_default_duration_lite_models_include_explicit_8s_aliases(self):
        expected_aliases = {
            "veo_3_1_t2v_lite_landscape_8s": "veo_3_1_t2v_lite_8s_landscape",
            "veo_3_1_i2v_lite_landscape_8s": "veo_3_1_i2v_lite_8s_landscape",
            "veo_3_1_interpolation_lite_landscape_8s": "veo_3_1_interpolation_lite_8s_landscape",
        }

        for alias, target in expected_aliases.items():
            self.assertIn(alias, MODEL_CONFIG)
            self.assertEqual(MODEL_CONFIG[alias], MODEL_CONFIG[target])
    def test_direct_upsampler_keys_are_not_public_models(self):
        self.assertNotIn("veo_3_1_upsampler_4k", MODEL_CONFIG)
        self.assertNotIn("veo_3_1_upsampler_1080p", MODEL_CONFIG)


class VeoLiteFlowClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = FlowClient(proxy_manager=None)
        self.client._acquire_video_launch_gate = AsyncMock(
            return_value=(True, None, None)
        )
        self.client._release_video_launch_gate = AsyncMock()
        self.client._get_recaptcha_token = AsyncMock(
            return_value=("recaptcha-token", "browser-1")
        )
        self.client._notify_browser_captcha_request_finished = AsyncMock()

    async def test_generate_video_text_uses_v2_payload_for_lite(self):
        operation_id = "11111111-1111-1111-1111-111111111111"
        project_id = "22222222-2222-2222-2222-222222222222"
        media_id = "33333333-3333-3333-3333-333333333333"
        self.client._call_flow_frontend_rpc = AsyncMock(
            return_value=[[operation_id, project_id, media_id]]
        )

        await self.client.generate_video_text(
            at="at-token",
            project_id=project_id,
            prompt="猫猫",
            model_key="veo_3_1_t2v_lite",
            aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
            use_v2_model_config=True,
            google_cookies="SID=session-cookie",
        )

        rpc = self.client._call_flow_frontend_rpc.await_args.kwargs
        self.assertEqual(rpc["rpc_id"], "YhhmEf")
        request_data = rpc["argument"][0][0]
        self.assertEqual(request_data[0], [None, None, [[["猫猫"]]]])
        self.assertEqual(request_data[1], "veo_3_1_t2v_lite")
        self.assertEqual(request_data[2], 2)
        self.assertTrue(rpc["argument"][2][0])

    async def test_transient_retry_keeps_original_model_key(self):
        operation_id = "11111111-1111-1111-1111-111111111111"
        project_id = "22222222-2222-2222-2222-222222222222"
        media_id = "33333333-3333-3333-3333-333333333333"
        self.client._call_flow_frontend_rpc = AsyncMock(
            side_effect=[
                RuntimeError("connection timed out"),
                [[operation_id, project_id, media_id]],
            ]
        )
        await self.client.generate_video_text(
            at="at-token",
            project_id=project_id,
            prompt="猫猫",
            model_key="macaron_cookie_t2v_6s",
            aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
            use_v2_model_config=True,
            google_cookies="SID=session-cookie",
        )
        calls = self.client._call_flow_frontend_rpc.await_args_list
        self.assertEqual(calls[0].kwargs["argument"][0][0][1], "macaron_cookie_t2v_6s")
        self.assertEqual(calls[1].kwargs["argument"][0][0][1], "macaron_cookie_t2v_6s")

    def test_reference_video_uses_current_prompt_and_media_envelopes(self):
        rpc_id, argument = self.client._build_frontend_video_generation_argument(
            project_id="22222222-2222-2222-2222-222222222222",
            prompt="参考图视频",
            model_key="macaron_cookie_r2v_10s",
            aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
            recaptcha_token="recaptcha-token",
            session_id="session-id",
            mode="references",
            reference_media_ids=["reference-media"],
        )

        self.assertEqual(rpc_id, "MZZa6b")
        request_data = argument[0][0]
        self.assertEqual(request_data[0], [None, None, [[["参考图视频"]]]])
        self.assertEqual(request_data[1], [[None, "reference-media"]])
    async def test_generate_video_text_normalizes_media_only_create_response(self):
        operation_id = "11111111-1111-1111-1111-111111111111"
        project_id = "22222222-2222-2222-2222-222222222222"
        media_id = "33333333-3333-3333-3333-333333333333"
        self.client._call_flow_frontend_rpc = AsyncMock(
            return_value=[[operation_id, project_id, media_id]]
        )

        result = await self.client.generate_video_text(
            at="at-token",
            project_id=project_id,
            prompt="猫猫",
            model_key="veo_3_1_t2v_lite",
            aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
            use_v2_model_config=True,
            google_cookies="SID=session-cookie",
        )

        self.assertEqual(result["operations"][0]["operation"]["name"], operation_id)
        self.assertEqual(result["operations"][0]["projectId"], project_id)
        self.assertEqual(
            result["operations"][0]["status"],
            "MEDIA_GENERATION_STATUS_ACTIVE",
        )


class RouteNormalizationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = FlowClient(proxy_manager=None)
        self.client._acquire_video_launch_gate = AsyncMock(
            return_value=(True, None, None)
        )
        self.client._release_video_launch_gate = AsyncMock()
        self.client._get_recaptcha_token = AsyncMock(
            return_value=("recaptcha-token", "browser-1")
        )
        self.client._notify_browser_captcha_request_finished = AsyncMock()

    async def test_openai_history_reference_image_can_drive_aspect_ratio(self):
        portrait_image = _make_image_bytes((900, 1600))
        request = ChatCompletionRequest(
            model="gemini-3.0-pro-image",
            messages=[
                ChatMessage(role="user", content="先生成一张图"),
                ChatMessage(
                    role="assistant",
                    content="![cat](https://example.com/cat.png)",
                ),
                ChatMessage(role="user", content="基于上一张图继续编辑"),
            ],
        )

        with patch(
            "src.api.routes.retrieve_image_data",
            new=AsyncMock(return_value=portrait_image),
        ):
            normalized = await _normalize_openai_request(request)

        self.assertEqual(normalized.model, "gemini-3.0-pro-image-portrait-2k")
        self.assertEqual(len(normalized.images), 1)

    async def test_check_video_status_uses_media_payload_and_normalizes_response(self):
        operation_id = "11111111-1111-1111-1111-111111111111"
        project_id = "22222222-2222-2222-2222-222222222222"
        media_id = "33333333-3333-3333-3333-333333333333"
        video_url = f"https://flow-content.google/video/{media_id}?token=abc"
        self.client._call_flow_frontend_rpc = AsyncMock(
            return_value=[operation_id, project_id, media_id, video_url]
        )

        result = await self.client.check_video_status(
            at="at-token",
            operations=[
                {
                    "operation": {"name": operation_id},
                    "name": operation_id,
                    "mediaName": media_id,
                    "projectId": project_id,
                }
            ],
            google_cookies="SID=session-cookie",
        )

        rpc = self.client._call_flow_frontend_rpc.await_args.kwargs
        self.assertEqual(rpc["rpc_id"], "jwpduf")
        self.assertEqual(rpc["argument"][2], [[operation_id]])
        operation = result["operations"][0]
        self.assertEqual(operation["operation"]["name"], operation_id)
        self.assertEqual(operation["status"], "MEDIA_GENERATION_STATUS_SUCCESSFUL")
        self.assertEqual(
            operation["operation"]["metadata"]["video"]["fifeUrl"],
            video_url,
        )

    async def test_check_video_status_surfaces_terminal_frontend_error(self):
        operation_id = "11111111-1111-1111-1111-111111111111"
        project_id = "22222222-2222-2222-2222-222222222222"
        media_id = "33333333-3333-3333-3333-333333333333"
        self.client._call_flow_frontend_rpc = AsyncMock(
            return_value=[
                operation_id,
                project_id,
                media_id,
                None,
                [4, [3, "PUBLIC_ERROR_PROMINENT_PEOPLE_FILTER_FAILED"]],
            ]
        )

        result = await self.client.check_video_status(
            at="at-token",
            operations=[
                {
                    "operation": {"name": operation_id},
                    "name": operation_id,
                    "mediaName": media_id,
                    "projectId": project_id,
                }
            ],
            google_cookies="SID=session-cookie",
        )

        operation = result["operations"][0]
        self.assertEqual(operation["status"], "MEDIA_GENERATION_STATUS_FAILED")
        self.assertEqual(
            operation["operation"]["error"]["code"],
            "PUBLIC_ERROR_PROMINENT_PEOPLE_FILTER_FAILED",
        )
        self.assertIn("人物安全过滤", operation["operation"]["error"]["message"])

    async def test_generate_video_start_end_uses_v2_payload_for_interpolation_lite(
        self,
    ):
        operation_id = "11111111-1111-1111-1111-111111111111"
        project_id = "22222222-2222-2222-2222-222222222222"
        media_id = "33333333-3333-3333-3333-333333333333"
        self.client._call_flow_frontend_rpc = AsyncMock(
            return_value=[[operation_id, project_id, media_id]]
        )

        await self.client.generate_video_start_end(
            at="at-token",
            project_id=project_id,
            prompt="变身猫猫",
            model_key="veo_3_1_interpolation_lite",
            aspect_ratio="VIDEO_ASPECT_RATIO_PORTRAIT",
            start_media_id="start-media",
            end_media_id="end-media",
            use_v2_model_config=True,
            google_cookies="SID=session-cookie",
        )

        rpc = self.client._call_flow_frontend_rpc.await_args.kwargs
        self.assertEqual(rpc["rpc_id"], "nprQif")
        request_data = rpc["argument"][0][0]
        self.assertEqual(request_data[0], [None, None, [[["变身猫猫"]]]])
        self.assertEqual(request_data[1], "veo_3_1_interpolation_lite")
        self.assertEqual(request_data[2], 1)
        self.assertEqual(request_data[4][1], "start-media")
        self.assertEqual(request_data[5][1], "end-media")


    def test_omni_1_2_flash_catalog_360p_tiers_and_resolver(self):
        import json
        from pathlib import Path
        from src.core.account_tiers import (
            PAYGATE_TIER_NOT_PAID,
            PAYGATE_TIER_ONE,
            get_required_paygate_tier_for_model,
        )
        from src.services.flow_frontend import FlowFrontendMixin

        listed = {item["id"] for item in _get_openai_model_catalog()}
        for expected in (
            "omni-1.2-flash",
            "omni-1.2-flash-i2v",
            "omni-1.2-flash-extend",
            "veo-3.1-lite",
            "veo-3.1-lite-i2v",
            "veo-3.1-lite-r2v",
            "veo-3.1-lite-extend",
        ):
            self.assertIn(expected, listed)

        for hidden in (
            "omni-1.2-flash-360p",
            "omni-1.2-flash-4s-landscape",
            "omni-1.2-flash-5s-landscape",
            "omni-1.2-flash-11s-landscape",
            "omni-1.2-flash-15s-portrait-360p",
            "veo-3.1-lite-8s-landscape",
            "macaron_cookie_t2v_8s",
            "macaron_cookie_t2v_8s_360p",
        ):
            self.assertNotIn(hidden, listed)
            self.assertIn(hidden, MODEL_CONFIG)

        self.assertEqual(
            resolve_model_name(
                "omni-1.2-flash",
                request=types.SimpleNamespace(
                    generationConfig=types.SimpleNamespace(
                        aspectRatio="portrait", imageSize="360p"
                    )
                ),
                model_config=MODEL_CONFIG,
            ),
            "omni-1.2-flash-8s-portrait-360p",
        )
        self.assertEqual(
            resolve_model_name(
                "omni-1.2-flash-15s-landscape",
                request=types.SimpleNamespace(
                    generationConfig=types.SimpleNamespace(resolution="360p")
                ),
                model_config=MODEL_CONFIG,
            ),
            "omni-1.2-flash-15s-landscape-360p",
        )
        self.assertEqual(
            resolve_model_name(
                "omni-1.2-flash-i2v",
                request=types.SimpleNamespace(
                    generationConfig=types.SimpleNamespace(
                        aspectRatio="portrait", resolution="360p"
                    )
                ),
                model_config=MODEL_CONFIG,
            ),
            "omni-1.2-flash-i2v-8s-portrait-360p",
        )

        self.assertEqual(
            get_required_paygate_tier_for_model("omni-1.2-flash-10s-landscape-360p"),
            PAYGATE_TIER_NOT_PAID,
        )
        self.assertEqual(
            get_required_paygate_tier_for_model("omni-1.2-flash-15s-landscape"),
            PAYGATE_TIER_ONE,
        )
        self.assertEqual(
            get_required_paygate_tier_for_model("omni-1.2-flash-extend-8s-portrait"),
            PAYGATE_TIER_ONE,
        )
        self.assertEqual(
            get_required_paygate_tier_for_model("macaron_cookie_extend_4s"),
            PAYGATE_TIER_ONE,
        )

        rpc_id_360, arg_360 = FlowFrontendMixin()._build_frontend_video_generation_argument(
            project_id="proj-1",
            prompt="ocean wave",
            model_key="macaron_cookie_t2v_4s_360p",
            aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
            mode="text",
            recaptcha_token="tok",
            session_id="sess-1",
            resolution="360p",
        )
        self.assertEqual(rpc_id_360, "YhhmEf")
        self.assertEqual(arg_360[0][0][7], [4])

        rpc_id_ref_360, arg_ref_360 = (
            FlowFrontendMixin()._build_frontend_video_generation_argument(
                project_id="proj-1",
                prompt="ocean wave",
                model_key="macaron_cookie_r2v_4s_360p",
                aspect_ratio="VIDEO_ASPECT_RATIO_PORTRAIT",
                mode="references",
                reference_media_ids=["m1"],
                recaptcha_token="tok",
                session_id="sess-1",
                resolution="360p",
            )
        )
        self.assertEqual(rpc_id_ref_360, "MZZa6b")
        self.assertEqual(arg_ref_360[0][0][11], [4])

        rpc_id_default, arg_default = (
            FlowFrontendMixin()._build_frontend_video_generation_argument(
                project_id="proj-1",
                prompt="ocean wave",
                model_key="macaron_cookie_t2v_4s",
                aspect_ratio="VIDEO_ASPECT_RATIO_LANDSCAPE",
                mode="text",
                recaptcha_token="tok",
                session_id="sess-1",
            )
        )
        self.assertEqual(rpc_id_default, "YhhmEf")
        self.assertEqual(arg_default[0][0][7], None)

        from src.core.account_tiers import PAYGATE_TIER_TWO, supports_model_for_tier

        self.assertTrue(
            supports_model_for_tier("omni-1.2-flash-8s-landscape-1080p", PAYGATE_TIER_TWO)
        )
        self.assertTrue(
            supports_model_for_tier("omni-1.2-flash-8s-landscape-4k", PAYGATE_TIER_TWO)
        )

        locales = json.loads(
            (Path(__file__).resolve().parents[1] / "static" / "locales" / "test.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertIn("Omni 1.2 Flash 视频", locales["zh-CN"])
        self.assertIn("Omni 1.2 Flash 视频", locales["en"])

if __name__ == "__main__":
    unittest.main()
