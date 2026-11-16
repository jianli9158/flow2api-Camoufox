import base64
import struct
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.api import flow_routes
from src.core.config import config
from src.core.models import Token
from src.services.flow_current import CurrentFlowClientMixin
from src.services.flow_features import FlowFeatureService
from src.services.flow_frontend import FlowFrontendMixin


class _DummyFlowClient(CurrentFlowClientMixin, FlowFrontendMixin):
    def __init__(self):
        self.db = None
        self.rpc_calls = []
        self.mock_rpc = AsyncMock()
        self.mock_recaptcha = AsyncMock(return_value=("recap-tok-123", "0:lease-1"))
        self.mock_finished = AsyncMock()

    @staticmethod
    def _get_runtime_config():
        return types.SimpleNamespace(
            flow_max_retries=1,
            flow_image_request_timeout=90,
            upsample_timeout=180,
        )

    @staticmethod
    def _get_control_plane_timeout() -> int:
        return 30

    async def _frontend_cookie(self, google_cookies=None, token_id=None) -> str:
        return "SID=abc"

    async def _get_recaptcha_token(
        self,
        project_id: str,
        action: str = "IMAGE_GENERATION",
        token_id=None,
        website_url=None,
        method_override=None,
    ):
        return await self.mock_recaptcha(
            project_id,
            action=action,
            token_id=token_id,
            website_url=website_url,
        )

    async def _notify_browser_captcha_request_finished(
        self, browser_id=None, *, success=None
    ):
        await self.mock_finished(browser_id, success=success)

    async def _current_rpc(
        self,
        *,
        rpc_id: str,
        argument,
        google_cookies=None,
        token_id=None,
        project_id=None,
        source_path=None,
        timeout=None,
    ):
        self.rpc_calls.append((rpc_id, argument))
        return await self.mock_rpc(
            rpc_id=rpc_id,
            argument=argument,
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
            source_path=source_path,
            timeout=timeout,
        )


PROJECT_ID = "52c3f6b4-bad4-4208-9b54-b1d54c996f77"
MEDIA_ID = "8e7bf974-e4a8-4448-b591-1f590cc8f52e"
WORKFLOW_ID = "ea10f113-8dd3-45d1-b359-d516d1992769"
CHAR_ID = "fed138b8-aafe-4fab-ba5a-9cba2aa32e47"
AUDIO_URL = f"https://flow-content.google/audio/{MEDIA_ID}?Expires=1794829273&Signature=abc"


class FlowCurrentPhase1Tests(unittest.IsolatedAsyncioTestCase):
    def test_pcm_to_wav_wraps_valid_riff_header_and_trims_partial_sample(self):
        # 5 bytes -> trimmed to 4 bytes (2 samples of 16-bit mono PCM)
        wav = CurrentFlowClientMixin.pcm_to_wav(b"\x01\x02\x03\x04\x05")
        self.assertEqual(wav[:4], b"RIFF")
        self.assertEqual(wav[8:16], b"WAVEfmt ")
        riff_size = struct.unpack("<I", wav[4:8])[0]
        channels, sample_rate, byte_rate, block_align, bits = struct.unpack(
            "<HIIHH", wav[22:36]
        )
        data_tag, data_size = struct.unpack("<4sI", wav[36:44])
        self.assertEqual(channels, 1)
        self.assertEqual(sample_rate, 24000)
        self.assertEqual(byte_rate, 48000)
        self.assertEqual(block_align, 2)
        self.assertEqual(bits, 16)
        self.assertEqual(data_tag, b"data")
        self.assertEqual(data_size, 4)
        self.assertEqual(riff_size, 36 + 4)
        self.assertEqual(wav[44:], b"\x01\x02\x03\x04")

    async def test_get_project_normalizes_voices_characters_and_workflows(self):
        client = _DummyFlowClient()
        client.mock_rpc.return_value = [
            [],
            [
                [
                    WORKFLOW_ID,
                    None,
                    None,
                    ["Narrator Voice", [1794807662, 0], None, None, MEDIA_ID],
                    PROJECT_ID,
                ]
            ],
            [
                [
                    MEDIA_ID,
                    PROJECT_ID,
                    WORKFLOW_ID,
                    None,
                    None,
                    [[1794807662, 0], None, None, None, None, None, None, None, None, 1, AUDIO_URL],
                    None,
                    None,
                    None,
                    None,
                    [
                        [
                            None,
                            None,
                            None,
                            None,
                            None,
                            AUDIO_URL,
                            "Hello world",
                            "Warm",
                            "Notes",
                            "models/gemini-v4s-tts-flow",
                            "audio/wav",
                            [["Achernar", "Narrator Voice"]],
                        ]
                    ],
                ]
            ],
            [
                [
                    "achernar",
                    3,
                    "Achernar",
                    [
                        "achernar",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        [["Achernar", "Female, soft, high pitch", True, "https://gstatic.com/Achernar.wav"]],
                    ],
                ]
            ],
            None,
            [
                [
                    PROJECT_ID,
                    CHAR_ID,
                    None,
                    [1, "Active Hero", [[["img-media-1"]], [[None, "aoede"]], "Friendly guide"], False, False],
                    None,
                    None,
                    [1794807197, 0],
                    [1794807198, 0],
                ],
                [
                    PROJECT_ID,
                    "00000000-0000-0000-0000-000000000099",
                    None,
                    [1, "Archived Char", [[], []], False, True],
                    None,
                    None,
                    [1794807197, 0],
                    [1794807198, 0],
                ],
            ],
        ]

        data = await client.get_project(PROJECT_ID, token_id=9)

        self.assertEqual(data["projectId"], PROJECT_ID)
        self.assertEqual(len(data["presetVoices"]), 1)
        self.assertEqual(data["presetVoices"][0]["id"], "achernar")
        self.assertEqual(data["presetVoices"][0]["name"], "Achernar")
        self.assertEqual(data["presetVoices"][0]["sampleUrl"], "https://gstatic.com/Achernar.wav")
        self.assertEqual(len(data["customVoices"]), 1)
        self.assertEqual(data["customVoices"][0]["name"], "Narrator Voice")
        self.assertEqual(data["customVoices"][0]["workflowId"], WORKFLOW_ID)
        self.assertEqual(data["customVoices"][0]["audioUrl"], AUDIO_URL)
        self.assertEqual(len(data["characters"]), 1)
        self.assertEqual(data["characters"][0]["id"], CHAR_ID)
        self.assertEqual(data["characters"][0]["voiceId"], "aoede")
        self.assertEqual(data["characters"][0]["imageRefs"], ["img-media-1"])
        self.assertEqual(data["characters"][0]["personalityNotes"], "Friendly guide")

    async def test_generate_voice_audio_normalizes_case_and_saves_when_requested(self):
        client = _DummyFlowClient()

        async def rpc_side_effect(*, rpc_id, argument, **_kwargs):
            if rpc_id == "no0P6":
                return [
                    [[MEDIA_ID, PROJECT_ID, WORKFLOW_ID]],
                    [[WORKFLOW_ID, None, None, [None, [1794807662, 0], None, None, MEDIA_ID], PROJECT_ID]],
                ]
            if rpc_id == "as29s":
                return [MEDIA_ID, PROJECT_ID, WORKFLOW_ID, None, None, [None, None, None, None, None, None, None, None, None, 12, AUDIO_URL]]
            if rpc_id in {"lt8g5", "mYWVGd"}:
                return []
            raise AssertionError(f"Unexpected rpc_id={rpc_id}")

        client.mock_rpc.side_effect = rpc_side_effect

        res = await client.generate_voice_audio(
            project_id=PROJECT_ID,
            text="Hello Google Flow!",
            voice="achernar",
            style="Calm narrator",
            name="Studio Achernar",
            save=True,
            token_id=9,
        )

        self.assertEqual(res["mediaId"], MEDIA_ID)
        self.assertEqual(res["workflowId"], WORKFLOW_ID)
        self.assertEqual(res["voice"], "Achernar")
        self.assertEqual(res["name"], "Studio Achernar")
        self.assertTrue(res["saved"])
        self.assertEqual(res["audioUrl"], AUDIO_URL)

        client.mock_recaptcha.assert_awaited_once()
        self.assertEqual(client.mock_recaptcha.await_args.kwargs["action"], "AUDIO_GENERATION")
        client.mock_finished.assert_awaited_once_with("0:lease-1", success=True)

        no0_arg = client.rpc_calls[0][1]
        self.assertEqual(no0_arg[0][0][1], [["Achernar", "Studio Achernar"]])
        self.assertEqual(no0_arg[0][0][2], "gemini_v4s_tts_flow")
        self.assertEqual(no0_arg[1][10], ["recap-tok-123", 1])
        self.assertEqual([rpc for rpc, _ in client.rpc_calls], ["no0P6", "as29s", "lt8g5", "mYWVGd"])

    async def test_generate_voice_audio_releases_lease_on_error(self):
        client = _DummyFlowClient()
        client.mock_rpc.side_effect = RuntimeError("Flow frontend RPC rejected: rpc=no0P6, code=[3]")

        with self.assertRaises(RuntimeError):
            await client.generate_voice_audio(
                project_id=PROJECT_ID,
                text="Test error release",
                voice="Achernar",
                token_id=9,
            )

        client.mock_finished.assert_awaited_once_with("0:lease-1", success=False)

    async def test_generate_voice_audio_retries_once_on_unusual_activity(self):
        client = _DummyFlowClient()
        client.mock_recaptcha = AsyncMock(
            side_effect=[("tok-1", "0:lease-1"), ("tok-2", "0:lease-2")]
        )
        client._notify_browser_captcha_error = AsyncMock()
        calls = 0

        async def rpc_side_effect(*, rpc_id, argument, **_kwargs):
            nonlocal calls
            if rpc_id == "no0P6":
                calls += 1
                if calls == 1:
                    raise RuntimeError(
                        "Flow frontend RPC rejected: rpc=no0P6, error=PUBLIC_ERROR_UNUSUAL_ACTIVITY"
                    )
                return [[[MEDIA_ID, PROJECT_ID, WORKFLOW_ID]], [[WORKFLOW_ID]]]
            if rpc_id == "as29s":
                return [MEDIA_ID, PROJECT_ID, WORKFLOW_ID, None, None, [None, None, None, None, None, None, None, None, None, 1, AUDIO_URL]]
            raise AssertionError(f"Unexpected rpc_id={rpc_id}")

        client.mock_rpc.side_effect = rpc_side_effect
        res = await client.generate_voice_audio(
            project_id=PROJECT_ID,
            text="Retry TTS",
            voice="Achernar",
            token_id=9,
        )
        self.assertEqual(res["mediaId"], MEDIA_ID)
        self.assertEqual(res["audioUrl"], AUDIO_URL)
        self.assertEqual(client.mock_recaptcha.await_count, 2)
        client._notify_browser_captcha_error.assert_awaited_once()
        self.assertEqual(
            client.mock_finished.await_args_list[0].kwargs["success"], False
        )
        self.assertEqual(
            client.mock_finished.await_args_list[1].kwargs["success"], True
        )

    async def test_character_crud_uses_exact_rpcs_and_never_calls_duplicate_on_delete(self):
        client = _DummyFlowClient()

        async def rpc_side_effect(*, rpc_id, argument, **_kwargs):
            if rpc_id == "C4BZMd":
                return [[PROJECT_ID, CHAR_ID, None, [1, "Hero", [[], []]], None, None, [1794807197, 0], [1794807197, 0]]]
            if rpc_id == "rzMKMb":
                return [[PROJECT_ID, CHAR_ID, None, [1, "Hero v2", [[["img-1"]], [[None, "aoede"]], "Brave"], False, False], None, None, [1794807197, 0], [1794807198, 0]]]
            if rpc_id == "Sc7aEb":
                return [["slot-media-1", PROJECT_ID, "slot-wf-1"]]
            if rpc_id == "LXpojc":
                return [[PROJECT_ID, CHAR_ID, None, [1, "Hero v2", [[], [[None, "aoede"]], "Brave"], False, False]]]
            if rpc_id == "wDg3C":
                return [[[[PROJECT_ID, CHAR_ID, None, [1, "Hero v2", [[["img-1"]], [[None, "aoede"]], "Brave"], False, False]]]]]
            if rpc_id == "cz8Z4b":
                return []

        client.mock_rpc.side_effect = rpc_side_effect

        created = await client.create_character(
            project_id=PROJECT_ID,
            display_name="Hero",
            voice_id="Aoede",
            personality_notes="Brave",
            image_refs=["img-1"],
            token_id=9,
        )
        self.assertEqual(created["id"], CHAR_ID)
        self.assertEqual(created["voiceId"], "aoede")
        self.assertEqual(created["imageRefs"], ["img-1"])

        fetched = await client.get_characters([CHAR_ID], project_id=PROJECT_ID, token_id=9)
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]["displayName"], "Hero v2")

        deleted = await client.delete_character(project_id=PROJECT_ID, entity_id=CHAR_ID, token_id=9)
        self.assertTrue(deleted["deleted"])

        called_rpcs = [rpc for rpc, _ in client.rpc_calls]
        self.assertNotIn("YIBQMe", called_rpcs)
        self.assertEqual(called_rpcs, ["C4BZMd", "rzMKMb", "Sc7aEb", "wDg3C", "wDg3C", "rzMKMb", "cz8Z4b"])
        update_arg = client.rpc_calls[1][1]
        self.assertEqual(update_arg[0][3][2][1], [[None, "aoede"]])
        self.assertEqual(update_arg[1], [["entity_info.character_info.audio_references", "entity_info.character_info.personality_notes"]])
        slot_arg = client.rpc_calls[2][1]
        self.assertEqual(slot_arg[0], "img-1")
        self.assertEqual(slot_arg[7], [None, None, [CHAR_ID, [0]]])
        archive_arg = client.rpc_calls[5][1]
        self.assertEqual(archive_arg[0][3][4], True)
        self.assertEqual(archive_arg[1], [["entity_info.archived"]])
        delete_arg = client.rpc_calls[6][1]
        self.assertEqual(delete_arg, [None, None, PROJECT_ID, [CHAR_ID]])
    async def test_delete_voice_archives_then_deletes_workflow(self):
        client = _DummyFlowClient()
        client.mock_rpc.return_value = []

        res = await client.delete_voice(
            project_id=PROJECT_ID,
            workflow_id=WORKFLOW_ID,
            token_id=9,
        )
        self.assertTrue(res["deleted"])
        self.assertEqual([rpc for rpc, _ in client.rpc_calls], ["pGCYOe", "cz8Z4b"])
        self.assertEqual(client.rpc_calls[0][1][1], [["metadata.archived"]])
        self.assertEqual(client.rpc_calls[1][1], [None, [WORKFLOW_ID], PROJECT_ID])


class FlowRoutesPhase1Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.token = Token(
            id=9,
            st="frontend-abc",
            at="flow-frontend-cookie",
            email="demo@example.com",
            is_active=True,
            current_project_id=PROJECT_ID,
            google_cookies="SID=abc",
        )
        self.flow_client = _DummyFlowClient()
        self.db = MagicMock()
        self.db.get_token = AsyncMock(return_value=self.token)
        self.db.get_all_tokens = AsyncMock(return_value=[self.token])
        self.db.get_active_tokens = AsyncMock(return_value=[self.token])

        self.token_manager = MagicMock()
        self.token_manager.ensure_valid_token = AsyncMock(return_value=self.token)
        self.token_manager.ensure_project_exists = AsyncMock(return_value=PROJECT_ID)

        self.load_balancer = MagicMock()
        self.load_balancer.select_token = AsyncMock(return_value=self.token)

        self.service = FlowFeatureService(
            self.flow_client,
            self.token_manager,
            self.load_balancer,
            self.db,
        )
        flow_routes.set_flow_feature_service(self.service)

        self.app = FastAPI()
        self.app.include_router(flow_routes.router)
        self._orig_api_key = config.api_key
        config.api_key = "test-secret-key"

    def tearDown(self):
        config.api_key = self._orig_api_key
        flow_routes.set_flow_feature_service(None)
    async def test_auth_required_and_endpoints_work_end_to_end(self):
        transport = ASGITransport(app=self.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            unauth = await client.get("/v1/flow/voices")
            self.assertEqual(unauth.status_code, 401)

            headers = {"Authorization": "Bearer test-secret-key"}

            # 1. GET /v1/flow/assets/{media_id}
            self.flow_client.mock_rpc.return_value = [
                MEDIA_ID,
                PROJECT_ID,
                WORKFLOW_ID,
                None,
                None,
                [None, None, None, None, None, None, None, None, None, 1, AUDIO_URL],
            ]
            asset_resp = await client.get(f"/v1/flow/assets/{MEDIA_ID}", headers=headers)
            self.assertEqual(asset_resp.status_code, 200)
            self.assertEqual(asset_resp.json()["url"], AUDIO_URL)
            self.assertEqual(asset_resp.json()["mediaType"], "audio")

            # 2. POST /v1/flow/assets (base64 upload + SSRF block)
            ssrf_resp = await client.post(
                "/v1/flow/assets",
                headers=headers,
                json={"image_url": "http://127.0.0.1:8001/secret.png"},
            )
            self.assertEqual(ssrf_resp.status_code, 400)

            self.flow_client.upload_image = AsyncMock(return_value="uploaded-img-uuid")
            up_resp = await client.post(
                "/v1/flow/assets",
                headers=headers,
                json={
                    "image_base64": "data:image/png;base64," + base64.b64encode(b"fake-png").decode(),
                    "email": "demo@example.com",
                },
            )
            self.assertEqual(up_resp.status_code, 200)
            self.assertEqual(up_resp.json()["mediaId"], "uploaded-img-uuid")

            # 3. POST /v1/flow/voices? response_format=wav
            self.flow_client.generate_voice_audio = AsyncMock(
                return_value={
                    "mediaId": MEDIA_ID,
                    "workflowId": WORKFLOW_ID,
                    "projectId": PROJECT_ID,
                    "voice": "Achernar",
                    "name": "Achernar",
                    "style": "",
                    "model": "gemini_v4s_tts_flow",
                    "saved": False,
                    "audioUrl": AUDIO_URL,
                    "format": "pcm_24khz_16bit_mono",
                    "frontendRpc": "no0P6",
                }
            )
            with patch.object(
                self.service, "_download_binary", AsyncMock(return_value=b"\x01\x00\x02\x00")
            ):
                wav_resp = await client.post(
                    "/v1/flow/voices",
                    headers=headers,
                    json={"text": "Hello WAV", "voice": "achernar", "response_format": "wav"},
                )
            self.assertEqual(wav_resp.status_code, 200)
            self.assertEqual(wav_resp.headers["content-type"], "audio/wav")
            self.assertTrue(wav_resp.content.startswith(b"RIFF"))
            self.assertEqual(wav_resp.headers["x-flow-media-id"], MEDIA_ID)

            # 4. Characters CRUD routes
            char_obj = {
                "id": CHAR_ID,
                "projectId": PROJECT_ID,
                "collectionId": None,
                "displayName": "Aria",
                "voiceId": "aoede",
                "voiceIds": ["aoede"],
                "imageRefs": [],
                "personalityNotes": "Curious",
                "favorited": False,
                "archived": False,
                "createdAt": None,
                "updatedAt": None,
            }
            self.flow_client.create_character = AsyncMock(return_value=char_obj)
            self.flow_client.get_characters = AsyncMock(return_value=[char_obj])
            self.flow_client.update_character = AsyncMock(return_value={**char_obj, "displayName": "Aria v2"})
            self.flow_client.delete_character = AsyncMock(return_value={"deleted": True, "projectId": PROJECT_ID, "characterId": CHAR_ID})

            c_create = await client.post(
                "/v1/flow/characters",
                headers=headers,
                json={"display_name": "Aria", "voice_id": "aoede", "personality_notes": "Curious"},
            )
            self.assertEqual(c_create.status_code, 200)
            self.assertEqual(c_create.json()["id"], CHAR_ID)

            c_get = await client.get(f"/v1/flow/characters/{CHAR_ID}", headers=headers)
            self.assertEqual(c_get.status_code, 200)
            self.assertEqual(c_get.json()["displayName"], "Aria")

            c_patch = await client.patch(
                f"/v1/flow/characters/{CHAR_ID}",
                headers=headers,
                json={"display_name": "Aria v2"},
            )
            self.assertEqual(c_patch.status_code, 200)
            self.assertEqual(c_patch.json()["displayName"], "Aria v2")

            c_del = await client.delete(f"/v1/flow/characters/{CHAR_ID}", headers=headers)
            self.assertEqual(c_del.status_code, 200)
            self.assertTrue(c_del.json()["deleted"])


if __name__ == "__main__":
    unittest.main()
