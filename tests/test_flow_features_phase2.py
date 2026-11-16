"""Unit and FastAPI contract tests for Phase 2: Flow Creation Agent & Settings."""

import asyncio
import json
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.api import flow_routes  # noqa: E402
from src.core.auth import verify_api_key_flexible  # noqa: E402
from src.services.flow_current import CurrentFlowClientMixin  # noqa: E402
from src.services.flow_features import FlowFeatureService  # noqa: E402
from src.services.flow_frontend import FlowFrontendMixin  # noqa: E402

PROJECT_ID = "52c3f6b4-bad4-4208-9b54-b1d54c996f77"
SESSION_ID = "88888888-1111-2222-3333-444444444444"
MEDIA_ID = "11111111-2222-3333-4444-555555555555"
CHAR_ID = "99999999-aaaa-bbbb-cccc-dddddddddddd"
WF_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


class _DummyPhase2FlowClient(CurrentFlowClientMixin, FlowFrontendMixin):
    def __init__(self):
        self.db = None
        self.mock_rpc = AsyncMock()
        self.mock_recaptcha = AsyncMock(return_value=("rec-chat-tok", "0:lease-chat-1"))
        self.mock_finished = AsyncMock()
        self.mock_stream_http = AsyncMock()

    def _get_runtime_config(self):
        return types.SimpleNamespace(captcha_method="http", browser_in_page_rpc=False)

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
        return await self.mock_rpc(
            rpc_id=rpc_id,
            argument=argument,
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
            source_path=source_path,
            timeout=timeout,
        )

    async def _frontend_cookie(self, google_cookies=None, token_id=None):
        return "SID=test"

    async def _get_recaptcha_token(
        self, project_id, action="IMAGE_GENERATION", token_id=None, website_url=None
    ):
        return await self.mock_recaptcha(
            project_id, action=action, token_id=token_id, website_url=website_url
        )

    async def _notify_browser_captcha_request_finished(
        self, browser_id, *, success=None
    ):
        return await self.mock_finished(browser_id, success=success)

    async def _call_flow_stream_chat_payload(
        self,
        *,
        project_id,
        f_request,
        cookie_header,
        timeout,
        cookie_storage=None,
    ):
        return await self.mock_stream_http(
            project_id=project_id,
            f_request=f_request,
            cookie_header=cookie_header,
            timeout=timeout,
            cookie_storage=cookie_storage,
        )


class FlowCurrentPhase2Tests(unittest.IsolatedAsyncioTestCase):
    def test_build_agent_user_message_with_attachments(self):
        client = _DummyPhase2FlowClient()
        plain = client._build_agent_user_message("Hello Flow")
        self.assertEqual(plain, [[[["Hello Flow"]]]])

        rich = client._build_agent_user_message(
            "Animate @Hero",
            image_refs=[{"mediaId": MEDIA_ID, "title": "ref1"}],
            character_refs=[{"id": CHAR_ID, "displayName": "Hero"}],
            workflow_refs=[WF_ID],
        )
        self.assertEqual(rich[0][0][0], ["Animate @Hero"])
        self.assertEqual(rich[0][0][1], [None, [[[MEDIA_ID, "ref1"]]]])
        self.assertEqual(rich[1], [[MEDIA_ID, "ref1"]])
        self.assertEqual(rich[2], [[CHAR_ID, "Hero"]])
        self.assertEqual(rich[3], [[WF_ID]])

        req_json = client._build_agent_stream_chat_request(
            project_id=PROJECT_ID,
            prompt="Approve",
            recaptcha_token="tok-abc",
            session_id=SESSION_ID,
            permission_action=1,
        )
        outer = json.loads(req_json)
        self.assertIsNone(outer[0])
        inner = json.loads(outer[1])
        self.assertEqual(inner[0], SESSION_ID)
        self.assertEqual(inner[1], [[[["Approve"]]]])
        self.assertEqual(inner[2][0], f"projects/{PROJECT_ID}")
        self.assertEqual(inner[2][2], ["tok-abc", 1])
        self.assertEqual(inner[4], 1)

    def test_decode_struct_a2ui_and_chunk_parsers(self):
        client = _DummyPhase2FlowClient()
        raw_struct = [
            [
                ["text", [None, None, "Hello world"]],
                ["count", [None, 2]],
                ["ok", [None, None, None, True]],
                ["tags", [None, None, None, None, None, [[[None, None, "a"]]]]],
            ]
        ]
        decoded = client._decode_proto_struct(raw_struct)
        self.assertEqual(
            decoded,
            {"text": "Hello world", "count": 2, "ok": True, "tags": ["a"]},
        )

        a2ui_raw = json.dumps(
            [
                {
                    "surfaceUpdate": {
                        "components": [
                            {
                                "component": {
                                    "Text": {
                                        "text": {"literalString": "Plan Title"},
                                        "usageHint": "h2",
                                    }
                                }
                            },
                            {
                                "component": {
                                    "MultipleChoice": {
                                        "options": [
                                            {"label": {"literalString": "Continue"}, "value": "go"}
                                        ]
                                    }
                                }
                            },
                        ]
                    }
                }
            ]
        )
        parsed_a2ui = client._parse_a2ui_payload(a2ui_raw)
        self.assertEqual(parsed_a2ui["text"], "## Plan Title")
        self.assertEqual(parsed_a2ui["options"], [{"label": "Continue", "value": "go"}])

        # Cumulative prefix merge deduplication (BOQ o$b)
        acc, d1 = client._merge_stream_text("", "Hello")
        acc, d2 = client._merge_stream_text(acc, "Hello world")
        acc, d3 = client._merge_stream_text(acc, "world")
        acc, d4 = client._merge_stream_text(acc, "!")
        self.assertEqual((d1, d2, d3, d4), ("Hello", " world", "", "!"))
        self.assertEqual(acc, "Hello world!")

        # WebChannel _.Dec event parsing (thinking, text, title, elicitation, media)
        dec_events = client._parse_webchannel_dec_event(
            [
                "evt-1",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                ["Checking scene...", False, 2],
                None,
                None,
                None,
                ["Sunset Reel"],
                ["perm-1", None, 20, ["IMAGE_GENERATION"], 1, 0],
            ]
        )
        kinds = [e["type"] for e in dec_events]
        self.assertEqual(kinds, ["thinking", "title", "permission"])
        self.assertEqual(dec_events[2]["totalCost"], 20)
        self.assertEqual(dec_events[2]["permissionId"], "perm-1")

    async def test_webchannel_request_buffer_envelopes_and_camoufox_approval(self):
        client = _DummyPhase2FlowClient()
        user_req = json.loads(
            client._build_agent_webchannel_request(
                project_id=PROJECT_ID,
                prompt="Draw sky",
                recaptcha_token="tok-wc-1",
                session_id=SESSION_ID,
            )
        )
        self.assertEqual(user_req[0], [SESSION_ID, [[[["Draw sky"]]]]])
        self.assertIsNone(user_req[2])
        self.assertEqual(user_req[5][0], f"projects/{PROJECT_ID}")
        self.assertEqual(user_req[5][2], ["tok-wc-1", 1])

        sticky_req = json.loads(
            client._build_agent_webchannel_request(
                project_id=PROJECT_ID,
                recaptcha_token="tok-wc-2",
                session_id=SESSION_ID,
                permission_id="perm-42",
                permission_action=2,
            )
        )
        self.assertIsNone(sticky_req[0])
        self.assertEqual(sticky_req[2], ["perm-42", True, 2])

        deny_req = json.loads(
            client._build_agent_webchannel_request(
                project_id=PROJECT_ID,
                recaptcha_token="tok-wc-3",
                session_id=SESSION_ID,
                permission_id="perm-42",
                permission_action=3,
            )
        )
        self.assertEqual(deny_req[2], ["perm-42", False])

        # Length-prefixed concatenated WebChannel stream with partial chunk boundary
        def _pb_str(val):
            return [None, None, val]

        def _pb_struct(pairs):
            return [None, None, None, None, [pairs]]

        def _pb_list(items):
            return [None, None, None, None, None, [items]]

        begin_msg = _pb_struct(
            [["beginRendering", _pb_struct([["surfaceId", _pb_str("main")], ["root", _pb_str("root-column")]])]]
        )[4]
        text_comp = _pb_struct(
            [
                ["id", _pb_str("text-1")],
                [
                    "component",
                    _pb_struct(
                        [["Text", _pb_struct([["text", _pb_struct([["literalString", _pb_str("Hello, Flow!")]])]])]]
                    ),
                ],
            ]
        )
        surf_msg = _pb_struct(
            [
                [
                    "surfaceUpdate",
                    _pb_struct([["surfaceId", _pb_str("main")], ["components", _pb_list([text_comp])]]),
                ]
            ]
        )[4]
        live_dec = [
            "evt-live-1",
            ["1794813245", 319487000],
            *[None] * 16,
            [[begin_msg, surf_msg]],
        ]
        frame1 = json.dumps([[3, [[SESSION_ID, None, [live_dec]]]]])
        frame2 = json.dumps([[4, [[SESSION_ID, 1]]]])
        wire = f"{len(frame1)}\n{frame1}{len(frame2)}\n{frame2}"
        envs1, rem = client._consume_webchannel_buffer(wire[:40])
        self.assertEqual(envs1, [])
        envs2, rem2 = client._consume_webchannel_buffer(rem + wire[40:])
        self.assertEqual(rem2, "")
        self.assertEqual(len(envs2), 2)
        parsed_wc, wc_done = client._parse_webchannel_envelopes(envs2)
        self.assertTrue(wc_done)
        self.assertEqual(parsed_wc[0]["type"], "a2ui")
        self.assertEqual(parsed_wc[0]["text"], "Hello, Flow!")

        # Camoufox stream_agent_chat closes stream immediately on wc_done and auto-resolves permission_id
        client._get_runtime_config = lambda: types.SimpleNamespace(
            captcha_method="camoufox", browser_in_page_rpc=True
        )
        captured_reqs = []
        closed_flag = False

        async def fake_stream_flow_agent_chat(*, project_id, request_builder, timeout=180, token_id=None):
            nonlocal closed_flag
            captured_reqs.append(json.loads(request_builder("tok-live")))
            try:
                yield {"transport": "webchannel", "status": 200, "delta": wire, "done": False}
                await asyncio.sleep(10)
            finally:
                closed_flag = True

        fake_service = types.SimpleNamespace(stream_flow_agent_chat=fake_stream_flow_agent_chat)
        client.mock_rpc.return_value = [
            [SESSION_ID, ["Gen Story"]],
            [
                [
                    [[[["Generate fox"]]]],
                    [],
                    [
                        [
                            "e-perm",
                            *[None] * 13,
                            ["perm-auto-99", None, 20, ["IMAGE_GENERATION"], 1, 0],
                        ]
                    ],
                ]
            ],
        ]
        with (
            patch("src.services.browser_captcha.BrowserCaptchaService._instance", fake_service),
            patch("src.services.browser_captcha.BrowserCaptchaService.get_instance", AsyncMock(return_value=fake_service)),
        ):
            res = await client.approve_agent_action(
                project_id=PROJECT_ID,
                session_id=SESSION_ID,
                action="approved",
                token_id=3,
            )
        self.assertTrue(closed_flag)
        self.assertEqual(res["text"], "Hello, Flow!")
        self.assertEqual(captured_reqs[0][2], ["perm-auto-99", True])
        self.assertEqual(res["endpoint"], "flowCreationAgent/streamCreationAgentChat")

        # WebChannel __sm__ gRPC error + close envelope raises RuntimeError immediately
        err_frame = json.dumps(
            [
                [
                    2,
                    {
                        "__sm__": {
                            "status": [
                                [
                                    [
                                        7,
                                        "reCAPTCHA evaluation failed",
                                        [["type.googleapis.com/google.rpc.ErrorInfo", ["PUBLIC_ERROR_UNUSUAL_ACTIVITY"]]],
                                    ]
                                ]
                            ]
                        }
                    },
                ],
                [3, ["close"]],
            ]
        )
        err_wire = f"14\n[[1,[\"noop\"]]]{len(err_frame)}\n{err_frame}"
        err_envs, err_rem = client._consume_webchannel_buffer(err_wire)
        self.assertEqual(err_rem, "")
        err_parsed, err_done = client._parse_webchannel_envelopes(err_envs)
        self.assertTrue(err_done)
        self.assertEqual(err_parsed[0]["type"], "error")
        self.assertIn("PUBLIC_ERROR_UNUSUAL_ACTIVITY", err_parsed[0]["message"])

        async def fake_err_stream(*, project_id, request_builder, timeout=180, token_id=None):
            yield {"transport": "webchannel", "status": 200, "delta": err_wire, "done": False}

        err_service = types.SimpleNamespace(stream_flow_agent_chat=fake_err_stream)
        with (
            patch("src.services.browser_captcha.BrowserCaptchaService._instance", err_service),
            patch("src.services.browser_captcha.BrowserCaptchaService.get_instance", AsyncMock(return_value=err_service)),
        ):
            with self.assertRaises(RuntimeError) as ctx_err:
                await client.agent_chat(
                    project_id=PROJECT_ID,
                    prompt="Fail test",
                    session_id=SESSION_ID,
                    token_id=3,
                )
            self.assertIn("PUBLIC_ERROR_UNUSUAL_ACTIVITY", str(ctx_err.exception))

    async def test_agent_chat_and_approve_action_http_fallback(self):
        client = _DummyPhase2FlowClient()
        frames = [
            [
                [
                    "wrb.fr",
                    None,
                    json.dumps(
                        [
                            [
                                [[["thought", [None, None, "Planning shot"]]]],
                                [
                                    ["e1", None, None, None, None, None, None, ["Desert Fox"]],
                                    [
                                        "e2",
                                        None,
                                        None,
                                        [
                                            "inv-1",
                                            "ask_for_permission",
                                            [[["total_cost", [None, 10]], ["num_images", [None, 1]]]],
                                        ],
                                    ],
                                ],
                            ]
                        ]
                    ),
                ],
                [
                    "wrb.fr",
                    None,
                    json.dumps(
                        [
                            [
                                [[["text", [None, None, "Ready to generate!"]]]],
                                [
                                    [
                                        "e3",
                                        None,
                                        None,
                                        None,
                                        [
                                            "inv-2",
                                            "generate_image",
                                            None,
                                            None,
                                            [[[["media_id", [None, None, MEDIA_ID]]]]],
                                        ],
                                    ]
                                ],
                            ]
                        ]
                    ),
                ],
            ]
        ]
        client.mock_stream_http.return_value = {"frames": frames, "responseText": ""}
        client.get_media = AsyncMock(
            return_value={
                "mediaId": MEDIA_ID,
                "mediaType": "image",
                "url": "https://flow-content.google/image/test.jpg",
            }
        )

        with patch("src.services.browser_captcha.BrowserCaptchaService._instance", None):
            res = await client.agent_chat(
                project_id=PROJECT_ID,
                prompt="Make a fox",
                session_id=SESSION_ID,
                token_id=3,
            )

        self.assertEqual(res["sessionId"], SESSION_ID)
        self.assertEqual(res["title"], "Desert Fox")
        self.assertEqual(res["thinking"], "Planning shot")
        self.assertEqual(res["text"], "Ready to generate!")
        self.assertEqual(res["pendingPermission"]["totalCost"], 10)
        self.assertEqual(res["media"][0]["url"], "https://flow-content.google/image/test.jpg")
        client.mock_recaptcha.assert_awaited_with(
            PROJECT_ID,
            action="CHAT_GENERATION",
            token_id=3,
            website_url=f"https://flow.google.com/project/{PROJECT_ID}",
        )
        client.mock_finished.assert_awaited_once_with("0:lease-chat-1", success=None)

        # Approval continuation
        client.mock_stream_http.reset_mock()
        with patch("src.services.browser_captcha.BrowserCaptchaService._instance", None):
            await client.approve_agent_action(
                project_id=PROJECT_ID,
                session_id=SESSION_ID,
                action="approved_sticky",
                token_id=3,
            )
        sent_freq = json.loads(client.mock_stream_http.call_args.kwargs["f_request"])
        inner = json.loads(sent_freq[1])
        self.assertEqual(inner[1], [[[["Always approve"]]]])
        self.assertEqual(inner[4], 2)

    async def test_session_and_settings_rpcs(self):
        client = _DummyPhase2FlowClient()

        async def rpc_side_effect(*, rpc_id, argument, **_kwargs):
            if rpc_id == "mrlkwd":
                self.assertEqual(argument, [PROJECT_ID, 25])
                return [[[SESSION_ID, ["My Story", None, [1794000000, 0]]]]]
            if rpc_id == "GN0Bre":
                self.assertEqual(argument, [SESSION_ID])
                return [
                    [SESSION_ID, ["My Story"]],
                    [
                        [
                            [[[["Draw a cat"]]]],
                            [[[[["text", [None, None, "Here is a cat."]]]], []]],
                            [],
                            [["p1", True, 1]],
                            2,
                        ]
                    ],
                ]
            if rpc_id == "TunYMc":
                self.assertEqual(argument, [SESSION_ID, "Renamed Story", PROJECT_ID])
                return []
            if rpc_id == "Dcn2Le":
                self.assertEqual(argument, [SESSION_ID])
                return []
            if rpc_id == "Yizz8d":
                return [None, "cl-42", ["ann-1"], None, None, True, False, True, None, False, None, None, True, True]
            if rpc_id == "DA4VGb":
                self.assertEqual(argument[0][13], False)
                self.assertEqual(argument[1], [["is_watermark_enabled_by_user"]])
                return [None, "cl-42", [], None, None, True, False, True, None, False, None, None, True, False]
            if rpc_id == "Zzl0ze":
                return [
                    [],
                    [],
                    [],
                    [],
                    None,
                    [],
                    None,
                    [["applet-1"], None, [[None, "IMAGE_ASPECT_RATIO_PORTRAIT", 2, "BELUGA"], ["veo_3_1", "VIDEO_ASPECT_RATIO_LANDSCAPE", 1]], 1, None, True],
                ]
            if rpc_id == "Kcr7Ub":
                self.assertEqual(argument[0], f"projects/{PROJECT_ID}")
                self.assertEqual(argument[1][3], 2)
                self.assertEqual(argument[2], [["credit_spend_approval_policy"]])
                return [[[], None, None, 2, None, True]]
            raise AssertionError(f"Unexpected rpc_id={rpc_id}")

        client.mock_rpc.side_effect = rpc_side_effect

        listed = await client.list_agent_sessions(PROJECT_ID, page_size=25)
        self.assertEqual(listed["sessions"][0]["title"], "My Story")

        sess = await client.get_agent_session(SESSION_ID, project_id=PROJECT_ID)
        self.assertEqual(sess["turns"][0]["user"]["text"], "Draw a cat")
        self.assertEqual(sess["turns"][0]["text"], "Here is a cat.")
        self.assertEqual(sess["turns"][0]["approvals"][0]["approved"], True)

        renamed = await client.rename_agent_session(SESSION_ID, "Renamed Story", project_id=PROJECT_ID)
        self.assertEqual(renamed["title"], "Renamed Story")

        deleted = await client.delete_agent_session(SESSION_ID)
        self.assertTrue(deleted["deleted"])

        prefs = await client.get_user_preferences()
        self.assertTrue(prefs["isWatermarkEnabledByUser"])
        self.assertEqual(prefs["lastAcknowledgedChangeLogId"], "cl-42")

        upd_prefs = await client.update_user_preferences({"is_watermark_enabled_by_user": False})
        self.assertFalse(upd_prefs["isWatermarkEnabledByUser"])

        proj_settings = await client.get_project_settings(PROJECT_ID)
        self.assertEqual(proj_settings["creditSpendApprovalPolicy"], 1)
        self.assertEqual(proj_settings["creditSpendApprovalPolicyName"], "always_ask")
        self.assertEqual(proj_settings["defaultGenerationSettings"]["imageDefaults"]["model"], "BELUGA")

        upd_proj = await client.update_project_settings(
            PROJECT_ID, {"credit_spend_approval_policy": "auto_approve"}
        )
        self.assertEqual(upd_proj["creditSpendApprovalPolicy"], 2)


class FlowAgentRoutesTests(unittest.TestCase):
    def setUp(self):
        self.flow_client = _DummyPhase2FlowClient()
        self.token = types.SimpleNamespace(
            id=3,
            email="agent@example.com",
            google_cookies="SID=test",
            current_project_id=PROJECT_ID,
            is_active=True,
            is_expired=False,
        )
        self.tm = types.SimpleNamespace(
            get_all_tokens=AsyncMock(return_value=[self.token]),
            ensure_valid_token=AsyncMock(return_value=self.token),
            record_success=AsyncMock(),
        )
        self.db = types.SimpleNamespace(
            get_token=AsyncMock(return_value=self.token),
        )
        self.svc = FlowFeatureService(
            flow_client=self.flow_client,
            token_manager=self.tm,
            load_balancer=None,
            db=self.db,
        )
        self.app = FastAPI()
        self.app.include_router(flow_routes.router)
        self.app.dependency_overrides[verify_api_key_flexible] = lambda: "test-key"
        self.prev_svc = flow_routes.flow_feature_service
        flow_routes.set_flow_feature_service(self.svc)
        self.client = TestClient(self.app)

    def tearDown(self):
        flow_routes.set_flow_feature_service(self.prev_svc)

    def test_sse_and_rest_routes(self):
        async def fake_stream(**kwargs):
            yield {"event": "session", "projectId": PROJECT_ID, "sessionId": SESSION_ID}
            yield {"event": "text", "delta": "Hi!", "text": "Hi!"}
            yield {
                "event": "done",
                "projectId": PROJECT_ID,
                "sessionId": SESSION_ID,
                "title": "Greeting",
                "text": "Hi!",
                "thinking": "",
                "a2ui": [],
                "toolCalls": [],
                "toolResults": [],
                "media": [],
                "pendingPermission": None,
                "errors": [],
                "endpoint": "FlowCreationAgentService/StreamChat",
            }

        self.flow_client.stream_agent_chat = fake_stream
        sse_resp = self.client.post(
            "/v1/flow/agent/chat",
            json={"prompt": "Hi", "stream": True, "token_id": 3},
        )
        self.assertEqual(sse_resp.status_code, 200)
        self.assertTrue(sse_resp.headers["content-type"].startswith("text/event-stream"))
        self.assertIn("event: text", sse_resp.text)
        self.assertIn('"delta": "Hi!"', sse_resp.text)
        self.assertIn("event: done", sse_resp.text)

        self.flow_client.mock_rpc.side_effect = [
            [[[SESSION_ID, ["Chat 1"]]]],
            [None, "cl-1", [], None, None, True, True, False, None, False, None, None, False, True],
        ]
        sess_resp = self.client.get("/v1/flow/agent/sessions", params={"token_id": 3})
        self.assertEqual(sess_resp.status_code, 200)
        self.assertEqual(sess_resp.json()["sessions"][0]["sessionId"], SESSION_ID)

        pref_resp = self.client.get("/v1/flow/user/preferences", params={"token_id": 3})
        self.assertEqual(pref_resp.status_code, 200)
        self.assertTrue(pref_resp.json()["isWatermarkEnabledByUser"])


if __name__ == "__main__":
    unittest.main()
