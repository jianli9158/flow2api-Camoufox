"""Cookie continuity across Flow bootstrap, RPC submission and XSRF retries."""

import asyncio
import contextvars
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs

import httpx
from curl_cffi import CurlInfo, CurlOpt
from curl_cffi.requests.cookies import CurlMorsel
from fastapi import FastAPI

from src.api import admin
from src.core.database import Database
from src.core.models import Token
from src.services.browser_cookie_utils import serialize_cookie_header_for_url
from src.services.flow_client import FlowClient
from src.services.protocol_login import _cookie_jar, _export_cookie_jar
from src.services.token_manager import TokenManager


BOOTSTRAP = (
    "boq_labs-ai-sandbox-frontend_20261007.14_p0 "
    '{"FdrFJe":"123", "SNlM0e":"bootstrap-xsrf"}'
)
RPC_SUCCESS = json.dumps([["wrb.fr", "nzlxg", "[120]"]])
RPC_UNAUTHORIZED = json.dumps([["er", None, None, None, None, 401]])


def _response(text, cookies, status=200):
    jar = _cookie_jar(json.dumps(cookies))
    return types.SimpleNamespace(
        text=text,
        url="https://flow.google.com/projects",
        status_code=status,
        headers={},
        infos={
            CurlInfo.COOKIELIST: [
                CurlMorsel.from_cookiejar_cookie(cookie).to_curl_format().encode()
                for cookie in jar
            ]
        },
    )


class FlowRpcCookieTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = FlowClient(proxy_manager=None)
        self.cookies = [
            {"name": "SID", "value": "account", "domain": ".google.com", "path": "/"},
            {
                "name": "OSID",
                "value": "session-old",
                "domain": "flow.google.com",
                "hostOnly": True,
                "path": "/",
            },
        ]
        self.requests = []
        self.sessions = []
        self.respond = AsyncMock()
        for module in ("flow_frontend", "flow_client"):
            self.enterContext(
                patch(
                    f"src.services.{module}.AsyncSession",
                    side_effect=self.make_session,
                )
            )

    def make_session(self, **session_kwargs):
        session = MagicMock()
        session.__aenter__.return_value = session
        session.curl_options = session_kwargs.setdefault("curl_options", {})
        self.sessions.append(session)

        async def request(method, url, **kwargs):
            headers = dict(kwargs["headers"])
            jar = session_kwargs.get("cookies")
            cookie_header = headers.get("Cookie", "")
            if jar is not None and "Cookie" not in headers:
                cookie_header = serialize_cookie_header_for_url(
                    _export_cookie_jar(jar, ""), url
                )
            extra = session_kwargs.get("curl_options", {}).get(CurlOpt.COOKIE, "")
            if extra:
                cookie_header = "; ".join(filter(None, (cookie_header, extra)))
            recorded = dict(
                method=method,
                url=url,
                cookie_header=cookie_header,
                headers=headers,
                body=kwargs.get("data", ""),
            )
            self.requests.append(recorded)
            return await self.respond(recorded)

        async def get(url, **kwargs):
            return await request("GET", url, **kwargs)

        async def post(url, **kwargs):
            return await request("POST", url, **kwargs)

        session.get = AsyncMock(side_effect=get)
        session.post = AsyncMock(side_effect=post)
        return session

    async def call_rpc(self, cookies=None):
        return await self.client.st_to_at(
            google_cookies=json.dumps(self.cookies if cookies is None else cookies)
        )

    async def test_bootstrap_cookie_rotation_deletion_and_scopes_reach_rpc(self):
        self.cookies.extend(
            [
                {
                    "name": "OSID",
                    "value": "parent",
                    "domain": ".google.com",
                    "path": "/",
                },
                {
                    "name": "removed",
                    "value": "old",
                    "domain": "flow.google.com",
                    "path": "/",
                },
                {
                    "name": "page",
                    "value": "only",
                    "domain": "flow.google.com",
                    "path": "/projects",
                },
                {
                    "name": "other",
                    "value": "private",
                    "domain": "accounts.google.com",
                    "hostOnly": True,
                    "path": "/",
                },
            ]
        )
        updated = [
            dict(cookie) for cookie in self.cookies if cookie["name"] != "removed"
        ]
        updated[1]["value"] = "session-new"
        updated.append(
            {"name": "rpc", "value": "only", "domain": "flow.google.com", "path": "/_/"}
        )
        self.respond.side_effect = [
            _response(BOOTSTRAP, updated),
            _response(RPC_SUCCESS, updated),
        ]

        result = await self.call_rpc()

        self.assertEqual(result["credits"], 120)
        bootstrap, rpc = self.requests
        self.assertIn("OSID=session-old", bootstrap["cookie_header"])
        self.assertIn("page=only", bootstrap["cookie_header"])
        self.assertIn("OSID=session-new", rpc["cookie_header"])
        self.assertIn("OSID=parent", rpc["cookie_header"])
        self.assertIn("SID=account", rpc["cookie_header"])
        self.assertIn("rpc=only", rpc["cookie_header"])
        for unwanted in ("session-old", "removed=", "page=", "other="):
            self.assertNotIn(unwanted, rpc["cookie_header"])
        self.assertEqual(self.cookies[1]["value"], "session-old")
        for session in self.sessions:
            session.__aexit__.assert_awaited_once()

    async def test_xsrf_retry_uses_cookies_rotated_and_deleted_by_first_rpc(self):
        bootstrap_cookies = [dict(cookie) for cookie in self.cookies]
        bootstrap_cookies[1]["value"] = "bootstrap-session"
        bootstrap_cookies.append(
            {
                "name": "removed",
                "value": "old",
                "domain": "flow.google.com",
                "path": "/",
            }
        )
        retry_cookies = [dict(cookie) for cookie in self.cookies]
        retry_cookies[1]["value"] = "retry-session"
        self.respond.side_effect = [
            _response(BOOTSTRAP, bootstrap_cookies),
            _response('[["xsrf","replacement-xsrf"]]', retry_cookies),
            _response(RPC_SUCCESS, retry_cookies),
        ]

        await self.call_rpc()

        first, retry = self.requests[1:]
        self.assertIn("OSID=bootstrap-session", first["cookie_header"])
        self.assertIn("removed=old", first["cookie_header"])
        self.assertIn("OSID=retry-session", retry["cookie_header"])
        self.assertNotIn("removed=", retry["cookie_header"])
        self.assertEqual(parse_qs(first["body"])["at"], ["bootstrap-xsrf"])
        self.assertEqual(parse_qs(retry["body"])["at"], ["replacement-xsrf"])
        self.assertEqual(
            parse_qs(first["body"])["f.req"], parse_qs(retry["body"])["f.req"]
        )

    async def test_empty_cookie_snapshot_does_not_restore_deleted_credentials(self):
        self.respond.side_effect = [_response(BOOTSTRAP, [])]

        with self.assertRaisesRegex(RuntimeError, "requires Google session cookies"):
            await self.call_rpc()

        self.assertEqual(len(self.requests), 1)
        self.sessions[0].__aexit__.assert_awaited_once()

    async def test_upstream_auth_rejection_is_preserved_without_retry(self):
        self.respond.side_effect = [
            _response(BOOTSTRAP, self.cookies),
            _response(RPC_UNAUTHORIZED, self.cookies),
        ]

        with self.assertRaisesRegex(RuntimeError, "rpc=nzlxg, code=401"):
            await self.call_rpc()

        self.assertEqual(len(self.requests), 2)
        for session in self.sessions:
            session.__aexit__.assert_awaited_once()

    async def test_plain_cookie_header_uses_refreshed_session(self):
        updated = [dict(cookie) for cookie in self.cookies]
        updated[1]["value"] = "session-new"
        self.respond.side_effect = [
            _response(BOOTSTRAP, updated),
            _response(RPC_SUCCESS, updated),
        ]

        await self.client.st_to_at(google_cookies="OSID=session-old; SID=account")

        self.assertIn("OSID=session-new", self.requests[1]["cookie_header"])
        self.assertNotIn("OSID=session-old", self.requests[1]["cookie_header"])

    async def test_concurrent_accounts_keep_separate_cookie_snapshots(self):
        first_bootstrap = asyncio.Event()
        second_bootstrap = asyncio.Event()

        async def respond(request):
            account = "first" if "SID=first" in request["cookie_header"] else "second"
            updated = [dict(cookie) for cookie in self.cookies]
            updated[0]["value"] = account
            updated[1]["value"] = f"{account}-new"
            if request["method"] == "GET":
                if account == "first":
                    first_bootstrap.set()
                    await second_bootstrap.wait()
                else:
                    await first_bootstrap.wait()
                    second_bootstrap.set()
                return _response(BOOTSTRAP, updated)
            self.assertIn(f"OSID={account}-new", request["cookie_header"])
            return _response(RPC_SUCCESS, updated)

        self.respond.side_effect = respond
        accounts = []
        for account in ("first", "second"):
            cookies = [dict(cookie) for cookie in self.cookies]
            cookies[0]["value"] = account
            cookies[1]["value"] = f"{account}-old"
            accounts.append(cookies)

        results = await asyncio.wait_for(
            asyncio.gather(*(self.call_rpc(cookies) for cookies in accounts)), timeout=2
        )

        self.assertEqual([result["credits"] for result in results], [120, 120])
        for result, account in zip(results, ("first", "second")):
            self.assertIn(
                f"OSID={account}-new",
                serialize_cookie_header_for_url(result["google_cookies"]),
            )

    async def test_stream_chat_uses_bootstrap_cookie_snapshot(self):
        updated = [dict(cookie) for cookie in self.cookies]
        updated[1]["value"] = "session-new"
        self.respond.side_effect = [
            _response(BOOTSTRAP, updated),
            _response("[]", updated),
        ]

        await self.client._call_flow_stream_chat(
            project_id="01234567-89ab-cdef-0123-456789abcdef",
            prompt="A coastal landscape",
            recaptcha_token="captcha-token",
            session_id="session-id",
            cookie_header="",
            cookie_storage=json.dumps(self.cookies),
            timeout=10,
        )

        self.assertIn("OSID=session-new", self.requests[1]["cookie_header"])
        self.assertNotIn("OSID=session-old", self.requests[1]["cookie_header"])

    async def test_legacy_duplicate_names_survive_bootstrap_and_rpc(self):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        self.respond.side_effect = [
            _response(BOOTSTRAP, snapshot),
            _response(RPC_SUCCESS, snapshot),
        ]

        await self.client.st_to_at(
            google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
        )

        for request in self.requests:
            self.assertEqual(request["cookie_header"].count("OSID="), 2)
            self.assertIn("OSID=domain-session", request["cookie_header"])
            self.assertIn("OSID=flow-session", request["cookie_header"])

    async def test_rotation_replaces_all_legacy_duplicates_for_that_scope(self):
        updated = [dict(cookie) for cookie in self.cookies]
        updated[1]["value"] = "rotated-session"
        self.respond.side_effect = [
            _response(BOOTSTRAP, updated),
            _response(RPC_SUCCESS, updated),
        ]

        result = await self.client.st_to_at(
            google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
        )

        self.assertEqual(self.requests[0]["cookie_header"].count("OSID="), 2)
        self.assertEqual(self.requests[1]["cookie_header"].count("OSID="), 1)
        self.assertIn("OSID=rotated-session", self.requests[1]["cookie_header"])
        self.assertIn("rotated-session", result["google_cookies"])

    async def test_exported_legacy_duplicates_survive_the_next_validation(self):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        self.respond.side_effect = [
            _response(BOOTSTRAP, snapshot),
            _response(RPC_SUCCESS, snapshot),
            _response(BOOTSTRAP, snapshot),
            _response(RPC_SUCCESS, snapshot),
        ]

        result = await self.client.st_to_at(
            google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
        )
        await self.client.st_to_at(google_cookies=result["google_cookies"])

        for request in self.requests:
            self.assertEqual(request["cookie_header"].count("OSID="), 2)

    async def test_same_name_set_cookie_at_another_path_keeps_legacy_pairs(self):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        snapshot.append(
            {
                "name": "OSID",
                "value": "rpc-session",
                "domain": "flow.google.com",
                "hostOnly": True,
                "path": "/_/",
            }
        )
        bootstrap = _response(BOOTSTRAP, snapshot)
        bootstrap.headers["set-cookie"] = "OSID=rpc-session; Path=/_/; Secure"
        self.respond.side_effect = [bootstrap, _response(RPC_SUCCESS, snapshot)]

        await self.client.st_to_at(
            google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
        )

        rpc = self.requests[1]["cookie_header"]
        self.assertEqual(rpc.count("OSID="), 3)
        for value in ("domain-session", "flow-session", "rpc-session"):
            self.assertIn("OSID=" + value, rpc)

    async def test_explicit_set_cookie_replaces_legacy_pairs_even_if_value_is_unchanged(
        self,
    ):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        bootstrap = _response(BOOTSTRAP, snapshot)
        bootstrap.headers["set-cookie"] = "OSID=domain-session; Path=/; Secure"
        self.respond.side_effect = [bootstrap, _response(RPC_SUCCESS, snapshot)]

        await self.client.st_to_at(
            google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
        )

        self.assertEqual(self.requests[1]["cookie_header"].count("OSID="), 1)

    async def test_runtime_cookie_supplements_jar_without_replacing_login(self):
        runtime = {"RUNTIME_SESSION": "browser-session", "OSID": "stale-session"}
        self.client._set_request_fingerprint({"session_cookies": runtime})
        updated = self.cookies + [
            {
                "name": "RUNTIME_SESSION",
                "value": "browser-session",
                "domain": "flow.google.com",
                "hostOnly": True,
                "path": "/",
            }
        ]
        self.respond.side_effect = [
            _response(BOOTSTRAP, updated),
            _response(RPC_SUCCESS, updated),
        ]

        await self.call_rpc()

        rpc = self.requests[1]["cookie_header"]
        self.assertIn("RUNTIME_SESSION=browser-session", rpc)
        self.assertIn(
            "RUNTIME_SESSION=browser-session", self.requests[0]["cookie_header"]
        )
        self.assertIn("OSID=session-old", rpc)
        self.assertNotIn("stale-session", rpc)
        self.assertEqual(runtime["OSID"], "stale-session")

    async def test_deleted_runtime_cookie_is_not_reintroduced_on_xsrf_retry(self):
        self.client._set_request_fingerprint(
            {"session_cookies": {"RUNTIME_SESSION": "browser-session"}}
        )
        initial = self.cookies + [
            {
                "name": "RUNTIME_SESSION",
                "value": "browser-session",
                "domain": "flow.google.com",
                "hostOnly": True,
                "path": "/",
            }
        ]
        self.respond.side_effect = [
            _response(BOOTSTRAP, initial),
            _response('[["xsrf","replacement"]]', self.cookies),
            _response(RPC_SUCCESS, self.cookies),
        ]

        await self.call_rpc()

        self.assertIn("RUNTIME_SESSION=", self.requests[0]["cookie_header"])
        self.assertIn("RUNTIME_SESSION=", self.requests[1]["cookie_header"])
        self.assertNotIn("RUNTIME_SESSION=", self.requests[2]["cookie_header"])

    async def test_plugin_push_carries_rotations_across_validation_and_project_creation(
        self,
    ):
        current = "session-old"
        counter = 0

        async def respond(request):
            nonlocal current, counter
            if f"OSID={current}" not in request["cookie_header"]:
                return _response("", [], status=401)
            counter += 1
            current = f"rotated-{counter}"
            snapshot = [dict(cookie) for cookie in self.cookies]
            snapshot[1]["value"] = current
            if request["method"] == "GET":
                return _response(BOOTSTRAP, snapshot)
            envelope = json.loads(parse_qs(request["body"])["f.req"][0])
            rpc_id = envelope[0][0][0]
            payload = (
                [f"01234567-89ab-cdef-0123-{counter:012d}"]
                if rpc_id == "jHPbke"
                else [120]
            )
            return _response(
                json.dumps([["wrb.fr", rpc_id, json.dumps(payload)]]), snapshot
            )

        self.respond.side_effect = respond
        with tempfile.TemporaryDirectory() as directory:
            database = Database(str(Path(directory) / "flow.db"))
            await database.init_db()
            await database.update_plugin_config(
                "connection-token", auto_enable_on_update=False
            )
            manager = TokenManager(database, self.client)
            app = FastAPI()
            app.include_router(admin.router)
            payload = {
                "google_cookies": json.dumps(self.cookies),
                "login_account": "account@example.com",
                "protocol_mode": "protocol",
            }
            with (
                patch.multiple(admin, db=database, token_manager=manager),
                patch.object(manager, "_get_project_pool_size", return_value=2),
            ):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://localhost"
                ) as http:
                    for action in ("added", "updated"):
                        response = await http.post(
                            "/api/plugin/update-token",
                            json=payload,
                            headers={"Authorization": "Bearer connection-token"},
                        )
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.json()["action"], action)
                        tokens = await database.get_all_tokens()
                        self.assertEqual(len(tokens), 1)
                        stored = serialize_cookie_header_for_url(
                            tokens[0].google_cookies
                        )
                        self.assertIn(f"OSID={current}", stored)
                        payload["google_cookies"] = tokens[0].google_cookies

    async def test_scheduled_fallback_persists_rotations_from_validation_rpc(
        self,
    ):
        current = "session-old"
        counter = 0

        async def respond(request):
            nonlocal current, counter
            if f"OSID={current}" not in request["cookie_header"]:
                return _response("", [], status=401)
            counter += 1
            current = f"rotated-{counter}"
            snapshot = [dict(cookie) for cookie in self.cookies]
            snapshot[1]["value"] = current
            return _response(
                BOOTSTRAP if request["method"] == "GET" else RPC_SUCCESS, snapshot
            )

        self.respond.side_effect = respond
        with tempfile.TemporaryDirectory() as directory:
            database = Database(str(Path(directory) / "flow.db"))
            await database.init_db()
            token_id = await database.add_token(
                Token(
                    st="frontend-old",
                    at="flow-frontend-cookie",
                    email="account@example.com",
                    google_cookies=json.dumps(self.cookies),
                )
            )
            self.client.db = database
            manager = TokenManager(database, self.client)
            with patch.object(manager, "_try_refresh_st", AsyncMock(return_value=None)):
                self.assertTrue(await manager._refresh_at_inner(token_id))

            saved = await database.get_token(token_id)
            self.assertIn(
                f"OSID={current}", serialize_cookie_header_for_url(saved.google_cookies)
            )
            self.assertEqual(saved.last_st_refresh_result, "success")
            self.assertEqual(len(self.requests), 2)
            fresh_request = contextvars.Context().run(
                asyncio.create_task,
                self.client.get_credits(
                    google_cookies=saved.google_cookies, token_id=token_id
                ),
            )
            self.assertEqual((await fresh_request)["credits"], 120)

    async def test_legacy_duplicates_follow_same_host_redirect(self):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        redirect = _response("", snapshot, status=302)
        redirect.headers["location"] = "/landing"
        self.respond.side_effect = [
            redirect,
            _response(BOOTSTRAP, snapshot),
            _response(RPC_SUCCESS, snapshot),
        ]

        await self.client.st_to_at(
            google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
        )

        self.assertEqual(len(self.requests), 3)
        self.assertEqual(self.requests[1]["url"], "https://flow.google.com/landing")
        for request in self.requests:
            self.assertEqual(request["cookie_header"].count("OSID="), 2)

    async def test_legacy_duplicate_values_are_not_forwarded_to_another_host(self):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        redirect = _response("", snapshot, status=302)
        redirect.headers["location"] = "https://accounts.google.com/signin"
        self.respond.return_value = redirect

        with self.assertRaisesRegex(RuntimeError, "重定向"):
            await self.client.st_to_at(
                google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
            )

        self.assertEqual(len(self.requests), 1)

    async def test_next_rpc_uses_rotated_bootstrap_cookie_after_transport_failure(self):
        updated = [dict(cookie) for cookie in self.cookies]
        updated[1]["value"] = "bootstrap-session"
        self.respond.side_effect = [
            _response(BOOTSTRAP, updated),
            RuntimeError("connection timed out"),
            _response(BOOTSTRAP, updated),
            _response(RPC_SUCCESS, updated),
        ]

        with self.assertRaisesRegex(Exception, "timed out"):
            await self.call_rpc()
        await self.call_rpc()

        self.assertIn("OSID=bootstrap-session", self.requests[2]["cookie_header"])

    async def test_legacy_cookie_redirect_cannot_downgrade_https(self):
        snapshot = [dict(cookie) for cookie in self.cookies]
        snapshot[1]["value"] = "domain-session"
        redirect = _response("", snapshot, status=302)
        redirect.headers["location"] = "http://flow.google.com/projects"
        self.respond.return_value = redirect

        with self.assertRaisesRegex(RuntimeError, "重定向"):
            await self.client.st_to_at(
                google_cookies="SID=account; OSID=domain-session; OSID=flow-session"
            )

        self.assertEqual(len(self.requests), 1)
