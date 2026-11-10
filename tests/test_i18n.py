"""Locale negotiation and API messages must leave application data intact."""

import asyncio
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from src.api import admin, routes
from src.core.auth import verify_api_key_flexible
from src.core.i18n import I18nMiddleware, negotiate_language, translate_message


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("", "zh-CN"),
        ("en-US,en;q=0.9", "en"),
        ("zh-TW,en;q=0.5", "zh-CN"),
        ("zh-CN;q=0.1,en-GB;q=0.8", "en"),
        ("en;q=0,zh;q=0.5", "zh-CN"),
        ("fr-FR,en;q=invalid", "zh-CN"),
        ("en;q=2,zh-CN", "zh-CN"),
    ],
)
def test_language_negotiation(header, expected):
    assert negotiate_language(header) == expected


def test_catalogs_have_matching_keys_and_interpolation_parameters():
    root = Path(__file__).resolve().parents[1] / "static" / "locales"
    for path in sorted(root.glob("*.json")):
        catalog = json.loads(path.read_text(encoding="utf-8"))
        assert catalog["zh-CN"].keys() == catalog["en"].keys(), path.name
        for language in ("zh-CN", "en"):
            for key, translation in catalog[language].items():
                assert sorted(re.findall(r"\{\w+\}", key)) == sorted(
                    re.findall(r"\{\w+\}", translation)
                ), (path.name, language, key)
        for key, translation in catalog["en"].items():
            assert not re.search(r"[\u4e00-\u9fff]", translation), (path.name, key)


def test_translation_preserves_interpolated_values_and_unknown_diagnostics():
    assert translate_message("Google Cookie 验证失败：upstream detail", "en") == (
        "Google Cookie validation failed: upstream detail"
    )
    assert translate_message("Token updated for 保存@example.com", "en") == (
        "Token updated for 保存@example.com"
    )
    assert translate_message("Unrecognized upstream error: 保存", "en") == (
        "Unrecognized upstream error: 保存"
    )


def test_nested_errors_translate_without_changing_other_parameters():
    diagnostic = "Flow frontend RPC rejected: rpc=as29s, code=[5]"
    source = "生成失败: 视频生成成功但获取媒体地址失败: " + diagnostic
    assert translate_message(source, "en") == (
        "Generation failed: Video generation succeeded, but the media URL could not be retrieved: "
        + diagnostic
    )
    assert translate_message("生成失败: 保存", "en") == "Generation failed: 保存"
    assert translate_message("Token updated for Invalid API key", "zh-CN") == (
        "已更新 Invalid API key 的 Token"
    )
    assert (
        translate_message("刷新余额失败: Google Cookie 验证失败：upstream detail", "en")
        == "Failed to refresh credits: Google Cookie validation failed: upstream detail"
    )
    assert translate_message(
        "添加Token失败: Google Cookie 验证失败：旧密码错误", "en"
    ) == (
        "Failed to add token: Google Cookie validation failed: The current password is incorrect"
    )


@pytest.mark.parametrize("path", ["/v1/models", "/v1beta/models", "/models"])
@pytest.mark.parametrize(
    ("language", "message"), [("en", "Invalid API key"), ("zh-CN", "API Key 无效")]
)
def test_model_list_aliases_localize_authentication_errors(path, language, message):
    app = FastAPI()
    app.add_middleware(I18nMiddleware)
    app.include_router(routes.router)

    def unauthorized():
        raise HTTPException(status_code=401, detail="Invalid API key")

    app.dependency_overrides[verify_api_key_flexible] = unauthorized

    async def check():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            response = await client.get(path, headers={"Accept-Language": language})
            assert response.status_code == 401
            assert response.json() == {"detail": message}
            assert response.headers["content-language"] == language
            assert "Accept-Language" in response.headers["vary"]

    asyncio.run(check())


def test_api_messages_are_localized_without_translating_payloads_or_streams():
    app = FastAPI()
    app.add_middleware(I18nMiddleware)

    @app.get("/api/message")
    async def message():
        await asyncio.sleep(0)
        return JSONResponse(
            {
                "message": "Token已启用",
                "remark": "Token已启用",
                "data": {"message": "Token已启用", "prompt": "保存"},
            },
            headers={"Vary": "Origin"},
        )

    @app.get("/api/logs/1")
    async def log():
        return {"message": "Token已启用", "request_body": "保存"}

    @app.get("/v1/error")
    async def error():
        return JSONResponse(
            {"error": {"message": "Prompt cannot be empty", "code": "invalid_request"}},
            status_code=400,
        )

    @app.get("/api/validation")
    async def validation():
        return JSONResponse(
            {"detail": [{"msg": "Field required", "input": "Field required"}]},
            status_code=422,
        )

    @app.get("/v1/stream")
    async def stream():
        async def chunks():
            yield 'data: {"content":"保存"}\n\n'
            yield "data: [DONE]\n\n"

        return StreamingResponse(chunks(), media_type="text/event-stream")

    async def check():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            english, chinese = await asyncio.gather(
                client.get("/api/message", headers={"Accept-Language": "en"}),
                client.get("/api/message", headers={"Accept-Language": "zh-CN"}),
            )
            assert english.json()["message"] == "Token enabled"
            assert chinese.json()["message"] == "Token已启用"
            assert english.json()["remark"] == "Token已启用"
            assert english.json()["data"] == {
                "message": "Token已启用",
                "prompt": "保存",
            }
            assert english.headers["content-language"] == "en"
            assert set(english.headers["vary"].split(", ")) == {
                "Origin",
                "Accept-Language",
            }
            assert int(english.headers["content-length"]) == len(english.content)
            raw_log = await client.get("/api/logs/1", headers={"Accept-Language": "en"})
            assert raw_log.json()["message"] == "Token已启用"
            result = await client.get("/v1/error", headers={"Accept-Language": "zh-CN"})
            assert result.status_code == 400
            assert result.json()["error"]["message"] == "提示词不能为空"
            result = await client.get(
                "/api/validation", headers={"Accept-Language": "zh-CN"}
            )
            assert result.json()["detail"][0] == {
                "msg": "此字段为必填项",
                "input": "Field required",
            }
            result = await client.get("/v1/stream", headers={"Accept-Language": "en"})
            assert result.text == 'data: {"content":"保存"}\n\ndata: [DONE]\n\n'
            assert "content-language" not in result.headers

    asyncio.run(check())


def test_existing_admin_endpoint_uses_request_language():
    app = FastAPI()
    app.add_middleware(I18nMiddleware)
    app.include_router(admin.router)
    app.dependency_overrides[admin.verify_admin_token] = lambda: "i18n-test-session"

    async def check():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            with patch.object(admin, "token_manager", AsyncMock()):
                response = await client.post(
                    "/api/tokens/1/enable", headers={"Accept-Language": "en-US"}
                )
            assert response.status_code == 200
            assert response.json() == {"success": True, "message": "Token enabled"}

    asyncio.run(check())


def test_translation_assets_do_not_expose_protected_html_pages():
    from src.main import app

    async def check():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost"
        ) as client:
            assert (await client.get("/static/i18n.js")).status_code == 200
            catalog = await client.get("/static/locales/login.json")
            assert catalog.status_code == 200
            assert catalog.json()["en"]["登录"] == "Sign in"
            for path in ("/static/manage.html", "/static/test.html"):
                assert (await client.get(path)).status_code == 404
            for path in ("/manage", "/test"):
                result = await client.get(path)
                assert result.status_code == 302
                assert result.headers["location"] == "/login"

    asyncio.run(check())
