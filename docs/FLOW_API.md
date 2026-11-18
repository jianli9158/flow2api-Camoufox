# Flow 原生特征接口文档 (`/v1/flow/*`)

本文档说明 Flow2API 暴露的全部 `/v1/flow/*` 原生 REST / SSE 接口，涵盖项目管理、参考图资产、TTS 语音、角色库、参考生视频、Creation Agent 以及用户/项目设置。

> 服务启动后也可直接访问交互式文档：
> - Swagger UI: `http://<host>:<port>/docs`
> - OpenAPI JSON: `http://<host>:<port>/openapi.json`

---

## 1. 鉴权方式

所有 `/v1/flow/*` 接口均校验 `[global].api_key`（默认 `han1234`），支持任意以下一种传参方式：

1. HTTP Header：`Authorization: Bearer <api_key>`
2. HTTP Header：`x-goog-api-key: <api_key>`
3. URL Query 参数：`?key=<api_key>`

---

## 2. 账号与项目绑定机制（必读）

Flow 的**自定义语音、角色库、上传资产、Agent 会话、项目设置**均归属于某个 Google 账号（`token_id` / `email`）下的某个 Flow 项目（`project_id`）；只有用户全局偏好（`/user/preferences`）是跨项目的账号级设置。

### 解析优先级
每次调用都会通过 `resolve_account_context` 按以下优先级确定执行账号：
1. **`token_id`**（最高优先级）：指定数据库中的 Token ID。
2. **`email`**：按登录邮箱匹配数据库中的账号（不区分大小写）。
3. **自动负载均衡**：若两者都不传，则通过负载均衡器自动挑选一个可用活跃账号。

项目 `project_id` 的解析规则：
- 传入 `project_id` 时使用该指定项目；
- 未传 `project_id` 时，自动读取该账号当前的默认 `current_project_id`（若为空则自动创建一个项目）。

### 多账号调用最佳实践
- **GET / DELETE** 接口通过 Query 传参：`?token_id=1&project_id=<uuid>` 或 `?email=alice@gmail.com&project_id=<uuid>`。
- **POST / PATCH** 接口（以及 `multipart/form-data` 上传）通过请求体字段传参：`token_id`、`email`、`project_id`。
- **所有接口返回值均会包含 `"tokenId"`、`"email"`（以及适用的 `"projectId"`）**。
- 创建了角色、自定义音色、图片资产或 Agent 会话后，后续查询、修改、引用或续聊**务必固定传入同一次返回的 `tokenId`（或 `email`）和 `projectId`**，避免多账号轮询切到别的账号导致“找不到资源”。

---

## 3. 调用日志记录

所有 `/v1/flow/*` 接口（含成功、失败以及 `stream: true` 的 SSE 流式请求）均会自动写入数据库 `request_logs` 表：
- 自动记录 `operation`（如 `flow_create_voice`、`flow_create_character`、`flow_agent_chat`、`flow_generate_video` 等）、实际命中的 `token_id`、HTTP 状态码、耗时与状态进度；
- 请求中的大体积 `image_base64` / 二进制上传内容与响应中的原始 WAV/PCM `audioBytes` 会自动截断或替换为字节数摘要，不会撑爆数据库；
- 可在管理后台 `/manage` 的 **「请求日志」** 页面检索与查看详情，或调用管理员接口 `GET /api/logs` / `GET /api/logs/{log_id}` 查询。

---

## 4. 接口明细与示例

以下示例默认地址为 `http://127.0.0.1:8001`，`API_KEY="han1234"`。

### 4.1 项目与资产

#### 1) 读取项目详情：`GET /v1/flow/projects/{project_id}`
读取指定项目下的工作流、媒体数量、内置音色、自定义音色、角色列表及项目设置。
- **Query 参数**：`email?: string`, `token_id?: int`

```bash
curl -s "http://127.0.0.1:8001/v1/flow/projects/<project_uuid>?token_id=1" \
  -H "Authorization: Bearer han1234"
```

#### 2) 查询单个资产：`GET /v1/flow/assets/{media_id}`
解析图片、视频或音频资产的签名下载地址（`url`）与元数据。
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

```bash
curl -s "http://127.0.0.1:8001/v1/flow/assets/<media_uuid>?project_id=<project_uuid>&token_id=1" \
  -H "Authorization: Bearer han1234"
```
典型响应：
```json
{
  "mediaId": "<media_uuid>",
  "mediaType": "image",
  "projectId": "<project_uuid>",
  "workflowId": "<workflow_uuid>",
  "url": "https://...",
  "tokenId": 1,
  "email": "alice@gmail.com",
  "frontendRpc": "as29s"
}
```

#### 3) 上传参考图资产：`POST /v1/flow/assets`
支持 **JSON** 与 **`multipart/form-data`** 两种请求格式，返回可用于生图、角色或参考生视频的 `mediaId`。
- **JSON 字段**：
  - `image_base64?: string`（支持纯 base64 或 `data:image/...;base64,...`）
  - `image_url?: string`（远端图片 URL，受 SSRF 安全校验约束）
  - `aspect_ratio?: string`（默认 `"IMAGE_ASPECT_RATIO_LANDSCAPE"`）
  - `project_id?: string`, `email?: string`, `token_id?: int`
- **Multipart 表单字段**：
  - `file`（二进制图片文件），或 `image_base64` / `image_url`
  - 可选表单文本字段：`aspect_ratio`、`project_id`、`email`、`token_id`

```bash
# 方式 A：JSON (base64 / image_url)
curl -s -X POST "http://127.0.0.1:8001/v1/flow/assets" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "image_url": "https://example.com/ref.png",
    "aspect_ratio": "IMAGE_ASPECT_RATIO_LANDSCAPE",
    "token_id": 1,
    "project_id": "<project_uuid>"
  }'

# 方式 B：Multipart 文件上传
curl -s -X POST "http://127.0.0.1:8001/v1/flow/assets" \
  -H "Authorization: Bearer han1234" \
  -F "file=@./ref.png" \
  -F "aspect_ratio=IMAGE_ASPECT_RATIO_PORTRAIT" \
  -F "token_id=1" \
  -F "project_id=<project_uuid>"
```

---

### 4.2 语音与 TTS

#### 4) 列出音色：`GET /v1/flow/voices`
返回项目可用的内置音色（`presetVoices`，共 30 个：`Achernar`, `Achird`, `Algenib`, `Algieba`, `Alnilam`, `Aoede`, `Autonoe`, `Callirrhoe`, `Charon`, `Despina`, `Enceladus`, `Erinome`, `Fenrir`, `Gacrux`, `Iapetus`, `Kore`, `Laomedeia`, `Leda`, `Orus`, `Puck`, `Pulcherrima`, `Rasalgethi`, `Sadachbia`, `Sadaltager`, `Schedar`, `Sulafat`, `Umbriel`, `Vindemiatrix`, `Zephyr`, `Zubenelgenubi`）及当前项目已保存的自定义语音（`customVoices`）。
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

#### 5) 查询单个音色：`GET /v1/flow/voices/{voice_ref}`
`voice_ref` 支持内置音色名、`customVoice` 的 `id`/`workflowId`/`name` 或 `mediaId` UUID。
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

#### 6) TTS 语音合成：`POST /v1/flow/voices`
- **Body 参数**：
  - `text: string`（必填，非空）
  - `voice: string`（默认 `"Achernar"`，支持 30 个内置音色名，不区分大小写）
  - `style: string`（可选，情绪/语速/口吻提示，例如 `"Warm, calm documentary narrator"`）
  - `name?: string`（保存自定义语音时的展示名称）
  - `model_key?: string`（默认 `"gemini_v4s_tts_flow"`）
  - `save: bool`（默认 `false`；为 `true` 时保存到项目语音资产库，可在 `customVoices` 中复用）
  - `response_format: "json" | "wav" | "pcm"`（默认 `"json"`；`wav`/`pcm` 直接返回二进制音频流及响应头 `X-Flow-Media-Id`、`X-Flow-Workflow-Id`、`X-Flow-Project-Id`）
  - `cache_audio: bool`（默认 `false`；`response_format="json"` 且开启时，服务端转换 WAV 写入 `/tmp/` 并返回 `wavUrl`）
  - `project_id?: string`, `email?: string`, `token_id?: int`

```bash
# 返回 JSON + 本地可试听 wavUrl
curl -s -X POST "http://127.0.0.1:8001/v1/flow/voices" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "欢迎来到 Flow 创作工作台。",
    "voice": "Aoede",
    "style": "Warm and expressive",
    "name": "开场旁白",
    "save": true,
    "response_format": "json",
    "cache_audio": true,
    "token_id": 1
  }'

# 直接下载 WAV 音频
curl -s -X POST "http://127.0.0.1:8001/v1/flow/voices" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{"text":"Hello Flow","voice":"Puck","response_format":"wav","token_id":1}' \
  --output output.wav
```

#### 7) 删除自定义语音：`DELETE /v1/flow/voices/{voice_ref}`
- **Query 参数**：`workflow_id?: string`, `project_id?: string`, `email?: string`, `token_id?: int`

---

### 4.3 角色管理（Characters）

#### 8) 列出项目角色：`GET /v1/flow/characters`
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

#### 9) 查询单个角色：`GET /v1/flow/characters/{character_id}`
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

#### 10) 创建角色：`POST /v1/flow/characters`
- **Body 参数**：
  - `display_name: string`（必填）
  - `voice_id?: string`（可传内置音色名如 `"aoede"`，或自定义语音 UUID）
  - `personality_notes?: string`（角色人设/性格说明）
  - `image_refs?: string[]`（已上传参考图的 `mediaId` 列表）
  - `project_id?: string`, `email?: string`, `token_id?: int`

```bash
curl -s -X POST "http://127.0.0.1:8001/v1/flow/characters" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "display_name": "小狐狸阿狸",
    "voice_id": "aoede",
    "personality_notes": "机智好奇、语速轻快的探险向导",
    "image_refs": ["<uploaded_media_uuid>"],
    "token_id": 1,
    "project_id": "<project_uuid>"
  }'
```

#### 11) 更新角色：`PATCH /v1/flow/characters/{character_id}`
- **Body 参数**：`display_name?: string`, `voice_id?: string`, `personality_notes?: string`, `image_refs?: string[]`, `favorited?: bool`, `project_id?: string`, `email?: string`, `token_id?: int`

#### 12) 删除角色：`DELETE /v1/flow/characters/{character_id}`
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

---

### 4.4 参考生视频（Reference Video）

#### 13) 使用角色/音色/参考图生成视频：`POST /v1/flow/videos`
将项目下已创建的角色、自定义音色与参考图传入 Flow 参考生视频接口（`MZZa6b`）；当 `model_key` 包含 `_i2v_` 且提供 `image_refs` 时，自动按首帧生视频（`eb1hJf`）模式提交 `image_refs[0]`。
- **Body 参数**：
  - `prompt: string`（必填）
  - `model_key: string`（默认 `"macaron_cookie_r2v_8s_relaxed"`；若遇到 `code=[5]` 会自动尝试去掉 `_relaxed`）
  - `aspect_ratio: string`（默认 `"VIDEO_ASPECT_RATIO_LANDSCAPE"`，可选 `"VIDEO_ASPECT_RATIO_PORTRAIT"`）
  - `character_refs?: Array<string | {id, displayName}>`（角色 UUID 或包含 `id`/`displayName` 的对象）
  - `voice_refs?: Array<string | {mediaId, name}>`（自定义语音 `mediaId` 或包含 `mediaId`/`name` 的对象）
  - `image_refs?: Array<string | {mediaId}>`（参考图 `mediaId` 列表）
  - `resolution?: string`（可选，如 `"720p"` / `"1080p"` / `"4K"`）
  - `wait: bool`（默认 `true` 同步轮询直至完成；设为 `false` 立即返回 `operations`）
  - `poll_interval: float`（默认 `5.0`，范围 `1.0`–`30.0`）
  - `timeout: int`（默认 `360`，范围 `30`–`900`）
  - `project_id?: string`, `email?: string`, `token_id?: int`

```bash
curl -s -X POST "http://127.0.0.1:8001/v1/flow/videos" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "清晨森林小径上奔跑并开口打招呼",
    "character_refs": [{"id": "<character_uuid>", "displayName": "小狐狸阿狸"}],
    "voice_refs": [{"mediaId": "<custom_voice_media_uuid>", "name": "开场旁白"}],
    "aspect_ratio": "VIDEO_ASPECT_RATIO_LANDSCAPE",
    "wait": true,
    "token_id": 1,
    "project_id": "<project_uuid>"
  }'
```

---

### 4.5 Creation Agent 多轮对话与审批

#### 14) 发起或继续 Agent 对话：`POST /v1/flow/agent/chat`
- **Body 参数**：
  - `prompt: string`（首次对话必填；若延续已有会话或附带引用可配合使用）
  - `session_id?: string`（不传则自动生成新会话 UUID，传入则继续同一会话上下文）
  - `image_refs?: any[]`、`character_refs?: any[]`、`workflow_refs?: any[]`
  - `stream: bool`（默认 `false` 返回完整聚合 JSON；`true` 返回 `text/event-stream` SSE）
  - `timeout: int`（默认 `180`，范围 `10`–`600`）
  - `project_id?: string`, `email?: string`, `token_id?: int`

```bash
curl -N -X POST "http://127.0.0.1:8001/v1/flow/agent/chat" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "帮我策划一组赛博朋克茶馆分镜并先出一张主视觉图",
    "stream": true,
    "token_id": 1
  }'
```

**SSE 事件类型（`stream: true` 时）**：
- `session`：`{"projectId": "...", "sessionId": "..."}`
- `title`：会话标题更新
- `thinking`：思考增量内容
- `text`：回复正文增量与累计内容（`delta` / `text`）
- `a2ui`：交互卡片 / 选项结构
- `tool_call` / `tool_result`：工具调用与执行结果
- `media`：生成产出的图片/视频媒体与签名链接
- `permission`：待确认扣费/生成权限卡片（含 `permissionId`、`totalCost` 等）
- `done`：整轮完成汇总（含 `tokenId`、`email`、`pendingPermission` 等）
- `error`：流内错误详情（`{"status": ..., "detail": ...}`）

#### 15) 审批 Agent 扣费/执行请求：`POST /v1/flow/agent/sessions/{session_id}/approve`
当上一轮返回 `pendingPermission`（或项目 `credit_spend_approval_policy` 为 `always_ask`）时调用：
- **Body 参数**：
  - `action: string | int`（默认 `"approved"`）：
    - 同意单次：`1` / `"approve"` / `"approved"` / `"allow"` / `"yes"`
    - 始终同意：`2` / `"approve_sticky"` / `"approved_sticky"` / `"always_approve"` / `"always_allow"` / `"sticky"`
    - 拒绝：`3` / `"deny"` / `"denied"` / `"reject"` / `"no"`
  - `label?: string`（可选按钮展示文案）
  - `permission_id?: string`（可选；在页面内 RPC 开启且未传时会自动从会话最后轮次查找）
  - `stream: bool`（默认 `false`；`true` 同样输出 SSE）
  - `timeout: int`（默认 `180`，范围 `10`–`600`）
  - `project_id?: string`, `email?: string`, `token_id?: int`

#### 16) 查询会话列表：`GET /v1/flow/agent/sessions`
- **Query 参数**：`project_id?: string`, `page_size?: int`（`1`–`200`）, `email?: string`, `token_id?: int`

#### 17) 读取会话历史轮次：`GET /v1/flow/agent/sessions/{session_id}`
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

#### 18) 重命名会话：`PATCH /v1/flow/agent/sessions/{session_id}`
- **Body 参数**：`title: string`（必填）, `project_id?: string`, `email?: string`, `token_id?: int`

#### 19) 删除会话：`DELETE /v1/flow/agent/sessions/{session_id}`
- **Query 参数**：`project_id?: string`, `email?: string`, `token_id?: int`

---

### 4.6 用户偏好与项目设置

#### 20) 读取 / 更新用户全局偏好
- `GET /v1/flow/user/preferences`（Query：`email?: string`, `token_id?: int`）
- `PATCH /v1/flow/user/preferences`（Body 可选字段 + `email` / `token_id`）：
  - `should_clear_prompt_box_on_submit?: bool`
  - `has_sound_on_tile_hover?: bool`
  - `is_tile_details_always_visible?: bool`
  - `should_return_silent_videos?: bool`
  - `is_chat_panel_open?: bool`
  - `is_watermark_enabled_by_user?: bool`
  - `last_acknowledged_change_log_id?: string`

#### 21) 读取 / 更新项目设置
- `GET /v1/flow/projects/{project_id}/settings`（Query：`email?: string`, `token_id?: int`）
- `PATCH /v1/flow/projects/{project_id}/settings`（Body 可选字段 + `email` / `token_id`）：
  - `chat_panel_open?: bool`
  - `credit_spend_approval_policy?: int | string`（`0`: `"unspecified"`, `1`: `"always_ask"`, `2`: `"auto_approve"`, `3`: `"ask_above_threshold"`）
  - `pinned_applets?: string[]`
  - `image_defaults?: {aspectRatio?, count?, model?}`
  - `video_defaults?: {model?, aspectRatio?, count?}`

```bash
curl -s -X PATCH "http://127.0.0.1:8001/v1/flow/projects/<project_uuid>/settings" \
  -H "Authorization: Bearer han1234" \
  -H "Content-Type: application/json" \
  -d '{
    "credit_spend_approval_policy": "auto_approve",
    "chat_panel_open": true,
    "token_id": 1
  }'
```

---

### 4.7 模型能力目录、自动建角、视频串联与内置 MCP

#### 22) 查询账号能力与模型目录：`GET /v1/flow/models`
- **Query 参数**：`email?: string`, `token_id?: int`
- 返回账号订阅等级（`accountTier`）、剩余积分（`credits`）、`imageModels`、`videoModels`（含 `supported` 等级判定）、`nativeVideoModels` 与 `supportedResolutions`。

#### 23) 两段式自动建角（Face -> Body Saga）：`POST /v1/flow/characters`
- 除原有 `display_name` / `voice_id` / `personality_notes` / `image_refs` 外，新增可选参数：
  - `face_prompt?: string`：自动生成角色面部参考图并绑定；
  - `body_prompt?: string`：自动以面部参考图为锚点生成全身造型图并一同绑定至角色；
  - `aspect_ratio?: string`（默认 `IMAGE_ASPECT_RATIO_PORTRAIT`）、`model_name?: string`（默认 `GEM_PIX_2`）。
- 返回体额外包含 `generatedMediaIds`、`faceMediaId`、`bodyMediaId`。

#### 24) `@mention` 自动解析与提示词清理：`POST /v1/flow/videos`
- 新增可选布尔字段：
  - `resolve_mentions: bool = true`：自动解析提示词中的 `@CharacterName` 或 `@"Multi Word Name"` 并注入对应 `character_refs` / `image_refs`；若命中没有参考图的空角色会直接返回 `400` 提示；
  - `clean_prompt: bool = false`：开启后清理 `8k`、`4k`、`masterpiece`、`ultra hd` 等遗留 SD 冗余标签。

#### 25) 尾帧提取与多段视频串联：`POST /v1/flow/videos/chain`
> 需要宿主机 `PATH` 中存在 `ffmpeg` 与 `ffprobe`。
- **Body 参数**：
  - `segments: [{prompt, model_key?, aspect_ratio?, character_refs?, voice_refs?, image_refs?, resolution?}]`（1–6 段）
  - `frame_offset_ms: int = 200`（距视频结尾提前偏移毫秒提取尾帧，规避末尾黑场淡出）
  - `concat: bool = true`（使用 `ffmpeg` 拼接为单条成片返回 `/tmp/chain_*.mp4`）
  - `clean_prompt: bool = false`、`resolve_mentions: bool = true`、`poll_interval: float = 5.0`、`timeout: int = 360`

#### 26) 内置 Streamable HTTP MCP 服务端：`GET / POST /mcp`
- 使用标准 `Authorization: Bearer <api_key>` 或 `x-goog-api-key` 鉴权。
- 支持 JSON-RPC 2.0 `initialize`、`ping`、`tools/list`、`tools/call`（内置 9 个工具：`flow_list_models`、`flow_get_project`、`flow_list_voices`、`flow_list_characters`、`flow_create_character`、`flow_generate_image`、`flow_generate_video`、`flow_get_asset`、`flow_tts`）。
- 高消费工具内置令牌桶限流（容量 8，每 20s 恢复 1 枚），超限返回 JSON-RPC `-32029`（HTTP `429`）。
