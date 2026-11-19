"""Configuration management for Flow2API"""

import os
try:
    import tomllib as tomli
except ImportError:
    import tomli
from pathlib import Path
from typing import Dict, Any, Optional

def normalize_captcha_method(method: Optional[str]) -> str:
    """把浏览器运行方式收敛到内置支持的取值。"""
    normalized = (method or "").strip().lower()
    if normalized in {"browser", "camoufox"}:
        return normalized
    return "camoufox"

class Config:
    """Application configuration"""

    def __init__(self):
        self._config = self._load_config()
        self._admin_username: Optional[str] = None
        self._admin_password: Optional[str] = None

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from setting.toml, falling back to the example file."""
        config_dir = Path(__file__).parent.parent.parent / "config"
        config_path = config_dir / "setting.toml"
        fallback_path = config_dir / "setting_example.toml"

        if config_path.exists() and not config_path.is_file():
            print(
                f"[Config] 检测到 {config_path} 不是普通文件，"
                f"将回退到 {fallback_path.name}。请检查 Docker 挂载或本地配置路径。"
            )
            config_path = fallback_path
        elif not config_path.exists():
            config_path = fallback_path

        if not config_path.is_file():
            raise FileNotFoundError(
                f"配置文件不存在或不可读取: {config_path}. "
                f"请确认 config 目录下存在可用的 setting.toml 或 setting_example.toml"
            )

        with open(config_path, "rb") as f:
            return tomli.load(f)

    def reload_config(self):
        """Reload configuration from file"""
        self._config = self._load_config()

    def get_raw_config(self) -> Dict[str, Any]:
        """Get raw configuration dictionary"""
        return self._config

    @property
    def admin_username(self) -> str:
        # If admin_username is set from database, use it; otherwise fall back to config file
        if self._admin_username is not None:
            return self._admin_username
        return self._config["global"]["admin_username"]

    @admin_username.setter
    def admin_username(self, value: str):
        self._admin_username = value
        self._config["global"]["admin_username"] = value

    def set_admin_username_from_db(self, username: str):
        """Set admin username from database"""
        self._admin_username = username

    # Flow2API specific properties
    @property
    def flow_labs_base_url(self) -> str:
        """Google Labs base URL for project management"""
        return self._config["flow"]["labs_base_url"]

    @property
    def flow_api_base_url(self) -> str:
        """Google AI Sandbox API base URL for generation"""
        return self._config["flow"]["api_base_url"]

    @property
    def flow_timeout(self) -> int:
        timeout = self._config.get("flow", {}).get("timeout", 120)
        try:
            return max(5, int(timeout))
        except Exception:
            return 120

    @property
    def flow_max_retries(self) -> int:
        retries = self._config.get("flow", {}).get("max_retries", 3)
        try:
            return max(1, int(retries))
        except Exception:
            return 3

    def set_flow_max_retries(self, retries: int):
        """Set flow max retries"""
        if "flow" not in self._config:
            self._config["flow"] = {}
        try:
            normalized = max(1, int(retries))
        except Exception:
            normalized = 3
        self._config["flow"]["max_retries"] = normalized

    @property
    def flow_image_request_timeout(self) -> int:
        """图片生成单次 HTTP 请求超时(秒)。"""
        default_timeout = min(self.flow_timeout, 40)
        timeout = self._config.get("flow", {}).get(
            "image_request_timeout", default_timeout
        )
        try:
            return max(5, int(timeout))
        except Exception:
            return self.flow_timeout

    @property
    def poll_interval(self) -> float:
        return self._config["flow"]["poll_interval"]

    @property
    def server_host(self) -> str:
        env_host = os.environ.get("FLOW2API_SERVER_HOST", "").strip()
        if env_host:
            return env_host
        return self._config["server"]["host"]

    @property
    def server_port(self) -> int:
        for env_key in ("FLOW2API_SERVER_PORT", "PORT"):
            raw_port = os.environ.get(env_key, "").strip()
            if raw_port:
                try:
                    port = int(raw_port)
                    if 1 <= port <= 65535:
                        return port
                except ValueError:
                    pass
        return int(self._config["server"]["port"])

    @property
    def server_path_prefix(self) -> str:
        raw = os.environ.get("FLOW2API_PATH_PREFIX", "").strip()
        if not raw:
            raw = str(self._config.get("server", {}).get("path_prefix", "") or "").strip()
        if not raw or raw == "/":
            return ""
        return "/" + raw.strip("/")

    @property
    def debug_enabled(self) -> bool:
        return self._config.get("debug", {}).get("enabled", False)

    @property
    def debug_log_requests(self) -> bool:
        return self._config.get("debug", {}).get("log_requests", True)

    @property
    def debug_log_responses(self) -> bool:
        return self._config.get("debug", {}).get("log_responses", True)

    @property
    def debug_mask_token(self) -> bool:
        return self._config.get("debug", {}).get("mask_token", True)

    # Mutable properties for runtime updates
    @property
    def api_key(self) -> str:
        return self._config["global"]["api_key"]

    @api_key.setter
    def api_key(self, value: str):
        self._config["global"]["api_key"] = value

    @property
    def admin_password(self) -> str:
        # If admin_password is set from database, use it; otherwise fall back to config file
        if self._admin_password is not None:
            return self._admin_password
        return self._config["global"]["admin_password"]

    @admin_password.setter
    def admin_password(self, value: str):
        self._admin_password = value
        self._config["global"]["admin_password"] = value

    def set_admin_password_from_db(self, password: str):
        """Set admin password from database"""
        self._admin_password = password

    def set_debug_enabled(self, enabled: bool):
        """Set debug mode enabled/disabled"""
        if "debug" not in self._config:
            self._config["debug"] = {}
        self._config["debug"]["enabled"] = enabled

    @property
    def image_timeout(self) -> int:
        """Get image generation timeout in seconds"""
        return self._config.get("generation", {}).get("image_timeout", 300)

    def set_image_timeout(self, timeout: int):
        """Set image generation timeout in seconds"""
        if "generation" not in self._config:
            self._config["generation"] = {}
        self._config["generation"]["image_timeout"] = timeout

    @property
    def video_timeout(self) -> int:
        """Get video generation timeout in seconds"""
        return self._config.get("generation", {}).get("video_timeout", 1500)

    def set_video_timeout(self, timeout: int):
        """Set video generation timeout in seconds"""
        if "generation" not in self._config:
            self._config["generation"] = {}
        self._config["generation"]["video_timeout"] = timeout

    @property
    def polling_mode_enabled(self) -> bool:
        """Get polling mode enabled status."""
        return self.call_logic_mode == "polling"

    @property
    def call_logic_mode(self) -> str:
        """Get call logic mode (default or polling)."""
        call_logic = self._config.get("call_logic", {})
        mode = call_logic.get("call_mode")
        if mode in ("default", "polling"):
            return mode
        if call_logic.get("polling_mode_enabled", False):
            return "polling"
        return "default"

    def set_polling_mode_enabled(self, enabled: bool):
        """Set polling mode enabled/disabled."""
        self.set_call_logic_mode("polling" if enabled else "default")

    def set_call_logic_mode(self, mode: str):
        """Set call logic mode (default or polling)."""
        normalized = "polling" if mode == "polling" else "default"
        if "call_logic" not in self._config:
            self._config["call_logic"] = {}
        self._config["call_logic"]["call_mode"] = normalized
        self._config["call_logic"]["polling_mode_enabled"] = normalized == "polling"

    @property
    def upsample_timeout(self) -> int:
        """Get upsample (4K/2K) timeout in seconds"""
        return self._config.get("generation", {}).get("upsample_timeout", 300)

    def set_upsample_timeout(self, timeout: int):
        """Set upsample (4K/2K) timeout in seconds"""
        if "generation" not in self._config:
            self._config["generation"] = {}
        self._config["generation"]["upsample_timeout"] = timeout

    # Cache configuration
    @property
    def cache_enabled(self) -> bool:
        """Get cache enabled status"""
        return self._config.get("cache", {}).get("enabled", False)

    def set_cache_enabled(self, enabled: bool):
        """Set cache enabled status"""
        if "cache" not in self._config:
            self._config["cache"] = {}
        self._config["cache"]["enabled"] = enabled

    @property
    def cache_timeout(self) -> int:
        """Get cache timeout in seconds"""
        return self._config.get("cache", {}).get("timeout", 7200)

    def set_cache_timeout(self, timeout: int):
        """Set cache timeout in seconds"""
        if "cache" not in self._config:
            self._config["cache"] = {}
        self._config["cache"]["timeout"] = timeout

    @property
    def cache_base_url(self) -> str:
        """Get cache base URL"""
        return self._config.get("cache", {}).get("base_url", "")

    def set_cache_base_url(self, base_url: str):
        """Set cache base URL"""
        if "cache" not in self._config:
            self._config["cache"] = {}
        self._config["cache"]["base_url"] = base_url

    # Captcha configuration
    @property
    def captcha_method(self) -> str:
        """Get captcha method"""
        return self._config.get("captcha", {}).get("captcha_method", "camoufox")

    def set_captcha_method(self, method: str):
        """Set captcha method"""
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["captcha_method"] = normalize_captcha_method(method)

    @property
    def browser_engine(self) -> str:
        """浏览器模式内核（固定为 Camoufox + WebDriver BiDi）。"""
        return "camoufox"

    def set_browser_engine(self, engine: str):
        _ = engine
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["browser_engine"] = "camoufox"
    @property
    def browser_in_page_rpc(self) -> bool:
        """Camoufox 模式下是否直接在页面内通过 XHR/ProtectedSend 执行 Flow batchexecute RPC。"""
        env_val = os.getenv("FLOW2API_BROWSER_IN_PAGE_RPC")
        if env_val is not None:
            return str(env_val).strip().lower() in {"1", "true", "yes", "on"}
        return bool(
            self._config.get("captcha", {}).get("browser_in_page_rpc", True)
        )

    @property
    def browser_count(self) -> int:
        """浏览器打码实例数量，browser/personal 模式共用。"""
        value = self._config.get("captcha", {}).get("browser_count", 1)
        try:
            return max(1, min(20, int(value)))
        except Exception:
            return 1

    def set_browser_count(self, value: int):
        """设置浏览器打码实例数量。"""
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["browser_count"] = max(1, min(20, int(value)))


    @property
    def browser_idle_ttl_seconds(self) -> int:
        value = self._config.get("captcha", {}).get("browser_idle_ttl_seconds", 600)
        try:
            return max(60, int(value))
        except Exception:
            return 600

    @property
    def browser_captcha_max_retries(self) -> int:
        """browser 模式单次打码最大重试次数。"""
        value = self._config.get("captcha", {}).get("browser_captcha_max_retries", 5)
        try:
            return max(1, min(20, int(value)))
        except Exception:
            return 5

    @property
    def browser_captcha_generation_retries(self) -> int:
        """生成接口因 reCAPTCHA 评估失败时允许的总重试次数。"""
        value = self._config.get("captcha", {}).get(
            "browser_captcha_generation_retries", 6
        )
        try:
            return max(1, min(20, int(value)))
        except Exception:
            return 6

    @property
    def personal_project_pool_size(self) -> int:
        """单个 Token 默认维护的项目池数量，仅影响项目轮换。"""
        value = self._config.get("captcha", {}).get("personal_project_pool_size", 4)
        try:
            return max(1, min(50, int(value)))
        except Exception:
            return 4

    def set_personal_project_pool_size(self, value: int):
        """设置单个 Token 默认维护的项目池数量，仅影响项目轮换"""
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["personal_project_pool_size"] = max(
            1, min(50, int(value))
        )

    @property
    def browser_personal_fresh_restart_every_n_solves(self) -> int:
        """内置浏览器成功打码多少次后使用全新 profile 重启，0 表示禁用。"""
        value = self._config.get("captcha", {}).get(
            "browser_personal_fresh_restart_every_n_solves", 10
        )
        try:
            return max(0, int(value))
        except Exception:
            return 10

    def set_browser_personal_fresh_restart_every_n_solves(self, value: int):
        """设置内置浏览器 fresh profile 轮换阈值。"""
        if "captcha" not in self._config:
            self._config["captcha"] = {}
        self._config["captcha"]["browser_personal_fresh_restart_every_n_solves"] = max(
            0, int(value)
        )

    @property
    def flow_clean_prompt(self) -> bool:
        """是否自动清理 Stable Diffusion 遗留冗余标签（如 8k、masterpiece）。"""
        return bool(self._config.get("flow", {}).get("clean_prompt", False))

    @property
    def mcp_no_spend(self) -> bool:
        """MCP 禁扣费模式：开启后隐藏并拦截会消耗 Veo 积分的付费视频生成工具。"""
        env_val = os.environ.get("FLOW2API_MCP_NO_SPEND", "").strip().lower()
        if env_val in {"1", "true", "yes", "on"}:
            return True
        if env_val in {"0", "false", "no", "off"}:
            return False
        return bool(self._config.get("mcp", {}).get("no_spend", False))


# Global config instance
config = Config()
