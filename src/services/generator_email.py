"""
Generator.email 邮箱服务实现
"""

import re
import time
import logging
from typing import Optional, Dict, Any, List

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..core.http_client import HTTPClient, RequestConfig
from ..config.constants import OTP_CODE_PATTERN


logger = logging.getLogger(__name__)


class GeneratorEmailService(BaseEmailService):
    """
    Generator.email 临时邮箱服务
    基于网页解析方式获取邮箱与验证码
    """

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        """
        初始化 Generator.email 服务

        Args:
            config: 配置字典，支持以下键:
                - base_url: 入口地址 (默认: https://generator.email)
                - timeout: 请求超时时间 (默认: 30)
                - max_retries: 最大重试次数 (默认: 3)
                - poll_interval: 轮询间隔 (默认: 6)
                - impersonate: 浏览器指纹 (默认: chrome110)
                - user_agent: User-Agent
                - proxy_url: 代理 URL
            name: 服务名称
        """
        super().__init__(EmailServiceType.GENERATOR_EMAIL, name)

        default_config = {
            "base_url": "https://generator.email",
            "timeout": 30,
            "max_retries": 3,
            "poll_interval": 6,
            "impersonate": "chrome110",
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/110.0.0.0 Safari/537.36"
            ),
            "proxy_url": None,
        }

        self.config = {**default_config, **(config or {})}
        self.base_url = self.config["base_url"].rstrip("/")

        http_config = RequestConfig(
            timeout=self.config["timeout"],
            max_retries=self.config["max_retries"],
            impersonate=self.config["impersonate"],
        )
        self.http_client = HTTPClient(
            proxy_url=self.config.get("proxy_url"),
            config=http_config
        )

        self.headers = {
            "User-Agent": self.config["user_agent"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        self._email_cache: Dict[str, Dict[str, Any]] = {}

    def _parse_email(self, html: str) -> Optional[str]:
        """从页面中提取邮箱地址。"""
        if not html:
            return None
        match = re.search(r'id="email_ch_text"[^>]*>([^<]+)</span>', html, re.I)
        if not match:
            match = re.search(r'id="email_ch_text"[^>]*>([^<]+)<', html, re.I)
        return match.group(1).strip() if match else None

    def _build_surl(self, email: str) -> Optional[str]:
        """构造 generator.email 的 surl cookie 值。"""
        if not email or "@" not in email:
            return None
        username, domain = email.split("@", 1)
        if not username or not domain:
            return None
        return f"{domain}/{username}"

    def _resolve_surl(self, email: str, email_id: Optional[str]) -> Optional[str]:
        if email_id:
            if "/" in email_id and "@" not in email_id:
                return email_id
            if "@" in email_id:
                return self._build_surl(email_id)
        return self._build_surl(email)

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        创建新的临时邮箱（读取网页生成的邮箱）
        """
        try:
            response = self.http_client.get(self.base_url, headers=self.headers)

            if response.status_code != 200:
                self.update_status(False, EmailServiceError(f"请求失败，状态码: {response.status_code}"))
                raise EmailServiceError(f"Generator.email 请求失败，状态码: {response.status_code}")

            email = self._parse_email(response.text)
            if not email:
                self.update_status(False, EmailServiceError("未解析到邮箱地址"))
                raise EmailServiceError("Generator.email 未解析到邮箱地址")

            service_id = self._build_surl(email)
            email_info = {
                "email": email,
                "service_id": service_id,
                "created_at": time.time(),
            }
            self._email_cache[email] = email_info

            logger.info(f"成功创建 Generator.email 邮箱: {email}")
            self.update_status(True)
            return email_info

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"创建 Generator.email 邮箱失败: {e}")

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        """
        从 Generator.email 获取验证码
        """
        surl_value = self._resolve_surl(email, email_id)
        if not surl_value:
            logger.warning(f"邮箱 {email} 无法构造 surl，跳过验证码获取")
            return None

        cookies = {"surl": surl_value}
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = self.http_client.get(
                    self.base_url,
                    headers=self.headers,
                    cookies=cookies
                )

                if response.status_code != 200:
                    time.sleep(self.config["poll_interval"])
                    continue

                html = response.text or ""
                if "openai" not in html.lower():
                    time.sleep(self.config["poll_interval"])
                    continue

                # 优先匹配常见标题格式
                code_match = re.search(r"Your ChatGPT code is (\d{6})", html, re.I)
                if not code_match:
                    code_match = re.search(pattern, html, re.I)

                if code_match:
                    code = code_match.group(1)
                    logger.info(f"获取验证码成功: {code}")
                    self.update_status(True)
                    return code

            except Exception as e:
                logger.debug(f"轮询 Generator.email 失败: {e}")

            time.sleep(self.config["poll_interval"])

        logger.warning(f"等待验证码超时: {email}")
        return None

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        """返回缓存的邮箱列表。"""
        return list(self._email_cache.values())

    def delete_email(self, email_id: str) -> bool:
        """Generator.email 不支持删除邮箱，这里仅清理缓存。"""
        if not email_id:
            return False
        keys_to_remove = [
            email for email, info in self._email_cache.items()
            if info.get("service_id") == email_id or email == email_id
        ]
        for key in keys_to_remove:
            self._email_cache.pop(key, None)
        return bool(keys_to_remove)

    def check_health(self) -> bool:
        """检查服务是否可用。"""
        try:
            response = self.http_client.get(self.base_url, headers=self.headers)
            if response.status_code != 200:
                self.update_status(False, EmailServiceError("状态码异常"))
                return False
            email = self._parse_email(response.text)
            ok = bool(email)
            self.update_status(ok)
            return ok
        except Exception as e:
            self.update_status(False, e)
            return False
