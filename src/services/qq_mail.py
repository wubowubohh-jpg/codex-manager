"""QQ mailbox service backed by IMAP polling."""

import email as email_lib
import imaplib
import logging
import re
import ssl
import time
from datetime import datetime, timedelta, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..config.constants import OTP_CODE_PATTERN


logger = logging.getLogger(__name__)


class QQMailService(BaseEmailService):
    """Read verification codes from a QQ mailbox via IMAP."""

    DEFAULT_IMAP_SERVER = "imap.qq.com"
    DEFAULT_IMAP_PORT = 993
    DEFAULT_USE_SSL = True

    DEFAULT_TIMEOUT = 120
    DEFAULT_POLL_INTERVAL = 5
    DEFAULT_TIME_WINDOW_MINUTES = 10
    DEFAULT_RECENT_MESSAGE_LIMIT = 5

    FOLDERS_TO_CHECK: List[Tuple[str, str]] = [
        ("INBOX", "inbox"),
        ("Junk", "junk"),
    ]

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        super().__init__(EmailServiceType.QQ_MAIL, name)

        raw_config = dict(config or {})
        qq_email = str(
            raw_config.get("qq_email")
            or raw_config.get("email")
            or raw_config.get("inbox_email")
            or ""
        ).strip()
        qq_auth_password = str(
            raw_config.get("qq_auth_password")
            or raw_config.get("auth_password")
            or raw_config.get("password")
            or ""
        ).strip()

        self.config = {
            "qq_email": qq_email,
            "qq_auth_password": qq_auth_password,
            "imap_server": str(raw_config.get("imap_server") or self.DEFAULT_IMAP_SERVER).strip() or self.DEFAULT_IMAP_SERVER,
            "imap_port": self._safe_int(raw_config.get("imap_port"), self.DEFAULT_IMAP_PORT),
            "use_ssl": self._safe_bool(raw_config.get("use_ssl"), self.DEFAULT_USE_SSL),
            "time_window_minutes": self._safe_int(
                raw_config.get("time_window_minutes"),
                self.DEFAULT_TIME_WINDOW_MINUTES,
            ),
            "recent_message_limit": self._safe_int(
                raw_config.get("recent_message_limit"),
                self.DEFAULT_RECENT_MESSAGE_LIMIT,
            ),
            "poll_interval": self._safe_int(raw_config.get("poll_interval"), self.DEFAULT_POLL_INTERVAL),
            "timeout": self._safe_int(raw_config.get("timeout"), self.DEFAULT_TIMEOUT),
            "sender_pattern": str(raw_config.get("sender_pattern") or "").strip(),
            "delete_after_read": self._safe_bool(raw_config.get("delete_after_read"), False),
            "receiver_alias_email": str(raw_config.get("receiver_alias_email") or "").strip().lower(),
            "receiver_alias_filter": self._safe_bool(raw_config.get("receiver_alias_filter"), True),
        }

        if not self.config["qq_email"]:
            raise ValueError("missing required config: qq_email")
        if not self.config["qq_auth_password"]:
            raise ValueError("missing required config: qq_auth_password")

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _safe_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _connect_imap(self) -> Optional[imaplib.IMAP4]:
        try:
            if self.config["use_ssl"]:
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                mail = imaplib.IMAP4_SSL(
                    self.config["imap_server"],
                    self.config["imap_port"],
                    ssl_context=context,
                )
            else:
                mail = imaplib.IMAP4(
                    self.config["imap_server"],
                    self.config["imap_port"],
                )
            mail.login(self.config["qq_email"], self.config["qq_auth_password"])
            return mail
        except Exception as exc:
            logger.warning("QQMail IMAP connection failed: %s", exc)
            return None

    @staticmethod
    def _decode_header_value(raw_value: Any) -> str:
        if not raw_value:
            return ""

        parts: List[str] = []
        for value, charset in decode_header(str(raw_value)):
            if isinstance(value, bytes):
                parts.append(value.decode(charset or "utf-8", errors="ignore"))
            else:
                parts.append(str(value))
        return " ".join(part for part in parts if part).strip()

    @staticmethod
    def _extract_body(msg: Any) -> str:
        body_parts: List[str] = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() not in {"text/plain", "text/html"}:
                    continue
                try:
                    payload = part.get_payload(decode=True)
                    if payload is None:
                        continue
                    charset = part.get_content_charset() or "utf-8"
                    body_parts.append(payload.decode(charset, errors="ignore"))
                except Exception:
                    continue
        else:
            try:
                payload = msg.get_payload(decode=True)
                if payload is not None:
                    charset = msg.get_content_charset() or "utf-8"
                    body_parts.append(payload.decode(charset, errors="ignore"))
            except Exception:
                pass
        return "\n".join(part for part in body_parts if part).strip()

    @staticmethod
    def _strip_html(text: str) -> str:
        cleaned = str(text or "")
        cleaned = unescape(cleaned)
        cleaned = re.sub(r'style\s*=\s*["\'][^"\']*["\']', "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"#[0-9a-fA-F]{6}", "", cleaned)
        cleaned = re.sub(r"rgba?\([^)]+\)", "", cleaned)
        cleaned = cleaned.replace("：", ":")
        cleaned = re.sub(r"[\u200b-\u200d\ufeff]", "", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip()

    def _extract_verification_code(
        self,
        text: str,
        pattern: str = OTP_CODE_PATTERN,
    ) -> Optional[str]:
        if not text:
            return None

        text_clean = self._strip_html(text)
        priority_patterns = [
            r"verification\s+code[:\s]+([0-9]{4,8})",
            r"verification\s+code\s+is[:\s]+([0-9]{4,8})",
            r"your\s+code[:\s]+([0-9]{4,8})",
            r"enter\s+the\s+\d+-digit\s+code[^0-9]{0,30}([0-9]{4,8})",
            r"enter\s+the\s+code[^0-9]{0,30}([0-9]{4,8})",
            r"验证码[:\s]+([0-9]{4,8})",
            r"请输入以下验证码[^0-9]{0,30}([0-9]{4,8})",
            r"以下验证码[^0-9]{0,30}([0-9]{4,8})",
            r"code[:\s]+([0-9]{4,8})",
            r"otp[:\s]+([0-9]{4,8})",
            r"pin[:\s]+([0-9]{4,8})",
        ]
        for regex in priority_patterns:
            match = re.search(regex, text_clean, re.IGNORECASE)
            if match:
                return match.group(1)

        try:
            match = re.search(pattern, text_clean)
        except re.error:
            match = None
        if match:
            return match.group(1) if match.groups() else match.group(0)

        for regex in (r"\b([0-9]{6})\b", r"\b([0-9]{5})\b", r"\b([0-9]{4})\b", r"\b([0-9]{8})\b"):
            for code in re.findall(regex, text_clean):
                if len(code) == 4 and code in {"3000", "5000", "8000", "8080", "9000"}:
                    continue
                return code

        return None

    def _parse_message_time(self, date_header: str) -> Optional[datetime]:
        if not date_header:
            return None
        try:
            value = parsedate_to_datetime(date_header)
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return value
        except Exception:
            return None

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        qq_email = self.config["qq_email"]
        email_info = {
            "email": qq_email,
            "service_id": qq_email,
            "id": qq_email,
            "account_id": qq_email,
            "created_at": time.time(),
        }
        self.update_status(True)
        return email_info

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
        exclude_codes: Optional[List[str]] = None,
    ) -> Optional[str]:
        excluded = {str(code).strip() for code in (exclude_codes or []) if str(code or "").strip()}
        receiver_alias_email = str(self.config.get("receiver_alias_email") or "").strip().lower()
        alias_filter_enabled = self._safe_bool(self.config.get("receiver_alias_filter"), True)
        use_alias_filter = bool(
            alias_filter_enabled
            and receiver_alias_email
            and receiver_alias_email != self.config["qq_email"].lower()
        )

        effective_timeout = timeout or self.config["timeout"]
        poll_interval = max(1, self._safe_int(self.config.get("poll_interval"), self.DEFAULT_POLL_INTERVAL))
        time_window_minutes = max(
            1,
            self._safe_int(self.config.get("time_window_minutes"), self.DEFAULT_TIME_WINDOW_MINUTES),
        )
        recent_message_limit = max(
            1,
            self._safe_int(self.config.get("recent_message_limit"), self.DEFAULT_RECENT_MESSAGE_LIMIT),
        )
        sender_pattern = str(self.config.get("sender_pattern") or "").strip().lower()

        start_time = time.time()
        while time.time() - start_time < effective_timeout:
            mail = self._connect_imap()
            if not mail:
                time.sleep(poll_interval)
                continue

            try:
                best_code = None
                best_code_time = None
                found_msg_info = None

                for folder_name, folder_display in self.FOLDERS_TO_CHECK:
                    try:
                        status, _ = mail.select(folder_name, readonly=True)
                        if status != "OK":
                            continue

                        status, messages = mail.search(None, "ALL")
                        if status != "OK" or not messages or not messages[0]:
                            continue

                        message_ids = messages[0].split()
                        for msg_id in reversed(message_ids[-recent_message_limit:]):
                            try:
                                status, msg_data = mail.fetch(msg_id, "(RFC822)")
                                if status != "OK" or not msg_data:
                                    continue

                                raw_bytes = None
                                for item in msg_data:
                                    if isinstance(item, tuple) and len(item) >= 2:
                                        raw_bytes = item[1]
                                        break
                                if not raw_bytes:
                                    continue

                                msg = email_lib.message_from_bytes(raw_bytes)
                                email_time = self._parse_message_time(str(msg.get("Date", "")))
                                if email_time:
                                    threshold = datetime.now(email_time.tzinfo) - timedelta(minutes=time_window_minutes)
                                    if email_time < threshold:
                                        continue
                                    if otp_sent_at and email_time.timestamp() + 2 < otp_sent_at:
                                        continue

                                to_header = str(msg.get("To", ""))
                                if use_alias_filter and receiver_alias_email not in to_header.lower():
                                    continue

                                from_header = str(msg.get("From", ""))
                                if sender_pattern:
                                    patterns = [sender_pattern]
                                    if "openai" not in sender_pattern:
                                        patterns.append("openai")
                                    if not any(item in from_header.lower() for item in patterns):
                                        continue

                                subject = self._decode_header_value(msg.get("Subject", ""))
                                body = self._extract_body(msg)
                                code = self._extract_verification_code(f"{subject} {body}", pattern)
                                if not code or code in excluded:
                                    continue

                                should_update = False
                                if best_code is None:
                                    should_update = True
                                elif email_time and best_code_time and email_time > best_code_time:
                                    should_update = True
                                elif best_code_time is None:
                                    should_update = True

                                if should_update:
                                    best_code = code
                                    best_code_time = email_time
                                    found_msg_info = (folder_name, msg_id)
                            except Exception as exc:
                                logger.debug("QQMail failed to parse message in %s: %s", folder_display, exc)
                                continue
                    except Exception as exc:
                        logger.debug("QQMail failed to inspect folder %s: %s", folder_display, exc)
                        continue

                if best_code:
                    if self.config.get("delete_after_read") and found_msg_info:
                        self._delete_message(found_msg_info[0], found_msg_info[1])
                    self.update_status(True)
                    return best_code
            finally:
                try:
                    mail.logout()
                except Exception:
                    pass

            time.sleep(poll_interval)

        error = EmailServiceError("QQMail verification code polling timed out")
        self.update_status(False, error)
        logger.warning(
            "QQMail timed out waiting for verification code: qq_email=%s alias=%s",
            self.config["qq_email"],
            receiver_alias_email or "-",
        )
        return None

    def _delete_message(self, folder_name: str, msg_id: bytes) -> None:
        mail = self._connect_imap()
        if not mail:
            return
        try:
            status, _ = mail.select(folder_name, readonly=False)
            if status == "OK":
                mail.store(msg_id, "+FLAGS", "\\Deleted")
                mail.expunge()
        except Exception as exc:
            logger.warning("QQMail failed to delete message: %s", exc)
        finally:
            try:
                mail.logout()
            except Exception:
                pass

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        qq_email = self.config["qq_email"]
        return [{
            "email": qq_email,
            "service_id": qq_email,
            "id": qq_email,
            "account_id": qq_email,
        }]

    def delete_email(self, email_id: str) -> bool:
        return False

    def check_health(self) -> bool:
        mail = self._connect_imap()
        if not mail:
            error = EmailServiceError("QQMail IMAP connection failed")
            self.update_status(False, error)
            return False

        try:
            self.update_status(True)
            return True
        finally:
            try:
                mail.logout()
            except Exception:
                pass

    def get_service_info(self) -> Dict[str, Any]:
        return {
            "service_type": self.service_type.value,
            "name": self.name,
            "qq_email": self.config.get("qq_email", ""),
            "imap_server": self.config.get("imap_server", self.DEFAULT_IMAP_SERVER),
            "imap_port": self.config.get("imap_port", self.DEFAULT_IMAP_PORT),
            "status": self.status.value,
        }
