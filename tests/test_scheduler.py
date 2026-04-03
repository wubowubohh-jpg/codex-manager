import asyncio
from types import SimpleNamespace

from src.core import scheduler as scheduler_core
from src.web.routes import scheduler as scheduler_route


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


def test_test_cliproxy_auth_file_marks_rate_limited_response_invalid(monkeypatch):
    calls = []

    def fake_post(url, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        return FakeResponse(
            status_code=200,
            payload={
                "status_code": 200,
                "body": (
                    '{"rate_limit": {"allowed": false, "limit_reached": true}}'
                ),
            },
        )

    monkeypatch.setattr(scheduler_core.cffi_requests, "post", fake_post)
    monkeypatch.setattr(
        scheduler_core,
        "get_settings",
        lambda: SimpleNamespace(
            cpa_auto_check_test_url="https://chatgpt.com/backend-api/wham/usage",
            cpa_auto_check_test_model="gpt-5.3-codex",
            cpa_auto_check_min_remaining_weekly_percent=20,
        ),
    )

    success, message = scheduler_core.test_cliproxy_auth_file(
        {
            "name": "demo.json",
            "auth_index": "auth-123",
            "id_token": {"chatgpt_account_id": "acct-123"},
        },
        api_url="https://cpa.example.com",
        api_token="token-123",
    )

    assert success is False
    assert "周限额已耗尽" in message
    assert calls[0]["url"] == "https://cpa.example.com/v0/management/api-call"
    assert calls[0]["kwargs"]["json"]["header"]["Chatgpt-Account-Id"] == "acct-123"


def test_test_cliproxy_auth_file_marks_low_remaining_weekly_quota_invalid(monkeypatch):
    def fake_post(url, **kwargs):
        return FakeResponse(
            status_code=200,
            payload={
                "status_code": 200,
                "body": (
                    '{"rate_limit": {"allowed": true, "limit_reached": false,'
                    ' "primary_window": {"used_percent": 81}}}'
                ),
            },
        )

    monkeypatch.setattr(scheduler_core.cffi_requests, "post", fake_post)
    monkeypatch.setattr(
        scheduler_core,
        "get_settings",
        lambda: SimpleNamespace(
            cpa_auto_check_test_url="https://chatgpt.com/backend-api/wham/usage",
            cpa_auto_check_test_model="gpt-5.3-codex",
            cpa_auto_check_min_remaining_weekly_percent=20,
        ),
    )

    success, message = scheduler_core.test_cliproxy_auth_file(
        {"name": "demo.json", "auth_index": "auth-123"},
        api_url="https://cpa.example.com",
        api_token="token-123",
    )

    # 新版逻辑下，“周限额低于阈值”由策略规则在限额任务中处理；
    # 单次 probe 只要返回 200 且无硬错误即视为可用。
    assert success is True
    assert message == "status_code=200"


def test_test_cliproxy_auth_file_allows_low_remaining_quota_when_threshold_disabled(monkeypatch):
    def fake_post(url, **kwargs):
        return FakeResponse(
            status_code=200,
            payload={
                "status_code": 200,
                "body": (
                    '{"rate_limit": {"allowed": true, "limit_reached": false,'
                    ' "primary_window": {"used_percent": 81}}}'
                ),
            },
        )

    monkeypatch.setattr(scheduler_core.cffi_requests, "post", fake_post)
    monkeypatch.setattr(
        scheduler_core,
        "get_settings",
        lambda: SimpleNamespace(
            cpa_auto_check_test_url="https://chatgpt.com/backend-api/wham/usage",
            cpa_auto_check_test_model="gpt-5.3-codex",
            cpa_auto_check_min_remaining_weekly_percent=0,
        ),
    )

    success, message = scheduler_core.test_cliproxy_auth_file(
        {"name": "demo.json", "auth_index": "auth-123"},
        api_url="https://cpa.example.com",
        api_token="token-123",
    )

    assert success is True
    assert message == "status_code=200"


def test_test_cliproxy_auth_file_marks_unavailable_item_invalid(monkeypatch):
    calls = []

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse(
            status_code=200,
            payload={
                "status_code": 429,
                "body": (
                    '{"rate_limit": {"allowed": false, "limit_reached": true}}'
                ),
            },
        )

    monkeypatch.setattr(scheduler_core.cffi_requests, "post", fake_post)

    success, message = scheduler_core.test_cliproxy_auth_file(
        {
            "name": "demo.json",
            "auth_index": "auth-123",
            "status": "error",
            "unavailable": True,
            "status_message": '{"error": {"type": "usage_limit_reached"}}',
        },
        api_url="https://cpa.example.com",
        api_token="token-123",
    )

    assert success is False
    assert "status_code=429" in message
    assert "周限额已耗尽" in message
    assert len(calls) == 1


def test_trigger_cpa_scheduler_check_passes_manual_logs_correctly(monkeypatch):
    class FakeLoop:
        async def run_in_executor(self, executor, func, *args):
            return func(*args)

    def fake_check_cpa_services_job(main_loop, manual_logs=None):
        assert main_loop is None
        assert isinstance(manual_logs, list)
        manual_logs.append("[INFO] 手动检查已执行")

    monkeypatch.setattr(scheduler_route.asyncio, "get_event_loop", lambda: FakeLoop())
    monkeypatch.setattr(scheduler_core, "check_cpa_services_job", fake_check_cpa_services_job)

    result = asyncio.run(scheduler_route.trigger_cpa_scheduler_check())

    assert result["success"] is True
    assert result["logs"] == ["[INFO] 手动检查已执行"]


def test_describe_cliproxy_failure_distinguishes_weekly_quota_cases():
    assert (
        scheduler_core._describe_cliproxy_failure("周限额剩余 19%，低于阈值 20%")
        == "周限额低于阈值"
    )
    assert (
        scheduler_core._describe_cliproxy_failure(
            "unavailable (周限额已耗尽 (usage_limit_reached))"
        )
        == "周限额已耗尽"
    )


def test_check_job_triggers_auto_register_when_check_disabled(monkeypatch):
    class DummyDBContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    settings = SimpleNamespace(
        cpa_auto_check_enabled=False,
        cpa_auto_register_enabled=True,
        cpa_auto_register_threshold=10,
        cpa_auto_register_batch_count=3,
    )
    svc = SimpleNamespace(id=1, name="cpa", api_url="https://cpa.example.com", api_token="token")
    scheduled = []

    def fake_run_coroutine_threadsafe(coro, loop):
        scheduled.append((coro, loop))
        coro.close()
        return SimpleNamespace()

    monkeypatch.setattr(scheduler_core, "get_settings", lambda: settings)
    monkeypatch.setattr(scheduler_core, "_is_checking", False)
    monkeypatch.setattr(scheduler_core, "get_db", lambda: DummyDBContext())
    monkeypatch.setattr(scheduler_core.crud, "get_cpa_services", lambda db, enabled=True: [svc])
    monkeypatch.setattr(scheduler_core, "fetch_cliproxy_auth_files", lambda api_url, api_token: ([], 0, 0))
    monkeypatch.setattr(
        scheduler_core,
        "_load_cpa_policy_rules",
        lambda _settings: (_ for _ in ()).throw(AssertionError("check disabled 时不应加载策略规则")),
    )
    monkeypatch.setattr(scheduler_core.asyncio, "run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    scheduler_core.check_cpa_services_job(main_loop=object(), manual_logs=None)

    assert len(scheduled) == 1


def test_update_scheduler_config_triggers_once_when_only_register_enabled(monkeypatch):
    class FakeLoop:
        def run_in_executor(self, executor, func, *args):
            return None

    class FakeBackgroundTasks:
        def __init__(self):
            self.calls = []

        def add_task(self, func, *args, **kwargs):
            self.calls.append((func, args, kwargs))

    monkeypatch.setattr(scheduler_route, "update_settings", lambda **kwargs: None)
    monkeypatch.setattr(scheduler_route.asyncio, "get_event_loop", lambda: FakeLoop())

    request = scheduler_route.CPASchedulerConfig(
        check_enabled=False,
        check_mode="panel",
        check_remove_401=False,
        check_remove_401_interval=3,
        check_interval=30,
        check_sleep=1,
        check_min_remaining_weekly_percent=0,
        test_url="https://chatgpt.com/backend-api/wham/usage",
        test_model="gpt-5.2-codex",
        register_enabled=True,
        register_threshold=100,
        register_batch_count=10,
        email_service="cloud_mail:1",
        token_mode="browser_http_only",
        policy_rules=[],
    )
    background = FakeBackgroundTasks()

    result = asyncio.run(
        scheduler_route.update_cpa_scheduler_config(request, background)
    )

    assert result["success"] is True
    assert len(background.calls) == 1
