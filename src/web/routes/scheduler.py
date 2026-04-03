import logging
import asyncio
import json
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel

from ...config.settings import get_settings, update_settings
from ...core.pending_oauth import get_oauth_pending_overview, list_oauth_pending_accounts

logger = logging.getLogger(__name__)
router = APIRouter()

SUPPORTED_TOKEN_MODES = {
    "browser",
    "browser_http_first",
    "browser_http_only",
}


def _normalize_scheduler_token_mode(mode: Optional[str]) -> str:
    value = str(mode or "browser_http_only").strip().lower()
    if value == "http_independent":
        return "browser_http_only"
    if value in SUPPORTED_TOKEN_MODES:
        return value
    logger.warning(f"调度器 token_mode={mode} 不受支持，回退 browser_http_only")
    return "browser_http_only"


def _normalize_policy_rules(raw: Any) -> List[Dict[str, Any]]:
    """规范化策略规则，避免脏数据写入配置。"""
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            raw = json.loads(text)
        except Exception:
            return []

    if not isinstance(raw, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        plans = item.get("plan_types") or item.get("plans") or []
        if not isinstance(plans, list):
            plans = []
        plan_types = []
        for plan in plans:
            val = str(plan or "").strip().lower()
            if val in {"free", "plus", "team", "pro", "unknown", "all"} and val not in plan_types:
                plan_types.append(val)
        if not plan_types:
            plan_types = ["all"]

        rule = {
            "id": str(item.get("id") or f"rule_{idx + 1}"),
            "enabled": bool(item.get("enabled", True)),
            "task": str(item.get("task") or "invalid").strip().lower(),
            "condition": str(item.get("condition") or "invalid_signal").strip().lower(),
            "operator": str(item.get("operator") or "lt").strip().lower(),
            "threshold": float(item.get("threshold") or 0),
            "target_status": str(item.get("target_status") or "all").strip().lower(),
            "action": str(item.get("action") or "remove").strip().lower(),
            "plan_types": plan_types,
            "fallback_to_weekly": bool(item.get("fallback_to_weekly", False)),
            "name": str(item.get("name") or "").strip(),
        }
        if rule["task"] not in {"invalid", "quota"}:
            rule["task"] = "invalid"
        if rule["condition"] not in {"invalid_signal", "weekly_remaining_percent", "five_hour_remaining_percent"}:
            rule["condition"] = "invalid_signal"
        if rule["operator"] not in {"lt", "lte", "gt", "gte", "eq", "neq"}:
            rule["operator"] = "lt"
        if rule["target_status"] not in {"all", "enabled", "disabled"}:
            rule["target_status"] = "all"
        if rule["action"] not in {"remove", "disable", "enable"}:
            rule["action"] = "remove"
        normalized.append(rule)
    return normalized


class CPASchedulerConfig(BaseModel):
    check_enabled: bool
    check_mode: str = "panel"
    check_remove_401: bool = False
    check_remove_401_interval: int = 3
    check_interval: int
    check_sleep: int
    check_min_remaining_weekly_percent: int = 20
    test_url: str
    test_model: str
    register_enabled: bool
    register_threshold: int
    register_batch_count: int
    email_service: str
    token_mode: str = "browser_http_only"
    policy_rules: List[Dict[str, Any]] = []

@router.get("/config")
async def get_cpa_scheduler_config():
    """获取CPA自动化配置"""
    settings = get_settings()
    policy_rules = _normalize_policy_rules(getattr(settings, "cpa_auto_policy_rules", "[]"))
    return {
        "check_enabled": settings.cpa_auto_check_enabled,
        "check_mode": settings.cpa_auto_check_mode,
        "check_remove_401": settings.cpa_auto_check_remove_401,
        "check_remove_401_interval": settings.cpa_auto_check_remove_401_interval,
        "check_interval": settings.cpa_auto_check_interval,
        "check_sleep": settings.cpa_auto_check_sleep_seconds,
        "check_min_remaining_weekly_percent": settings.cpa_auto_check_min_remaining_weekly_percent,
        "test_url": settings.cpa_auto_check_test_url,
        "test_model": settings.cpa_auto_check_test_model,
        "register_enabled": settings.cpa_auto_register_enabled,
        "register_threshold": settings.cpa_auto_register_threshold,
        "register_batch_count": settings.cpa_auto_register_batch_count,
        "email_service": settings.cpa_auto_register_email_service,
        "token_mode": _normalize_scheduler_token_mode(settings.cpa_auto_register_token_mode),
        "policy_rules": policy_rules,
    }

@router.get("/logs")
async def get_system_logs(since_id: int = 0):
    """获取后台产生的进度系统日志"""
    from ...core.scheduler import system_logs, global_log_counter
    
    # 如果前端请求的游标超出了当前服务器计数（通常由于你刚刚在终端重启了脚本导致内存清空），我们将它重置为从头开始拿
    if since_id > global_log_counter:
        since_id = 0
        
    # 这里我们只取从 since_id 后产生的新日志
    logs = [item for item in system_logs if item["id"] > since_id]
    last_id = logs[-1]["id"] if logs else since_id
    return {"success": True, "logs": logs, "last_id": last_id}

@router.post("/config")
async def update_cpa_scheduler_config(request: CPASchedulerConfig, background_tasks: BackgroundTasks):
    """保存CPA自动化配置"""
    if request.check_mode not in ("probe", "panel"):
        raise HTTPException(status_code=400, detail="检测方式必须为 probe 或 panel")
    token_mode = _normalize_scheduler_token_mode(request.token_mode)
    policy_rules = _normalize_policy_rules(request.policy_rules)
    update_settings(
        cpa_auto_check_enabled=request.check_enabled,
        cpa_auto_check_mode=request.check_mode,
        cpa_auto_check_remove_401=request.check_remove_401,
        cpa_auto_check_remove_401_interval=request.check_remove_401_interval,
        cpa_auto_check_interval=request.check_interval,
        cpa_auto_check_sleep_seconds=request.check_sleep,
        cpa_auto_check_min_remaining_weekly_percent=request.check_min_remaining_weekly_percent,
        cpa_auto_check_test_url=request.test_url,
        cpa_auto_check_test_model=request.test_model,
        cpa_auto_register_enabled=request.register_enabled,
        cpa_auto_register_threshold=request.register_threshold,
        cpa_auto_register_batch_count=request.register_batch_count,
        cpa_auto_register_email_service=request.email_service,
        cpa_auto_register_token_mode=token_mode,
        cpa_auto_policy_rules=json.dumps(policy_rules, ensure_ascii=False),
    )

    # 若关闭自动注册，尝试取消正在执行的自动注册批量任务
    if not request.register_enabled:
        try:
            from ...core.scheduler import cancel_auto_register_batches
            cancelled = cancel_auto_register_batches()
            if cancelled > 0:
                logger.info(f"已请求停止 {cancelled} 个自动注册批量任务")
        except Exception as e:
            logger.warning(f"停止自动注册批量任务失败: {e}")
    
    # 若启用了自动任务（体检或补注册），保存后立刻在后台触发一次检查/补充
    if request.check_enabled or request.register_enabled:
        from ...core.scheduler import request_cpa_check_once
        loop = asyncio.get_event_loop()
        background_tasks.add_task(loop.run_in_executor, None, request_cpa_check_once, loop, "config")

    return {"success": True, "message": "定时任务配置已保存"}

@router.post("/trigger")
async def trigger_cpa_scheduler_check():
    """手动触发一次 CPA 检查并返回结果日志"""
    from ...core.scheduler import check_cpa_services_job
    
    manual_logs = []
    try:
        loop = asyncio.get_event_loop()
        # 在线程池中执行，并正确传入 manual_logs 参数。
        await loop.run_in_executor(None, check_cpa_services_job, None, manual_logs)
        return {"success": True, "logs": manual_logs, "message": "检查执行完毕！"}
    except Exception as e:
        return {"success": False, "logs": manual_logs, "message": str(e)}

@router.post("/trigger-401")
async def trigger_cpa_scheduler_remove_401():
    """手动触发一次 401/403/usage_limit_reached 快速剔除并返回结果日志"""
    from ...core.scheduler import check_cpa_services_401_job

    manual_logs = []
    if not get_settings().cpa_auto_check_remove_401:
        return {
            "success": False,
            "logs": manual_logs,
            "message": "请先勾选“直接剔除面板明确报错的凭证（401、403、usage_limit_reached）”"
        }
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, check_cpa_services_401_job, None, manual_logs, True)
        return {"success": True, "logs": manual_logs, "message": "401/403/usage_limit_reached 快速剔除执行完毕！"}
    except Exception as e:
        return {"success": False, "logs": manual_logs, "message": str(e)}


@router.get("/oauth-pending/status")
async def get_oauth_pending_status():
    """获取待 OAuth 授权队列状态。"""
    settings = get_settings()
    overview = get_oauth_pending_overview()
    overview.update(
        {
            "enabled": settings.oauth_pending_enabled,
            "poll_interval_seconds": settings.oauth_pending_poll_interval_seconds,
            "max_attempts": settings.oauth_pending_max_attempts,
        }
    )
    return {"success": True, "data": overview}


@router.get("/oauth-pending/accounts")
async def get_oauth_pending_accounts(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=200, description="每页数量"),
    status: Optional[str] = Query(None, description="待授权状态筛选"),
):
    """获取待 OAuth 授权账号列表。"""
    try:
        data = list_oauth_pending_accounts(page=page, page_size=page_size, status=status)
        return {"success": True, "data": data}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/oauth-pending/trigger")
async def trigger_oauth_pending_once():
    """手动触发一次待 OAuth 授权补授权任务。"""
    from ...core.scheduler import process_oauth_pending_job

    manual_logs = []
    try:
        loop = asyncio.get_event_loop()
        summary = await loop.run_in_executor(None, process_oauth_pending_job, manual_logs)
        return {
            "success": True,
            "logs": manual_logs,
            "summary": summary or {},
            "message": "待授权补授权执行完毕",
        }
    except Exception as e:
        return {"success": False, "logs": manual_logs, "message": str(e)}
