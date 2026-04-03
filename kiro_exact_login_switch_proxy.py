#!/usr/bin/env python3
"""
完全模拟 Kiro 的 AWS 登录流程
使用与 Kiro 相同的 OAuth 2.0 授权码流程 + PKCE
支持自动注册和指纹浏览器
"""

import json
import hashlib
import secrets
import webbrowser
import http.server
import socketserver
import urllib.parse
from pathlib import Path
from datetime import datetime, timedelta
from threading import Thread
import boto3
import asyncio
import random
import string
import requests
import time
import imaplib
import email
import re
import threading
from email.header import decode_header
from email.utils import parsedate_to_datetime
from camoufox.async_api import AsyncCamoufox

# ========== 代理配置 ==========
PROXY_CONFIG = {
    'use_proxy': True,  # 是否使用代理
    'proxy_url': 'http://127.0.0.1:7897',  # 固定代理地址
    'proxy_switch_url': 'http://127.0.0.1:5030/switch',  # 切换节点API

    # 浏览器住宅代理配置（优先级高于 proxy_url，启用后浏览器走此代理）
    # 注意：Playwright/Firefox 不支持 SOCKS5 认证，因此使用 HTTP 协议（大多数住宅代理同时支持 HTTP 和 SOCKS5）
    'use_residential_proxy': True,  # 是否启用住宅代理（浏览器）
    'residential_host': 'us.srp.b2proxy.com',  # 代理主机
    'residential_port': 30000,  # 代理端口
    'residential_username': 'USER016391-ip-167.148.70.122',  # 代理用户名
    'residential_password': '8f7c65',  # 代理密码
}

# ========== kiro-rs-commercial 同步配置 ==========
SYNC_CONFIG = {
    'enabled': True,  # 是否启用同步到 kiro-rs-commercial
    'api_url': 'https://claude-code-app.zeabur.app/api/admin/credentials',  # Admin API 地址
    'admin_api_key': 'Qw123456@..',  # Admin API Key（对应 config.json 的 adminApiKey）

    # 同步请求代理配置（调用 Admin API 时使用的代理，非凭据级代理）
    'use_sync_proxy': False,  # 是否使用代理发送同步请求
    'sync_proxy_url': 'http://127.0.0.1:7897',  # 同步请求代理地址

    # 凭据级代理（写入凭据的 proxyUrl，kiro-rs-commercial 用该代理访问 AWS API）
    # 'credential_proxy_url': "",  # None=不设置, 'direct'=显式直连, 'http://...'=指定代理
    # 'credential_proxy_username': "",  # 凭据级代理用户名
    # 'credential_proxy_password': "",  # 凭据级代理密码
}

# ========== AWS 错误检测配置 ==========
ERROR_INDICATORS = [
    "Sorry, there was an error",
    "processing your request",
    "Please try again",
    "It's not you, it's us",
    "We couldn't complete your request",
    "couldn't complete your request",
    "抱歉，处理您的请求时出错",
    "请重试",
    "Something went wrong",
    "An error occurred",
]

# ========== 多语言支持（GeoIP 会根据代理出口 IP 设置浏览器语言，授权页面可能显示不同语言） ==========
# 授权按钮文本（用于构建 CSS 选择器）
ALLOW_BUTTON_TEXTS = [
    "Allow access",                     # English
    "Autoriser l'accès", "Autoriser",   # French
    "Zugriff erlauben", "Erlauben",     # German
    "Permitir acceso", "Permitir el acceso",  # Spanish
    "Permitir acesso",                  # Portuguese
    "Consenti l'accesso", "Consenti",   # Italian
    "アクセスを許可",                    # Japanese
    "액세스 허용",                       # Korean
    "允许访问", "允许",                  # Chinese
    "Allow",                            # English fallback (放最后，避免误匹配)
]

DENY_BUTTON_TEXTS = [
    "Deny access",                      # English
    "Refuser l'accès", "Refuser",       # French
    "Zugriff verweigern",              # German
    "Denegar acceso",                   # Spanish
    "Negar acesso",                     # Portuguese
    "Nega l'accesso",                   # Italian
    "アクセスを拒否",                    # Japanese
    "액세스 거부",                       # Korean
    "拒绝访问", "拒绝",                  # Chinese
    "Deny",                             # English fallback
]

# 授权页面特征文本（用于检测是否已跳转到授权页面）
AUTH_PAGE_INDICATORS = [
    # English
    "Allow Kiro IDE", "Allow access", "wants to access",
    "access your data", "Deny access", "to access the following", "Show details",
    # French
    "Autoriser Kiro IDE", "Autoriser l'accès", "accéder à vos données",
    "Refuser l'accès", "Afficher les détails", "accéder aux éléments",
    # German
    "Zugriff erlauben", "auf Ihre Daten zugreifen", "Zugriff verweigern",
    "Details anzeigen",
    # Spanish
    "Permitir acceso", "acceder a sus datos", "Denegar acceso",
    "Mostrar detalles",
    # Portuguese
    "Permitir acesso", "acessar seus dados", "Negar acesso",
    # Italian
    "Consenti l'accesso", "accedere ai tuoi dati",
    # Japanese
    "アクセスを許可", "データにアクセス",
    # Korean
    "액세스 허용", "데이터에 액세스",
    # Chinese
    "授权访问", "允许访问", "请求访问", "访问您的数据",
    # 通用关键词（语言无关）
    "Kiro IDE",
]

# 密码页面特征文本
PASSWORD_PAGE_INDICATORS = [
    # English
    "Create your password", "Enter password", "Confirm password",
    "Re-enter password", "Show password", "Password must",
    # French
    "Créez votre mot de passe", "mot de passe", "Confirmer le mot de passe",
    # German
    "Passwort erstellen", "Passwort eingeben", "Passwort bestätigen",
    # Spanish
    "Crea tu contraseña", "contraseña", "Confirmar contraseña",
    # Portuguese
    "Crie sua senha", "senha", "Confirmar senha",
    # Italian
    "Crea la tua password", "password",
    # Japanese
    "パスワードを作成", "パスワード",
    # Korean
    "비밀번호 만들기", "비밀번호",
    # Chinese
    "创建密码", "输入密码", "确认密码", "密码必须",
    # 技术标识（语言无关）
    "newPasswordInput", "retypePasswordInput",
]

# 登录页面特征文本（说明邮箱已注册）
LOGIN_PAGE_INDICATORS = [
    # English
    "Sign in with your AWS Builder ID", "Enter password",
    "Forgot password", "This is a trusted device",
    # French
    "Connectez-vous avec votre AWS Builder ID", "Saisir le mot de passe",
    "Mot de passe oublié",
    # German
    "Melden Sie sich mit Ihrer AWS Builder ID an", "Passwort eingeben",
    "Passwort vergessen",
    # Spanish
    "Inicia sesión con tu AWS Builder ID", "Ingresa la contraseña",
    "Olvidaste tu contraseña",
    # Chinese
    "登录您的 AWS Builder ID", "输入密码", "忘记密码",
    # Japanese
    "AWS Builder ID でサインイン",
    # Korean
    "AWS Builder ID로 로그인",
]

# 下一页面特征（姓名页面、验证码页面或登录页面）
NEXT_PAGE_AFTER_EMAIL_INDICATORS = [
    # English
    "Enter your name", "What's your name",
    "Verify your email", "verification code", "6-digit",
    "Sign in with your AWS Builder ID", "Enter password",
    # French
    "Entrez votre nom", "Quel est votre nom",
    "Vérifiez votre e-mail", "code de vérification",
    "Connectez-vous avec votre AWS Builder ID",
    # German
    "Geben Sie Ihren Namen ein", "Bestätigen Sie Ihre E-Mail",
    "Bestätigungscode",
    # Spanish
    "Ingresa tu nombre", "Verifica tu correo",
    "código de verificación",
    # Chinese
    "输入您的姓名", "验证您的电子邮件", "验证码",
]

# 验证码页面特征文本
VERIFICATION_CODE_INDICATORS = [
    # English
    "verification code", "Enter the code", "We sent a code to",
    "Enter the 6-digit code", "6-digit",
    # French
    "code de vérification", "Saisir le code", "Nous avons envoyé un code",
    # German
    "Bestätigungscode", "Code eingeben",
    # Spanish
    "código de verificación", "Ingresa el código",
    # Chinese
    "输入代码", "我们已发送验证码", "6位验证码",
]

def build_allow_button_selectors():
    """根据多语言文本构建授权按钮的 CSS 选择器列表"""
    selectors = []
    for text in ALLOW_BUTTON_TEXTS:
        selectors.append(f'button:has-text("{text}")')
    selectors.append('input[type="submit"][value*="Allow"]')
    selectors.append('input[type="submit"][value*="Autoriser"]')
    selectors.append('input[type="submit"][value*="Erlauben"]')
    selectors.append('input[type="submit"][value*="Permitir"]')
    for text in ALLOW_BUTTON_TEXTS:
        selectors.append(f'[role="button"]:has-text("{text}")')
    return selectors

def build_allow_button_locator_str():
    """构建授权按钮的合并 CSS 选择器字符串（用于 page.locator）"""
    parts = []
    for text in ALLOW_BUTTON_TEXTS:
        parts.append(f'button:has-text("{text}")')
    return ', '.join(parts)

def build_deny_button_locator_str():
    """构建拒绝按钮的合并 CSS 选择器字符串"""
    parts = []
    for text in DENY_BUTTON_TEXTS:
        parts.append(f'button:has-text("{text}")')
        parts.append(f'a:has-text("{text}")')
    return ', '.join(parts)

# ========== 执行配置 ==========
RUN_CONFIG = {
    # AWS 配置
    'start_url': 'https://view.awsapps.com/start',  # AWS SSO Start URL
    'region': 'us-east-1',  # AWS Region（Builder ID 只支持 us-east-1，不可更改）
    # 'region':'eu-central-1',  # ❌ Builder ID 的 start_url 不支持非 us-east-1 区域
    # 登录模式配置
    'auto_register': True,  # 是否启用自动注册模式（True=自动注册+指纹浏览器，False=手动登录+标准浏览器）
    'email': None,  # 注册使用的邮箱（配合 auto_register 使用，None=随机生成）
    
    # 浏览器配置
    'headless':  False,  # 是否使用无头模式运行浏览器
    
    # 执行配置
    'run_count': 500,  # 每次运行的执行次数
    'interval_seconds': 5,  # 每次执行之间的间隔秒数（建议>=10秒，避免AWS频率限制）
}

# ========== 邮箱配置 ==========
EMAIL_CONFIG = {
    'source': 'outlook',  # 邮箱来源: 'file' / 'boomlify' / 'kiro_api' / 'outlook' / 'yahoo'
    'file_path': 'icound.txt',  # 邮箱文件路径
    'used_file_path': 'icound_used.txt',  # 已使用邮箱文件路径

    'boomlify_api_keys': [],
    # Kiro API 邮箱配置
    'kiro_email_suffix': 'ggboyp.asia',  # Kiro邮箱后缀域名
    'kiro_email_prefix_length': 12,  # Kiro邮箱前缀长度（建议10-15位）
    'kiro_verification_api': 'http://111.231.77.203:7002/cursor/get-email-code',  # 验证码获取API
    
    # Outlook 邮箱配置
    'outlook_file': 'outlook.txt',  # Outlook邮箱文件路径（格式: 邮箱----密码----client_id----refresh_token）
    'outlook_verification_api': 'http://127.0.0.1:7002/get-email-code',  # Outlook验证码获取API

    # Yahoo 邮箱配置
    'yahoo_file': 'yahoo.txt',  # Yahoo邮箱文件路径（格式: 邮箱----授权码密码）
    
    # IMAP 配置（用于 file 模式）
    'imap': {
        'qq': {
            'server': 'imap.qq.com',
            'port': 993,
            'use_ssl': True,
        },
        'yahoo': {
            'server': 'imap.mail.yahoo.com',
            'port': 993,
            'use_ssl': True,
        }
    },
    
    # 邮箱密码/授权码（用于 file 模式）
    'passwords': {
        "2285987529@qq.com": "cagmrvlknpoqecia",
    },
}

# ==================== Outlook 邮箱池 ====================

class OutlookEmailPool:
    """邮箱池管理（Outlook，线程安全）
    
    outlook.txt 格式: 邮箱----密码----client_id----refresh_token
    """
    
    def __init__(self):
        self.accounts = []
        self.used = set()
        self.account_lock = threading.Lock()
        
        # 文件路径
        self.outlook_file = Path(__file__).parent / EMAIL_CONFIG.get('outlook_file', 'outlook.txt')
        self._load_outlook()
    
    def _load_outlook(self):
        """加载 Outlook 邮箱账号"""
        try:
            with open(self.outlook_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and '----' in line:
                        parts = line.split('----')
                        if len(parts) >= 4:
                            self.accounts.append({
                                'email': parts[0].strip(),
                                'password': parts[1].strip(),
                                'client_id': parts[2].strip(),
                                'refresh_token': parts[3].strip(),
                                'type': 'outlook'
                            })
            print(f"✅ 已加载 {len(self.accounts)} 个 Outlook 邮箱账号")
        except FileNotFoundError:
            print(f"⚠️ 未找到 outlook.txt 文件: {self.outlook_file}")
        except Exception as e:
            print(f"❌ 加载 Outlook 邮箱失败: {e}")
    
    def get_account(self):
        """获取一个未使用的邮箱（线程安全）
        
        Returns:
            dict: {'email': xxx, 'password': xxx, 'client_id': xxx, 'refresh_token': xxx, 'type': 'outlook'}
            或 None
        """
        with self.account_lock:
            if not self.accounts:
                return None
            
            available = [acc for acc in self.accounts if acc['email'] not in self.used]
            
            if not available:
                print(f"⚠️ 所有 Outlook 邮箱已使用，清空使用记录允许复用")
                self.used.clear()
                available = self.accounts
            
            account = random.choice(available)
            self.used.add(account['email'])
            return account
    
    def remove_account(self, email):
        """从邮箱池和文件中永久删除邮箱（注册成功后调用）"""
        with self.account_lock:
            try:
                self.accounts = [acc for acc in self.accounts if acc['email'] != email]
                self.used.discard(email)
                
                # 从文件中删除
                try:
                    with open(self.outlook_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    new_lines = []
                    for line in lines:
                        line_stripped = line.strip()
                        if not line_stripped:
                            continue
                        if '----' in line_stripped:
                            line_email = line_stripped.split('----')[0].strip()
                            if line_email != email:
                                new_lines.append(line)
                    with open(self.outlook_file, 'w', encoding='utf-8') as f:
                        f.writelines(new_lines)
                    print(f"   📝 已从文件删除: {self.outlook_file}")
                except Exception as e:
                    print(f"   ❌ 从文件删除失败: {e}")
                
                print(f"✅ 已从邮箱池删除: {email}")
            except Exception as e:
                print(f"❌ 删除邮箱失败 ({email}): {e}")
    
    def has_accounts(self):
        """检查是否有可用的邮箱"""
        return len(self.accounts) > 0


# ==================== Yahoo 邮箱池 ====================

class YahooEmailPool:
    """Yahoo 邮箱池管理（线程安全）

    yahoo.txt 格式: 邮箱----授权码密码
    """

    def __init__(self):
        self.accounts = []
        self.used = set()
        self.account_lock = threading.Lock()

        self.yahoo_file = Path(__file__).parent / EMAIL_CONFIG.get('yahoo_file', 'yahoo.txt')
        self._load_yahoo()

    def _load_yahoo(self):
        """加载 Yahoo 邮箱账号"""
        try:
            with open(self.yahoo_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and '----' in line:
                        parts = line.split('----')
                        if len(parts) >= 2:
                            password = parts[1].strip()
                            self.accounts.append({
                                'email': parts[0].strip(),
                                'password': password,
                                'type': 'yahoo',
                            })
            print(f"✅ 已加载 {len(self.accounts)} 个 Yahoo 邮箱账号")
        except FileNotFoundError:
            print(f"⚠️ 未找到 yahoo.txt 文件: {self.yahoo_file}")
        except Exception as e:
            print(f"❌ 加载 Yahoo 邮箱失败: {e}")

    def get_account(self):
        """获取一个未使用的 Yahoo 邮箱（线程安全）"""
        with self.account_lock:
            if not self.accounts:
                return None

            available = [acc for acc in self.accounts if acc['email'] not in self.used]

            if not available:
                print("⚠️ 所有 Yahoo 邮箱已使用，清空使用记录允许复用")
                self.used.clear()
                available = self.accounts

            account = random.choice(available)
            self.used.add(account['email'])
            return account

    def remove_account(self, email):
        """从邮箱池和文件中永久删除 Yahoo 邮箱"""
        with self.account_lock:
            try:
                self.accounts = [acc for acc in self.accounts if acc['email'] != email]
                self.used.discard(email)

                try:
                    with open(self.yahoo_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    new_lines = []
                    for line in lines:
                        line_stripped = line.strip()
                        if not line_stripped:
                            continue
                        if '----' in line_stripped:
                            line_email = line_stripped.split('----')[0].strip()
                            if line_email != email:
                                new_lines.append(line)
                    with open(self.yahoo_file, 'w', encoding='utf-8') as f:
                        f.writelines(new_lines)
                    print(f"   📝 已从文件删除: {self.yahoo_file}")
                except Exception as e:
                    print(f"   ❌ 从文件删除失败: {e}")

                print(f"✅ 已从 Yahoo 邮箱池删除: {email}")
            except Exception as e:
                print(f"❌ 删除 Yahoo 邮箱失败 ({email}): {e}")

    def has_accounts(self):
        """检查是否还有 Yahoo 邮箱可用"""
        return len(self.accounts) > 0


# 全局邮箱池实例
outlook_pool = None
yahoo_pool = None


def get_outlook_verification_code(account_info: dict, max_retries=30, retry_interval=3):
    """从 API 获取 Outlook 邮箱验证码
    
    Args:
        account_info: Outlook 邮箱信息字典
            {'email': xxx, 'refresh_token': xxx, 'client_id': xxx, 'type': 'outlook'}
        max_retries: 最大重试次数
        retry_interval: 重试间隔（秒）
    
    Returns:
        str: 验证码，如果获取失败返回 None
        'TOKEN_EXPIRED': 如果 refresh_token 已过期
    """
    api_base = EMAIL_CONFIG.get('outlook_verification_api', 'http://127.0.0.1:7002/get-email-code')
    email_addr = account_info['email']
    refresh_token = account_info['refresh_token']
    client_id = account_info['client_id']
    
    # 构建 API URL（参考 windsurf_kookeey.py 的方式传递 refresh_token 和 client_id）
    api_url = f"{api_base}?email={email_addr}&refresh_token={refresh_token}&client_id={client_id}"
    
    print(f"   🔍 开始获取验证码 (Outlook API)...")
    print(f"   📧 邮箱: {email_addr}")
    
    # 禁用代理，直接连接本地服务
    proxies = {
        'http': None,
        'https': None,
    }
    
    token_expired = False
    
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, timeout=15, proxies=proxies)
            
            if response.status_code == 200:
                data = response.json()
                
                # 检查是否是 token 过期错误
                if data.get('code') == 0:
                    error_msg = data.get('message', '').lower()
                    if 'expired' in error_msg or 'invalid_grant' in error_msg or 'aadsts70000' in error_msg:
                        print(f"   ⚠️ Refresh Token 已过期，需要重新授权")
                        print(f"   📧 过期邮箱: {email_addr}")
                        token_expired = True
                        break
                
                if data.get('code') == 1 and data.get('data') and data['data'].get('code'):
                    code = str(data['data']['code'])
                    print(f"   ✅ 成功获取验证码: {code}")
                    return code
                else:
                    if (attempt + 1) % 5 == 0:
                        print(f"   ⏳ 验证码尚未到达... [{attempt + 1}/{max_retries}]")
            else:
                if (attempt + 1) % 5 == 0:
                    print(f"   ⚠️ 接口返回错误: HTTP {response.status_code}")
            
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
                
        except Exception as e:
            if (attempt + 1) % 5 == 0:
                print(f"   ❌ 请求失败: {str(e)[:50]}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    if token_expired:
        return 'TOKEN_EXPIRED'
    
    print(f"   ❌ 获取验证码失败，已达到最大重试次数")
    return None


# ==================== Boomlify 临时邮箱客户端 ====================

class BoomlifyMailClient:
    """Boomlify 临时邮箱 API 客户端（支持多API key轮换）"""
    def __init__(self, api_keys):
        # 支持单个key或多个keys
        if isinstance(api_keys, str):
            self.api_keys = [api_keys]
        else:
            self.api_keys = list(api_keys)
        
        self.current_key_index = 0
        self.api_key = self.api_keys[0]
        self.base_url = "https://v1.boomlify.com/api/v1"
        self.headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json"
        }
        self.current_email = None
        self.email_id = None
    
    def _switch_to_next_key(self) -> bool:
        """切换到下一个API key"""
        if self.current_key_index < len(self.api_keys) - 1:
            self.current_key_index += 1
            self.api_key = self.api_keys[self.current_key_index]
            self.headers["X-API-Key"] = self.api_key
            print(f"   ⚠️ 切换到备用API key ({self.current_key_index + 1}/{len(self.api_keys)})")
            return True
        return False
    
    def create_email(self):
        """创建新的临时邮箱地址（支持多key自动切换）"""
        url = f"{self.base_url}/emails/create"
        last_error = None
        
        # 尝试所有可用的API keys
        for attempt in range(len(self.api_keys)):
            try:
                response = requests.post(url, headers=self.headers, json={}, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if data.get("success"):
                    if isinstance(data.get("email"), dict):
                        email_obj = data.get("email")
                        self.current_email = email_obj.get("address")
                        self.email_id = email_obj.get("id")
                    else:
                        self.current_email = data.get("email")
                        self.email_id = data.get("id")
                    
                    if attempt > 0:
                        print(f"   ✅ 使用备用key成功创建邮箱")
                    return data
                else:
                    last_error = f"创建邮箱失败: {data.get('message', '未知错误')}"
                    # 尝试切换到下一个key
                    if not self._switch_to_next_key():
                        raise Exception(last_error)
            except Exception as e:
                last_error = str(e)
                # 如果还有其他key可用，切换到下一个
                if not self._switch_to_next_key():
                    raise Exception(f"所有API key都失败: {last_error}")
        
        raise Exception(f"创建邮箱失败: {last_error}")
    
    def get_emails(self):
        """获取邮件列表"""
        if not self.email_id:
            raise Exception("没有可用的邮箱ID")
        
        url = f"{self.base_url}/emails/{self.email_id}/messages"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                return data.get("messages", [])
            else:
                raise Exception(f"获取邮件失败: {data.get('message')}")
        except Exception as e:
            raise Exception(f"API 请求失败: {str(e)}")
    
    def wait_for_email(self, timeout: int = 120, check_interval: int = 5):
        """等待邮件到达"""
        start_time = time.time()
        attempt = 0
        
        while time.time() - start_time < timeout:
            attempt += 1
            try:
                emails = self.get_emails()
                if emails:
                    print(f"   ✅ 收到邮件 (第{attempt}次检查)")
                    return emails[0]
                
                elapsed = int(time.time() - start_time)
                if attempt % 3 == 0:
                    print(f"   ⏳ 等待邮件... [{elapsed}s/{timeout}s]")
                time.sleep(check_interval)
            except Exception as e:
                if attempt % 5 == 0:
                    print(f"   ⚠️ 检查邮件失败: {str(e)[:50]}")
                time.sleep(check_interval)
        
        print(f"   ❌ 等待邮件超时 ({timeout}秒)")
        return None
    
    def get_verification_code(self, email_content: str, email_subject: str = ""):
        """从邮件内容或主题中提取验证码"""
        full_text = f"{email_subject} {email_content}"
        
        # 匹配各种验证码格式
        patterns = [
            r'\b(\d{6})\b',  # 6位数字
            r'\b(\d{5})\b',  # 5位数字
            r'\b(\d{4})\b',  # 4位数字
            r'code[:\s]+(\d+)',  # code: 123456
            r'verification[:\s]+(\d+)',  # verification: 123456
            r'confirm[:\s]+(\d+)',  # confirm: 123456
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            if matches:
                for code in matches:
                    if len(code) >= 4:  # 至少4位数字
                        return code
        
        return None


def generate_kiro_api_email(suffix="ggboyp.asia", length=12):
    """生成随机邮箱（Kiro API方式）- 增强复杂度版本
    
    Args:
        suffix: 邮箱后缀域名
        length: 邮箱前缀长度（建议10-15位）
    
    Returns:
        str: 完整的邮箱地址
    """
    # 确保长度至少为 10 位，增加复杂度
    length = max(length, 10)
    
    # 使用大小写字母 + 数字混合，增加复杂度
    # 策略：前半部分小写字母为主，中间穿插数字，后半部分大小写混合
    prefix_parts = []
    
    # 第一部分：小写字母 (3-5位)
    part1_len = random.randint(3, 5)
    prefix_parts.append(''.join(random.choices(string.ascii_lowercase, k=part1_len)))
    
    # 第二部分：数字 (2-3位)
    part2_len = random.randint(2, 3)
    prefix_parts.append(''.join(random.choices(string.digits, k=part2_len)))
    
    # 第三部分：小写字母 (2-4位)
    part3_len = random.randint(2, 4)
    prefix_parts.append(''.join(random.choices(string.ascii_lowercase, k=part3_len)))
    
    # 第四部分：大小写混合或数字 (剩余长度)
    current_len = sum(len(p) for p in prefix_parts)
    remaining = length - current_len
    if remaining > 0:
        # 50% 概率使用大小写混合，50% 使用数字
        if random.random() > 0.5:
            prefix_parts.append(''.join(random.choices(string.ascii_letters, k=remaining)))
        else:
            prefix_parts.append(''.join(random.choices(string.ascii_lowercase + string.digits, k=remaining)))
    
    # 随机打乱顺序（可选，增加随机性）
    random.shuffle(prefix_parts)
    prefix = ''.join(prefix_parts)
    
    # 确保长度正确
    if len(prefix) > length:
        prefix = prefix[:length]
    elif len(prefix) < length:
        # 补充到指定长度
        prefix += ''.join(random.choices(string.ascii_lowercase + string.digits, k=length - len(prefix)))
    
    email = f"{prefix}@{suffix}"
    return email


def get_kiro_api_verification_code(email_addr: str, max_retries=30, retry_interval=3):
    """从Kiro API获取邮箱验证码
    
    Args:
        email_addr: 邮箱地址
        max_retries: 最大重试次数
        retry_interval: 重试间隔（秒）
    
    Returns:
        str: 验证码，如果获取失败返回 None
    """
    api_base = EMAIL_CONFIG.get('kiro_verification_api', 'http://111.231.77.203:7002/cursor/get-email-code')
    api_url = f"{api_base}?email={email_addr}"
    
    print(f"   🔍 开始获取验证码 (Kiro API)...")
    print(f"   📧 邮箱: {email_addr}")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # 实际返回格式: {"code":1,"data":{"code":"959973"},"message":"成功获取验证码"}
                if data.get('code') == 1 and data.get('data') and data['data'].get('code'):
                    code = str(data['data']['code'])  # 确保转换为字符串
                    print(f"   ✅ 成功获取验证码: {code}")
                    return code
                else:
                    if (attempt + 1) % 5 == 0:
                        print(f"   ⏳ 验证码尚未到达... [{attempt + 1}/{max_retries}]")
            else:
                if (attempt + 1) % 5 == 0:
                    print(f"   ⚠️ 接口返回错误: HTTP {response.status_code}")
            
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
                
        except Exception as e:
            if (attempt + 1) % 5 == 0:
                print(f"   ❌ 请求失败: {str(e)[:50]}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    print(f"   ❌ 获取验证码失败，已达到最大重试次数")
    return None

# ==================== 邮箱文件管理 ====================

def get_email_from_file():
    """从 icound.txt 随机读取一个未使用的邮箱"""
    import random
    
    file_path = Path(__file__).parent / EMAIL_CONFIG.get('file_path', 'icound.txt')
    used_file_path = Path(__file__).parent / EMAIL_CONFIG.get('used_file_path', 'icound_used.txt')
    
    print(f"📂 读取邮箱文件: {file_path}")
    
    if not file_path.exists():
        print(f"   ❌ 邮箱文件不存在: {file_path}")
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            all_emails = [line.strip() for line in f if line.strip() and '@' in line]
    except Exception as e:
        print(f"   ❌ 读取邮箱文件失败: {str(e)[:50]}")
        return None
    
    if not all_emails:
        print(f"   ❌ 邮箱文件为空")
        return None
    
    # 读取已使用的邮箱
    used_emails = set()
    if used_file_path.exists():
        try:
            with open(used_file_path, 'r', encoding='utf-8') as f:
                used_emails = {line.strip() for line in f if line.strip()}
        except Exception as e:
            print(f"   ⚠️ 读取已使用邮箱文件失败: {str(e)[:50]}")
    
    # 获取所有未使用的邮箱
    available_emails = [email for email in all_emails if email not in used_emails]
    
    if available_emails:
        # 随机选择一个
        selected_email = random.choice(available_emails)
        print(f"   ✅ 随机选择邮箱: {selected_email}")
        print(f"   📊 剩余可用邮箱: {len(available_emails)} 个")
        return selected_email
    else:
        print(f"   ❌ 没有可用邮箱（共 {len(all_emails)} 个，已使用 {len(used_emails)} 个）")
        return None

def mark_email_as_used(email_addr):
    """标记邮箱为已使用（带去重检查）"""
    used_file_path = Path(__file__).parent / EMAIL_CONFIG.get('used_file_path', 'icound_used.txt')
    
    try:
        # 先检查是否已经标记过
        existing_emails = set()
        if used_file_path.exists():
            with open(used_file_path, 'r', encoding='utf-8') as f:
                existing_emails = {line.strip() for line in f if line.strip()}
        
        if email_addr in existing_emails:
            print(f"   💡 邮箱已在已使用列表中，跳过: {email_addr}")
            return True
        
        # 追加写入
        with open(used_file_path, 'a', encoding='utf-8') as f:
            f.write(f"{email_addr}\n")
        print(f"   ✅ 已标记邮箱为已使用: {email_addr}")
        return True
    except Exception as e:
        print(f"   ⚠️ 标记邮箱失败: {str(e)[:50]}")
        return False

def retire_email_account(email_addr, email_source):
    """Mark an email as unavailable after success or when it is already registered."""
    global outlook_pool, yahoo_pool

    if email_source == 'file':
        return mark_email_as_used(email_addr)

    if email_source == 'outlook' and outlook_pool:
        outlook_pool.remove_account(email_addr)
        return True

    if email_source == 'yahoo' and yahoo_pool:
        yahoo_pool.remove_account(email_addr)
        return True

    return False


def get_imap_listener_account(email_source, email_addr, email_extra_data=None):
    """Return the mailbox credentials used by the IMAP listener."""
    if email_source in ['file', 'kiro_api']:
        passwords = EMAIL_CONFIG.get('passwords', {})
        for receive_email, receive_password in passwords.items():
            if '@qq.com' in receive_email.lower():
                return {
                    'receive_email': receive_email,
                    'receive_password': receive_password,
                    'sender_pattern': 'aws',
                    'target_email': email_addr,
                }
        return None

    if email_source == 'yahoo' and email_extra_data:
        yahoo_password = email_extra_data.get('password')
        if not yahoo_password:
            return None
        return {
            'receive_email': email_extra_data['email'],
            'receive_password': yahoo_password,
            'sender_pattern': 'aws',
            'target_email': email_addr,
        }

    return None


def generate_random_email():
    """获取邮箱地址（支持多种来源）
    
    Returns:
        tuple: (email, email_source, extra_data)
            - email: 邮箱地址
            - email_source: 邮箱来源 ('file', 'boomlify', 'kiro_api', 'outlook', 'yahoo')
            - extra_data: 额外数据（如 BoomlifyMailClient 实例, 或邮箱账号信息字典）
    """
    global outlook_pool, yahoo_pool
    source = EMAIL_CONFIG.get('source', 'file')
    
    if source == 'outlook':
        print(f"📧 从 Outlook 邮箱池获取邮箱...")
        # 初始化 Outlook 邮箱池（仅首次）
        if outlook_pool is None:
            outlook_pool = OutlookEmailPool()
        
        if not outlook_pool.has_accounts():
            print(f"   ❌ Outlook 邮箱池为空，请检查 outlook.txt 文件")
            return None, None, None
        
        account = outlook_pool.get_account()
        if account:
            print(f"   ✅ Outlook 邮箱: {account['email']}")
            return account['email'], 'outlook', account
        else:
            print(f"   ❌ 获取 Outlook 邮箱失败")
            return None, None, None
    
    elif source == 'yahoo':
        print("📨 从 Yahoo 邮箱池获取邮箱...")
        if yahoo_pool is None:
            yahoo_pool = YahooEmailPool()

        if not yahoo_pool.has_accounts():
            print("   ❌ Yahoo 邮箱池为空，请检查 yahoo.txt 文件")
            return None, None, None

        account = yahoo_pool.get_account()
        if account:
            print(f"   ✅ Yahoo 邮箱: {account['email']}")
            return account['email'], 'yahoo', account
        else:
            print("   ❌ 获取 Yahoo 邮箱失败")
            return None, None, None

    elif source == 'boomlify':
        print(f"📧 创建临时邮箱（Boomlify方式）...")
        api_keys = EMAIL_CONFIG.get('boomlify_api_keys', [])
        if not api_keys:
            print(f"   ❌ 未配置 Boomlify API keys")
            return None, None, None
        
        print(f"   可用API key数量: {len(api_keys)}")
        try:
            temp_mail = BoomlifyMailClient(api_keys)
            result = temp_mail.create_email()
            if temp_mail.current_email:
                print(f"   ✅ 临时邮箱: {temp_mail.current_email}")
                return temp_mail.current_email, 'boomlify', temp_mail
            else:
                print(f"   ❌ 创建临时邮箱失败")
                return None, None, None
        except Exception as e:
            print(f"   ❌ Boomlify API 错误: {str(e)[:50]}")
            return None, None, None
    
    elif source == 'kiro_api':
        print(f"📧 生成随机邮箱（Kiro API方式）...")
        suffix = EMAIL_CONFIG.get('kiro_email_suffix', 'ggboyp.asia')
        length = EMAIL_CONFIG.get('kiro_email_prefix_length', 8)
        email = generate_kiro_api_email(suffix, length)
        print(f"   ✅ 随机邮箱: {email}")
        return email, 'kiro_api', None
    
    else:  # file
        print(f"📧 从文件读取邮箱...")
        email = get_email_from_file()
        if email:
            return email, 'file', None
        return None, None, None

# ==================== IMAP 验证码接收 ====================

def get_imap_config(email_addr):
    """根据邮箱地址获取 IMAP 配置"""
    imap_configs = EMAIL_CONFIG.get('imap', {})
    email_lower = (email_addr or '').lower()

    if '@yahoo.com' in email_lower or '@ymail.com' in email_lower:
        return imap_configs.get('yahoo', {})

    if '@qq.com' in email_lower:
        return imap_configs.get('qq', {})

    return {}


def get_imap_folder_candidates(email_addr):
    """根据邮箱类型返回需要检查的邮件夹。"""
    email_lower = (email_addr or '').lower()

    if '@yahoo.com' in email_lower or '@ymail.com' in email_lower:
        return [
            ('INBOX', '收件箱'),
            ('Bulk Mail', '垃圾邮件'),
            ('Spam', '垃圾邮件'),
            ('Junk', '垃圾邮件'),
        ]

    return [
        ('INBOX', '收件箱'),
        ('Junk', '垃圾邮件'),
    ]


def get_imap_scan_settings(email_addr):
    """根据邮箱类型返回 IMAP 扫描配置。"""
    email_lower = (email_addr or '').lower()

    if '@yahoo.com' in email_lower or '@ymail.com' in email_lower:
        return {
            'time_window_minutes': 40,
            'recent_message_limit': 10,
        }

    return {
        'time_window_minutes': 10,
        'recent_message_limit': 5,
    }

def _connect_imap_legacy(imap_server, imap_port, email_addr, password, use_ssl=True):
    """连接到 IMAP 服务器"""
    try:
        if use_ssl:
            # 创建 SSL 上下文，允许更宽松的 SSL 设置
            import ssl
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            mail = imaplib.IMAP4_SSL(imap_server, imap_port, ssl_context=context)
        else:
            mail = imaplib.IMAP4(imap_server, imap_port)
        
        mail.login(email_addr, password)
        return mail
    except Exception as e:
        error_msg = str(e)
        if "SSL" in error_msg or "EOF" in error_msg:
            print(f"   ❌ IMAP SSL 连接失败: {error_msg}")
        else:
            print(f"   ❌ IMAP 连接失败: {error_msg}")
        return None

def connect_imap(imap_server, imap_port, email_addr, password, use_ssl=True):
    """连接到 IMAP 服务器，支持多个密码候选依次尝试。"""
    passwords = password if isinstance(password, (list, tuple)) else [password]
    last_error = None

    for password_item in passwords:
        if not password_item:
            continue

        mail = None
        try:
            if use_ssl:
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                mail = imaplib.IMAP4_SSL(imap_server, imap_port, ssl_context=context)
            else:
                mail = imaplib.IMAP4(imap_server, imap_port)

            mail.login(email_addr, password_item)
            return mail
        except Exception as e:
            last_error = str(e)
            try:
                if mail:
                    mail.logout()
            except Exception:
                pass

    if last_error:
        if "SSL" in last_error or "EOF" in last_error:
            print(f"   ❌ IMAP SSL 连接失败: {last_error}")
        else:
            print(f"   ❌ IMAP 连接失败: {last_error}")
    return None


def extract_verification_code(text, debug=False):
    """从文本中提取验证码"""
    if debug:
        print(f"      🔍 原始内容长度: {len(text)} 字符")
    
    # 清理HTML标签
    text_clean = re.sub(r'style\s*=\s*["\'][^"\']*["\']', '', text, flags=re.IGNORECASE)
    text_clean = re.sub(r'<[^>]+>', ' ', text_clean)
    text_clean = re.sub(r'#[0-9a-fA-F]{6}', '', text_clean)
    text_clean = re.sub(r'rgba?\([^)]+\)', '', text_clean)
    
    # 优先匹配有关键词的模式
    priority_patterns = [
        r'verification\s+code[:\s]+([0-9]{4,8})',
        r'verification\s+code\s+is[:\s]+([0-9]{4,8})',
        r'your\s+code[:\s]+([0-9]{4,8})',
        r'code[:\s]+([0-9]{4,8})',
        r'验证码[：:\s]+([0-9]{4,8})',
        r'OTP[:\s]+([0-9]{4,8})',
        r'pin[:\s]+([0-9]{4,8})',
    ]
    
    for i, pattern in enumerate(priority_patterns):
        match = re.search(pattern, text_clean, re.IGNORECASE)
        if match:
            code = match.group(1)
            if 4 <= len(code) <= 8:
                if debug:
                    print(f"      ✅ 匹配成功: 优先模式 #{i+1}")
                return code
    
    # 回退到纯数字匹配
    fallback_patterns = [
        r'\b([0-9]{6})\b',
        r'\b([0-9]{5})\b',
        r'\b([0-9]{4})\b',
        r'\b([0-9]{8})\b',
    ]
    
    for i, pattern in enumerate(fallback_patterns):
        matches = re.findall(pattern, text_clean)
        for code in matches:
            if len(code) == 4:
                if code in ['8080', '3000', '5000', '8000', '9000']:
                    continue
            if debug:
                print(f"      ✅ 匹配成功: 回退模式 #{i+1}")
            return code
    
    if debug:
        print(f"      ❌ 未找到验证码")
    
    return None

def extract_verification_code(text, debug=False):
    """从文本中提取验证码，兼容 AWS Builder ID 的中英文模板。"""
    if debug:
        print(f"      🔍 原始内容长度: {len(text)} 字符")

    text_clean = re.sub(r'style\s*=\s*["\'][^"\']*["\']', '', text, flags=re.IGNORECASE)
    text_clean = re.sub(r'<[^>]+>', ' ', text_clean)
    text_clean = re.sub(r'#[0-9a-fA-F]{6}', '', text_clean)
    text_clean = re.sub(r'rgba?\([^)]+\)', '', text_clean)
    text_clean = text_clean.replace('：:', ':')
    text_clean = text_clean.replace('::', ':')
    text_clean = re.sub(r'[\u200b-\u200d\ufeff]', '', text_clean)
    text_clean = re.sub(r'\s+', ' ', text_clean)

    priority_patterns = [
        r'verification\s+code[:\s]+([0-9]{4,8})',
        r'verification\s+code\s+is[:\s]+([0-9]{4,8})',
        r'your\s+code[:\s]+([0-9]{4,8})',
        r'enter\s+the\s+\d+-digit\s+code[^0-9]{0,30}([0-9]{4,8})',
        r'enter\s+the\s+code[^0-9]{0,30}([0-9]{4,8})',
        r'验证码[：:\s]+([0-9]{4,8})',
        r'请输入以下验证码[^0-9]{0,30}([0-9]{4,8})',
        r'以下验证码[^0-9]{0,30}([0-9]{4,8})',
        r'code[:\s]+([0-9]{4,8})',
        r'OTP[:\s]+([0-9]{4,8})',
        r'pin[:\s]+([0-9]{4,8})',
    ]

    for i, pattern in enumerate(priority_patterns):
        match = re.search(pattern, text_clean, re.IGNORECASE)
        if match:
            code = match.group(1)
            if 4 <= len(code) <= 8:
                if debug:
                    print(f"      ✅ 匹配成功: 优先模式 #{i + 1}")
                return code

    fallback_patterns = [
        r'\b([0-9]{6})\b',
        r'\b([0-9]{5})\b',
        r'\b([0-9]{4})\b',
        r'\b([0-9]{8})\b',
    ]

    for i, pattern in enumerate(fallback_patterns):
        matches = re.findall(pattern, text_clean)
        for code in matches:
            if len(code) == 4 and code in ['8080', '3000', '5000', '8000', '9000']:
                continue
            if debug:
                print(f"      ✅ 匹配成功: 回退模式 #{i + 1}")
            return code

    if debug:
        print("      ❌ 未找到验证码")

    return None


class IMAPCodeListener:
    """IMAP 验证码监听器（支持后台线程）"""
    
    def __init__(self, receive_email, receive_password, sender_pattern=None, target_email=None):
        self.receive_email = receive_email
        self.receive_password = receive_password
        self.sender_pattern = sender_pattern
        self.target_email = target_email
        self.verification_code = None
        self._code_time = None
        self.is_running = False
        self.thread = None
        
    def start_listening(self):
        """启动后台监听线程"""
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self._listen_for_code, daemon=True)
            self.thread.start()
            print(f"   ✅ IMAP 后台监听已启动: {self.receive_email}")
    
    def _listen_for_code(self):
        """后台监听验证码（线程方法）"""
        max_retries = 180
        retry_interval = 5
        
        print(f"   🔌 IMAP 后台线程: 开始连接...")
        
        imap_config = get_imap_config(self.receive_email)
        if not imap_config:
            print(f"   ❌ IMAP 后台线程: 未找到 IMAP 配置")
            return
        
        imap_server = imap_config.get('server')
        imap_port = imap_config.get('port', 993)
        use_ssl = imap_config.get('use_ssl', True)
        
        print(f"   🔗 IMAP 后台线程: 连接 {imap_server}:{imap_port}")
        
        for attempt in range(max_retries):
            if not self.is_running:
                print(f"   ⏹️  IMAP 后台线程: 已停止")
                break
                
            try:
                if attempt == 0:
                    print(f"   🔌 IMAP 后台线程: 正在连接服务器...")
                    
                mail = connect_imap(imap_server, imap_port, self.receive_email, self.receive_password, use_ssl)
                if not mail:
                    if attempt == 0:
                        print(f"   ⚠️ IMAP 后台线程: 连接失败，将重试...")
                    time.sleep(retry_interval)
                    continue
                
                if attempt == 0:
                    print(f"   ✅ IMAP 后台线程: 已连接到服务器")
                
                # 检查收件箱和垃圾邮箱（QQ邮箱）
                folders_to_check = [
                    ('INBOX', '收件箱'),
                    ('Junk', '垃圾邮件'),
                ]
                
                scan_settings = get_imap_scan_settings(self.receive_email)
                time_window_minutes = scan_settings.get('time_window_minutes', 10)
                recent_message_limit = scan_settings.get('recent_message_limit', 5)
                folders_to_check = get_imap_folder_candidates(self.receive_email)

                checked_folders = []
                total_messages = 0
                code_found = False
                checked_count = 0
                found_msg_info = None
                
                for folder_name, folder_display in folders_to_check:
                    try:
                        status, data = mail.select(folder_name, readonly=True)
                        if status != 'OK':
                            continue
                        
                        status, messages = mail.search(None, 'ALL')
                        if status != 'OK' or not messages[0]:
                            continue
                        
                        folder_message_ids = messages[0].split()
                        if not folder_message_ids:
                            continue
                        
                        total_messages += len(folder_message_ids)
                        checked_folders.append(f"{folder_display}({len(folder_message_ids)}封)")
                        
                        if attempt == 0:
                            print(f"   📂 {folder_display}: {len(folder_message_ids)} 封邮件")
                        
                        for msg_id in reversed(folder_message_ids[-recent_message_limit:]):
                            try:
                                status, msg_data = mail.fetch(msg_id, '(RFC822)')
                                if status != 'OK':
                                    continue
                                
                                msg = email.message_from_bytes(msg_data[0][1])
                                
                                date_header = msg.get('Date', '')
                                email_time = None
                                try:
                                    email_time = parsedate_to_datetime(date_header)
                                    time_threshold = datetime.now(email_time.tzinfo) - timedelta(minutes=time_window_minutes)
                                    if email_time < time_threshold:
                                        if attempt == 0:
                                            time_diff = (datetime.now(email_time.tzinfo) - email_time).total_seconds() / 60
                                            print(f"      ⏰ 邮件太旧 ({time_diff:.1f} 分钟前)，跳过")
                                        continue
                                except:
                                    pass
                                
                                from_header = msg.get('From', '')
                                to_header = msg.get('To', '')
                                
                                subject = msg.get('Subject', '')
                                if subject:
                                    subject_decoded = decode_header(subject)[0]
                                    if isinstance(subject_decoded[0], bytes):
                                        subject = subject_decoded[0].decode(subject_decoded[1] or 'utf-8', errors='ignore')
                                    else:
                                        subject = subject_decoded[0]
                                
                                if attempt == 0:
                                    checked_count += 1
                                    print(f"   📧 检查邮件 #{checked_count} ({folder_display}):")
                                    print(f"      发件人: {from_header[:80]}")
                                    print(f"      收件人: {to_header[:80]}")
                                    if email_time:
                                        time_diff = (datetime.now(email_time.tzinfo) - email_time).total_seconds() / 60
                                        print(f"      时间: {time_diff:.1f} 分钟前")
                                    print(f"      主题: {subject[:80] if subject else '(无主题)'}")
                                
                                body = ''
                                if msg.is_multipart():
                                    for part in msg.walk():
                                        content_type = part.get_content_type()
                                        if content_type == 'text/plain' or content_type == 'text/html':
                                            try:
                                                payload = part.get_payload(decode=True)
                                                charset = part.get_content_charset() or 'utf-8'
                                                body += payload.decode(charset, errors='ignore')
                                            except:
                                                pass
                                else:
                                    try:
                                        payload = msg.get_payload(decode=True)
                                        charset = msg.get_content_charset() or 'utf-8'
                                        body = payload.decode(charset, errors='ignore')
                                    except:
                                        pass
                                
                                if attempt == 0:
                                    content_preview = (subject + ' ' + body)[:150].replace('\n', ' ').replace('\r', '')
                                    print(f"      内容预览: {content_preview}...")
                                
                                if self.target_email:
                                    if self.target_email.lower() not in to_header.lower():
                                        if attempt == 0:
                                            print(f"      ❌ 跳过: 收件人不匹配 (期望: {self.target_email})")
                                        continue
                                    elif attempt == 0:
                                        print(f"      ✅ 收件人匹配")
                                
                                sender_match = True
                                if self.sender_pattern:
                                    patterns_to_check = [
                                        self.sender_pattern.lower(),
                                        'aws',
                                        'builder',
                                    ]
                                    sender_match = any(p in from_header.lower() for p in patterns_to_check)
                                    
                                    if not sender_match:
                                        if attempt == 0:
                                            print(f"      ⚠️ 发件人不匹配 (期望: {self.sender_pattern}, AWS, 或 Builder)")
                                        continue
                                
                                code = extract_verification_code(subject + ' ' + body, debug=(attempt == 0))
                                if code:
                                    if attempt == 0:
                                        print(f"   🎉 找到验证码 {code}")
                                        print(f"      发件人: {from_header[:50]}...")
                                        print(f"      主题: {subject[:50]}...")
                                        print(f"      文件夹: {folder_display}")
                                        if email_time:
                                            print(f"      时间: {email_time.strftime('%Y-%m-%d %H:%M:%S')}")
                                    
                                    should_update = False
                                    if not self.verification_code:
                                        should_update = True
                                    elif email_time and self._code_time:
                                        if email_time > self._code_time:
                                            should_update = True
                                    elif not self._code_time:
                                        should_update = True
                                    
                                    if should_update:
                                        self.verification_code = code
                                        self._code_time = email_time if email_time else datetime.now()
                                        code_found = True
                                        found_msg_info = (folder_name, msg_id)
                                        if attempt == 0:
                                            print(f"      ✅ 更新为最新验证码")
                                    elif attempt == 0:
                                        print(f"      ⏰ 忽略（已有更新的验证码）")
                                else:
                                    if attempt == 0:
                                        print(f"      ⚠️ 未找到验证码")
                            
                            except Exception as e:
                                if attempt == 0:
                                    print(f"      ❌ 解析邮件失败: {str(e)[:50]}")
                                continue
                    
                    except Exception as e:
                        if attempt == 0:
                            print(f"   ⚠️ 检查 {folder_display} 失败: {str(e)[:50]}")
                        continue
                
                mail.logout()
                
                if code_found and self.verification_code:
                    print(f"   🎉 IMAP 后台线程: 最终验证码 {self.verification_code}")
                    if hasattr(self, '_code_time') and self._code_time:
                        print(f"   ⏰ 验证码时间: {self._code_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    if found_msg_info:
                        try:
                            print(f"   🗑️ 正在删除验证码邮件...")
                            del_mail = connect_imap(imap_server, imap_port, self.receive_email, self.receive_password, use_ssl)
                            if del_mail:
                                folder_name, msg_id = found_msg_info
                                status, _ = del_mail.select(folder_name, readonly=False)
                                if status == 'OK':
                                    del_mail.store(msg_id, '+FLAGS', '\\Deleted')
                                    del_mail.expunge()
                                    print(f"   ✅ 验证码邮件已删除")
                                del_mail.logout()
                        except Exception as e:
                            print(f"   ⚠️ 删除邮件失败: {str(e)[:50]}")
                    
                    self.is_running = False
                    return
                
                if attempt == 0:
                    print(f"   📊 首次检查完成: 检查了 {checked_count} 封邮件")
                    if checked_count == 0:
                        print(f"   💡 提示: 收件箱可能为空或所有邮件都被过滤")
                    else:
                        print(f"   💡 未找到验证码，将继续监听新邮件...")
                
                if (attempt + 1) % 10 == 0:
                    elapsed = (attempt + 1) * retry_interval
                    print(f"   ⏳ IMAP 后台线程: 等待验证码... [{attempt + 1}/{max_retries}] (已等待 {elapsed}秒)")
                
                time.sleep(retry_interval)
            
            except Exception as e:
                if attempt == 0:
                    print(f"   ❌ IMAP 后台线程: 连接异常 - {str(e)[:100]}")
                time.sleep(retry_interval)
        
        print(f"   ⏱️ IMAP 后台线程: 超时，未收到验证码")
    
    def get_code(self, timeout=300):
        """获取验证码（阻塞等待直到收到或超时）"""
        start_time = time.time()
        last_log_time = start_time
        
        while time.time() - start_time < timeout:
            if self.verification_code:
                return self.verification_code
            
            current_time = time.time()
            if current_time - last_log_time >= 30:
                elapsed = int(current_time - start_time)
                remaining = int(timeout - elapsed)
                print(f"   ⏳ 等待后台监听器... (已等待 {elapsed}秒, 剩余 {remaining}秒)")
                last_log_time = current_time
            
            time.sleep(1)
        
        print(f"   ⏱️ 后台监听器超时 (等待了 {timeout}秒)")
        return None
    
    def stop(self):
        """停止监听"""
        self.is_running = False
        print(f"   ⏹️ IMAP 后台线程: 已停止")

def test_proxy_connection(proxy_url: str) -> bool:
    """测试代理连接是否正常"""
    try:
        print(f"🔍 测试代理连接: {proxy_url}...")
        
        proxies = {
            'http': proxy_url,
            'https': proxy_url
        }
        
        # 测试连接到一个简单的网站
        response = requests.get('http://www.google.com', proxies=proxies, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ 代理连接正常")
            return True
        else:
            print(f"⚠️ 代理返回状态码: {response.status_code}")
            return False
            
    except requests.exceptions.ProxyError as e:
        print(f"❌ 代理连接失败 (ProxyError): {e}")
        return False
    except requests.exceptions.ConnectTimeout as e:
        print(f"❌ 代理连接超时: {e}")
        return False
    except Exception as e:
        print(f"❌ 代理测试失败: {e}")
        return False

def switch_proxy_node() -> bool:
    """切换代理节点"""
    try:
        switch_url = PROXY_CONFIG.get('proxy_switch_url', 'http://127.0.0.1:5030/switch')
        
        print(f"🔄 切换代理节点...")
        response = requests.post(switch_url, json={}, timeout=10)
        
        if response.status_code == 200:
            print(f"✅ 代理节点切换成功")
            return True
        else:
            print(f"⚠️ 代理节点切换失败: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"⚠️ 代理节点切换失败: {e}")
        return False


async def random_click(element, **kwargs):
    """在元素的随机位置点击，模拟真人操作（避免总是点击中心点）
    
    Args:
        element: Playwright Locator 对象
        **kwargs: 传递给 click() 的额外参数（如 timeout, no_wait_after 等）
    """
    import random
    try:
        box = await element.bounding_box()
        if box and box['width'] > 4 and box['height'] > 4:
            # 在元素内部 10%~90% 的区域随机取点，避免点到边缘
            margin_x = box['width'] * 0.1
            margin_y = box['height'] * 0.1
            rx = random.uniform(margin_x, box['width'] - margin_x)
            ry = random.uniform(margin_y, box['height'] - margin_y)
            await element.click(position={'x': rx, 'y': ry}, **kwargs)
        else:
            await element.click(**kwargs)
    except Exception:
        # 获取 bounding_box 失败时回退到普通点击
        await element.click(**kwargs)


async def has_error_popup(page) -> bool:
    """检测是否有 AWS 错误弹窗（异步版本）
    
    Args:
        page: Playwright Page 对象
    
    Returns:
        bool: 是否检测到错误弹窗
    """
    try:
        page_text = await page.locator('body').inner_text(timeout=2000)
        has_error = any(indicator in page_text for indicator in ERROR_INDICATORS)
        if has_error:
            print(f"   ⚠️ 检测到错误提示")
            return True
    except:
        pass
    
    return False


async def click_and_wait_with_retry(page, button_selectors, page_indicators, action_name="操作", max_retries=10, next_page_indicators=None):
    """点击按钮并等待页面跳转，带错误检测和重试机制
    
    Args:
        page: Playwright Page 对象
        button_selectors: 按钮选择器列表
        page_indicators: 当前页面特征文本列表（用于检测是否仍在当前页面）
        action_name: 操作名称（用于日志）
        max_retries: 最大重试次数
        next_page_indicators: 下一个页面的特征文本列表（优先检测是否已到达下一页）
    
    Returns:
        bool: 是否成功跳转
    """
    retry_count = 0
    last_button_clicked = False
    
    while retry_count < max_retries:
        retry_count += 1
        if retry_count > 1:
            print(f"   🔄 第 {retry_count} 次尝试{action_name}...")
        
        # 尝试点击按钮
        button_clicked = False
        for selector in button_selectors:
            try:
                button = page.locator(selector).first
                if await button.count() > 0 and await button.is_visible():
                    await random_click(button)
                    print(f"   ✅ 已点击按钮")
                    button_clicked = True
                    break
            except:
                continue
        
        if not button_clicked:
            # 如果上一次点击成功，但现在按钮不见了，说明页面已跳转
            if last_button_clicked:
                print(f"   ✅ 按钮已消失，页面已跳转成功")
                return True
            print(f"   ⚠️ 未找到按钮，等待后重试...")
            await asyncio.sleep(2)
            continue
        
        last_button_clicked = button_clicked
        
        # 等待页面响应
        await asyncio.sleep(2)
        
        # 检测是否有错误
        if await has_error_popup(page):
            print(f"   🔄 检测到错误，准备重试...")
            await asyncio.sleep(2)
            continue
        
        # 检测页面状态
        try:
            page_text = await page.locator('body').inner_text(timeout=3000)
            
            # 优先检测是否已到达下一个页面（更可靠）
            if next_page_indicators:
                reached_next = any(indicator in page_text for indicator in next_page_indicators)
                if reached_next:
                    print(f"   ✅ {action_name}成功，已到达下一页面")
                    return True
            
            # 再检测是否仍在当前页面
            still_on_page = any(indicator in page_text for indicator in page_indicators)
            
            if still_on_page:
                print(f"   ⚠️ 仍在当前页面，提交可能未成功，重试...")
                await asyncio.sleep(3)
                continue
            else:
                # 已离开当前页面
                print(f"   ✅ {action_name}成功，页面已跳转")
                return True
        except:
            # 获取页面文本失败，可能页面正在加载
            print(f"   💡 页面正在加载...")
            await asyncio.sleep(2)
            return True
    
    print(f"   ❌ {action_name}失败，已达到最大重试次数")
    return False

class KiroExactLogin:
    """完全模拟 Kiro 的登录流程"""
    
    def __init__(self, start_url, region='us-east-1'):
        self.start_url = start_url
        self.region = region
        # 保存到当前目录的 accounts 文件夹
        self.kiro_token_path = Path.cwd() / 'accounts' / 'kiro-auth-token.json'
        
        # OAuth 参数
        self.state = None
        self.code_verifier = None
        self.code_challenge = None
        self.authorization_code = None
        self.client_info = None
        self.server = None
        self.port = None
    
    def generate_pkce_params(self):
        """生成 PKCE 参数（与 Kiro 相同的方式）"""
        # 1. 生成 code_verifier (32 字节随机数)
        self.code_verifier = secrets.token_urlsafe(32)
        
        # 2. 生成 code_challenge (SHA256 hash)
        challenge_bytes = hashlib.sha256(self.code_verifier.encode()).digest()
        self.code_challenge = secrets.token_urlsafe(len(challenge_bytes)).replace('=', '')[:43]
        
        # 简化版：直接使用 base64url 编码
        import base64
        challenge_bytes = hashlib.sha256(self.code_verifier.encode()).digest()
        self.code_challenge = base64.urlsafe_b64encode(challenge_bytes).decode().rstrip('=')
        
        print(f"✅ PKCE 参数已生成")
        print(f"   Code Verifier: {self.code_verifier[:30]}...")
        print(f"   Code Challenge: {self.code_challenge[:30]}...")
    
    def generate_state(self):
        """生成 state 参数"""
        import uuid
        self.state = str(uuid.uuid4())
        print(f"✅ State 已生成: {self.state}")
    
    def register_client(self):
        """注册 OIDC 客户端（与 Kiro 相同）"""
        print("\n1️⃣ 注册 OIDC 客户端...")
        
        # 临时保存并清除代理设置（避免代理的SSL证书问题）
        import os
        original_proxy = {}
        proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
        
        for var in proxy_vars:
            if var in os.environ:
                original_proxy[var] = os.environ[var]
                del os.environ[var]
        
        if original_proxy:
            print("   💡 检测到代理设置，临时清除以直连AWS（避免SSL证书问题）")
        
        try:
            # 尝试1: 正常创建客户端（启用 SSL 验证，不使用代理）
            client = boto3.client('sso-oidc', region_name=self.region)
            
            response = client.register_client(
                clientName='Kiro IDE',
                clientType='public',
                scopes=[
                    'codewhisperer:completions',
                    'codewhisperer:analysis',
                    'codewhisperer:conversations',
                    'codewhisperer:transformations',
                    'codewhisperer:taskassist'
                ],
                grantTypes=['authorization_code', 'refresh_token'],
                redirectUris=['http://127.0.0.1/oauth/callback'],
                issuerUrl=self.start_url
            )
            
            self.client_info = {
                'clientId': response['clientId'],
                'clientSecret': response['clientSecret'],
                'expiresAt': response['clientSecretExpiresAt']
            }
            
            print(f"✅ 客户端注册成功")
            print(f"   Client ID: {self.client_info['clientId'][:30]}...")
            
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ 客户端注册失败: {error_msg}")
            
            # 如果是 SSL 错误，提供解决方案
            if 'SSL' in error_msg or 'ssl' in error_msg.lower():
                print(f"\n⚠️  检测到 SSL 错误，可能的原因：")
                print(f"   1. 网络需要代理访问 AWS")
                print(f"   2. SSL 证书验证问题")
                print(f"   3. 防火墙或网络限制")
                print(f"\n💡 建议解决方案：")
                print(f"   1. 检查网络连接")
                print(f"   2. 配置代理: set HTTPS_PROXY=http://your-proxy:port")
                print(f"   3. 尝试使用 VPN")
                print(f"   4. 检查防火墙设置")
                
                # 尝试2: 设置环境变量禁用 SSL 验证（不推荐，仅用于测试）
                print(f"\n🔄 尝试禁用 SSL 验证（仅用于调试）...")
                try:
                    import os
                    import ssl
                    # 设置环境变量
                    os.environ['PYTHONHTTPSVERIFY'] = '0'
                    os.environ['CURL_CA_BUNDLE'] = ''
                    
                    # 重新创建客户端
                    import botocore.config
                    config = botocore.config.Config(
                        retries={'max_attempts': 3, 'mode': 'standard'}
                    )
                    client = boto3.client('sso-oidc', region_name=self.region, config=config, verify=False)
                    
                    response = client.register_client(
                        clientName='Kiro IDE',
                        clientType='public',
                        scopes=[
                            'codewhisperer:completions',
                            'codewhisperer:analysis',
                            'codewhisperer:conversations',
                            'codewhisperer:transformations',
                            'codewhisperer:taskassist'
                        ],
                        grantTypes=['authorization_code', 'refresh_token'],
                        redirectUris=['http://127.0.0.1/oauth/callback'],
                        issuerUrl=self.start_url
                    )
                    
                    self.client_info = {
                        'clientId': response['clientId'],
                        'clientSecret': response['clientSecret'],
                        'expiresAt': response['clientSecretExpiresAt']
                    }
                    
                    print(f"✅ 客户端注册成功（已禁用 SSL 验证）")
                    print(f"   Client ID: {self.client_info['clientId'][:30]}...")
                    
                    return True
                except Exception as e2:
                    print(f"❌ 禁用 SSL 验证后仍然失败: {e2}")
            
            return False
        
        finally:
            # 恢复代理设置
            for var, value in original_proxy.items():
                os.environ[var] = value
            if original_proxy:
                print("   💡 已恢复代理设置")
    
    def start_callback_server(self):
        """启动本地回调服务器（与 Kiro 相同）"""
        print("\n2️⃣ 启动本地回调服务器...")
        
        # 尝试端口范围（与 Kiro 类似）
        port_range = range(49153, 53154)  # Kiro 使用的端口范围
        
        class CallbackHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, parent=None, **kwargs):
                self.parent = parent
                super().__init__(*args, **kwargs)
            
            def do_GET(self):
                # 解析回调 URL
                parsed = urllib.parse.urlparse(self.path)
                
                if parsed.path == '/oauth/callback':
                    # 获取授权码
                    params = urllib.parse.parse_qs(parsed.query)
                    
                    if 'code' in params:
                        self.parent.authorization_code = params['code'][0]
                        
                        # 返回成功页面
                        self.send_response(200)
                        self.send_header('Content-type', 'text/html')
                        self.end_headers()
                        self.wfile.write(b'''
                            <html>
                            <body>
                                <h1>Authorization Successful!</h1>
                                <p>You can close this window now.</p>
                            </body>
                            </html>
                        ''')
                        
                        print(f"\n✅ 收到授权码: {self.parent.authorization_code[:20]}...")
                    else:
                        # 错误处理
                        self.send_response(400)
                        self.send_header('Content-type', 'text/html')
                        self.end_headers()
                        self.wfile.write(b'<html><body><h1>Error</h1></body></html>')
                
                return
            
            def log_message(self, format, *args):
                # 禁用日志输出
                pass
        
        # 尝试找到可用端口
        for port in port_range:
            try:
                handler = lambda *args, **kwargs: CallbackHandler(*args, parent=self, **kwargs)
                self.server = socketserver.TCPServer(("127.0.0.1", port), handler)
                self.port = port
                print(f"✅ 回调服务器已启动在端口: {port}")
                break
            except OSError:
                continue
        
        if not self.server:
            print("❌ 无法找到可用端口")
            return False
        
        # 在后台线程运行服务器
        server_thread = Thread(target=self.server.serve_forever, daemon=True)
        server_thread.start()
        
        return True
    
    def build_authorization_url(self):
        """构建授权 URL（与 Kiro 完全相同）"""
        redirect_uri = f"http://127.0.0.1:{self.port}/oauth/callback"
        
        # 构建参数（与 Kiro 源码相同）
        params = {
            'response_type': 'code',
            'client_id': self.client_info['clientId'],
            'redirect_uri': redirect_uri,
            'scopes': ','.join([
                'codewhisperer:completions',
                'codewhisperer:analysis',
                'codewhisperer:conversations',
                'codewhisperer:transformations',
                'codewhisperer:taskassist'
            ]),
            'state': self.state,
            'code_challenge': self.code_challenge,
            'code_challenge_method': 'S256'
        }
        
        # 构建 URL
        auth_url = f"https://oidc.{self.region}.amazonaws.com/authorize?" + \
                   urllib.parse.urlencode(params)
        
        return auth_url
    
    async def async_open_browser_for_authorization(self, auto_register=False, email=None, headless=False):
        """使用指纹浏览器打开授权页面并可选自动注册
        
        Args:
            auto_register: 是否自动注册
            email: 注册使用的邮箱，如果为None则自动生成
            headless: 是否使用无头模式
        """
        print("\n3️⃣ 使用指纹浏览器打开授权页面...")
        
        auth_url = self.build_authorization_url()
        
        print(f"\n🌐 授权 URL:")
        print(f"   {auth_url[:100]}...")
        
        if auto_register:
            print(f"\n🤖 自动注册模式已启用")
            print(f"   注册邮箱: {email}")
        
        try:
            # 配置代理
            proxy_config = None
            use_proxy = PROXY_CONFIG.get('use_proxy', False)
            use_residential = PROXY_CONFIG.get('use_residential_proxy', False)
            
            if use_residential:
                # 住宅代理模式（优先级最高，使用 HTTP 协议 + 认证）
                rp_host = PROXY_CONFIG.get('residential_host', '')
                rp_port = PROXY_CONFIG.get('residential_port', 30000)
                rp_user = PROXY_CONFIG.get('residential_username', '')
                rp_pass = PROXY_CONFIG.get('residential_password', '')
                
                proxy_config = {
                    'server': f'http://{rp_host}:{rp_port}',
                    'bypass': '127.0.0.1,localhost',
                }
                if rp_user:
                    proxy_config['username'] = rp_user
                if rp_pass:
                    proxy_config['password'] = rp_pass
                
                print(f"✅ 使用住宅代理: {rp_host}:{rp_port}")
                if rp_user:
                    print(f"   🔑 认证用户: {rp_user}")
                print(f"   📍 已排除本地地址 (127.0.0.1, localhost)")
                
                # 预检测代理出口IP（用于 GeoIP 欺骗，避免 Camoufox 内部 Proxy 类不支持 bypass 参数）
                residential_geoip = False
                try:
                    ip_proxies = {
                        'http': f'http://{rp_user}:{rp_pass}@{rp_host}:{rp_port}',
                        'https': f'http://{rp_user}:{rp_pass}@{rp_host}:{rp_port}',
                    }
                    ip_resp = requests.get('https://api.ipify.org', proxies=ip_proxies, timeout=15)
                    if ip_resp.status_code == 200:
                        residential_geoip = ip_resp.text.strip()
                        print(f"   🌍 代理出口IP: {residential_geoip}")
                except Exception as e:
                    print(f"   ⚠️ 无法检测代理IP，跳过GeoIP欺骗: {str(e)[:50]}")
            elif use_proxy:
                # HTTP 代理模式
                # 先切换代理节点，再打开浏览器
                print(f"\n🔄 切换代理节点...")
                switch_proxy_node()
                import time
                time.sleep(2)  # 等待代理切换稳定
                
                proxy_url = PROXY_CONFIG.get('proxy_url', 'http://127.0.0.1:7897')
                proxy_config = {
                    'server': proxy_url,
                }
                print(f"✅ 使用 HTTP 代理: {proxy_url}")
                
                # 测试代理连接
                if not test_proxy_connection(proxy_url):
                    print(f"⚠️ 代理连接测试失败，但将继续尝试...")
                    print(f"💡 请确保代理服务 {proxy_url} 正在运行\n")
                else:
                    print()
            else:
                print(f"💡 不使用代理\n")
            
            print(f"\n🦊 启动 AsyncCamoufox 指纹浏览器...")
            print(f"   运行模式: {'无头模式' if headless else '可视模式'}")
            # geoip: 住宅代理时传入预检测的IP字符串（避免 Camoufox 内部用 Proxy(**proxy) 解析 bypass 报错）
            geoip_value = residential_geoip if (use_residential and 'residential_geoip' in dir()) else False
            async with AsyncCamoufox(
                proxy=proxy_config,
                headless=headless,
                geoip=geoip_value,
                humanize=True,
            ) as browser:
                print(f"✅ 指纹浏览器已启动")
                
                context = await browser.new_context(
                    ignore_https_errors=True
                )
                page = await context.new_page()
                
                # 设置为0表示无限超时
                page.set_default_timeout(0)
                page.set_default_navigation_timeout(0)
                
                print(f"✅ 新标签页已创建")
                
                # 访问授权页面（带超时检测）
                print(f"\n📱 正在访问授权页面（超时60秒）...")
                try:
                    await page.goto(auth_url, wait_until='networkidle', timeout=60000)  # 60秒超时
                except Exception as nav_error:
                    print(f"   ❌ 页面加载超时或失败: {nav_error}")
                    print(f"   ⚠️ 网络问题，不标记邮箱，直接跳过")
                    return 'skip'
                
                # 检测页面加载失败（连接错误）
                page_load_error_indicators = [
                    "Secure Connection Failed",
                    "Connection Failed", 
                    "PR_END_OF_FILE_ERROR",
                    "SSL_ERROR",
                    "NET_ERROR",
                    "ERR_CONNECTION",
                    "ERR_TIMED_OUT",
                    "This site can't be reached",
                    "Unable to connect",
                    "连接失败",
                    "无法连接",
                ]
                
                try:
                    page_content = await page.content()
                    page_text = await page.inner_text('body') if await page.locator('body').count() > 0 else ""
                    
                    for indicator in page_load_error_indicators:
                        if indicator.lower() in page_content.lower() or indicator.lower() in page_text.lower():
                            print(f"   ❌ 检测到页面加载错误: {indicator}")
                            print(f"   ⚠️ 网络问题，不标记邮箱，直接跳过")
                            return 'skip'
                except Exception as check_error:
                    print(f"   ⚠️ 检查页面内容时出错: {check_error}")
                
                print(f"✅ 授权页面已加载")
                
                if auto_register:
                    # 自动注册流程
                    register_result = await self._auto_register(page, email)
                    
                    # 检查是否需要跳过或出错
                    if register_result == 'skip':
                        print(f"\n⏭️ 跳过当前流程，进入下一次循环")
                        return 'skip'  # 返回特殊状态，告诉调用方跳过
                    elif register_result == 'error':
                        print(f"\n❌ 注册流程出错，跳过当前账号")
                        return 'skip'  # 出错也跳过，不要卡住
                else:
                    # 手动授权流程
                    print(f"\n⏳ 等待手动授权...")
                
                # 检查是否已经收到授权码（自动注册流程可能已经完成）
                if self.authorization_code:
                    print(f"\n✅ 授权成功！")
                    return True
                
                # 等待授权码（最多等待60秒，避免无限等待）
                print(f"\n⏳ 等待授权（最多60秒）...")
                
                wait_count = 0
                max_wait = 120  # 60秒 (120次 * 0.5秒)
                while not self.authorization_code and wait_count < max_wait:
                    await asyncio.sleep(0.5)
                    wait_count += 1
                    if wait_count % 20 == 0:  # 每10秒打印一次
                        print(f"   ⏳ 等待授权中... [{wait_count // 2}s/60s]")
                
                if self.authorization_code:
                    print(f"\n✅ 授权成功！")
                    return True
                else:
                    print(f"\n⚠️ 等待授权超时（60秒），跳过当前账号")
                    return 'skip'
                
        except Exception as e:
            print(f"❌ 浏览器操作失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def _auto_register(self, page, email):
        """自动注册流程（优化版 - 学习自 windsurf_create_complete.py）
        
        Args:
            page: Playwright page 对象
            email: 注册邮箱
        
        Returns:
            str: 'success' 成功, 'skip' 跳过（邮箱已注册）, 'error' 错误
        """
        print(f"\n{'='*70}")
        print(f"🤖 开始自动注册流程")
        print(f"{'='*70}")
        
        try:
            # ========== 步骤 0: 处理 Cookie 弹窗 ==========
            print(f"\n🍪 步骤 0/7: 检查并关闭Cookie弹窗...")
            try:
                # 等待最多5秒检查是否有Accept按钮
                accept_selectors = [
                    'button:has-text("Accept")',
                    'button:has-text("Accept all")',
                    'button:has-text("Accept cookies")',
                    '[id*="accept" i]',
                    '[class*="accept" i]',
                ]
                
                cookie_handled = False
                for selector in accept_selectors:
                    try:
                        accept_btn = page.locator(selector).first
                        if await accept_btn.count() > 0 and await accept_btn.is_visible():
                            print(f"   ✅ 检测到Cookie弹窗")
                            await random_click(accept_btn)
                            print(f"   ✅ 已点击Accept按钮")
                            await asyncio.sleep(1)
                            cookie_handled = True
                            break
                    except:
                        continue
                
                if not cookie_handled:
                    print(f"   💡 未检测到Cookie弹窗")
            except Exception as e:
                print(f"   💡 处理Cookie弹窗时出错，继续流程...")
            
            # ========== 步骤 1: 等待并填写邮箱 ==========
            print(f"\n📝 步骤 1/7: 填写邮箱...")
            print(f"   ⏳ 持续查找邮箱输入框（最多等待60秒）...")
            
            email_selectors = [
                'input[placeholder*="@"]',                      # placeholder 包含 @ 符号（通用）
                'input[placeholder*="example.com"]',            # placeholder 包含 example.com
                'input[placeholder*="email"]',                  # placeholder 包含 email
                'input[id*="email"]',                           # id 包含 email
                'input[autocomplete="on"][type="text"]',        # AWS 常用的文本输入框
                'input.awsui_input',                            # AWS UI 输入框类
            ]
            
            email_filled = False
            attempt = 0
            max_email_wait = 300  # 60秒 (300次 * 0.2秒)
            
            # 最多等待60秒
            while attempt < max_email_wait:
                try:
                    for selector in email_selectors:
                        email_input = page.locator(selector).first
                        if await email_input.count() > 0 and await email_input.is_visible():
                            elapsed = (attempt + 1) * 0.2
                            print(f"   ✅ 找到邮箱输入框 (选择器: {selector}, 耗时: {elapsed:.1f}秒)")
                            print(f"   📧 正在填写邮箱: {email}")
                            
                            # 使用 type 而不是 fill，模拟真实输入
                            await random_click(email_input)
                            await asyncio.sleep(0.1)
                            await email_input.type(email, delay=25)
                            
                            print(f"   ✅ 邮箱已成功填写")
                            email_filled = True
                            break
                    
                    if email_filled:
                        break
                        
                except Exception as e:
                    pass
                
                # 每5秒显示进度
                if (attempt + 1) % 25 == 0:  # 25次 * 0.2秒 = 5秒
                    elapsed = (attempt + 1) * 0.2
                    print(f"   ⏳ 持续查找中... [{int(elapsed)}秒/60秒]")
                
                attempt += 1
                await asyncio.sleep(0.2)
            
            # 检查是否超时
            if not email_filled:
                print(f"   ❌ 60秒内未找到邮箱输入框，页面可能加载失败")
                print(f"   ⏭️ 跳过当前账号，进入下一个...")
                return 'skip'
            
            await asyncio.sleep(0.2)
            
            # ========== 步骤 2: 点击提交按钮（邮箱） ==========
            print(f"\n🔘 步骤 2/7: 点击提交（邮箱）...")
            
            submit_selectors = [
                'button:has-text("Continue")',
                'button:has-text("Next")',
                'button:has-text("Continuer")',    # French
                'button:has-text("Weiter")',        # German
                'button:has-text("Continuar")',     # Spanish/Portuguese
                'button:has-text("Avanti")',        # Italian
                'button:has-text("続行")',             # Japanese
                'button:has-text("計続")',             # Japanese alt
                'button:has-text("继续")',             # Chinese
                'button[type="submit"]',
                'button:has-text("Sign")',
                'input[type="submit"]',
            ]
            
            # 邮箱页面特征（用于检测是否仍在邮箱页面）
            email_page_indicators = [
                "Get started",
                "Continue with Google",
                "AWS Customer Agreement",
            ]
            
            # 下一页面特征（姓名页面、验证码页面或登录页面）- 多语言
            next_page_indicators = NEXT_PAGE_AFTER_EMAIL_INDICATORS
            
            # 邮箱提交重试循环（最多重试10次）
            email_retry = 0
            email_submit_success = False
            max_email_retry = 10  # 最大重试次数
            
            while not email_submit_success and email_retry < max_email_retry:
                email_retry += 1
                if email_retry > 1:
                    print(f"   🔄 第 {email_retry}/{max_email_retry} 次尝试提交邮箱...")
                
                # 点击提交按钮
                button_clicked = False
                for selector in submit_selectors:
                    try:
                        button = page.locator(selector).first
                        if await button.count() > 0 and await button.is_visible():
                            await random_click(button)
                            print(f"   ✅ 已点击提交按钮")
                            button_clicked = True
                            break
                    except:
                        continue
                
                if not button_clicked:
                    print(f"   ⚠️ 未找到提交按钮，等待后重试...")
                    await asyncio.sleep(2)
                    continue
                
                # 等待页面响应并多次检测（增加检测时间）
                for detect_attempt in range(4):  # 检测4次，每次2.5秒，共10秒
                    await asyncio.sleep(2.5)  # 增加到2.5秒间隔
                    
                    # 检测是否有错误弹窗
                    if await has_error_popup(page):
                        print(f"   ⚠️ 检测到错误弹窗，准备重试...")
                        break  # 跳出检测循环，进入重试
                    
                    # 检测是否已跳转到下一页面
                    try:
                        page_text = await page.locator('body').inner_text(timeout=2000)
                        
                        # 检测是否到达下一页面
                        reached_next = any(ind in page_text for ind in next_page_indicators)
                        if reached_next:
                            print(f"   ✅ 邮箱提交成功，已跳转到下一页面")
                            email_submit_success = True
                            break
                        
                        # 检测是否仍在邮箱页面
                        still_on_email = any(ind in page_text for ind in email_page_indicators)
                        if not still_on_email:
                            print(f"   ✅ 邮箱提交成功，页面已跳转")
                            email_submit_success = True
                            break
                    except:
                        pass
                    
                    print(f"   ⏳ 检测页面状态... [{detect_attempt + 1}/4] (等待页面跳转)")
                
                if email_submit_success:
                    break
                
                # 如果检测到错误，等待后重试
                await asyncio.sleep(2)
            
            # 检查是否超过最大重试次数
            if not email_submit_success and email_retry >= max_email_retry:
                print(f"   ❌ 提交邮箱失败超过 {max_email_retry} 次，跳过当前账号")
                print(f"   ⏭️ 进入下一个...")
                return 'skip'  # 返回跳过状态，不标记邮箱为已使用
            
            await asyncio.sleep(0.3)
            
            # ========== 检测是否跳转到登录页面（邮箱已注册） ==========
            print(f"\n🔍 检测页面状态...")
            
            # 登录页面特征（说明邮箱已注册）- 多语言
            login_page_indicators = LOGIN_PAGE_INDICATORS
            
            try:
                await asyncio.sleep(1)  # 等待页面加载
                page_text = await page.locator('body').inner_text(timeout=3000)
                
                # 检测是否是登录页面
                is_login_page = any(indicator in page_text for indicator in login_page_indicators)
                
                # 额外检测：只有一个密码输入框（登录页面），而不是两个（注册页面）
                if is_login_page:
                    password_inputs = page.locator('input[type="password"]')
                    password_count = await password_inputs.count()
                    
                    # 登录页面只有1个密码框，注册页面有2个（密码+确认密码）
                    if password_count == 1:
                        print(f"   ⚠️ 检测到登录页面，该邮箱已注册！")
                        print(f"   🗑️ 标记邮箱为已使用: {email}")
                        retire_email_account(email, getattr(self, 'email_source', 'file'))
                        print(f"   ⏭️ 跳过此邮箱，进入下一次循环")
                        return 'skip'  # 返回跳过状态
                    
                print(f"   ✅ 页面状态正常，继续注册流程")
            except Exception as e:
                print(f"   💡 页面检测异常，继续流程: {str(e)[:50]}")
            
            # ========== 启动 IMAP 验证码监听（file / kiro_api / yahoo 模式）==========
            imap_listener = None
            email_source = getattr(self, 'email_source', 'file')
            
            # file / kiro_api / yahoo 模式都使用 IMAP 监听
            if email_source in ['file', 'kiro_api', 'yahoo']:
                listener_account = get_imap_listener_account(
                    email_source=email_source,
                    email_addr=email,
                    email_extra_data=getattr(self, 'email_extra_data', None),
                )

                if listener_account:
                    print(f"\n📡 启动 IMAP 验证码监听...")
                    print(f"   📧 监听邮箱: {listener_account['receive_email']}")
                    print(f"   🎯 目标收件人: {email}")
                    
                    imap_listener = IMAPCodeListener(
                        receive_email=listener_account['receive_email'],
                        receive_password=listener_account['receive_password'],
                        sender_pattern=listener_account.get('sender_pattern'),
                        target_email=listener_account.get('target_email')
                    )
                    imap_listener.start_listening()
                    print(f"   ✅ IMAP 后台监听已启动")
                else:
                    print(f"   ⚠️ {email_source} 模式未配置可用的 IMAP 凭据，无法启动监听")
            elif email_source == 'outlook':
                print(f"\n📡 邮箱来源: outlook，跳过 IMAP 监听")
            else:
                print(f"\n📡 邮箱来源: {email_source}，使用其他方式获取验证码")
            
            # ========== 步骤 3: 填写姓名（如果需要） ==========
            print(f"\n👤 步骤 3/7: 检查是否需要填写姓名...")
            
            # 先处理可能出现的 Cookie 弹窗
            print(f"   🍪 检查 Cookie 弹窗...")
            try:
                accept_selectors = [
                    '#awsccc-cb-btn-accept',
                    '[data-id="awsccc-cb-btn-accept"]',
                    'button.awsccc-cs-btn-content:has-text("Accept")',
                    'button:has-text("Accept all")',
                    'button:has-text("Accept cookies")',
                ]
                
                cookie_found = False
                for selector in accept_selectors:
                    try:
                        accept_btn = page.locator(selector).first
                        if await accept_btn.count() > 0 and await accept_btn.is_visible(timeout=1000):
                            print(f"   ✅ 检测到 Cookie 弹窗 (选择器: {selector})")
                            await random_click(accept_btn, timeout=3000)
                            print(f"   ✅ 已点击 Accept 按钮")
                            await asyncio.sleep(0.5)
                            cookie_found = True
                            break
                    except:
                        continue
                if not cookie_found:
                    print(f"   💡 未检测到 Cookie 弹窗")
            except Exception as e:
                print(f"   💡 处理 Cookie 弹窗时出错，继续流程...")
            
            # 生成随机姓名（提前生成）
            import random
            first_names = ['John', 'Jane', 'Alex', 'Maria', 'David', 'Sarah']
            last_names = ['Smith', 'Johnson', 'Brown', 'Garcia', 'Silva', 'Lee']
            full_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            
            # 姓名输入框选择器（更通用）
            name_selectors = [
                'input[placeholder*="Maria"]',          # AWS 常用
                'input[placeholder*="José"]',           # AWS 常用
                'input[class*="awsui_input"][type="text"]:not([placeholder*="@"]):not([placeholder*="digit"])',  # AWS UI 输入框（排除邮箱和验证码）
            ]
            
            # 验证码页面特征 - 多语言
            verification_indicators = VERIFICATION_CODE_INDICATORS
            
            # 登录页面特征（邮箱已注册，需要登录）- 多语言
            login_page_indicators = LOGIN_PAGE_INDICATORS
            
            name_filled = False
            attempt = 0
            max_name_wait = 120  # 最多等待60秒（120次 * 0.5秒）
            
            print(f"   🎲 生成的姓名: {full_name}")
            print(f"   ⏳ 持续查找姓名输入框（最多60秒）...")
            
            # 持续查找直到找到姓名输入框、验证码页面或登录页面
            while attempt < max_name_wait:
                try:
                    # 每次循环都检测 Cookie 弹窗（可能在页面加载后才出现）
                    if attempt % 4 == 0:  # 每2秒检测一次 Cookie 弹窗
                        try:
                            cookie_selectors = [
                                '#awsccc-cb-btn-accept',
                                '[data-id="awsccc-cb-btn-accept"]',
                                'button.awsccc-cs-btn-content:has-text("Accept")',
                                'button:has-text("Accept all")',
                            ]
                            for cookie_sel in cookie_selectors:
                                cookie_btn = page.locator(cookie_sel).first
                                if await cookie_btn.count() > 0 and await cookie_btn.is_visible(timeout=500):
                                    print(f"   🍪 检测到 Cookie 弹窗，正在关闭...")
                                    await random_click(cookie_btn, timeout=3000)
                                    print(f"   ✅ 已关闭 Cookie 弹窗")
                                    await asyncio.sleep(0.5)
                                    break
                        except:
                            pass
                    
                    # 先检测是否已经跳转到验证码页面（说明不需要填写姓名）
                    try:
                        page_text = await page.locator('body').inner_text(timeout=1000)
                        if any(indicator in page_text for indicator in verification_indicators):
                            print(f"   💡 检测到已跳转到验证码页面，跳过姓名填写")
                            break
                        
                        # 检测是否跳转到登录页面（邮箱已注册）
                        if any(indicator in page_text for indicator in login_page_indicators):
                            print(f"   ⚠️ 检测到登录页面，该邮箱已注册！")
                            print(f"   🗑️ 标记邮箱为已使用: {email}")
                            retire_email_account(email, getattr(self, 'email_source', 'file'))
                            if imap_listener:
                                imap_listener.stop()
                            return 'skip'  # 返回跳过状态
                        
                        # 检测是否只有1个密码输入框（登录页面特征）
                        password_inputs = page.locator('input[type="password"]')
                        password_count = await password_inputs.count()
                        
                        # 登录页面只有1个密码框，注册页面有2个（密码+确认密码）
                        if password_count == 1:
                            print(f"   ⚠️ 检测到登录页面（单密码框），该邮箱已注册！")
                            print(f"   🗑️ 标记邮箱为已使用: {email}")
                            retire_email_account(email, getattr(self, 'email_source', 'file'))
                            if imap_listener:
                                imap_listener.stop()
                            return 'skip'  # 返回跳过状态
                    except:
                        pass
                    
                    for selector in name_selectors:
                        try:
                            name_input = page.locator(selector).first
                            if await name_input.count() > 0 and await name_input.is_visible():
                                elapsed = (attempt + 1) * 0.5
                                print(f"   ✅ 检测到姓名输入框 (选择器: {selector}, 耗时: {elapsed:.1f}秒)")
                                print(f"   ⌨️  开始输入姓名...")
                                
                                await random_click(name_input)
                                await asyncio.sleep(0.1)
                                await name_input.type(full_name, delay=25)
                                
                                print(f"   ✅ 已填写姓名: {full_name}")
                                name_filled = True
                                break
                        except Exception as e:
                            continue
                    
                    if name_filled:
                        break
                        
                except Exception as e:
                    pass
                
                # 每5秒显示进度
                if (attempt + 1) % 10 == 0:
                    elapsed = (attempt + 1) * 0.5
                    print(f"   ⏳ 持续查找中... [{int(elapsed)}秒]")
                
                attempt += 1
                await asyncio.sleep(0.5)
            
            # 检查是否超时
            if attempt >= max_name_wait and not name_filled:
                print(f"   ⚠️ 60秒内未检测到姓名输入框，检查页面状态...")
                
                # 最后再检测一次是否是登录页面
                try:
                    page_text = await page.locator('body').inner_text(timeout=2000)
                    if any(indicator in page_text for indicator in login_page_indicators):
                        print(f"   ⚠️ 确认是登录页面，该邮箱已注册！")
                        print(f"   🗑️ 标记邮箱为已使用: {email}")
                        retire_email_account(email, getattr(self, 'email_source', 'file'))
                        if imap_listener:
                            imap_listener.stop()
                        return 'skip'
                    
                    # 检测单密码框
                    password_inputs = page.locator('input[type="password"]')
                    password_count = await password_inputs.count()
                    if password_count == 1:
                        print(f"   ⚠️ 确认是登录页面（单密码框），该邮箱已注册！")
                        print(f"   🗑️ 标记邮箱为已使用: {email}")
                        retire_email_account(email, getattr(self, 'email_source', 'file'))
                        if imap_listener:
                            imap_listener.stop()
                        return 'skip'
                except:
                    pass
                
                print(f"   💡 继续流程，可能已跳转到验证码页面...")
            
            if name_filled:
                await asyncio.sleep(0.2)  # 优化：从0.5s减少到0.2s
                
                # 点击 Continue 按钮（带错误检测和重试）
                print(f"   🔘 点击继续按钮...")
                
                continue_selectors = [
                    'button:has-text("Continue")',
                    'button:has-text("Next")',
                    'button:has-text("Continuer")',    # French
                    'button:has-text("Weiter")',        # German
                    'button:has-text("Continuar")',     # Spanish/Portuguese
                    'button:has-text("Avanti")',        # Italian
                    'button:has-text("继续")',             # Chinese
                    'button[type="submit"]',
                ]
                
                # 姓名页面特征
                name_page_indicators = [
                    "Enter your name", "What's your name", "Your name", 
                    "输入姓名", "你的名字", "Maria", "José"
                ]
                
                # 使用带重试的点击函数
                name_submit_success = await click_and_wait_with_retry(
                    page,
                    continue_selectors,
                    name_page_indicators,
                    action_name="提交姓名",
                    max_retries=10
                )
                
                if not name_submit_success:
                    print(f"   ⚠️ 提交姓名可能未成功，继续流程...")
                
                await asyncio.sleep(0.3)
            
            # 如果没有填写姓名（已跳转到验证码页面），不需要额外提示
            
            # ========== 步骤 4: 等待验证码页面 ==========
            print(f"\n📬 步骤 4/7: 等待验证码页面...")
            print(f"   ⏳ 查找验证码输入框（最多300秒）...")
            
            verification_found = False
            attempt = 0
            max_verification_wait = 1500  # 300秒 (1500次 * 0.2秒)
            
            while attempt < max_verification_wait:
                try:
                    # 查找验证码输入框（优先匹配 6-digit 或 verification）
                    code_selectors = [
                        'input[placeholder*="digit" i]',              # placeholder 包含 digit
                        'input[placeholder*="verification" i]',       # placeholder 包含 verification
                        'input[placeholder*="code" i]',               # placeholder 包含 code
                        'input[aria-labelledby*="formField"][type="text"]',  # AWS 的 formField
                    ]
                    
                    for selector in code_selectors:
                        code_input = page.locator(selector).first
                        if await code_input.count() > 0 and await code_input.is_visible():
                            elapsed = (attempt + 1) * 0.2
                            print(f"   ✅ 验证码页面已出现 (耗时 {elapsed:.1f} 秒)")
                            print(f"   使用选择器: {selector}")
                            verification_found = True
                            break
                    
                    if verification_found:
                        break
                        
                except:
                    pass
                
                # 每5秒显示进度
                if (attempt + 1) % 25 == 0:
                    elapsed = (attempt + 1) * 0.2
                    print(f"   ⏳ 持续查找中... [{int(elapsed)}秒/300秒]")
                
                attempt += 1
                await asyncio.sleep(0.2)
            
            if not verification_found:
                print(f"   ❌ 300秒内未找到验证码页面，跳过当前账号")
                if imap_listener:
                    imap_listener.stop()
                return 'skip'
            
            # ========== 处理 Cookie 弹窗（如果存在） ==========
            print(f"\n🍪 检查 Cookie 弹窗...")
            try:
                cookie_selectors = [
                    'button:has-text("Accept")',
                    'button:has-text("Accept all")',
                    'button.awsccc-cs-btn-content:has-text("Accept")',
                    '[data-id="awsccc-cb-btn-accept"]',
                    '#awsccc-cb-btn-accept',
                ]
                
                cookie_handled = False
                for selector in cookie_selectors:
                    try:
                        cookie_btn = page.locator(selector).first
                        if await cookie_btn.count() > 0 and await cookie_btn.is_visible():
                            print(f"   ✅ 检测到 Cookie 弹窗")
                            await random_click(cookie_btn)
                            print(f"   ✅ 已点击 Accept 按钮")
                            await asyncio.sleep(1)
                            cookie_handled = True
                            break
                    except:
                        continue
                
                if not cookie_handled:
                    print(f"   💡 未检测到 Cookie 弹窗")
            except Exception as e:
                print(f"   💡 处理 Cookie 弹窗时出错: {str(e)[:50]}")
            
            # ========== 步骤 5: 获取并输入验证码 ==========
            print(f"\n🔑 步骤 5/7: 获取并输入验证码...")
            
            verification_code = None
            email_source = getattr(self, 'email_source', 'file')
            email_extra_data = getattr(self, 'email_extra_data', None)
            
            if email_source == 'boomlify' and email_extra_data:
                # Boomlify 方式：从临时邮箱获取验证码
                print(f"   📧 等待 Boomlify 临时邮箱收到验证码...")
                temp_mail = email_extra_data
                
                # 等待邮件到达
                email_msg = temp_mail.wait_for_email(timeout=120, check_interval=5)
                if email_msg:
                    # 从邮件中提取验证码（Boomlify 使用 body_text 和 body_html）
                    subject = email_msg.get('subject', '')
                    # 尝试多种字段名
                    body = (email_msg.get('body_text', '') or 
                            email_msg.get('body_html', '') or 
                            email_msg.get('body', '') or 
                            email_msg.get('text', '') or 
                            email_msg.get('html', '') or
                            email_msg.get('content', ''))
                    
                    # 打印邮件详情用于调试
                    print(f"   📧 邮件主题: {subject}")
                    print(f"   📧 邮件内容预览: {body[:200] if body else '(空)'}...")
                    
                    # 如果 body 为空，打印所有可用字段
                    if not body:
                        print(f"   ⚠️ 邮件内容为空，可用字段: {list(email_msg.keys())}")
                        # 尝试从任意字段提取
                        for key, value in email_msg.items():
                            if isinstance(value, str) and len(value) > 50:
                                body = value
                                print(f"   💡 使用字段 '{key}' 作为邮件内容")
                                break
                    
                    verification_code = temp_mail.get_verification_code(body, subject)
                    if verification_code:
                        print(f"   ✅ 从 Boomlify 邮件提取验证码: {verification_code}")
                    else:
                        print(f"   ⚠️ 无法从邮件中提取验证码")
                else:
                    print(f"   ❌ Boomlify 邮箱未收到邮件")
            
            elif email_source == 'kiro_api':
                # Kiro API 方式：改为使用 IMAP 监听获取验证码
                print(f"   📧 等待 IMAP 监听器获取验证码...")
                if imap_listener:
                    # 等待后台监听器获取验证码（最多4分钟）
                    verification_code = imap_listener.get_code(timeout=240)
                else:
                    print(f"   ❌ IMAP 监听器未启动")
                    verification_code = None
            
            elif email_source == 'outlook' and email_extra_data:
                # Outlook 方式：通过 API 传递 refresh_token 和 client_id 获取验证码
                print(f"   📧 从 Outlook API 获取验证码...")
                verification_code = get_outlook_verification_code(email_extra_data, max_retries=40, retry_interval=3)
                
                # 检查是否是 token 过期
                if verification_code == 'TOKEN_EXPIRED':
                    print(f"   🗑️ Token 已过期，从邮箱池删除该邮箱")
                    retire_email_account(email, getattr(self, 'email_source', 'file'))
                    print(f"   ⏭️ 跳过当前账号，进入下一个...")
                    return 'skip'
            
            else:
                # File/IMAP 方式：从 IMAP 监听器获取验证码
                print(f"   📧 等待 IMAP 监听器获取验证码...")
                if imap_listener:
                    # 等待后台监听器获取验证码（最多4分钟）
                    verification_code = imap_listener.get_code(timeout=240)
            
            if not verification_code or verification_code == 'TOKEN_EXPIRED':
                print(f"   ❌ 获取验证码失败或超时")
                if imap_listener:
                    imap_listener.stop()
                print(f"   ⏭️ 跳过当前账号，进入下一个...")
                return 'skip'  # 返回跳过状态，不要卡在无限等待
            
            # 输入验证码
            print(f"\n   🔑 验证码: {verification_code}")
            print(f"   ⌨️  正在输入验证码...")
            
            try:
                # 使用多个选择器查找验证码输入框
                code_selectors = [
                    'input[placeholder*="digit" i]',              # placeholder 包含 digit
                    'input[placeholder*="verification" i]',       # placeholder 包含 verification
                    'input[placeholder*="code" i]',               # placeholder 包含 code
                    'input[aria-labelledby*="formField"][type="text"]',  # AWS 的 formField
                ]
                
                code_input = None
                for selector in code_selectors:
                    try:
                        temp_input = page.locator(selector).first
                        if await temp_input.count() > 0 and await temp_input.is_visible():
                            code_input = temp_input
                            print(f"   🎯 使用选择器: {selector}")
                            break
                    except:
                        continue
                
                if not code_input:
                    print(f"   ⚠️ 未找到验证码输入框")
                    return 'error'
                
                # 验证码错误检测特征
                code_error_indicators = [
                    "New verification code needed",
                    "invalid code",
                    "incorrect code",
                    "that code didn't work",
                    "code didn't work for us",
                    "验证码无效",
                    "验证码错误",
                    "too many times",
                    "error processing your request",  # 通用服务器错误
                    "Sorry, there was an error",      # AWS 通用错误
                    "Please try again",                # 请求重试提示
                ]
                
                # 需要重新发送验证码的错误（验证码已失效）
                resend_required_indicators = [
                    "New verification code needed",
                    "too many times",
                ]
                
                # 验证码提交循环（支持重新发送，减少重试次数避免卡住）
                max_code_retries = 3
                code_retry = 0
                code_submit_success = False
                
                while code_retry < max_code_retries and not code_submit_success:
                    code_retry += 1
                    
                    # 最高优先级：检查是否已收到授权码
                    if self.authorization_code:
                        print(f"   ✅ 已收到授权码，跳过验证码重试")
                        code_submit_success = True
                        break
                    
                    # 检测是否有授权按钮（多语言）
                    try:
                        allow_buttons = page.locator(build_allow_button_locator_str())
                        if await allow_buttons.count() > 0:
                            first_allow = allow_buttons.first
                            if await first_allow.is_visible():
                                print(f"   ✅ 检测到授权按钮，跳过验证码重试")
                                code_submit_success = True
                                break
                    except:
                        pass
                    
                    # 检测是否已经跳转到密码页面或授权页面
                    try:
                        page_text = await page.locator('body').inner_text(timeout=2000)
                        
                        # 检测授权页面（优先）- 多语言
                        if any(ind in page_text for ind in AUTH_PAGE_INDICATORS):
                            print(f"   ✅ 检测到已在授权页面，跳过验证码重试")
                            code_submit_success = True
                            break
                        
                        # 检测密码页面 - 多语言
                        if any(ind in page_text for ind in PASSWORD_PAGE_INDICATORS):
                            print(f"   ✅ 检测到已在密码页面，跳过验证码重试")
                            code_submit_success = True
                            break
                    except:
                        pass
                    
                    if code_retry > 1:
                        print(f"\n   🔄 第 {code_retry} 次尝试验证码...")
                        
                        # 检测是否需要重新发送验证码
                        try:
                            page_text = await page.locator('body').inner_text(timeout=2000)
                            needs_resend = any(ind in page_text for ind in resend_required_indicators)
                            needs_new_code = any(ind in page_text for ind in code_error_indicators)
                            
                            if needs_resend:
                                # 验证码已失效，需要点击 Resend
                                print(f"   ⚠️ 验证码已失效，点击 Resend code...")
                                
                                resend_selectors = [
                                    'button:has-text("Resend code")',
                                    'button:has-text("Resend")',
                                    'a:has-text("Resend code")',
                                    '[class*="resend"]',
                                ]
                                
                                for resend_sel in resend_selectors:
                                    try:
                                        resend_btn = page.locator(resend_sel).first
                                        if await resend_btn.count() > 0 and await resend_btn.is_visible():
                                            await random_click(resend_btn)
                                            print(f"   ✅ 已点击 Resend code")
                                            break
                                    except:
                                        continue
                                
                                # 等待新验证码发送
                                await asyncio.sleep(3)
                            
                            if needs_new_code:
                                # 验证码错误，等待获取新验证码
                                print(f"   ⏳ 等待获取新验证码...")
                                
                                if imap_listener:
                                    # 清除旧验证码，继续监听新邮件
                                    old_code = imap_listener.verification_code
                                    imap_listener.verification_code = None
                                    imap_listener._code_time = None
                                    
                                    # 重新启动监听
                                    if not imap_listener.is_running:
                                        imap_listener.start_listening()
                                    
                                    # 等待新验证码（最多60秒）
                                    new_code = imap_listener.get_code(timeout=60)
                                    
                                    if new_code and new_code != old_code:
                                        verification_code = new_code
                                        verification_code_str = str(verification_code)
                                        print(f"   ✅ 获取到新验证码: {verification_code_str}")
                                    else:
                                        print(f"   ⚠️ 未获取到新验证码，使用当前验证码重试")
                                        if new_code:
                                            verification_code = new_code
                                            verification_code_str = str(verification_code)
                        except Exception as e:
                            print(f"   ⚠️ 检测验证码错误时出错: {str(e)[:50]}")
                    
                    # 清空输入框
                    try:
                        await random_click(code_input)
                        await code_input.fill('')
                        await asyncio.sleep(0.1)
                    except:
                        pass
                    
                    # 输入验证码
                    verification_code_str = str(verification_code)
                    print(f"   📝 输入验证码: '{verification_code_str}'")
                    
                    await code_input.type(verification_code_str, delay=30)
                    
                    print(f"   ✅ 验证码已输入")
                    await asyncio.sleep(0.3)
                    
                    # 点击继续按钮
                    print(f"   🔘 点击继续按钮...")
                    
                    continue_selectors = [
                        'button:has-text("Continue")',
                        'button:has-text("Verify")',
                        'button:has-text("Continuer")',    # French
                        'button:has-text("Vérifier")',      # French
                        'button:has-text("Weiter")',        # German
                        'button:has-text("Continuar")',     # Spanish/Portuguese
                        'button:has-text("继续")',             # Chinese
                        'button[type="submit"]',
                    ]
                    
                    for cont_sel in continue_selectors:
                        try:
                            cont_btn = page.locator(cont_sel).first
                            if await cont_btn.count() > 0 and await cont_btn.is_visible():
                                await random_click(cont_btn)
                                print(f"   ✅ 已点击继续按钮")
                                break
                        except:
                            continue
                    
                    # 等待页面响应（给服务器处理时间）
                    await asyncio.sleep(2)
                    
                    # 检测并关闭错误弹窗
                    try:
                        error_popup_selectors = [
                            '[role="alert"]',
                            '.awsui-alert-error',
                            '[class*="error"][class*="alert"]',
                            '[class*="Error"]',
                        ]
                        
                        error_detected = False
                        for error_sel in error_popup_selectors:
                            try:
                                error_popup = page.locator(error_sel).first
                                if await error_popup.count() > 0 and await error_popup.is_visible(timeout=1000):
                                    error_text = await error_popup.inner_text()
                                    if any(ind in error_text for ind in code_error_indicators):
                                        print(f"   ⚠️ 检测到错误提示: {error_text[:100]}")
                                        error_detected = True
                                        
                                        # 尝试关闭错误弹窗（查找关闭按钮）
                                        close_selectors = [
                                            f'{error_sel} button[aria-label*="dismiss" i]',
                                            f'{error_sel} button[aria-label*="close" i]',
                                            f'{error_sel} button:has-text("×")',
                                            f'{error_sel} [class*="dismiss"]',
                                        ]
                                        
                                        for close_sel in close_selectors:
                                            try:
                                                close_btn = page.locator(close_sel).first
                                                if await close_btn.count() > 0:
                                                    await random_click(close_btn, timeout=2000)
                                                    print(f"   ✅ 已关闭错误弹窗")
                                                    await asyncio.sleep(0.5)
                                                    break
                                            except:
                                                continue
                                        break
                            except:
                                continue
                        
                        if error_detected:
                            print(f"   🔄 检测到错误，准备重试...")
                            # 跳出当前检测循环，进入下一次重试
                            continue
                    except Exception as e:
                        print(f"   💡 错误弹窗检测异常: {str(e)[:50]}")
                    
                    # 等待页面响应并多次检测（检测60次，每次2.5秒，共150秒）
                    max_detect_attempts = 60
                    detected_next_page = False
                    
                    for detect_attempt in range(max_detect_attempts):
                        await asyncio.sleep(2.5)  # 增加到2.5秒间隔
                        
                        # 最高优先级：检查是否已收到授权码
                        if self.authorization_code:
                            print(f"   ✅ 已收到授权码，验证码提交成功")
                            code_submit_success = True
                            detected_next_page = True
                            break
                        
                        # 优先使用 DOM 元素检测（比文本检测更可靠）
                        try:
                            # 检测是否有授权按钮（多语言）
                            allow_buttons = page.locator(build_allow_button_locator_str())
                            if await allow_buttons.count() > 0:
                                first_allow = allow_buttons.first
                                if await first_allow.is_visible():
                                    print(f"   ✅ 检测到授权按钮，已跳转到授权页面")
                                    code_submit_success = True
                                    detected_next_page = True
                                    break
                        except:
                            pass
                        
                        try:
                            # 检测是否有密码输入框
                            password_inputs = page.locator('input[type="password"]')
                            pwd_count = await password_inputs.count()
                            if pwd_count >= 1:
                                first_pwd = password_inputs.nth(0)
                                if await first_pwd.is_visible():
                                    print(f"   ✅ 检测到密码输入框({pwd_count}个)，已跳转到密码页面")
                                    code_submit_success = True
                                    detected_next_page = True
                                    break
                        except:
                            pass
                        
                        # 检测页面文本
                        try:
                            page_text = await page.locator('body').inner_text(timeout=1000)
                            
                            # 检测是否有验证码错误
                            has_code_error = any(ind in page_text for ind in code_error_indicators)
                            if has_code_error:
                                print(f"   ⚠️ 验证码错误，需要重试...")
                                break  # 跳出检测循环，进入重试
                            
                            # 检测是否已跳转到授权页面 - 多语言
                            on_auth_page = any(ind in page_text for ind in AUTH_PAGE_INDICATORS)
                            
                            if on_auth_page:
                                print(f"   ✅ 验证码提交成功，已跳转到授权页面")
                                code_submit_success = True
                                detected_next_page = True
                                break
                            
                            # 检测是否已跳转到密码页面 - 多语言
                            on_password_page = any(ind in page_text for ind in PASSWORD_PAGE_INDICATORS)
                            
                            if on_password_page:
                                print(f"   ✅ 验证码提交成功，已跳转到密码页面")
                                code_submit_success = True
                                detected_next_page = True
                                break
                        except:
                            pass
                        
                        print(f"   ⏳ 检测页面状态... [{detect_attempt + 1}/{max_detect_attempts}] (等待跳转到授权/密码页面)")
                    
                    # 如果已检测到下一页面，跳出重试循环
                    if detected_next_page:
                        break
                    
                    # 最高优先级：检测是否已经收到授权码
                    if self.authorization_code:
                        print(f"   ✅ 已收到授权码，验证码提交成功")
                        code_submit_success = True
                        break
                    
                    # 检测页面状态（优先检测是否到达授权页面）
                    try:
                        page_text = await page.locator('body').inner_text(timeout=2000)
                        
                        # 授权页面特征（最优先检测）- 多语言
                        on_auth_page = any(ind in page_text for ind in AUTH_PAGE_INDICATORS)
                        if on_auth_page:
                            print(f"   ✅ 验证码提交成功，已跳转到授权页面")
                            code_submit_success = True
                            break
                        
                        # 下一页面的特征（密码页面）- 多语言
                        password_or_success = PASSWORD_PAGE_INDICATORS + ["Authorization Successful"]
                        
                        # 检测是否到达密码页面
                        on_next_page = any(ind in page_text for ind in password_or_success)
                        if on_next_page:
                            print(f"   ✅ 验证码提交成功，已跳转到密码页面")
                            code_submit_success = True
                            break
                        
                        # 验证码页面的核心特征
                        code_page_indicators = [
                            "Enter the 6-digit code",
                            "We sent a code to",
                            "Resend code",
                        ]
                        
                        # 检测是否仍在验证码页面
                        still_on_code_page = any(ind in page_text for ind in code_page_indicators)
                        
                        if not still_on_code_page:
                            print(f"   ✅ 验证码提交成功，页面已跳转")
                            code_submit_success = True
                            break
                        else:
                            # 检查重试次数，超过3次就强制继续（避免卡住）
                            if code_retry >= 2:
                                print(f"   ⚠️ 重试 {code_retry + 1} 次，强制继续下一步...")
                                code_submit_success = True
                                break
                            print(f"   ⚠️ 仍在验证码页面，重试...")
                    except:
                        # 页面可能正在加载，认为成功
                        code_submit_success = True
                        break
                
                if not code_submit_success:
                    print(f"   ❌ 验证码提交失败，已达到最大重试次数")
                
                await asyncio.sleep(1)
                
                # ========== 步骤 6: 创建密码（如果需要） ==========
                print(f"\n🔐 步骤 6/7: 检查是否需要创建密码...")
                
                # 提前生成强密码
                import random
                import string
                uppercase = random.choice(string.ascii_uppercase)
                lowercase = ''.join(random.choices(string.ascii_lowercase, k=4))
                digits = ''.join(random.choices(string.digits, k=3))
                special = random.choice('!@#$%')
                password = uppercase + lowercase + digits + special
                # 打乱顺序
                password_list = list(password)
                random.shuffle(password_list)
                password = ''.join(password_list)
                
                print(f"   🔑 生成的密码: {password}")
                print(f"   ⏳ 持续查找密码输入框（最多等待30秒）...")
                
                password_filled = False
                attempt = 0
                max_password_wait = 60  # 30秒 (60次 * 0.5秒)
                
                # 授权页面特征 - 多语言
                auth_page_indicators = AUTH_PAGE_INDICATORS
                
                # 持续查找直到找到密码输入框、跳转到授权页面、收到授权码、或超时
                while attempt < max_password_wait:
                    # 检查是否已经收到授权码
                    if self.authorization_code:
                        print(f"   💡 已收到授权码，跳过密码设置")
                        break
                    
                    try:
                        # 检测是否已经跳转到授权页面（说明不需要设置密码）
                        try:
                            page_text = await page.locator('body').inner_text(timeout=1000)
                            if any(indicator in page_text for indicator in auth_page_indicators):
                                print(f"   💡 检测到已跳转到授权页面，跳过密码设置")
                                break
                        except:
                            pass
                        
                        # 查找密码输入框
                        password_inputs = page.locator('input[type="password"]')
                        
                        # 检查是否有至少2个密码输入框
                        if await password_inputs.count() >= 2:
                            # 确认第一个输入框可见
                            first_password = password_inputs.nth(0)
                            if await first_password.is_visible():
                                elapsed = (attempt + 1) * 0.5
                                print(f"   ✅ 检测到密码输入框 (耗时 {elapsed:.1f} 秒)")
                                print(f"   ⌨️  开始填写密码...")
                                
                                # 填写第一个密码框
                                await random_click(first_password)
                                await asyncio.sleep(0.1)
                                await first_password.type(password, delay=25)
                                print(f"   ✅ 已填写密码")
                                
                                await asyncio.sleep(0.3)
                                
                                # 关闭密码验证提示框（点击页面空白处或按 Tab 键）
                                try:
                                    # 方法1: 按 Tab 键切换到下一个输入框
                                    await page.keyboard.press('Tab')
                                    await asyncio.sleep(0.2)
                                except:
                                    pass
                                
                                # 填写第二个密码框
                                second_password = password_inputs.nth(1)
                                
                                # 确保第二个输入框可见并点击
                                try:
                                    await second_password.scroll_into_view_if_needed()
                                except:
                                    pass
                                
                                await random_click(second_password)
                                await asyncio.sleep(0.2)
                                
                                # 清空可能存在的内容
                                await second_password.fill('')
                                await asyncio.sleep(0.1)
                                
                                await second_password.type(password, delay=25)
                                print(f"   ✅ 已确认密码")
                                
                                password_filled = True
                                break
                                
                    except Exception as e:
                        pass
                    
                    # 每5秒显示进度（减少输出频率）
                    if (attempt + 1) % 10 == 0:
                        elapsed = (attempt + 1) * 0.5
                        minutes = int(elapsed // 60)
                        seconds = int(elapsed % 60)
                        if minutes > 0:
                            print(f"   ⏳ 持续查找中... [{minutes}分{seconds}秒]")
                        else:
                            print(f"   ⏳ 持续查找中... [{seconds}秒]")
                    
                    attempt += 1
                    await asyncio.sleep(0.5)
                
                # 检查是否超时（30秒未检测到密码页面，直接跳过，不标记邮箱）
                if attempt >= max_password_wait and not password_filled and not self.authorization_code:
                    print(f"   ❌ 等待密码页面超时（30秒）")
                    print(f"   ⏭️ 跳过当前账号，进入下一个...")
                    if imap_listener:
                        imap_listener.stop()
                    return 'skip'  # 返回跳过状态，不标记邮箱为已使用
                
                if password_filled:
                    await asyncio.sleep(0.2)  # 优化：从0.5s减少到0.2s
                    
                    # 点击 Continue（带错误检测和重试）
                    print(f"   🔘 点击 Continue...")
                    
                    continue_selectors = [
                        'button:has-text("Continue")',
                        'button:has-text("Create")',
                        'button:has-text("Continuer")',    # French
                        'button:has-text("Créer")',         # French
                        'button:has-text("Weiter")',        # German
                        'button:has-text("Erstellen")',     # German
                        'button:has-text("Continuar")',     # Spanish/Portuguese
                        'button:has-text("Crear")',         # Spanish
                        'button:has-text("继续")',             # Chinese
                        'button:has-text("创建")',             # Chinese
                        'button[type="submit"]',
                    ]
                    
                    # 密码页面特征 - 多语言
                    password_page_indicators = PASSWORD_PAGE_INDICATORS
                    
                    # 下一页面特征（授权页面）- 多语言
                    auth_page_indicators = AUTH_PAGE_INDICATORS
                    
                    # 密码提交重试循环（减少重试次数避免卡住）
                    max_password_retries = 2  # 减少到2次
                    password_submit_success = False
                    
                    for password_retry in range(max_password_retries):
                        # 最高优先级：检查是否已收到授权码
                        if self.authorization_code:
                            print(f"   ✅ 已收到授权码，跳过密码提交检测")
                            password_submit_success = True
                            break
                        
                        if password_retry > 0:
                            print(f"   🔄 第 {password_retry + 1} 次尝试提交密码...")
                        
                        # 点击 Continue 按钮
                        button_clicked = False
                        for selector in continue_selectors:
                            try:
                                button = page.locator(selector).first
                                if await button.count() > 0 and await button.is_visible():
                                    await random_click(button)
                                    print(f"   ✅ 已点击按钮")
                                    button_clicked = True
                                    break
                            except:
                                continue
                        
                        if not button_clicked:
                            print(f"   ⚠️ 未找到按钮，等待后重试...")
                            await asyncio.sleep(2)
                            continue
                        
                        # 等待页面响应并检测（增加检测时间，每次3秒，共30秒）
                        detected_next_page = False
                        for detect_attempt in range(10):  # 检测10次，每次3秒，共30秒
                            await asyncio.sleep(3)
                            
                            # 最高优先级：检查是否已收到授权码
                            if self.authorization_code:
                                print(f"   ✅ 已收到授权码，密码提交成功")
                                password_submit_success = True
                                detected_next_page = True
                                break
                            
                            # 检测是否有错误
                            if await has_error_popup(page):
                                print(f"   ⚠️ 检测到错误，准备重试...")
                                break
                            
                            # 检测是否有授权按钮（多语言）- 最可靠
                            try:
                                allow_buttons = page.locator(build_allow_button_locator_str())
                                allow_count = await allow_buttons.count()
                                if allow_count > 0:
                                    first_allow = allow_buttons.first
                                    if await first_allow.is_visible():
                                        print(f"   ✅ 密码提交成功，检测到授权按钮 (找到{allow_count}个)")
                                        password_submit_success = True
                                        detected_next_page = True
                                        break
                            except Exception as e:
                                if detect_attempt == 0:
                                    print(f"   ⚠️ 检测授权按钮异常: {str(e)[:30]}")
                            
                            # 检测是否有拒绝按钮（授权页面的另一个标志）- 多语言
                            try:
                                deny_buttons = page.locator(build_deny_button_locator_str())
                                if await deny_buttons.count() > 0:
                                    print(f"   ✅ 密码提交成功，检测到拒绝按钮（授权页面）")
                                    password_submit_success = True
                                    detected_next_page = True
                                    break
                            except:
                                pass
                            
                            # 检测是否已跳转到授权页面（文本检测）
                            try:
                                page_text = await page.locator('body').inner_text(timeout=3000)
                                
                                # 检测授权页面
                                on_auth_page = any(ind in page_text for ind in auth_page_indicators)
                                if on_auth_page:
                                    print(f"   ✅ 密码提交成功，已跳转到授权页面")
                                    password_submit_success = True
                                    detected_next_page = True
                                    break
                                
                                # 检测是否仍在密码页面
                                still_on_password = any(ind in page_text for ind in password_page_indicators)
                                if not still_on_password:
                                    print(f"   ✅ 密码提交成功，页面已跳转")
                                    password_submit_success = True
                                    detected_next_page = True
                                    break
                            except:
                                pass
                            
                            print(f"   ⏳ 检测页面状态... [{detect_attempt + 1}/10] (等待跳转到授权页面)")
                        
                        if detected_next_page:
                            break
                        
                        # 如果检测都没有跳转，判断为仍在当前页面
                        print(f"   ⚠️ 30秒内未检测到授权页面，重试...")
                        await asyncio.sleep(1)
                    
                    if not password_submit_success:
                        print(f"   ⚠️ 提交密码可能未成功，继续流程...")
                else:
                    print(f"   💡 30秒内未检测到密码输入框")
                
                await asyncio.sleep(0.5)  # 优化：从2s减少到0.5s
                
                # ========== 步骤 7: 点击授权按钮（多语言） ==========
                print(f"\n🔓 步骤 7/7: 查找授权页面...")
                print(f"   ⏳ 轮询查找授权按钮（最多60秒，支持多语言）...")
                
                allow_selectors = build_allow_button_selectors()
                
                allow_clicked = False
                max_find_wait = 120  # 查找按钮最多60秒 (120次 * 0.5秒)
                
                # 阶段1: 查找 Allow access 按钮
                for attempt in range(max_find_wait):
                    if self.authorization_code:
                        print(f"   ✅ 已收到授权码，完成授权流程")
                        allow_clicked = True
                        break
                    
                    try:
                        if await has_error_popup(page):
                            print(f"   🔄 检测到错误，等待页面恢复...")
                            await asyncio.sleep(2)
                            continue
                        
                        button_found = False
                        for selector in allow_selectors:
                            if self.authorization_code:
                                allow_clicked = True
                                break
                            try:
                                allow_button = page.locator(selector).first
                                if await allow_button.count() > 0 and await allow_button.is_visible():
                                    button_found = True
                                    break
                            except:
                                continue
                        
                        if allow_clicked:
                            break
                        
                        if button_found:
                            print(f"   ✅ 检测到授权页面 (耗时 {(attempt + 1) * 0.5:.1f} 秒)")
                            
                            # 阶段2: 点击一次，然后一直等待授权码回调
                            print(f"   🎯 点击授权按钮...")
                            try:
                                await random_click(allow_button, no_wait_after=True)
                                print(f"   ✅ 已点击授权按钮")
                            except Exception as click_err:
                                print(f"   ⚠️ 点击异常: {str(click_err)[:50]}")
                            
                            # 一直等待授权码回调（无限等待，由外层调用方控制超时）
                            print(f"   ⏳ 等待授权码回调...")
                            while not self.authorization_code:
                                await asyncio.sleep(0.5)
                            
                            print(f"   ✅ 授权码已收到")
                            allow_clicked = True
                            break
                        else:
                            # 没找到按钮，继续轮询
                            if (attempt + 1) % 6 == 0:
                                elapsed = (attempt + 1) * 0.5
                                print(f"   ⏳ 查找中... [{elapsed:.0f}s/60s]")
                        
                    except Exception as e:
                        print(f"   ⚠️ 循环异常: {str(e)[:50]}")
                    
                    await asyncio.sleep(0.5)
                
                # 最后检查
                if not allow_clicked and self.authorization_code:
                    allow_clicked = True
                
                if allow_clicked:
                    print(f"   🎉 授权成功！")
                    print(f"   📝 授权码: {self.authorization_code[:30] if self.authorization_code else 'None'}...")
                    await asyncio.sleep(1)
                else:
                    print(f"   ❌ 60秒内未找到授权按钮，跳过")
                    return 'error'
                
                # 停止 IMAP 监听器
                if imap_listener:
                    imap_listener.stop()
                
                # 标记邮箱为已使用
                email_source = getattr(self, 'email_source', 'file')
                retire_email_account(email, getattr(self, 'email_source', 'file'))
                
                print(f"\n{'='*70}")
                print(f"✅ 自动注册完成！正在继续获取账户Token...")
                print(f"   授权码: {self.authorization_code[:30] if self.authorization_code else 'None'}...")
                print(f"{'='*70}")
                
                return 'success'  # 返回成功状态
                
            except Exception as e:
                print(f"   ❌ 输入验证码失败: {e}")
                import traceback
                traceback.print_exc()
                if imap_listener:
                    imap_listener.stop()
                return 'error'  # 返回错误状态
                
        except Exception as e:
            print(f"\n❌ 自动注册流程出错: {e}")
            import traceback
            traceback.print_exc()
            if imap_listener:
                imap_listener.stop()
            return 'error'  # 返回错误状态
    
    def open_browser_for_authorization(self):
        """打开浏览器进行授权（同步版本，使用标准浏览器）"""
        print("\n3️⃣ 打开浏览器进行授权...")
        
        auth_url = self.build_authorization_url()
        
        print(f"\n🌐 授权 URL:")
        print(f"   {auth_url[:100]}...")
        print(f"\n📱 正在打开浏览器...")
        
        webbrowser.open(auth_url)
        
        # 等待授权码（无时间限制）
        print("\n⏳ 等待授权（无时间限制，耐心等待）...")
        
        while not self.authorization_code:
            time.sleep(0.5)
        
        return True
    
    def exchange_code_for_token(self):
        """用授权码换取 token"""
        print("\n4️⃣ 用授权码换取 token...")
        
        # 临时清除代理设置（避免代理的SSL证书问题）
        import os
        original_proxy = {}
        proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
        
        for var in proxy_vars:
            if var in os.environ:
                original_proxy[var] = os.environ[var]
                del os.environ[var]
        
        try:
            # 尝试1: 正常创建客户端（不使用代理）
            client = boto3.client('sso-oidc', region_name=self.region)
            
            response = client.create_token(
                clientId=self.client_info['clientId'],
                clientSecret=self.client_info['clientSecret'],
                grantType='authorization_code',
                redirectUri=f"http://127.0.0.1:{self.port}/oauth/callback",
                code=self.authorization_code,
                codeVerifier=self.code_verifier
            )
            
            print("✅ Token 获取成功！")
            print(f"\n📋 Token 信息:")
            print(f"   Access Token: {response['accessToken'][:50]}...")
            print(f"   Expires In: {response['expiresIn']} 秒")
            
            if 'refreshToken' in response:
                print(f"   Refresh Token: {response['refreshToken'][:50]}...")
            else:
                print(f"   Refresh Token: ⚠️ 不存在")
            
            return response
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Token 获取失败: {error_msg}")
            
            # 如果是 SSL 错误，尝试禁用 SSL 验证
            if 'SSL' in error_msg or 'ssl' in error_msg.lower():
                print(f"\n🔄 检测到 SSL 错误，尝试禁用 SSL 验证...")
                try:
                    import os
                    os.environ['PYTHONHTTPSVERIFY'] = '0'
                    os.environ['CURL_CA_BUNDLE'] = ''
                    
                    import botocore.config
                    config = botocore.config.Config(
                        retries={'max_attempts': 3, 'mode': 'standard'}
                    )
                    client = boto3.client('sso-oidc', region_name=self.region, config=config, verify=False)
                    
                    response = client.create_token(
                        clientId=self.client_info['clientId'],
                        clientSecret=self.client_info['clientSecret'],
                        grantType='authorization_code',
                        redirectUri=f"http://127.0.0.1:{self.port}/oauth/callback",
                        code=self.authorization_code,
                        codeVerifier=self.code_verifier
                    )
                    
                    print("✅ Token 获取成功（已禁用 SSL 验证）！")
                    print(f"\n📋 Token 信息:")
                    print(f"   Access Token: {response['accessToken'][:50]}...")
                    print(f"   Expires In: {response['expiresIn']} 秒")
                    
                    if 'refreshToken' in response:
                        print(f"   Refresh Token: {response['refreshToken'][:50]}...")
                    else:
                        print(f"   Refresh Token: ⚠️ 不存在")
                    
                    return response
                except Exception as e2:
                    print(f"❌ 禁用 SSL 验证后仍然失败: {e2}")
            
            return None
        
        finally:
            # 恢复代理设置
            for var, value in original_proxy.items():
                os.environ[var] = value
    
    def save_client_registration(self, client_id_hash):
        """保存客户端注册信息（用于 refreshToken）"""
        try:
            cache_dir = Path.home() / '.aws' / 'sso' / 'cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            client_reg_file = cache_dir / f"{client_id_hash}.json"
            
            with open(client_reg_file, 'w') as f:
                json.dump(self.client_info, f, indent=2)
            
            print(f"   💾 客户端注册信息已保存")
            return True
        except Exception as e:
            print(f"   ⚠️ 保存客户端注册信息失败: {e}")
            return False
    
    def convert_to_kiro_token(self, token_response):
        """转换为 Kiro token 格式"""
        print("\n5️⃣ 转换为 Kiro token 格式...")
        
        # 计算过期时间
        expires_at = datetime.now()
        expires_at = datetime.fromtimestamp(
            expires_at.timestamp() + token_response['expiresIn']
        )
        expires_at_str = expires_at.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # 生成 clientIdHash
        client_id_hash = hashlib.sha1(
            json.dumps({'startUrl': self.start_url}).encode()
        ).hexdigest()
        
        # 保存客户端注册信息（用于刷新 token）
        self.save_client_registration(client_id_hash)
        
        # 判断 provider
        if 'view.awsapps.com' in self.start_url:
            provider = 'BuilderId'
        elif 'amzn.awsapps.com' in self.start_url:
            provider = 'Internal'
        else:
            provider = 'Enterprise'
        
        # 构建 Kiro token（包含客户端认证信息）
        kiro_token = {
            'accessToken': token_response['accessToken'],
            'refreshToken': token_response.get('refreshToken', ''),
            'expiresAt': expires_at_str,
            'clientIdHash': client_id_hash,
            'clientId': self.client_info['clientId'],
            'clientSecret': self.client_info['clientSecret'],
            'authMethod': 'IdC',
            'provider': provider,
            'region': self.region
        }
        
        print("✅ 转换成功！")
        print(f"\n📄 Kiro Token:")
        print(json.dumps(kiro_token, indent=2, ensure_ascii=False))
        
        return kiro_token
    
    def save_token(self, kiro_token, email=None):
        """保存 token到当前目录并上传到后端
        
        Args:
            kiro_token: token数据
            email: 邮箱地址（用于文件名）
        """
        print("\n6️⃣ 保存 token...")
        
        try:
            # 创建 accounts 目录
            accounts_dir = Path.cwd() / 'accounts'
            accounts_dir.mkdir(parents=True, exist_ok=True)
            
            # 如果提供了邮箱，用邮箱作为文件名
            if email:
                # 清理邮箱中的特殊字符
                safe_email = email.replace('@', '_at_').replace('.', '_')
                filename = f"kiro-token-{safe_email}.json"
            else:
                # 使用时间戳
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"kiro-token-{timestamp}.json"
            
            token_path = accounts_dir / filename
            
            # 保存token
            with open(token_path, 'w', encoding='utf-8') as f:
                json.dump(kiro_token, f, indent=2, ensure_ascii=False)
            
            print(f"✅ Token 已保存到: {token_path}")
            
            # 同时保存到默认文件（兼容）
            default_path = accounts_dir / 'kiro-auth-token.json'
            with open(default_path, 'w', encoding='utf-8') as f:
                json.dump(kiro_token, f, indent=2, ensure_ascii=False)
            print(f"✅ 默认Token已保存到: {default_path}")
            
            # 上传到后端服务器
            self.upload_token_to_server(kiro_token, email)
            
            # 同步凭据到 kiro-rs-commercial
            self.sync_to_kiro_rs(kiro_token, email)
            
            print("\n🎉 登录完成！")
            
            return True
            
        except Exception as e:
            print(f"❌ 保存失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def upload_token_to_server(self, kiro_token, email=None):
        """上传token到后端服务器
        
        Args:
            kiro_token: token数据
            email: 邮箱地址（必传）
        """
        print("\n📤 上传token到服务器...")
        
        try:
            # 检查email是否存在（必传字段）
            if not email:
                print(f"⚠️ 缺少邮箱参数，跳过上传到服务器")
                return
            
            api_url = "http://43.167.227.63:7002/api/kiro/login/store"
            
            # 准备请求数据（email必传，包含客户端认证信息）
            payload = {
                'email': email,  # 必传字段
                'accessToken': kiro_token['accessToken'],
                'refreshToken': kiro_token['refreshToken'],
                'expiresAt': kiro_token['expiresAt'],
                'clientIdHash': kiro_token['clientIdHash'],
                'clientId': kiro_token.get('clientId', ''),
                'clientSecret': kiro_token.get('clientSecret', ''),
                'authMethod': kiro_token['authMethod'],
                'provider': kiro_token['provider'],
                'region': kiro_token['region']
            }
            
            print(f"   📧 邮箱: {email}")
            print(f"   🔑 clientIdHash: {kiro_token['clientIdHash']}")
            print(f"   ⏰ 过期时间: {kiro_token['expiresAt']}")
            
            # 发送POST请求
            response = requests.post(
                api_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Token已上传到服务器")
                print(f"   响应: {result}")
            else:
                print(f"⚠️ 上传失败: HTTP {response.status_code}")
                print(f"   响应: {response.text}")
                
        except Exception as e:
            print(f"⚠️ 上传到服务器失败: {e}")
            # 不中断流程，仅警告
    
    def sync_to_kiro_rs(self, kiro_token, email=None):
        """同步凭据到 kiro-rs-commercial Admin API
        
        调用 POST /api/admin/credentials 接口添加新凭据，
        支持配置同步请求代理和凭据级代理。
        
        Args:
            kiro_token: token数据（包含 refreshToken, clientId, clientSecret 等）
            email: 邮箱地址
        """
        if not SYNC_CONFIG.get('enabled', False):
            print("\n⏭️ kiro-rs-commercial 同步已禁用，跳过")
            return
        
        api_url = SYNC_CONFIG.get('api_url', '')
        admin_api_key = SYNC_CONFIG.get('admin_api_key', '')
        
        if not api_url or not admin_api_key:
            print("\n⚠️ kiro-rs-commercial 同步配置不完整（缺少 api_url 或 admin_api_key），跳过")
            return
        
        print(f"\n📤 同步凭据到 kiro-rs-commercial...")
        print(f"   🔗 API: {api_url}")
        
        try:
            # 构建请求体（camelCase，与 AddCredentialRequest 对齐）
            payload = {
                'refreshToken': kiro_token.get('refreshToken', ''),
                'authMethod': 'idc',  # kiro-rs-commercial 统一使用 idc 表示 IdC/Builder-ID
                'clientId': kiro_token.get('clientId'),
                'clientSecret': kiro_token.get('clientSecret'),
                'region': kiro_token.get('region'),
                'email': email,
                'priority': 1
            }
            
            # 凭据级代理配置（写入凭据本身，kiro-rs-commercial 用此代理访问 AWS）
            cred_proxy = SYNC_CONFIG.get('credential_proxy_url')
            if cred_proxy is not None:
                payload['proxyUrl'] = cred_proxy
                cred_proxy_user = SYNC_CONFIG.get('credential_proxy_username')
                cred_proxy_pass = SYNC_CONFIG.get('credential_proxy_password')
                if cred_proxy_user:
                    payload['proxyUsername'] = cred_proxy_user
                if cred_proxy_pass:
                    payload['proxyPassword'] = cred_proxy_pass
                print(f"   🌐 凭据级代理: {cred_proxy}")
            
            # 移除值为 None 的字段
            payload = {k: v for k, v in payload.items() if v is not None}
            
            if not payload.get('refreshToken'):
                print(f"   ⚠️ refreshToken 为空，跳过同步")
                return
            
            # 请求头
            headers = {
                'Content-Type': 'application/json',
                'x-api-key': admin_api_key,
            }
            
            # 同步请求代理配置（发送 HTTP 请求时使用的代理）
            proxies = None
            if SYNC_CONFIG.get('use_sync_proxy', False):
                sync_proxy = SYNC_CONFIG.get('sync_proxy_url', '')
                if sync_proxy:
                    proxies = {
                        'http': sync_proxy,
                        'https': sync_proxy,
                    }
                    print(f"   🔀 同步请求代理: {sync_proxy}")
            
            print(f"   📧 邮箱: {email}")
            print(f"   🔑 authMethod: idc")
            print(f"   🌍 region: {kiro_token.get('region')}")
            
            # 发送 POST 请求
            response = requests.post(
                api_url,
                json=payload,
                headers=headers,
                proxies=proxies,
                timeout=15,
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    cred_id = result.get('credentialId', '?')
                    print(f"   ✅ 同步成功！凭据 ID: {cred_id}")
                    if result.get('email'):
                        print(f"   📧 邮箱: {result['email']}")
                else:
                    print(f"   ⚠️ 同步返回失败: {result.get('message', '未知错误')}")
            else:
                print(f"   ⚠️ 同步失败: HTTP {response.status_code}")
                try:
                    err_body = response.json()
                    print(f"   📝 错误详情: {err_body.get('message', response.text[:200])}")
                except:
                    print(f"   📝 响应: {response.text[:200]}")
                    
        except requests.exceptions.ConnectionError as e:
            print(f"   ⚠️ 无法连接到 kiro-rs-commercial: {str(e)[:100]}")
            print(f"   💡 请确认服务是否在 {api_url} 上运行")
        except requests.exceptions.Timeout:
            print(f"   ⚠️ 同步请求超时（15秒）")
        except Exception as e:
            print(f"   ⚠️ 同步到 kiro-rs-commercial 失败: {e}")
    
    def cleanup(self):
        """清理资源"""
        if self.server:
            self.server.shutdown()
            print("\n🧹 回调服务器已关闭")
    
    async def async_login(self, auto_register=False, email=None, headless=False, email_source=None, email_extra_data=None):
        """异步登录流程（使用指纹浏览器）
        
        Args:
            auto_register: 是否自动注册
            email: 注册使用的邮箱
            headless: 是否使用无头模式
            email_source: 邮箱来源 ('file', 'boomlify', 'kiro_api', 'outlook', 'yahoo')
            email_extra_data: 额外数据（如 BoomlifyMailClient 或邮箱账号信息）
        """
        print("=" * 70)
        if auto_register:
            print("🔐 Kiro 自动注册 + 登录流程")
        else:
            print("🔐 Kiro 精确登录流程（指纹浏览器）")
        print("=" * 70)
        print(f"\nStart URL: {self.start_url}")
        print(f"Region: {self.region}")
        if email_source:
            print(f"邮箱来源: {email_source}")
        
        # 保存邮箱来源信息供后续使用
        self.email_source = email_source
        self.email_extra_data = email_extra_data
        
        try:
            # 1. 生成参数
            self.generate_pkce_params()
            self.generate_state()
            
            # 2. 注册客户端
            if not self.register_client():
                return False
            
            # 3. 启动回调服务器
            if not self.start_callback_server():
                return False
            
            # 4. 使用指纹浏览器打开授权页面（可选自动注册）
            browser_result = await self.async_open_browser_for_authorization(auto_register=auto_register, email=email, headless=headless)
            
            # 检查返回值：True=成功, False=失败, 'skip'=跳过
            if browser_result == 'skip':
                return 'skip'  # 传递跳过状态给调用方
            elif not browser_result:
                return False
            
            # 5. 换取 token
            token_response = self.exchange_code_for_token()
            if not token_response:
                return False
            
            # 6. 转换格式
            kiro_token = self.convert_to_kiro_token(token_response)
            
            # 7. 保存（传入邮箱用于文件名）
            if not self.save_token(kiro_token, email=email):
                return False
            
            return True
            
        finally:
            self.cleanup()
    
    def login(self):
        """完整登录流程（同步版本，使用标准浏览器）"""
        print("=" * 70)
        print("🔐 Kiro 精确登录流程")
        print("=" * 70)
        print(f"\nStart URL: {self.start_url}")
        print(f"Region: {self.region}")
        
        try:
            # 1. 生成参数
            self.generate_pkce_params()
            self.generate_state()
            
            # 2. 注册客户端
            if not self.register_client():
                return False
            
            # 3. 启动回调服务器
            if not self.start_callback_server():
                return False
            
            # 4. 打开浏览器授权
            if not self.open_browser_for_authorization():
                return False
            
            # 5. 换取 token
            token_response = self.exchange_code_for_token()
            if not token_response:
                return False
            
            # 6. 转换格式
            kiro_token = self.convert_to_kiro_token(token_response)
            
            # 7. 保存（手动登录不传邮箱）
            if not self.save_token(kiro_token, email=None):
                return False
            
            return True
            
        finally:
            self.cleanup()


def main():
    """
    完全模拟 Kiro 的 AWS 登录流程（支持自动注册和指纹浏览器）
    
    使用方法：
    1. 修改文件顶部的 RUN_CONFIG 配置项
    2. 直接运行: python kiro_exact_login_switch_proxy.py
    
    主要配置项：
    - start_url: AWS SSO Start URL
    - auto_register: 是否自动注册（True/False）
    - headless: 是否无头模式（True/False）
    - run_count: 执行次数
    - interval_seconds: 执行间隔（秒）
    """
    
    # 从配置读取所有参数
    print("\n" + "=" * 70)
    print("📋 当前配置")
    print("=" * 70)
    print(f"   Start URL: {RUN_CONFIG['start_url']}")
    print(f"   Region: {RUN_CONFIG['region']}")
    print(f"   登录模式: {'自动注册（指纹浏览器）' if RUN_CONFIG['auto_register'] else '手动登录（标准浏览器）'}")
    print(f"   无头模式: {'是' if RUN_CONFIG['headless'] else '否'}")
    print(f"   执行次数: {RUN_CONFIG['run_count']}")
    print(f"   执行间隔: {RUN_CONFIG['interval_seconds']} 秒")
    print(f"   邮箱来源: {EMAIL_CONFIG['source']}")
    if EMAIL_CONFIG['source'] == 'outlook':
        print(f"   Outlook文件: {EMAIL_CONFIG.get('outlook_file', 'outlook.txt')}")
        print(f"   Outlook验证码API: {EMAIL_CONFIG.get('outlook_verification_api', '')}")
    elif EMAIL_CONFIG['source'] == 'yahoo':
        print(f"   Yahoo文件: {EMAIL_CONFIG.get('yahoo_file', 'yahoo.txt')}")
    if RUN_CONFIG['email']:
        print(f"   指定邮箱: {RUN_CONFIG['email']}")
    # kiro-rs-commercial 同步配置
    if SYNC_CONFIG.get('enabled', False):
        print(f"   kiro-rs同步: 已启用")
        print(f"   kiro-rs API: {SYNC_CONFIG.get('api_url', '')}")
        if SYNC_CONFIG.get('use_sync_proxy', False):
            print(f"   同步代理: {SYNC_CONFIG.get('sync_proxy_url', '')}")
        if SYNC_CONFIG.get('credential_proxy_url') is not None:
            print(f"   凭据级代理: {SYNC_CONFIG.get('credential_proxy_url')}")
    else:
        print(f"   kiro-rs同步: 已禁用")
    print("=" * 70)
    
    
    # ========== 代理预检查和切换 ==========
    use_proxy = PROXY_CONFIG.get('use_proxy', False)
    use_residential = PROXY_CONFIG.get('use_residential_proxy', False)
    if use_residential:
        print("\n" + "=" * 70)
        print("住宅代理已启用，跳过 Clash 代理预检查")
        print("=" * 70)
        rp_host = PROXY_CONFIG.get('residential_host', '')
        rp_port = PROXY_CONFIG.get('residential_port', 30000)
        rp_user = PROXY_CONFIG.get('residential_username', '')
        print(f"   🏠 代理: {rp_host}:{rp_port}")
        if rp_user:
            print(f"   🔑 用户: {rp_user}")
        print("=" * 70 + "\n")
    elif use_proxy:
        print("\n" + "=" * 70)
        print("代理服务预检查")
        print("=" * 70)
        
        # 先切换一次代理节点
        print(f"💡 初次运行，先切换代理节点...")
        switch_proxy_node()
        print()
        
        # 测试代理连接
        proxy_url = PROXY_CONFIG.get('proxy_url', 'http://127.0.0.1:7897')
        proxy_ok = test_proxy_connection(proxy_url)
        if not proxy_ok:
            print(f"❌ 代理服务不可用！")
            print(f"💡 请检查：")
            print(f"   1. 代理服务是否在 {proxy_url} 上运行")
            print(f"   2. 代理端口是否正确")
            print(f"   3. 防火墙是否阻止连接\n")
            
            user_input = input(f"是否继续运行？(y/n): ").strip().lower()
            if user_input != 'y':
                print(f"程序已取消")
                return
        
        print("=" * 70 + "\n")
    
    # ========== 循环执行 ==========
    success_count = 0
    fail_count = 0
    
    for run_index in range(RUN_CONFIG['run_count']):
        # 如果有多次执行，显示当前进度
        if RUN_CONFIG['run_count'] > 1:
            print("\n" + "=" * 70)
            print(f"🔄 执行进度: [{run_index + 1}/{RUN_CONFIG['run_count']}]")
            print("=" * 70 + "\n")
        
        # ========== 如果是自动注册且没有邮箱，每次循环都生成新邮箱 ==========
        current_email = None
        email_source = None
        email_extra_data = None
        
        if RUN_CONFIG['auto_register']:
            if not RUN_CONFIG['email']:
                current_email, email_source, email_extra_data = generate_random_email()
                if not current_email:
                    print(f"❌ 获取邮箱失败，跳过本次执行")
                    fail_count += 1
                    continue
            else:
                current_email = RUN_CONFIG['email']
                email_source = 'manual'
            print(f"📧 将使用邮箱: {current_email} (来源: {email_source})")
        
        # 每次循环都切换代理节点（仅 Clash 代理模式，住宅代理不需要切换）
        if use_proxy and not use_residential:
            print(f"\n🔄 第 {run_index + 1} 次执行，切换代理节点...")
            switch_proxy_node()
            
            # 切换后等待一下，让代理节点稳定
            print(f"⏳ 等待 3 秒让代理节点稳定...")
            time.sleep(3)
            
            # 验证代理连接
            proxy_url = PROXY_CONFIG.get('proxy_url', 'http://127.0.0.1:7897')
            proxy_ok = test_proxy_connection(proxy_url)
            if not proxy_ok:
                print(f"⚠️ 代理连接不可用，尝试再次切换...")
                switch_proxy_node()
                time.sleep(3)
                proxy_ok = test_proxy_connection(proxy_url)
                if not proxy_ok:
                    print(f"❌ 代理仍然不可用，跳过本次执行")
                    fail_count += 1
                    continue
            print()
        
        login = KiroExactLogin(RUN_CONFIG['start_url'], RUN_CONFIG['region'])
        
        # 判断使用哪种登录方式（带重试机制）
        max_retries = 2  # 最多重试2次
        success = False
        
        for retry in range(max_retries):
            try:
                # 如果是重试，需要重新获取邮箱（因为之前的邮箱可能已被标记为已使用）
                if retry > 0 and RUN_CONFIG['auto_register'] and not RUN_CONFIG['email']:
                    print(f"\n🔄 第 {retry + 1} 次重试，重新获取邮箱...")
                    current_email, email_source, email_extra_data = generate_random_email()
                    if not current_email:
                        print(f"❌ 获取邮箱失败，跳过重试")
                        break
                    print(f"📧 新邮箱: {current_email} (来源: {email_source})")
                    # 重新创建 login 实例
                    login = KiroExactLogin(RUN_CONFIG['start_url'], RUN_CONFIG['region'])
                
                if RUN_CONFIG['auto_register']:
                    # 自动注册模式（指纹浏览器）
                    if retry > 0:
                        print(f"\n🔄 第 {retry + 1} 次重试...")
                    print("\n🦊 自动注册模式（指纹浏览器）")
                    
                    # 运行异步登录（传递邮箱来源信息）
                    result = asyncio.run(login.async_login(
                        auto_register=True,
                        email=current_email,
                        headless=RUN_CONFIG['headless'],
                        email_source=email_source,
                        email_extra_data=email_extra_data
                    ))
                    
                    # 处理返回值
                    if result == 'skip':
                        # 邮箱已注册或出错，不重试，直接进入下一个循环
                        print(f"\n⏭️ 跳过当前账号，进入下一个...")
                        success = 'skip'
                        break  # 跳出重试循环
                    else:
                        success = result
                else:
                    # 手动登录模式（标准浏览器）
                    print("\n🌐 手动登录模式（标准浏览器）")
                    success = login.login()
                
                # 如果成功，退出重试循环
                if success and success != 'skip':
                    break
                    
            except Exception as e:
                print(f"\n⚠️ 执行异常: {e}")
                if retry < max_retries - 1:
                    print(f"🔄 将在 5 秒后重试...")
                    time.sleep(5)
                    # 重试前再次切换代理
                    if use_proxy and not use_residential:
                        print(f"🔄 重试前切换代理节点...")
                        switch_proxy_node()
                        time.sleep(3)
                else:
                    print(f"❌ 已达到最大重试次数")
                    success = False
        
        # 统计结果
        if success == 'skip':
            # 跳过的不计入成功或失败，但需要继续下一次循环
            print("\n" + "=" * 70)
            print(f"⏭️ 第 {run_index + 1} 次执行跳过（邮箱已注册）")
            print("=" * 70)
        elif success:
            success_count += 1
            print("\n" + "=" * 70)
            print(f"✅ 第 {run_index + 1} 次执行成功！")
            print("=" * 70)
        else:
            fail_count += 1
            print("\n" + "=" * 70)
            print(f"❌ 第 {run_index + 1} 次执行失败")
            print("=" * 70)
        
        # 如果不是最后一次执行，等待一下（避免 AWS 频率限制）
        if run_index < RUN_CONFIG['run_count'] - 1:
            wait_time = max(RUN_CONFIG['interval_seconds'], 5)  # 至少等待5秒
            print(f"\n⏳ 等待 {wait_time} 秒后继续（避免频率限制）...")
            time.sleep(wait_time)
    
    # ========== 显示总结 ==========
    if RUN_CONFIG['run_count'] > 1:
        print("\n" + "=" * 70)
        print("📊 执行总结")
        print("=" * 70)
        print(f"   总执行次数: {RUN_CONFIG['run_count']}")
        print(f"   成功次数: {success_count}")
        print(f"   失败次数: {fail_count}")
        print(f"   成功率: {(success_count / RUN_CONFIG['run_count'] * 100):.1f}%")
        print("=" * 70)


if __name__ == '__main__':
    main()
