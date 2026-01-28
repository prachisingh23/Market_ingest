from datetime import datetime, timezone
from typing import List, Optional

import brevo_python
from brevo_python import TransactionalEmailsApi, ApiClient, SendSmtpEmail
from brevo_python.rest import ApiException

from config import settings
from utils.logger import log


# ============================================
# BREVO CONFIG (API key from .env via settings)
# ============================================
configuration = brevo_python.Configuration()
configuration.api_key["api-key"] = settings.mail_api_key


# Default recipients
DEFAULT_RECIPIENTS: List[str] = [
    "vipulkumar15463@gmail.com",
    'vipuldhankar17170277@gmail.com',
    'vkbrt947@gmail.com'
]


# ============================================
# CORE EMAIL SENDER (SYNC, NO PRINT)
# ============================================
def send_email(recipients: List[str], subject: str, body: str):
    try:
        api_instance = TransactionalEmailsApi(ApiClient(configuration))

        send_smtp_email = SendSmtpEmail(
            to=[{"email": r} for r in recipients],
            sender={
                "name": settings.mail_from_name,
                "email": settings.mail_from,
            },
            subject=subject,
            html_content=body,
        )

        response = api_instance.send_transac_email(send_smtp_email)

        log.info(
            "Email sent | recipients={} | subject={}",
            recipients,
            subject,
        )
        return response

    except ApiException as e:
        log.error(
            "Brevo ApiException | recipients={} | subject={} | error={}",
            recipients,
            subject,
            str(e),
        )
        raise Exception(f"Brevo error: {e}")

    except Exception:
        log.exception(
            "General email error | recipients={} | subject={}",
            recipients,
            subject,
        )
        raise


# ============================================
# 1) Token fetch success + Redis run success
# ============================================
def mail_token_fetch_and_redis_success(
    recipients: List[str],
    broker: str,
    token: str,
    redis_status: str = "OK",
):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = "✅ Token Fetch + Redis Success"

    body = f"""
    <html><body>
      <h2 style="color:#4CAF50;">✅ SUCCESS</h2>
      <p><b>Time:</b> {now}</p>
      <p><b>Broker:</b> {broker}</p>
      <p><b>Token:</b> {token}</p>
      <p><b>Redis:</b> {redis_status}</p>
      <p>Token fetched successfully and Redis run successfully.</p>
    </body></html>
    """

    log.info(
        "Trigger mail: token+redis success | broker={} | token={} | redis={}",
        broker,
        token,
        redis_status,
    )

    send_email(recipients, subject, body)


# ============================================
# 2) Token NOT updated in DB
# ============================================
def mail_token_not_updated_in_db(
    recipients: List[str],
    broker: str,
    token: str,
    reason: Optional[str] = None,
):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = "⚠Token NOT Updated in DB"

    body = f"""
    <html><body>
      <h2 style="color:#FF9800;">⚠️ WARNING</h2>
      <p><b>Time:</b> {now}</p>
      <p><b>Broker:</b> {broker}</p>
      <p><b>Token:</b> {token}</p>
      <p><b>Reason:</b> {reason or "N/A"}</p>
      <p>Token fetched/generated but DB update did NOT happen.</p>
    </body></html>
    """

    log.warning(
        "Trigger mail: token NOT updated in DB | broker={} | token={} | reason={}",
        broker,
        token,
        reason,
    )

    send_email(recipients, subject, body)


# ============================================
# 3) Redis crash / down
# ============================================
def mail_redis_crash(
    recipients: List[str],
    redis_url: str,
    error: str,
    context: Optional[str] = None,
):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = "🔥 Redis Crash / Down"

    body = f"""
    <html><body>
      <h2 style="color:#D32F2F;">🔥 CRITICAL</h2>
      <p><b>Time:</b> {now}</p>
      <p><b>Redis URL:</b> {redis_url}</p>
      <p><b>Context:</b> {context or "N/A"}</p>
      <pre>{error}</pre>
    </body></html>
    """

    log.critical(
        "Trigger mail: REDIS CRASH | redis_url={} | context={} | error={}",
        redis_url,
        context,
        error,
    )

    send_email(recipients, subject, body)


# ============================================
# OPTIONAL DEFAULT WRAPPERS
# ============================================
def mail_success_default(broker: str, token: str, redis_status: str = "OK"):
    mail_token_fetch_and_redis_success(
        DEFAULT_RECIPIENTS,
        broker,
        token,
        redis_status,
    )


def mail_db_not_updated_default(
    broker: str,
    token: str,
    reason: Optional[str] = None,
):
    mail_token_not_updated_in_db(
        DEFAULT_RECIPIENTS,
        broker,
        token,
        reason,
    )


def mail_redis_crash_default(
    redis_url: str,
    error: str,
    context: Optional[str] = None,
):
    mail_redis_crash(
        DEFAULT_RECIPIENTS,
        redis_url,
        error,
        context,
    )
# mail_redis_crash_default('redis://localhost:6379', 'ConnectionError: Redis server is down', 'While trying to connect to Redis')