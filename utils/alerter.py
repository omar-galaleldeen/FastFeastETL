import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)
_cfg         = get_config()

# Read config
enabled = str(_cfg['alerting']['enabled']).lower() in ['true', '1', 'yes']
host = _cfg['alerting']['smtp']['host']
port = int(_cfg['alerting']['smtp']['port'])
user_name = _cfg['alerting']['smtp']['user_name']
password = _cfg['alerting']['smtp']['password']
sender =  _cfg['alerting']['smtp']['sender']
receivers: list[str] =  _cfg['alerting']['smtp']['receivers']

def send_alert(error: str, message: str) -> None:
    if not enabled:
        print("    ⚠️ [ALERTER] Alerting is DISABLED in config.")
        logger.info("Alerting is disabled")
        return

    print("    ⏳ [ALERTER] Triggered! Starting background thread to send email...")
    thread = threading.Thread(
        target=_send_email,
        args=(error, message)
        # REMOVED: daemon=True -> This ensures the thread finishes sending before dying
    )
    thread.start()

def _send_email(error: str, message: str) -> None:
    try: 
        subject = f"Alert: {error}"
        body = f"""Hello,

An alert has been triggered in the FastFeast pipeline.

Error: {error}

Details:
{message}

-- 
This is an automated alert from your reliable Data Pipeline.
"""
         
        email_msg = MIMEMultipart()
        email_msg["From"] = sender
        email_msg["To"] = ", ".join(receivers)
        email_msg["Subject"] = subject
        email_msg.attach(MIMEText(body, "plain"))

        print("    📧 [ALERTER] Connecting to Gmail SMTP...")
        with SMTP(host, port) as smtp:
            smtp.starttls()
            smtp.login(user_name, password)
            smtp.send_message(email_msg)
            
        print("    ✅ [ALERTER] Email sent successfully to your inbox!")
        logger.info("Alert email sent successfully.")
            
    except Exception as e:
        print(f"    ❌ [ALERTER] Failed to send email: {e}")
        logger.error(f"Sending Alert Failed: {e}")