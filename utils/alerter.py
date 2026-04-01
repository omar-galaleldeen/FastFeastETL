import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)
_cfg         = get_config()

enabled = _cfg['alerting']['enabled']
host = _cfg['alerting']['smtp_host']
port = _cfg['alerting']['smtp_port']
user_name = _cfg['alerting']['user_name']
password = _cfg['alerting']['password']
sender =  _cfg['alerting']['sender']
receivers: list[str] =  _cfg['alerting']['receivers']

def send_alert(error: str, message: str) -> None:

    if not enabled:
        logger.info("Alerting is disabled")
        return

    thread = threading.Thread(
        target=_send_email,
        args=(error, message),
        daemon=True
    )
    thread.start()


def _send_email(error: str, message: str) -> None:
    try: 
        subject = (f"Alert: {error}")
        body = f"""
        Hello,

        An alert has been triggered in the system.

        Error: {error}

        Details:
        {message}

        -- 
        This is an automated alert from your system.
        """
         
       # Use a distinct variable name (email_msg) to avoid shadowing the 'message' parameter
        email_msg = MIMEMultipart()
        email_msg["From"] = sender
        email_msg["To"] = ", ".join(receivers)
        email_msg["Subject"] = subject
        email_msg.attach(MIMEText(body, "plain"))

        with SMTP(host, port) as smtp:
            smtp.starttls()
            smtp.login(user_name, password)
            smtp.send_message(email_msg)
            logger.info("Alert email sent successfully.")
            
    except Exception as e:
        logger.error(f"Sending Alert Failed: {e}")