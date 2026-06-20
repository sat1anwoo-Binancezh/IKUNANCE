import smtplib
from email.mime.text import MIMEText


SMTP_HOSTS = {
    "gmail.com": "smtp.gmail.com",
    "qq.com": "smtp.qq.com",
    "163.com": "smtp.163.com",
    "126.com": "smtp.126.com",
    "outlook.com": "smtp.office365.com",
    "hotmail.com": "smtp.office365.com",
}


def send_email(subject: str, content: str, sender: str, password: str):
    return send_email_sync(subject, content, sender, password)


def send_email_sync(subject: str, content: str, sender: str, password: str):
    sender = (sender or "").strip()
    if not sender or not password:
        return False, "missing sender or password"
    domain = sender.split("@")[-1].lower() if "@" in sender else ""
    host = SMTP_HOSTS.get(domain, "smtp.qq.com")
    try:
        message = MIMEText(content, "plain", "utf-8")
        message["Subject"] = subject
        message["From"] = sender
        message["To"] = sender
        with smtplib.SMTP_SSL(host, 465, timeout=12) as smtp:
            smtp.login(sender, password)
            smtp.sendmail(sender, [sender], message.as_string())
        return True, "email sent"
    except Exception as exc:
        return False, str(exc)
