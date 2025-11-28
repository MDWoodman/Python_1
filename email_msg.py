
# Email account constants
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 465
IMAP_SERVER = 'imap.gmail.com'
IMAP_PORT = 993
EMAIL_USERNAME = 'cfd.python@gmail.com'
EMAIL_PASSWORD = 'dwynvdjwlxuzavqe'




import smtplib
import imaplib
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def test_send_receive(subject: str = None, body: str = None, to_addr: str = None, wait_seconds: int = 30, poll_interval: int = 5):
    """
    Send a test email (to self by default) and poll inbox until the test subject is found or timeout.
    Returns dict: {'sent': bool, 'received': bool, 'subject': str, 'error': str|None}
    """
    import time, uuid
    try:
        test_subj = subject or f"Test Email {uuid.uuid4()}"
        test_body = body or f"Test body for {test_subj}"
        to_addr = to_addr or EMAIL_USERNAME

        # send
        msg = create_email_message(test_subj, test_body)
        send_email(msg)

        # poll inbox
        end_time = time.time() + wait_seconds
        while time.time() < end_time:
            messages = receive_emails(num_emails=20)
            for m in messages:
                subj = (m.get('Subject') or '')
                if test_subj in subj:
                    return {'sent': True, 'received': True, 'subject': test_subj, 'error': None}
            time.sleep(poll_interval)

        return {'sent': True, 'received': False, 'subject': test_subj, 'error': 'Timeout waiting for email'}
    except Exception as e:
        return {'sent': False, 'received': False, 'subject': subject, 'error': str(e)}
# ...existing code...

def create_email_message(subject, body):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USERNAME
    msg['To'] = EMAIL_USERNAME
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    return msg

def send_email(msg):
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(msg['From'], msg['To'], msg.as_string())

def receive_emails(mailbox='INBOX', num_emails=5):
    with imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT) as mail:
        mail.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        mail.select(mailbox)
        typ, data = mail.search(None, 'ALL')
        mail_ids = data[0].split()
        latest_ids = mail_ids[-num_emails:]
        messages = []
        for num in latest_ids:
            typ, msg_data = mail.fetch(num, '(RFC822)')
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    messages.append(msg)
        return messages

def check_email_for_signal(subject_keyword):
    messages = receive_emails()
    for msg in messages:
        if subject_keyword in msg['Subject']:
            return True
    return False
def create_and_send_email(subject, body):
    msg = create_email_message(subject, body)
    send_email(msg)
