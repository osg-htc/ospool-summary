import smtplib

SMTP_SERVER = "smtp.wiscmail.wisc.edu"
FROM_ADDRESS = "clock@wisc.edu"

import smtplib
import logging
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

logging = logging.getLogger('chtc_projects_on_ospool')

def send_email(send_from: str, send_to: str | list, subject: str, text: str, files=None, server=SMTP_SERVER):

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to if isinstance(send_to, str) else ', '.join(send_to)  # Handle the two types
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()


if __name__ == "__main__":
    send_email(FROM_ADDRESS, ['clock@wisc.edu'], "Test Email", "This is a text email.", ['./send_email.py'], SMTP_SERVER)
