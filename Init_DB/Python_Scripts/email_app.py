import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import log_config
from dotenv import load_dotenv

load_dotenv("../../aws_common.env")
app_logger = log_config.start_log()


def send_email(sender_email, sender_password, receiver_email, subject, message):
    # Set up the SMTP server
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    # Create a multipart message
    msg = MIMEMultipart("alternative")
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Attach message to email
    msg.attach(MIMEText(message, 'plain'))

    # Start the SMTP session
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()

    try:
        # Login to the SMTP server
        server.login(sender_email, sender_password)

        # Send email
        server.sendmail(sender_email, receiver_email, msg.as_string())
        app_logger.info("Email sent successfully!")
    except Exception as e:
        app_logger.info("Failed to send email. Error:", str(e))
    finally:
        # Close the SMTP session
        server.quit()


def send_message(text_msg: list, status: bool = True):
    sender = os.getenv("SENDER")
    password = os.getenv("EMAIL_PASSWORD")

    # Input receiver's email address
    receiver = os.getenv("RECEIVER")
    run_on=os.getenv("EMAIL_ENV")
    # Input email subject and message
    if status:
        subject = f"OD: ETL Pipeline Status for {os.getenv('EMAIL_ENV')}, {os.getenv('POSTGRES_SCHEMA')}: (Success)"
    else:
        subject = f"OD: ETL Pipeline Status for {os.getenv('EMAIL_ENV')}, {os.getenv('POSTGRES_SCHEMA')}: (Failure)"

    message = "\n".join(text_msg)

    # Send email
    try:
        send_email(sender, password, receiver, subject, message)
    except Exception as err_main:
        app_logger.error(err_main.args[0])
    else:
        app_logger.info("Sending Email was successful.")


