import django
django.setup()

from utils.email import send_email
from sentry_sdk import capture_exception, set_context, capture_message


def verify_email_details(email_details):
    is_verified = False
    error_message = ""

    to_emails_ids = email_details.get("to_emails_ids", [])
    if not to_emails_ids:
        error_message = "To Email Ids are missing"
        return is_verified, error_message

    subject = email_details.get("subject", None)
    if not subject:
        error_message = "Subject is missing"
        return is_verified, error_message

    attachments = email_details.get("attachments", [])
    if not isinstance(attachments, list):
        error_message = "Attachments should be list of dict"
        return is_verified, error_message

    for index, attachment in enumerate(attachments):
        if not isinstance(attachment, dict):
            error_message = f"Attachments should be list of dict at {index} position"
            return is_verified, error_message
        if not attachment.get("attachment_path", None):
            error_message = f"Attachment Path is missing in {index} item of the list"
            return is_verified, error_message
        if not attachment.get("file_extension", None):
            error_message = f"Attachment File Extension is missing in {index} item of the dict"
            return is_verified, error_message


    is_verified = True
    return is_verified, error_message



def email_delivering_consumers(email_details):
    """
    Sends email using sendgrid.
    :param email_details:
        email_details =
        {
          "to_emails_ids" -> list() : [],
          "cc_emails_ids" -> list() : [],
          "bcc_emails_ids" -> list() : [],
          "subject" -> str : "Email Subject",
          "plain_body" -> str : "Body",
          "html_body" -> str : "HTML Body",
          "attachments" -> URLs : [
              {
                 "attachment_path":"/path/to/file or Pre signed URL",
                 "attachment_filename":"attachment file name that needs to be sent. if not specified, default is 'attachment' ",
                 "file_extension":"file type extension"
              }
          ]
        }
    :return: success or failure
    """

    set_context("email_details", email_details)
    is_verified, error_message = verify_email_details(email_details)
    try:
        if not is_verified:
            capture_message(f"Failed to send email. Reason : {error_message}")
            raise ValueError(f"Email data Validation Failed: {error_message}")

        to_emails_ids = email_details.get("to_emails_ids", [])
        cc_emails_ids = email_details.get("cc_emails_ids", [])
        bcc_emails_ids = email_details.get("bcc_emails_ids", [])
        subject = email_details.get("subject", "")
        plain_body = email_details.get("plain_body", "")
        html_body = email_details.get("html_body", "")
        attachments = email_details.get("attachments", [])

        mail_sent_successfully = send_email(
            to_email = to_emails_ids,
            cc_email = cc_emails_ids,
            bcc_email = bcc_emails_ids,
            subject = subject,
            plain_body = plain_body,
            html_body = html_body,
            attachments = attachments
        )

        if mail_sent_successfully:
            print("Email sent Successfully")
            return True
        else:
            capture_message("Email sending Failed")
            raise Exception("Email sending Failed")
    except Exception as e:
        capture_exception(e)
        return False



