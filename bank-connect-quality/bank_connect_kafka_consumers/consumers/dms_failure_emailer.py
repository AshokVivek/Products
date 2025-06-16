import django

django.setup()

from django.conf import settings

from bank_connect.helpers.tata_capital.dms_emailer import DMSEmailer
from finbox_dashboard.models import Organization


def dms_failure_emailer(failure_email_data):
    print(f"This is the request for the sending failure email = {failure_email_data}")
    organization_id = settings.TCAP_ORGANIZATION_ID
    try:
        organization = Organization.objects.get(id=organization_id)
        response = DMSEmailer(organization, failure_email_data).send_failure_email()
    except Exception as e:
        print("Exception occurred while sending DMS failure email = {}".format(e))
        return

    if not response.get("success", False):
        print("Failure occurred while sending DMS failure email = {}".format(response.get("error_message", "")))
    else:
        print("Successfully sent the email")

    return
