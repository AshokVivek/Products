import django
django.setup()

from django.conf import settings

from bank_connect.helpers.tata_capital.mis_generator import GenerateMISReport
from finbox_dashboard.models import Organization


def tcap_mis_generation(mis_generation_data):
    print(f"This is the request for the mis generation = {mis_generation_data}")
    organization_id = settings.TCAP_ORGANIZATION_ID

    organization = Organization.objects.get(id=organization_id)
    resp = GenerateMISReport(organization).__call__()

    if not resp.get("success", False):
        print("Failure occurred while generating MIS = {}".format(resp.get("error_message", "")))
    else:
        print("Successfully sent the email")
