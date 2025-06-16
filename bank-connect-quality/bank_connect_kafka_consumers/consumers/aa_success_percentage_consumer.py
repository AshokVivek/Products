import django
django.setup()
from django.conf import settings
from bank_connect.models import BankMaster, FinsenseAASession, Statement
from django.db.models import Subquery


from bank_connect.redis_utils import put_value

HIGHLY_STABLE_AA_FLOW_BANK_SUCCESS_PERCENTAGE_DICT_REDIS_KEY = settings.HIGHLY_STABLE_AA_FLOW_BANK_SUCCESS_PERCENTAGE_DICT_REDIS_KEY


def aa_success_percentage_consumer(_):
    aa_available_banks = list(BankMaster.objects.filter(is_aa_available=True).values_list("name", flat=True))

    highly_stable_aa_success_percent_dict = {}
    for bank_name in aa_available_banks:
        sample_set_count = 100

        # Subquery to get the last 1000 session IDs
        last_n_session_ids = (
            FinsenseAASession.objects.filter(bank_name__iexact=bank_name)
            .order_by("-created_at")
            .values_list("session_id", flat=True)[:sample_set_count]
        )

        # query to get statemenrt id list which were successful out of last n sessions
        successful_statement_ids = Statement.objects.filter(
            statement_id__in=Subquery(last_n_session_ids),
            is_complete=True,
            is_extracted=True,
            statement_status=0,
        ).values_list("statement_id", flat=True)

        # calculting success percentage
        success_percentage = round((len(successful_statement_ids) * 100.0) / sample_set_count, 2)

        highly_stable_aa_success_percent_dict[bank_name.lower()] = success_percentage

    # save in redis for next use
    print("success_percentage -> ", highly_stable_aa_success_percent_dict)
    put_value(
        key=HIGHLY_STABLE_AA_FLOW_BANK_SUCCESS_PERCENTAGE_DICT_REDIS_KEY,
        value=highly_stable_aa_success_percent_dict,
        timeout=7200,
    )
