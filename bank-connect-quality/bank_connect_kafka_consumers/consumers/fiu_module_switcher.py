import django

django.setup()

from django.conf import settings
from bank_connect.models import FinsenseAASession, BankMaster
from fiu_module.models import AccountAggregatorSession
from fiu_module.views import trigger_fiu_module_and_get_data
from pydantic import BaseModel
from datetime import datetime, timedelta
import pandas as pd

FIU_MODULE_BASE_URL = settings.FIU_MODULE_BASE_URL
FIU_MODULE_AUTHORIZATION_KEY = settings.FIU_MODULE_AUTHORIZATION_KEY

class ScoreParams(BaseModel):
    avg_latency: float
    avg_success_rate: float
    fip_id: str


class SwitcherInput(BaseModel):
    aa_vendor: str
    score_params: list[ScoreParams]


class AutoSwitchRequestBodyModel(BaseModel):
    from_ts: datetime
    to_ts: datetime
    data: list[SwitcherInput]

    # @field_validator("from_ts", "to_ts", mode="before")
    # def check_datetime_value(cls, value: str):
    #     return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def get_bank_name_fip_id_mapping():
    mapped_data = {}
    bank_master_objects = BankMaster.objects.filter(aa_fip_id__isnull=False).values(
        "name", "aa_fip_id"
    )
    for obj in bank_master_objects:
        mapped_data[obj["name"].lower()] = obj["aa_fip_id"]
    return mapped_data


def get_finvu_data_block(since_time, to_time, mapped_data):
    finvu_objects = FinsenseAASession.objects.filter(
        created_at__gte=since_time,
        created_at__lte=to_time,
        status__isnull=False,
    )
    print(f"Number of FinVu objects - {len(finvu_objects)}")
    finvu_objects = [obj.__dict__ for obj in finvu_objects]
    df = pd.DataFrame(finvu_objects)
    df["fip_id"] = ""
    for index, item in df.iterrows():
        df["fip_id"][index] = mapped_data.get(item.get("bank_name", "").lower(), "")

    finvu_data = SwitcherInput(aa_vendor="finvu-aa", score_params=[])

    for bank, fip_id in mapped_data.items():
        print(f"Bank Name - {bank}")
        bank_score_params = ScoreParams(
            fip_id=fip_id, avg_latency=0, avg_success_rate=100
        )

        try:
            filtered_df = df[df["bank_name"].str.lower() == bank]
            print(f"Filtered DF Length - {len(filtered_df)}")

            filtered_df_READY = filtered_df[filtered_df["status"] == "READY"]
            bank_score_params.avg_success_rate = (
                len(filtered_df_READY) / len(filtered_df) if len(filtered_df) else 1
            ) * 100
        except Exception:
            pass

        finvu_data.score_params.append(bank_score_params)

    return finvu_data


def get_data_for_fiu_module(
    since_time, to_time, mapped_banks, auto_switcher_input: AutoSwitchRequestBodyModel
):
    aa_session_objects = AccountAggregatorSession.objects.filter(
        created_at__gte=since_time,
        created_at__lte=to_time,
        consent_status__isnull=False,
    ).values()

    df = pd.DataFrame(aa_session_objects)
    print("Length of DataFrame - ", len(df))

    if len(df) == 0:
        return auto_switcher_input

    present_aa = df.aa_entity.unique()
    print("Present AA - ", present_aa)

    for aa in present_aa:
        print("AA Name - ", aa)
        aa_data = SwitcherInput(aa_vendor=aa, score_params=[])
        aa_df_filtered = df[df["aa_entity"] == aa]

        for _bank, fip_id in mapped_banks.items():
            print(f"Bank: {_bank}, fip_id: {fip_id}")

            bank_score_params = ScoreParams(
                fip_id=fip_id, avg_latency=0, avg_success_rate=0
            )
            if len(aa_df_filtered) == 0:
                # TODO: If there are no requests for a particular AA of a particular bank, then instead of 100, it should be the parent rate
                bank_score_params.avg_success_rate = 100
            else:
                total_data = aa_df_filtered[(aa_df_filtered["fip_id"] == fip_id)]
                # TODO: Set a SLA of x minutes post which consent will be marked FAILED due to User failure or due to BankNotResponding with data
                # TODO: Have Parallel crons to mark statement as failed
                completed_df = total_data[total_data["consent_status"] == "COMPLETED"]
                bank_score_params.avg_success_rate = (
                    (len(completed_df) / len(total_data)) if len(total_data) > 0 else 1
                ) * 100
                aa_data.score_params.append(bank_score_params)

        auto_switcher_input.data.append(aa_data)
    return auto_switcher_input


def call_fiu_auto_switch(payload):
    url = f"{FIU_MODULE_BASE_URL}/internal/auto_switch"
    headers = {
        'accept': 'application/json',
        'Authorization': FIU_MODULE_AUTHORIZATION_KEY,
        'Content-Type': 'application/json'
    }
    data = trigger_fiu_module_and_get_data(
        url = url,
        payload=payload,
        headers=headers,
        timeout=60,
        method="POST"
    )
    print("Response from FIU Module - ", data)


def fiu_module_switcher(_details):
    print("Switcher Consumer Invoked")
    print(f"Details : {_details}")

    mapped_data = get_bank_name_fip_id_mapping()

    invoked_at = datetime.now()
    # because we need to perform analysis from t-17 to t-2
    to_time = invoked_at - timedelta(minutes=2)
    since_time = to_time - timedelta(minutes=15)

    print(f"Invoked At : {invoked_at}, To Time : {to_time}, Since Time : {since_time}")

    finvu_data = get_finvu_data_block(
        since_time=since_time, to_time=to_time, mapped_data=mapped_data
    )

    print("Finvu Data retrieved")

    auto_switcher_input = AutoSwitchRequestBodyModel(
        from_ts=since_time, to_ts=to_time, data=[finvu_data]
    )

    print("Auto Switch Input Instantiated")

    auto_switcher_input = get_data_for_fiu_module(
        since_time=since_time,
        to_time=to_time,
        mapped_banks=mapped_data,
        auto_switcher_input=auto_switcher_input
    )

    payload = auto_switcher_input.dict()

    payload["from_ts"] = str(payload["from_ts"])
    payload["to_ts"] = str(payload["to_ts"])

    call_fiu_auto_switch(payload)
