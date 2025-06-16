from datetime import datetime, timedelta
from app.freshdesk import insert_freshdesk_data_to_clickhouse
from fastapi import Depends, FastAPI, HTTPException
from fastapi import status, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
import sentry_sdk
import asyncio
import schedule
from .conf import ACCESS_TOKEN_EXPIRE_MINUTES, STAGE, redis_cli, EXTRACTION_ISSUE_SCHEDULING_INTERVAL, COMP_SCHEDULING_INTERVAL
from .crud import create_user, get_user_list
from .schemas import Token, NewUser, DateRange
from .utils import pwd_context, authenticate_user, create_access_token, send_inconsistent_data
from .dependencies import get_current_user
from .template_dashboard import template_router
from .database_utils import portal_db, quality_database
from .template_solutioning.router import solutioning_router
from .retrigger.retrigger_lambdas import invocation_router
from .retrigger.ddb_status import ddb_router
from .template_solutioning.quality_data import quality_router
from .template_solutioning.credit_data import credit_router
from .proxy.proxy_router import proxy_router
from .template_solutioning.redis import redis_router
from .template_solutioning.fraud_metadata import fraud_metadata_router
from .categorisation.router import categorisation_router
from .single_category.router import single_category_router
from .extraction_issues.router import extraction_issue_router
from .template_solutioning.identity_mismatch import identity_mismatch_router
from .template_solutioning.general import general_router
import os
import json
from fastapi_utils.tasks import repeat_every
from .template_solutioning.ingest import ingest_router
from .categorisation.data_cron import perform_categorisation_check
from .categorisation.comp_mapping_cron import perfios_bankconnect_comp_mapping
from app.extraction_issues.schedulers.extraction import get_latest_extraction_issues
from app.template_solutioning.null_identity_quality import null_identity_router
from app.constants import UPDATE_STATE_RETRIGGER_REPEAT_AFTER
from app.cron_utils import get_all_processing_statements_and_trigger_update_state
from app.fsmlib_data_update.fsmlib_general_data_update import fsmlib_update_router
from app.inconsistency.statement.base import statement_level_inconsistency
file_path = '/tmp'

app = FastAPI(
    title="BankConnct Quality",
    summary="Contains BankConnect's Quality Resources",
    contact={
        "name": "Siddhant Tiwary",
        "email": "siddhant.tiwary@finbox.in"
    },
    docs_url=None,
    redoc_url=None,
    openapi_url=None
)

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def sentry_exception(request: Request, call_next):
    # print("sentry exception middleware called")
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        if STAGE == "prod":
            with sentry_sdk.push_scope() as scope:
                scope.set_context("request", request)
                scope.user = {
                    "ip_address": request.client.host,
                }
                sentry_sdk.capture_exception(e)
        raise e

# This is now updated to run at every 1 hour
def is_created_at_grater_than_3_hrs(file_path):
    ti_c = os.path.getctime(file_path)
    last_6_hour_date_time = datetime.now() - timedelta(hours = 1)
    time_stamp_before_6_hrs = last_6_hour_date_time.timestamp()
    difference = time_stamp_before_6_hrs - ti_c
    return difference>=0

def remove_file_grater_than_3_hrs():
    files = os.listdir(file_path)
    for file in files:
        if is_created_at_grater_than_3_hrs(file_path+'/'+file):
            os.remove(file_path+'/'+file)
    
    print("Cron Ran Successfully and removed files")

def schedule_job():
    schedule.every().day.at("09:00", "Asia/Kolkata").do(lambda: asyncio.create_task(send_inconsistent_data()))
    # schedule.every().day.at("08:00", "Asia/Kolkata").do(lambda: asyncio.create_task(perform_categorisation_check()))
    schedule.every(int(EXTRACTION_ISSUE_SCHEDULING_INTERVAL)).minutes.do(lambda: asyncio.create_task(get_latest_extraction_issues()))
    schedule.every(COMP_SCHEDULING_INTERVAL).minutes.do(lambda: asyncio.create_task(perfios_bankconnect_comp_mapping())) # runs every COMP_SCHEDULING_INTERVAL minutes
    schedule.every().day.at("03:00", "Asia/Kolkata").do(lambda: asyncio.create_task(insert_freshdesk_data_to_clickhouse()))
    print("jobs scheduled")

async def run_scheduler():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

@app.on_event("startup")
@repeat_every(seconds=3600)  # 1 hour
def remove_files():
    print('Cron Running to remove 1 hour old files')
    remove_file_grater_than_3_hrs()

# @repeat_every(seconds=1800) # 30 minutes
# async def run_comp_mapping():
#     await perfios_bankconnect_comp_mapping()

@app.on_event("startup")
async def startup():
    # await quality_db.connect()
    await portal_db.connect()
    await quality_database.connect()
    if STAGE == "prod":
        schedule_job()
        asyncio.create_task(run_scheduler())


@app.on_event("startup")
@repeat_every(seconds=UPDATE_STATE_RETRIGGER_REPEAT_AFTER)
async def invoke_update_state_for_processing():
    print('Invoking Updatestate for processing statements')
    await get_all_processing_statements_and_trigger_update_state(UPDATE_STATE_RETRIGGER_REPEAT_AFTER)
    print(f'Invoked all processing statements for the past {UPDATE_STATE_RETRIGGER_REPEAT_AFTER} seconds')
    
@app.on_event("shutdown")
async def shutdown():
    # await quality_db.disconnect()
    await portal_db.disconnect()
    await quality_database.disconnect()


@app.get("/")
async def root():
    return {"message": "Hello from BankConnect :)"}

@app.get("/api/send-inconsistent-data")
async def schedule_task(date_range: DateRange, background_tasks: BackgroundTasks, user=Depends(get_current_user)):
    redis_cli.delete('job_executed')
    if user.user_type!="superuser":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="not authorised",
        )
    from_date = date_range.from_date
    to_date = date_range.to_date
    date_ranges = []
    if from_date and to_date:
        date_format = "%Y-%m-%d"
        try:
            from_date = datetime.strptime(from_date, date_format)
            to_date = datetime.strptime(to_date, date_format)
            if from_date>to_date:
                return {"message": "from_date can not be greater than to_date"}
            while from_date<=to_date:
                date_ranges.append(from_date.strftime('%Y-%m-%d'))
                from_date += timedelta(days=1)
        except Exception:
            return {"message": f"Not a valid date or date format not in {date_format}"}
    background_tasks.add_task(send_inconsistent_data, date_ranges)
    return {"message": "Send Inconsistent Data scheduled"}

@app.post("/api/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = await create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)

    return {"access_token": access_token, "token_type": "bearer", "user_type": user.user_type}


@app.post("/api/user/create")
async def create_new_user(new_user: NewUser, user=Depends(get_current_user)):
    hashed_password = pwd_context.hash(new_user.password)
    await create_user(quality_database,  new_user.username, hashed_password)
    redis_cli.delete('user_list')
    return {"detail": "new user is created"}

@app.get("/api/user/list")
async def list_user(user=Depends(get_current_user)):
    if user.user_type!="superuser":
        return {"message": "not authorised"}
    user_list = redis_cli.get("user_list")
    if user_list is not None:
        user_list = json.loads(user_list)
    else:
        data = await get_user_list(quality_database)
        user_list = []
        for i in data:
            item = dict(i)
            user_list.append(item)
        redis_cli.set("user_list", json.dumps(user_list), ex=3600)
    
    return {"user_list": user_list}


app.include_router(template_router.router, prefix="/api")
app.include_router(proxy_router, prefix="/proxy")
app.include_router(solutioning_router, prefix="/template")
app.include_router(invocation_router, prefix="/invoke")
app.include_router(ddb_router, prefix="/update_ddb")
app.include_router(quality_router, prefix="/quality")
app.include_router(credit_router, prefix="/credit_card")
app.include_router(redis_router, prefix="/redis")
app.include_router(fraud_metadata_router, prefix="/fraud")
app.include_router(categorisation_router, prefix="/categorisation")
app.include_router(single_category_router, prefix="/category")
app.include_router(ingest_router, prefix='/ingest')
app.include_router(extraction_issue_router, prefix='/extraction-issue')
app.include_router(identity_mismatch_router, prefix='/identity_mismatch')
app.include_router(null_identity_router, prefix='/null_identity')
app.include_router(general_router, prefix='/general_data')
app.include_router(fsmlib_update_router, prefix='/fsmlib_update_requests')
app.include_router(statement_level_inconsistency, prefix='/inconsistency/statement')

# app.include_router(migrate_router, prefix='/migrate')