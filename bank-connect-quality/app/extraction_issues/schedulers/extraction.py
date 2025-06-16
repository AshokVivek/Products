import time
import random
import traceback
from datetime import datetime, timedelta
from dateutil import tz

import sentry_sdk
from slack_sdk import WebClient
import os

from app.conf import EXTRACTION_ISSUE_SLACK_TOKEN, EXTRACTION_ISSUE_SLACK_CHANNEL, EXTRACTION_ISSUE_SCHEDULING_INTERVAL, redis_cli
from app.database_utils import clickhouse_client
from app.constants import DATE_FORMAT

EXTRACTION_ISSUES_CRON_LOCKFILE = '/tmp/extraction_issues_cron_job.lock'
async def get_latest_extraction_issues():
    if os.path.exists(EXTRACTION_ISSUES_CRON_LOCKFILE):
        print("Another instance is already running, skipping for this worker")
        return
    
    with open(EXTRACTION_ISSUES_CRON_LOCKFILE, "w") as f:
        pass

    current_time = datetime.now()
    if not (current_time.hour >= 2 and current_time.hour <= 17):
        os.remove(EXTRACTION_ISSUES_CRON_LOCKFILE)
        return

    time.sleep(random.randint(1, 3))
    job_executed = redis_cli.get("extraction_issues_job_executed")
    if job_executed:
        print("Send Extraction Data already scheduled")
        os.remove(EXTRACTION_ISSUES_CRON_LOCKFILE)
        return
    redis_cli.set("extraction_issues_job_executed", 1, 600)

    # figure out time window
    
    slack_client = WebClient(token=EXTRACTION_ISSUE_SLACK_TOKEN)
    to_date = current_time.strftime(DATE_FORMAT) 
    datetime_obj_from_date = (current_time - timedelta(minutes=int(EXTRACTION_ISSUE_SCHEDULING_INTERVAL)))
    from_date = datetime_obj_from_date.strftime(DATE_FORMAT)
    to_zone = tz.gettz('Asia/Kolkata')
    from_date_ist = datetime_obj_from_date.astimezone(to_zone)
    to_date_ist = current_time.astimezone(to_zone)

    message = f"Date Range : {from_date_ist} - {to_date_ist} \n"
    try:
        query = f'''
                    SELECT 
                        extraction_issue_type,
                        COUNT(DISTINCT statement_id) AS statement_count,
                        COUNT(*) AS transaction_count,
                        is_extraction_problem_confirmed,
                    FROM 
                        extractionIssue
                    where 
                        created_at >= '{from_date}' AND created_at < '{to_date}'
                    GROUP BY extraction_issue_type, is_extraction_problem_confirmed
                    ORDER BY extraction_issue_type, is_extraction_problem_confirmed
                '''
        
        result_df = clickhouse_client.query_df(query)
        if result_df.empty:
            message = 'No extraction Issues found in the given Date Range' 
        else:
            if 'is_extraction_problem_confirmed' in result_df.columns:
                result_df = result_df.replace({'is_extraction_problem_confirmed' : { None : 'Waiting for Review', False : 'False Positive', True : 'Actual Issue' }})
            
            pivot_df = result_df.pivot_table(
                index='extraction_issue_type', 
                columns='is_extraction_problem_confirmed', 
                values=['transaction_count'], 
                aggfunc='sum'
                )
            
            pivot_df = pivot_df.fillna(0)
            pivot_df.rename(columns={'transaction_count':''}, inplace=True)
            message += "```\n" + pivot_df.to_markdown() + "\n```"

        response = slack_client.chat_postMessage(
            channel=EXTRACTION_ISSUE_SLACK_CHANNEL,
            text = message
        )
        os.remove(EXTRACTION_ISSUES_CRON_LOCKFILE)
        return
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(traceback.format_exc())
    
    os.remove(EXTRACTION_ISSUES_CRON_LOCKFILE)