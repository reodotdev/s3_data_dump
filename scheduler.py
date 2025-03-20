import json
import time
import s3_dump
from datetime import datetime, timedelta
from utils.common import send_slack_alert


def check_to_sync(dt, frequency):
    timedeltas = {
        "daily": timedelta(days=-1),
        "monthly": timedelta(days=-30),
        "weekly": timedelta(days=-7),
        "quaterly": timedelta(days=-90),
        "biweekly": timedelta(days=-14),
        "halfyearly": timedelta(days=-180)
    }

    if dt < datetime.now()+timedeltas[frequency]:
        return True
    return False

data_extractor_func = {
    "product_usage": {
        "s3": s3_dump.fetch_product_usage
    }
}

def seconds_until_next_day():
    # Get the current datetime
    now = datetime.now()

    # Calculate the datetime for the next day at midnight
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # Compute the time difference between tomorrow and now
    delta = tomorrow - now

    # Return the total seconds until the next day
    return int(delta.total_seconds())

try:
    while True:
        input_js = {}
        with open("input_js.json", "r") as f:
            input_js = json.loads(f.read())
        message = ""
        for tenant, sync_dets in input_js.items():
            for idx, sync_det in enumerate(sync_dets):
                last_sync_dt = datetime.strptime("1980-01-01", "%Y-%m-%d") if sync_det["last_sync_at"] is None else datetime.strptime(sync_det["last_sync_at"], "%Y-%m-%d")
                if check_to_sync(last_sync_dt, sync_det["frequency"]):
                    data_extractor_func[sync_det["data"]][sync_det["storage_type"]](
                        tenant_id = tenant,
                        from_date = (last_sync_dt+timedelta(days=1)).strftime("%Y-%m-%d"),
                        to_date = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d"),
                        bucket_name = sync_det["bucket_name"],
                        **sync_det["storage_api_keys"]
                    )
                    input_js[tenant][idx]["last_sync_at"] = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
                    message+=f"Ran sync for {tenant}. {sync_det['data']} data was exported to their {sync_det['storage_type']} \n"

        with open("input_js.json", "w+") as f:
            f.write(json.dumps(input_js))

        if message!="":
            send_slack_alert(f"""Report for the date {datetime.now().strftime("%Y-%m-%d")}: \n {message}""")
        else:
            send_slack_alert(f"""No updates for today!""")
        thread_sleep_time = seconds_until_next_day()
        time.sleep(thread_sleep_time)
except Exception as e:
    send_slack_alert(f"""Error while creating report: {e}""")
    raise e



