from airflow.sdk import Variable, Metadata, asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@asset(uri="s3://assets/fingrid_api_datasets.json", schedule="@daily")
def fingrid_datasets_to_s3(self):
    import time
    import json
    import requests

    nextPage = 1
    hdr = {
        'Cache-Control': 'no-cache',
        'x-api-key': f'{Variable.get("fingrid_apikey")}',
    }
    wait = 20
    pagesize = 8000
    sets = []
    while nextPage != None:
        url = f"https://data.fingrid.fi/api/datasets?page={nextPage}&pageSize={pagesize}&orderBy=id"
        response = requests.get(url=url, headers=hdr)
        pagedata = json.loads(response.content)
        for dataset in pagedata['data']:
            tmp={ "id": dataset['id'], "name": dataset['nameEn']}
            sets.append(tmp)
        try:
            nextPage = pagedata['pagination']['nextPage']
            time.sleep(wait)
        except Exception:
            nextPage = None

    s3 = S3Hook('assets', transfer_config_args={
        'use_threads': False
        })

    ss = json.dumps(sets)
    s3.load_bytes(ss.encode(),bucket_name="assets",  key="fingrid_api_datasets.json", replace=True)
    yield Metadata(self, {"row_count": len(ss)})
