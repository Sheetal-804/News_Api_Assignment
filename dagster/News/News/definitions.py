from dagster import Definitions, load_assets_from_modules

# from News.resources import (
#     S3BucketResource,
# )
from News.utils.dagster import load_jobs_from_modules

from . import assets, jobs
from .jobs import (
    daily_news_job_schedule,
)

all_assets = load_assets_from_modules([assets])
all_jobs = load_jobs_from_modules([jobs])
all_schedules = [
    daily_news_job_schedule,
]

defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    # resources={
    #     "rawdata_bucket": S3BucketResource(
    #         endpoint_url="https://mum-objectstore.e2enetworks.net/",
    #         access_key="PA6KYSVG2CI4GY5H2GUV",
    #         secret_key="FISFE0LB22UPGPVS1HAV0SA03MLYIF2Y046QWMWH",
    #         bucket_name="crismac-dl-raw-bucket-dev",
    #         path_prefix="",
    #     ),
    # },
)
