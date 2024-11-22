from dagster import (
    AssetSelection,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

from .partitions import (
    daily_t_partitions,
)

daily_news_job_schedule = build_schedule_from_partitioned_job(
    define_asset_job(
        "daily_news_job",
        selection=AssetSelection.key_prefixes(["news", "BBC"]),
        partitions_def=daily_t_partitions,
    )
)
