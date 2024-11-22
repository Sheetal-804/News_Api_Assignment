from dagster import DailyPartitionsDefinition, TimeWindowPartitionsDefinition

from News.utils import timezone

daily_partitions = DailyPartitionsDefinition(
    start_date="2023-01-01", timezone=timezone.IST.zone, fmt="%Y-%m-%d"
)
"""Daily partitions for T-1 day partition."""

daily_t_partitions = DailyPartitionsDefinition(
    start_date="2023-01-01",
    timezone=timezone.IST.zone,
    fmt="%Y-%m-%d",
    end_offset=1,
)
"""Daily partitions for T day partition."""

weekday_partitions = TimeWindowPartitionsDefinition(
    start="2023-01-01",
    timezone=timezone.IST.zone,
    fmt="%Y-%m-%d",
    # At 00:00 on every day-of-week from Monday through Saturday.
    cron_schedule="0 0 * * 1-6",
)
"""Weekly partitions runs at 00:00 on Mon-Sat days for T-1 day partition."""

weekday_t_partitions = TimeWindowPartitionsDefinition(
    start="2023-01-01",
    timezone=timezone.IST.zone,
    fmt="%Y-%m-%d",
    end_offset=1,
    # At 20:00 on every day-of-week from Monday through Friday.
    cron_schedule="0 20 * * 1-5",
)
"""Weekly partitions runs at 20:00 on Mon-Sat days for T day partition."""
