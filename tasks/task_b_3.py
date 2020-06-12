import pyspark.sql.functions as F
from pyspark.sql.functions import col

import config


def task_b_3(json_parsed_df):
    result = json_parsed_df.select(
        col('event_id'),
        col('event_name'),
        F.from_unixtime(col('time') / 1000).alias('event_time'),
        col('topic_name').alias("topics"),
        col('group_name'),
        col('country_name'),
        col('group_city')
    ).send_to_cassandra(
        config.CASSANDRA_CONFIG,
        "event_by_id",
        config.CASSANDRA_NAMESPACE,
        config.LOG_PREFIX
    )

    return result
