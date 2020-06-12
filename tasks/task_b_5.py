from pyspark.sql.functions import col

import config


def task_b_5(json_parsed_df):
    result = json_parsed_df.select(
        col('group_id'),
        col('event_id')
    ).send_to_cassandra(
        config.CASSANDRA_CONFIG,
        "events_by_group",
        config.CASSANDRA_NAMESPACE,
        config.LOG_PREFIX
    )

    return result
