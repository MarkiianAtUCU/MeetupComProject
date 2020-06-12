from pyspark.sql.functions import col

import config


def task_b_4(json_parsed_df):
    result = json_parsed_df.select(
        col('group_city'),
        col('group_name'),
        col('group_id')
    ).send_to_cassandra(
        config.CASSANDRA_CONFIG,
        "groups_by_city",
        config.CASSANDRA_NAMESPACE,
        config.LOG_PREFIX
    )

    return result
