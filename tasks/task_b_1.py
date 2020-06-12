from pyspark.sql.functions import col

import config


def task_b_1(json_parsed_df):
    result = json_parsed_df.select(
        col('country_name')
    ).send_to_cassandra(
        config.CASSANDRA_CONFIG,
        "countries_set",
        config.CASSANDRA_NAMESPACE,
        config.LOG_PREFIX
    )

    return result
