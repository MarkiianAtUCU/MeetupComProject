from pyspark.sql.functions import col

import config


def task_b_2(json_parsed_df):
    result = json_parsed_df.select(
        col('country_name'),
        col('group_city'),
    ).send_to_cassandra(
        config.CASSANDRA_CONFIG,
        "cities_by_country",
        config.CASSANDRA_NAMESPACE,
        config.LOG_PREFIX
    )

    return result
