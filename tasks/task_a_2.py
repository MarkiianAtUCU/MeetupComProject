import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.functions import struct, window, col, lit

from utils.spark import kafka_source

import config


def concat_maps(r):
    res = dict()
    for i in r:
        key = next(iter(i))
        if key in res:
            res[key] += i[key]
        else:
            res[key] = i[key]

    return [{key: list(set(res[key]))} for key in res]


concat_maps_udf = F.udf(concat_maps,
                        T.ArrayType(
                            T.MapType(
                                T.StringType(),
                                T.ArrayType(
                                    T.StringType()
                                )
                            )
                        )
                        )


def task_a_2_step_0(json_parsed_df, states_names_df):
    result = json_parsed_df.filter(col('group_country') == 'us').withColumn(
        'topic_exploded', F.explode('topic_name')).withWatermark(
        "timestamp",
        "1 second"
    ).groupBy(
        F.window("timestamp", "1 minute", "1 minute"), 'group_state'
    ).agg(
        F.collect_set('topic_exploded').alias('topic_list')
    ).join(states_names_df, col("group_state") == states_names_df.code).select(
        F.struct(
            col('window.start').alias("datetime_start"),
            col('window.end').alias("datetime_end"),
            F.create_map(["state_name", "topic_list"]).alias("map_topics")
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "topics-by-state_step-0", config.LOG_PREFIX)

    return result


def task_a_2_step_1_final(spark):
    a2_struct = T.StructType([
        T.StructField("datetime_start", T.TimestampType()),
        T.StructField("datetime_end", T.TimestampType()),
        T.StructField("map_topics", T.MapType(
            T.StringType(),
            T.ArrayType(T.StringType())
        ))
    ])

    result = kafka_source(spark, config.BOOTSTRAP_SERVERS, "topics-by-state_step-0").parse_json(a2_struct) \
        .withWatermark("datetime_end", "1 second").groupBy(
        F.window("datetime_end", "3 minute", "1 minute")
    ) \
        .agg(
        F.first("window.start").alias("timestamp_start"),
        F.first("window.end").alias("timestamp_end"),
        F.collect_list("map_topics").alias("statistics")
    ) \
        .select(
        F.struct(
            F.concat(F.hour('timestamp_start'), lit(":"), F.minute('timestamp_start')).alias("time_start"),
            F.concat(F.hour('timestamp_end'), lit(":"), F.minute('timestamp_end')).alias("time_end"),
            concat_maps_udf(col('statistics')).alias("statistics")
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "topics-by-state", config.LOG_PREFIX)

    return result
