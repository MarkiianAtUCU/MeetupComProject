import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.functions import struct, window, col, lit

from utils.spark import kafka_source

import config

a3_struct_common = T.StructType([
    T.StructField("timetamp_start", T.TimestampType()),
    T.StructField("timetamp_end", T.TimestampType()),
    T.StructField("country_name", T.StringType()),
    T.StructField("topic_name_exp", T.StringType()),
    T.StructField("topic_sum", T.IntegerType()),
])


def task_a_3_step_0(json_parsed_df):
    result = json_parsed_df.withColumn('topic_name_exp', F.explode('topic_name')) \
        .withWatermark("timestamp", "1 minute").groupBy(
        F.window("timestamp", "1 minute", "1 minute"),
        'country_name',
        'topic_name_exp'
    ).agg(
        F.count('topic_name_exp').alias('topic_count')
    ).select(
        F.struct(
            col('window.end').alias("datetime_end"),
            col('country_name'),
            col('topic_name_exp'),
            col('topic_count')
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "popular-topics-by-country_step-0", config.LOG_PREFIX)

    return result


def task_a_3_step_1(spark):
    a3_struct = T.StructType([
        T.StructField("datetime_end", T.TimestampType()),
        T.StructField("country_name", T.StringType()),
        T.StructField("topic_name_exp", T.StringType()),
        T.StructField("topic_count", T.IntegerType()),
    ])

    result = kafka_source(spark, config.BOOTSTRAP_SERVERS, "popular-topics-by-country_step-0").parse_json(a3_struct) \
        .withWatermark("datetime_end", "1 minute").groupBy(
        F.window("datetime_end", "6 minute", "1 minute"), 'country_name', 'topic_name_exp'
    ).agg(
        F.sum('topic_count').alias("topic_sum")
    ).select(
        F.struct(
            col("window.start").alias("timetamp_start"),
            col("window.end").alias("timetamp_end"),
            col("country_name"),
            col("topic_name_exp"),
            col("topic_sum")
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "popular-topics-by-country_step-1", config.LOG_PREFIX)

    return result


def task_a_3_step_2(spark):
    result = kafka_source(spark, config.BOOTSTRAP_SERVERS, "popular-topics-by-country_step-1").parse_json(a3_struct_common) \
        .withColumn(
        "topic_map",
        F.struct(
            col('topic_sum'),
            col('topic_name_exp'),
        )).withWatermark("timetamp_start", "1 minute").groupBy(
        "timetamp_start",
        "timetamp_end",
        "country_name"
    ).agg(
        F.max("topic_map").alias("max_map")
    ).select(
        F.struct(
            col('timetamp_start'),
            col('timetamp_end'),
            col('country_name'),
            col('max_map.topic_name_exp').alias("topic_name_exp"),
            col('max_map.topic_sum').alias("topic_sum")
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "popular-topics-by-country_step-2", config.LOG_PREFIX)

    return result


def task_a_3_step_3_final(spark):
    result = kafka_source(spark, config.BOOTSTRAP_SERVERS, "popular-topics-by-country_step-2").parse_json(a3_struct_common) \
        .withWatermark("timetamp_start", "1 minute").groupBy(
        "timetamp_start",
        "timetamp_end"
    ).agg(
        F.collect_list(
            F.create_map(
                [
                    "country_name",
                    F.create_map(
                        [
                            "topic_name_exp",
                            "topic_sum"
                        ]
                    )
                ]
            )
        ).alias("statistics")
    ).select(
        F.struct(
            F.concat(F.hour('timetamp_start'), lit(":"), F.minute('timetamp_start')).alias("time_start"),
            F.concat(F.hour('timetamp_end'), lit(":"), F.minute('timetamp_end')).alias("time_end"),
            col('statistics')
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "popular-topics-by-country2", config.LOG_PREFIX)

    return result
