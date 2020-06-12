import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.functions import col, struct, lit

from utils.spark import kafka_source

import config


def sum_maps(r):
    res = dict()
    for i in r:
        key = next(iter(i))
        if key in res:
            res[key] += i[key]
        else:
            res[key] = i[key]

    return [{key: res[key]} for key in res]


sum_maps_udf = F.udf(sum_maps,
                     T.ArrayType(
                         T.MapType(
                             T.StringType(),
                             T.IntegerType()
                         )
                     )
                     )


def task_a_1_step_0(json_parsed_df):
    result = json_parsed_df.withWatermark("timestamp", "1 minute").groupBy(
        F.window("timestamp", "1 hour", "1 hour"),
        'country_name'
    ).agg(
        F.count('country_name').alias('count')
    ).select(
        F.struct(
            col('window.start').alias("datetime_start"),
            col('window.end').alias("datetime_end"),
            F.create_map(["country_name", "count"]).alias("map_item")
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "topics-by-country_step-0", config.LOG_PREFIX)
    return result


def task_a_1_step_1_final(spark):
    a1_struct = T.StructType([
        T.StructField("datetime_start", T.TimestampType()),
        T.StructField("datetime_end", T.TimestampType()),
        T.StructField("map_item", T.MapType(T.StringType(), T.IntegerType())),
    ])

    result = kafka_source(spark, config.BOOTSTRAP_SERVERS, "topics-by-country_step-0").parse_json(a1_struct) \
        .withWatermark("datetime_end", "1 minute").groupBy(
        F.window("datetime_end", "6 hour", "1 hour")
    ).agg(
        F.first("window.start").alias("timestamp_start"),
        F.first("window.end").alias("timestamp_end"),
        F.collect_list("map_item").alias("statistics")
    ).select(
        F.struct(
            F.concat(F.hour('timestamp_start'), lit(":"), F.minute('timestamp_start')).alias("time_start"),
            F.concat(F.hour('timestamp_end'), lit(":"), F.minute('timestamp_end')).alias("time_end"),
            col('timestamp_end').alias("time_end"),
            sum_maps_udf(col('statistics')).alias("statistics")
        ).alias("res")
    ).send_to_kafka(config.BOOTSTRAP_SERVERS, "topics-by-country", config.LOG_PREFIX)

    return result
