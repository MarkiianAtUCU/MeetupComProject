import pyspark.sql.functions as F
from pyspark.sql.functions import col


def send_to_kafka(self, servers, topic, ckp_prefix):
    return self.select(F.to_json('res').alias('value')).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(servers)) \
        .option("topic", topic) \
        .option("checkpointLocation", f"{ckp_prefix}kafka-{topic}")


def send_to_cassandra(self, connection_config, table, keyspace, ckp_prefix):
    return self.writeStream.format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", f"{ckp_prefix}cassandra-{table}") \
        .options(**connection_config) \
        .option("keyspace", keyspace) \
        .option("table", table)


def kafka_source(spark, servers, topic):
    return spark. \
        readStream. \
        format("kafka"). \
        option("kafka.bootstrap.servers", ",".join(servers)). \
        option("subscribe", topic). \
        option("startingOffsets", "earliest"). \
        load()


def parse_json(self, schema):
    return self.select(
        F.from_json(col("value").cast("string"), schema).alias("json_parsed")
    ).select("json_parsed.*")
