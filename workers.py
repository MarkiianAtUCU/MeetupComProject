import findspark
import os

findspark.init()
import pyspark
from pyspark.sql import SparkSession

import config
from utils.utils import add_method
from utils.spark import send_to_kafka, send_to_cassandra, parse_json

from tasks.preparation import prepare
from tasks.task_a_1 import task_a_1_step_0, task_a_1_step_1_final
from tasks.task_a_2 import task_a_2_step_0, task_a_2_step_1_final
from tasks.task_a_3 import task_a_3_step_0, task_a_3_step_1, task_a_3_step_2, task_a_3_step_3_final
from tasks.task_b_1 import task_b_1
from tasks.task_b_2 import task_b_2
from tasks.task_b_3 import task_b_3
from tasks.task_b_4 import task_b_4
from tasks.task_b_5 import task_b_5

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,com.github.jnr:jffi:1.2.19 pyspark-shell'

conf = pyspark.SparkConf().setAll(config.SPARK_CLUSTER)
spark = SparkSession.builder.config(conf=conf).master(config.SPARK_MASTER_URI).getOrCreate()

add_method(pyspark.sql.dataframe.DataFrame, send_to_kafka)
add_method(pyspark.sql.dataframe.DataFrame, send_to_cassandra)
add_method(pyspark.sql.dataframe.DataFrame, parse_json)

prepared_df = prepare(spark)

print("Task A-1:")
task_a_1_step_0(prepared_df).start()
print("    started step 0")
task_a_1_step_1_final(spark).start()
print("    started step 1")
print("====")

print("Task A-2:")
states_names_df = spark.read.json("data/USstate.json")
task_a_2_step_0(prepared_df, states_names_df).start()
print("    started step 0")
task_a_2_step_1_final(spark).start()
print("    started step 1")
print("====")

print("Task A-3:")
task_a_3_step_0(prepared_df).start()
print("    started step 0")
task_a_3_step_1(spark).start()
print("    started step 1")
task_a_3_step_2(spark).start()
print("    started step 2")
last_worker = task_a_3_step_3_final(spark).start()
print("    started step 3")
print("====")

print("Task B-1:")
task_b_1(prepared_df).start()
print("    started task")
print("====")

print("Task B-2:")
task_b_2(prepared_df).start()
print("    started task")
print("====")

print("Task B-3:")
task_b_3(prepared_df).start()
print("    started task")
print("====")

print("Task B-4:")
task_b_4(prepared_df).start()
print("    started task")
print("====")

print("Task B-5:")
last_worker = task_b_5(prepared_df).start()
print("    started task")
print("====")

print("Waiting for termination")
last_worker.awaitTermination()
