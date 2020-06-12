HTTP_STREAM_URL = "http://stream.meetup.com/2/rsvps"

SPARK_MASTER_URI = "spark:/<ip>:7077"
SPARK_CLUSTER = [
    ('spark.executor.memory', '1500m'),
    ('spark.executor.cores', '1'),
]

BOOTSTRAP_SERVERS = ['<ip1>:9092', '<ip2>:9092', '<ip3>:9092']
LOG_PREFIX = "ckp/log_meetup_reporter-"

CASSANDRA_CONFIG = {
    "spark.cassandra.connection.host": '<ip>',
    "spark.cassandra.auth.username": '<username>',
    "spark.cassandra.auth.password": '<password>'
}
CASSANDRA_NAMESPACE = "meetup_net_project"
