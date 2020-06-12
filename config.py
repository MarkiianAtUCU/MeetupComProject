HTTP_STREAM_URL = "http://stream.meetup.com/2/rsvps"

SPARK_MASTER_URI = "spark:/<ip>:7077"
SPARK_CLUSTER = [
    ('spark.executor.memory', '1500m'),
    ('spark.executor.cores', '1'),
]

# BOOTSTRAP_SERVERS = ['<ip1>:9092', '<ip2>:9092', '<ip3>:9092']
BOOTSTRAP_SERVERS = ['localhost:9092']
LOG_PREFIX = "ckp/log_meetup_reporter-"

CASSANDRA_CONFIG = {
    "spark.cassandra.connection.host": '34.240.57.255',
    "spark.cassandra.auth.username": 'cassandra',
    "spark.cassandra.auth.password": 'cassandra'
}
CASSANDRA_NAMESPACE = "meetup_net_project"
