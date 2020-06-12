from flask import Flask, jsonify
from db.kafka import OneTopicKafkaConsumer
from db.cassandra import Cassandra

import config

app = Flask(__name__)


@app.route('/list_of_countries', methods=["GET"])
def list_of_countries():
    obj = db.list_of_countries()
    return jsonify(obj), 200


@app.route('/cities_in_country/<string:country_id>', methods=["GET"])
def cities_in_country(country_id):
    obj = db.cities_in_country(country_id)
    return jsonify(obj), 200


@app.route('/get_event/<string:event_id>', methods=["GET"])
def get_event(event_id):
    obj = db.get_event(event_id)
    return jsonify(obj), 200


@app.route('/list_of_groups_by_city/<string:city>', methods=["GET"])
def list_of_groups_by_city(city):
    obj = db.list_of_groups_by_city(city)
    return jsonify(obj), 200


@app.route('/events_by_group/<string:group_id>', methods=["GET"])
def events_by_group(group_id):
    obj = db.events_by_group(group_id)
    return jsonify(obj), 200


@app.route('/statistics/events_number', methods=["GET"])
def statistics_events_number():
    return jsonify(events_number_kafka.get_last()), 200


@app.route('/statistics/us_groups', methods=["GET"])
def statistics_us_groups():
    return jsonify(us_groups_kafka.get_last()), 200


@app.route('/statistics/popular_topics', methods=["GET"])
def statistics_popular_topics():
    return jsonify(popular_topics_kafka.get_last()), 200


if __name__ == "__main__":
    events_number_kafka = OneTopicKafkaConsumer("topics-by-country", config.BOOTSTRAP_SERVERS)
    us_groups_kafka = OneTopicKafkaConsumer("topics-by-state", config.BOOTSTRAP_SERVERS)
    popular_topics_kafka = OneTopicKafkaConsumer("popular-topics-by-country", config.BOOTSTRAP_SERVERS)

    db = Cassandra(
        config.CASSANDRA_CONFIG["spark.cassandra.auth.username"],
        config.CASSANDRA_CONFIG["spark.cassandra.auth.password"],
        [config.CASSANDRA_CONFIG["spark.cassandra.connection.host"]],
        config.CASSANDRA_NAMESPACE
    )
    app.run(debug=False)
