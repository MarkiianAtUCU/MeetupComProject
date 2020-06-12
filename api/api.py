from flask import Flask, jsonify
# from db.cassandra import Cassandra
from kafka import KafkaConsumer, TopicPartition
import json

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

@app.route('/statistics', methods=["GET"])
def statistics():
    consumer.seek_to_end(partition)
    end = consumer.position(partition)
    last = int(end) - 1
    consumer.seek(partition, last)
    end_offset = consumer.end_offsets([partition])[partition]
    value = {}
    for i in consumer:
        value = i.value
        if i.offset == end_offset - 1: break
    
    return jsonify({value}), 200

if __name__ == "__main__":
    partition = TopicPartition("test", 0)
    consumer = KafkaConsumer(
        enable_auto_commit=False,
        auto_offset_reset="earliest", 
        bootstrap_servers=["localhost:9092"]),
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))))
    consumer.assign([partition])
    db = Cassandra("<user>", "<password>", "<ips>", "<keyspace='meetup_net_project'>")
    app.run(debug=False)