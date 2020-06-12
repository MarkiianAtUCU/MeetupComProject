from flask import Flask, jsonify
from db.cassandra import Cassandra

app = Flask(__name__)

@app.route('/list_of_countries', methods=["GET"])
def list_of_countries():
    obj = db.list_of_countries()
    return jsonify(obj), 200

@app.route('/cities_in_country/<string:country_id>', methods=["GET"])
def cities_in_country(country_id):
    obj = cities_in_country(country_id)
    return jsonify(obj), 200

@app.route('/get_event/<string:event_id>', methods=["GET"])
def get_event(event_id):
    obj = get_event(event_id)
    return jsonify(obj), 200

@app.route('/list_of_groups_by_city/<string:city>', methods=["GET"])
def list_of_groups_by_city(city):
    obj = list_of_groups_by_city(city)
    return jsonify(obj), 200

@app.route('/events_by_group/<string:group_id>', methods=["GET"])
def events_by_group(group_id):
    obj = db.events_by_group(group_id)
    return jsonify(obj), 200

if __name__ == "__main__":
    db = Cassandra("<user>", "<password>", "<ips>", "<keyspace='meetup_net_project'>")
    app.run(debug=False)