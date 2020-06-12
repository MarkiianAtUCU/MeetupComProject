from dse.cluster import Cluster
from dse.auth import PlainTextAuthProvider

def to_json(lst):
    result = []
    for i in lst:
        result.append(dict(i._asdict()))
    return result

class Cassandra:
    def __init__(self, user, password, ips, keyspace):
        self._auth_provider = PlainTextAuthProvider(user, password)
        self._cluster = Cluster(ips, auth_provider=self._auth_provider)
        self.session = self._cluster.connect(keyspace=keyspace)

    def list_of_countries(self):
        result = self.session.execute("select * from countries_set;")
        return to_json(result)

    def cities_in_country(self, country_id):
        result = self.session.execute(f"select * from cities_by_country where country_name='{country_id}';")
        return to_json(result)

    def get_event(self, event_id):
        result = self.session.execute(f"select * from event_by_id where event_id='{event_id}';")
        return to_json(result)[0]

    def list_of_groups_by_city(self, city):
        result = self.session.execute(f"select * from groups_by_city where group_city='{city}';")
        return to_json(result)

    def events_by_group(self, group_id):
        result = self.session.execute(f"select * from events_by_group where group_id='{group_id}';")
        result = to_json(result)
        events_list = [i["event_id"] for i in result]
        events_list = ["\'" + i + "\'" for i in events_list]
        new_result = self.session.execute(f"select * from event_by_id where event_id in ({','.join(events_list)}) ;")

        return to_json(new_result)

if __name__ == "__main__":
    pass
