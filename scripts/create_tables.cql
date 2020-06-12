CREATE TABLE countries_set (
  country_name text,
  primary key (country_name)
);

CREATE TABLE cities_by_country (
  country_name text,
  group_city text,
  primary key ((country_name), group_city)
);

CREATE TABLE event_by_id (
  event_id text,
  event_name text,
  event_time text,
  topics list<text>,
  group_name text,
  country_name text,
  group_city text,
  primary key (event_id)
);

CREATE TABLE groups_by_city (
  group_city text,
  group_id text,
  group_name text,
  primary key ((group_city), group_id)
);

CREATE TABLE events_by_group (
  group_id text,
  event_id text,
  primary key ((group_id), event_id)
);