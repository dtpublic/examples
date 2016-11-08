CREATE KEYSPACE IF NOT EXISTS testapp WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE testapp;

CREATE TABLE IF NOT EXISTS testapp.dt_meta(dt_app_id TEXT, dt_operator_id INT, dt_window BIGINT, PRIMARY KEY (dt_app_id, dt_operator_id));

CREATE TABLE IF NOT EXISTS testapp.TestUser(id uuid PRIMARY KEY, fname text, lname text, city text);