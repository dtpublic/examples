CREATE KEYSPACE IF NOT EXISTS testapp WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE testapp;

CREATE TABLE IF NOT EXISTS testapp.dt_meta(dt_app_id TEXT, dt_operator_id INT, dt_window BIGINT, PRIMARY KEY (dt_app_id, dt_operator_id));

CREATE TABLE IF NOT EXISTS testapp.TestUser(id uuid PRIMARY KEY, fname text, lname text, city text);

INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'pune', 'Neha', 'Singh');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'Lindon', 'Tina', 'Laura');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'Sunnyvale', 'Tom', 'Zing');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'Banglore', 'Priya', 'Gupta');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'Hydrabad', 'Seeta', 'Reddy');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'San Jose', 'Sam', 'Honk');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'pune', 'Rita', 'Sony');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'Sunnyvale', 'Megh', 'Gill');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'San Jose', 'Tina', 'Mann');
INSERT INTO testapp.testuser(id,city,fname,lname) VALUES(now(), 'San Jose', 'Sarah', 'Tom');
