CREATE KEYSPACE IF NOT EXISTS nexustiles WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE sea_surface_temp  (
tile_id    uuid PRIMARY KEY,
tile_blob  blob
);