CREATE KEYSPACE IF NOT EXISTS nexustiles WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

DROP TABLE IF EXISTS nexustiles.sea_surface_temp;

CREATE TABLE nexustiles.sea_surface_temp  (
	tile_id    	uuid PRIMARY KEY,
	tile_blob  	blob
);
