DROP TABLE IF EXISTS sea_surface_temp;

CREATE TABLE sea_surface_temp  (
	tile_id    	uuid PRIMARY KEY,
	tile_blob  	blob
);
