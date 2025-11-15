CREATE OR REPLACE TABLE {baselayer_name} AS SELECT * FROM '{path_to_baselayer}';

ALTER TABLE {baselayer_name} ADD COLUMN geom GEOMETRY;
UPDATE {baselayer_name} SET geom = ST_GeomFromWKB({baselayer_gomcol});

ALTER TABLE {baselayer_name} DROP COLUMN {baselayer_gomcol};

CREATE INDEX IF NOT EXISTS {baselayer_name}_geometry_rtree ON {baselayer_name} USING rtree(geom);