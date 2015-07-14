--neighbor cells view
DROP MATERIALIZED VIEW IF EXISTS neighbor_cells CASCADE;
CREATE MATERIALIZED VIEW neighbor_cells AS (
 SELECT row_number() OVER () AS id,
    a.id AS cellid1,
    b.id AS cellid2
   FROM voronoi a,
    voronoi b
  WHERE b.id <> a.id AND st_intersects(a.geom, b.geom)
)
WITH DATA;

--neighbor route view
DROP MATERIALIZED VIEW IF EXISTS neighbor_links CASCADE;
CREATE MATERIALIZED VIEW neighbor_links AS (
	SELECT neighbor_cells.id,
	cellid1::int4 AS source,
	cellid2::int4 AS target,
	ST_Distance(ST_Centroid(v1.geom), ST_Centroid(v2.geom)) AS cost
	FROM neighbor_cells,
	voronoi v1,
	voronoi v2
	WHERE v1.id = cellid1 AND v2.id = cellid2
)
WITH DATA;

CREATE INDEX neighbor_links_source_idx ON neighbor_links USING btree(source);
CREATE INDEX neighbor_links_target_idx ON neighbor_links USING btree(target);

CREATE OR REPLACE FUNCTION route_neighbors(integer[]) RETURNS SETOF integer AS
$BODY$
    BEGIN
    FOR i IN 1 .. array_length($1,1)-1
    LOOP
	RETURN QUERY(SELECT r.id1
				   FROM pgr_dijkstra(
						'SELECT * FROM neighbor_links',
						$1[i],$1[i+1], false, false) AS r);
    END LOOP;
    RETURN;
    END
$BODY$
    LANGUAGE 'plpgsql'
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;