--Drop exisiting antenna related objects
DROP TABLE IF EXISTS public.trips_patched CASCADE;

CREATE TABLE public.trips_patched
(
  agent_id integer NOT NULL,
  commute_direction integer,
  orig_taz integer,
  dest_taz integer,
  cellpath int[],
  geom geometry(LineString,4326),
  CONSTRAINT trips_patched_pkey PRIMARY KEY (agent_id, commute_direction)
)
WITH (
  OIDS=FALSE
);