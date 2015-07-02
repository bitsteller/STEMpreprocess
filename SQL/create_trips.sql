--Drop exisiting antenna related objects
DROP TABLE IF EXISTS public.trips CASCADE;

CREATE TABLE public.trips
(
  agent_id integer NOT NULL,
  commute_direction integer,
  orig_taz integer,
  dest_taz integer,
  cellpath int[],
  CONSTRAINT trips_pkey PRIMARY KEY (agent_id, commute_direction)
)
WITH (
  OIDS=FALSE
);

COMMENT ON TABLE public.trips IS 
'Contains the trips with their respective cellpath used for route estimation';

COMMENT ON COLUMN public.trips.agent_id IS 
'a unique identifier of the agent making the trip';

COMMENT ON COLUMN public.trips.cellpath IS 
'an array of cell ids visited along the trip, in their order of occurance during the travel';