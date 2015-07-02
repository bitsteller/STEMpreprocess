import time, signal, json, random, sys
from multiprocessing import Pool
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def patch(tripid):
	"""Patches the given trip by adding intermediate cells to the cellpath. Processing is done directly in the database.
	Args:
		tripid: the id of the trip to patch"""
	global conn, cur

	#fetch 


 	a,b,c = segment
	y = None #when no start or endpoint, no border points or no routes were found, null value will be added to the database

	#find start and end nodes
	sql = "	SELECT xid, ST_Y(x.geom) AS xlat, ST_X(x.geom) AS xlon, zid, ST_Y(z.geom) AS zlat, ST_X(z.geom) AS zlon\
			FROM closest_junction(%s, %s) AS xid, closest_junction(%s, %s) AS zid, hh_2po_4pgr_vertices AS x, hh_2po_4pgr_vertices AS z\
			WHERE x.id = xid AND z.id = zid"
	cur.execute(sql, (c,a,a,c))

	if cur.rowcount > 0: #start and end point found
		x, xlat, xlon, z, zlat, zlon = cur.fetchone()

		#fetch waypoint candidates
		sql = "	SELECT junction_id AS yid, ST_Y(y.geom) AS ylat, ST_X(y.geom) AS ylon\
				FROM boundary_junctions, hh_2po_4pgr_vertices AS y\
				WHERE antenna_id = %s AND y.id = boundary_junctions.junction_id"
		cur.execute(sql, (b,))
		y_candidates = cur.fetchall()

		#calculate route cost for all waypoints
		costs = []
		for y, ylat, ylon in y_candidates:
			costs.append(route_cost(xlat, xlon, ylat, ylon, zlat, zlon))
			#To see route, export gpx file:	print("\n".join(urllib2.urlopen('http://www.server.com:5000/viaroute?output=gpx&loc=' + str(xlat) + ',' + str(xlon) + '&loc=' + str(ylat) + ',' + str(ylon) + '&loc=' + str(zlat) + ',' + str(zlon)).readlines()))

		#select cheapest waypoint
		if len(costs) > 0 and min(costs) < float("inf"): #at least one feasible route found
			y = y_candidates[costs.index(min(costs))][0]

	sql = "	INSERT INTO test_routes (start_point, end_point, geom) \
					WITH route AS (SELECT ST_SetSRID(ST_MakeLine(ST_GeomFromText(%(linestr)s)),4326) AS geom) \
					SELECT ST_StartPoint(route.geom), ST_EndPoint(route.geom), route.geom FROM route;"
			cur.execute(sql, {"linestr": util.to_pglinestring(route)})
			conn.commit()

def calculate_route(xlat, xlon, ylat, ylon):
	"""Calculates the cost from x via y to z; OSRM backend needs to listen at port 5000
	Args:
		xlat: latitude of the start point
		xlon: longitude of the start point
		ylat: latitude of the via point
		ylon: longitude of the via point
	Returns:
		A lists of (lat, lon) tuples describing the geometry of the routes"""

	data = json.load(urllib2.urlopen('http://www.server.com:5000/viaroute?loc=' + str(xlat) + ',' + str(xlon) + '&loc=' + str(ylat) + ',' + str(ylon)))
	results = []
	if "route_geometry" in data:
		return [(lat/10.0, lon/10.0) for lat, lon in PolylineCodec().decode(data["route_geometry"])]
	return None

def signal_handler(signal, frame):
	global mapper, request_stop
	request_stop = True
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")
	sys.exit(1)

mapper = None
request_stop = False
cur = None
conn = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	mconn = util.db_connect()
	mcur = mconn.cursor()

	extract_segments()

	print("Creating trips_patched table...")
	#mcur.execute(open("SQL/04_Routing_Network_Loading/create_waypoints.sql", 'r').read())
	mconn.commit()

	print("Patching trajectories...")
	mapper = util.ParMap(patch, initializer = init)
	mapper(config.TRIPS)

