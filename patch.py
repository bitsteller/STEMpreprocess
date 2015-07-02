import time, signal, json, random, sys
from multiprocessing import Pool
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def patch(agent_id):
	"""Patches the given trip by adding intermediate cells to the cellpath. Processing is done directly in the database.
	Args:
		tripid: the id of the trip to patch"""
	global conn, cur, commute_direction

	#fetch trip
	cur.execute("SELECT agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath FROM trips WHERE agent_id = %s AND commute_direction = %s", (agent_id,commute_direction))
	try:
		agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath = cur.fetchone()
	except Exception, e:
		return #no trip found, skip

	#fetch centroids
	if len(cellpath) >= 2: #patching needs at least 2 cells in cellpath
		cur.execute("SELECT id, ST_X(ST_Centroid(voronoi.geom)) AS lon, ST_Y(ST_Centroid(voronoi.geom)) AS lat FROM voronoi WHERE id = ANY(%s)", (cellpath,))
		centroids = {}

		for cellid, lon, lat in cur:
			centroids[cellid] = (lat, lon)

	 	route = calculate_route([centroids[cellid] for cellid in cellpath])
	 	if route != None:
	 		sql = "	INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) \
					WITH route AS (SELECT ST_SetSRID(ST_MakeLine(ST_GeomFromText(%(linestr)s)),4326) AS geom) \
					SELECT %(agent_id)s, %(commute_direction)s, %(orig_TAZ)s, %(dest_TAZ)s, \
							SELECT array_agg(id) FROM voronoi WHERE ST_Intersects(voronoi.geom, route.geom);"
			cur.execute(sql, {"agent_id": agent_id, "commute_direction": commute_direction, "orig_TAZ": orig_TAZ, "dest_TAZ": dest_TAZ, "cellpath": cellpath, "linestr": util.to_pglinestring(route)})
		else: #keep unpatched path
			print("WARNING: " + str(agent_id) + "," + str(commute_direction) + " could not be patched: no route found")
			sql = "INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) SELECT %s, %s, %s, %s, %s"
			cur.execute(sql, (agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath))	
	else: #keep unpatched path
		print("WARNING: " + str(agent_id) + "," + str(commute_direction) + " could not be patched: too few cells")
		sql = "INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) SELECT %s, %s, %s, %s, %s"
		cur.execute(sql, (agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath))

	cur.commit()

def calculate_route(loclist):
	"""Calculates the cost from x via y to z; OSRM backend needs to listen at port 5000
	Args:
		loclist: list of (lat, lon) pairs to visit (at least 2)
	Returns:
		A lists of (lat, lon) tuples describing the geometry of the routes"""

	parameters = "&".join([str(lat) + "," + str(lon) for lat, lon in loclist])
	data = json.load(urllib2.urlopen('http://www.server.com:5000/viaroute?' + parameters))
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
commute_direction = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating trips_patched table...")
	mcur.execute(open("SQL/create_trips_patched.sql", 'r').read())
	mconn.commit()

	print("Patching trajectories (commute_direction=0)...")
	mapper = util.ParMap(patch, initializer = init)
	commute_direction = 0
	mapper(config.AGENTS)


	print("Patching trajectories (commute_direction=1)...")
	mapper = util.ParMap(patch, initializer = init)
	commute_direction = 1
	mapper(config.AGENTS)

