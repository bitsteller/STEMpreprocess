import time, signal, json, random, sys
from multiprocessing import Pool
import urllib2 #for OSRM queries
import psycopg2 #for postgres DB access
from polyline.codec import PolylineCodec #to decode geometries from OSRM

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def patch(agent_id, snap = False):
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
			if snap:
				centroids[cellid] = locate((lat, lon)) #snap to road network
			else:
				centroids[cellid] = (lat, lon)

	 	route = calculate_route([centroids[cellid] for cellid in cellpath])
	 	if route != None:
	 		sql = "	INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath, geom) \
					WITH route AS (SELECT ST_SetSRID(ST_MakeLine(ST_GeomFromText(%(linestr)s)),4326) AS geom) \
					SELECT %(agent_id)s, %(commute_direction)s, %(orig_TAZ)s, %(dest_TAZ)s, \
							(SELECT array_agg(id) FROM voronoi WHERE ST_Intersects(voronoi.geom, route.geom)) AS cellpath, geom \
					FROM route;"
			cur.execute(sql, {"agent_id": agent_id, "commute_direction": commute_direction, "orig_TAZ": orig_TAZ, "dest_TAZ": dest_TAZ, "linestr": util.to_pglinestring(route)})
		else: #keep unpatched path
			pass
			#if not snap: #try with snapping
			#	patch(agent_id, snap = True)
			#	return
			#else: #give up
			#	print("WARNING: " + str(agent_id) + "," + str(commute_direction) + " could not be patched: no route found, unpatched path is kept")
			#	sql = "INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) SELECT %s, %s, %s, %s, %s"
			#	cur.execute(sql, (agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath))	
	else: #keep unpatched path
		pass
	#	print("WARNING: " + str(agent_id) + "," + str(commute_direction) + " could not be patched: too few cells, skipping")
	#	sql = "INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) SELECT %s, %s, %s, %s, %s"
	#	cur.execute(sql, (agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath))

	conn.commit()

def calculate_route(loclist, attempts = 3):
	"""Calculates the cost from x via y to z; OSRM backend needs to listen at port 5000
	Args:
		loclist: list of (lat, lon) pairs to visit (at least 2)
	Returns:
		A lists of (lat, lon) tuples describing the geometry of the routes"""

	parameters = "&".join(["loc=" + str(lat) + "," + str(lon) for lat, lon in loclist])
	data = None
	try:
		data = json.load(urllib2.urlopen('http://www.server.com:5000/viaroute?' + parameters))
	except Exception, e:
		print("WARNING: " + e.message)
		if attempts > 0:
			time.sleep(5)
			return calculate_route(loclist, attempts = attempts - 1)
		else:
			raise e

	if "route_geometry" in data:
		return [(lat/10.0, lon/10.0) for lat, lon in PolylineCodec().decode(data["route_geometry"])]
	return None

def locate(location, attempts = 10):
	"""returns coordinate snapped to nearest node; OSRM backend needs to listen at port 5000
	Args:
		location: a tuple (lat, lon) to snap to the map
	Returns:
		A snapped coordinate (lat, lon) tuple describing closest position on the road network"""

	lat, lon = location
	parameters = "loc=" + str(lat) + "," + str(lon)
	data = None
	try:
		data = json.load(urllib2.urlopen('http://www.server.com:5000/locate?' + parameters))
	except Exception, e:
		print("WARNING: " + e.message)
		if attempts > 0:
			time.sleep(10*60/attemps)
			return locate(location, attempts = attempts - 1)
		else:
			raise e

	if "mapped_coordinate" in data:
		return tuple(data["mapped_coordinate"])
	else: #no node found, return none
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

	for d in [0]:
		commute_direction = d
		print("Patching trajectories (commute_direction=" + str(commute_direction) + ")...")
		sql = "SELECT COUNT(*) FROM trips WHERE trips.commute_direction = %s AND NOT EXISTS(SELECT * FROM trips_patched WHERE trips.agent_id = trips_patched.agent_id AND commute_direction = %s)"
		mcur.execute(sql, (commute_direction,commute_direction))
		count = mcur.fetchone()[0]

		sql = "SELECT DISTINCT agent_id FROM trips WHERE trips.commute_direction = %s AND NOT EXISTS(SELECT * FROM trips_patched WHERE trips.agent_id = trips_patched.agent_id AND commute_direction = %s)"
		mcur.execute(sql, (commute_direction,commute_direction))

		agents = (agent_id for (agent_id,) in mcur)
		
		mapper = util.ParMap(patch, initializer = init)
		mapper(agents, chunksize = 100, length = count)

