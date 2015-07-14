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

  	cur.execute("INSERT INTO trips_patched(agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) \
  				 SELECT %(agent_id)s, %(commute_direction)s, %(orig_TAZ)s, %(dest_TAZ)s, \
  				 		(SELECT array_agg(cellid) FROM (SELECT DISTINCT cellid FROM route_neighbors(%(cellpath)s) AS cellid) AS cells)",
  				{"agent_id": agent_id, "commute_direction": commute_direction, "orig_TAZ": orig_TAZ, "dest_TAZ": dest_TAZ, "cellpath": cellpath})
	conn.commit()

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

	print("Creating neighbor cell view...")
	mcur.execute(open("SQL/create_neighbor_cells.sql", 'r').read())
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

