import time, signal, json, random, itertools, math, sys
import psycopg2 #for postgres DB access
import numpy as np
import scipy
import matplotlib.pyplot as plt

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def read_trip(args):
	"""Parses a line of a trip csv
	Args:
		args: a tuple (i, line), where i is the line number and line the a one-line string from the trip csv
	Returns:
		A list [(user_id, cellpath))] where cellpath is a ordered list of cell ids visited on the trip"""

	i, line = args

	t = util.parse_trip(line)
	if not t == None:
		agentid, commute_direction, orig_TAZ, dest_TAZ, cellpath = t 
		return [((agentid, commute_direction), (orig_TAZ, dest_TAZ, cellpath))]
	else:
		return []

def upload_trip(args):
	"""Uploads an antenna position to the database.
	Args:
		args: a tuple ((agentid, commute_direction), (orig_TAZ, dest_TAZ, cellpath))
	"""
	global conn, cur

	key, value = args
	agentid, commute_direction = key
	orig_TAZ, dest_TAZ, cellpath = value[0]
	assert len(value) == 1

	sql = "	INSERT INTO trips (agent_id, commute_direction, orig_TAZ, dest_TAZ, cellpath) \
			VALUES (%s, %s, %s, %s, %s);"
	cur.execute(sql, (agentid, commute_direction, orig_TAZ, dest_TAZ, cellpath))
	conn.commit()

	return None

def signal_handler(signal, frame):
	global mapper, request_stop
	if mapper:
		mapper.stop()
	request_stop = True
	print("Aborting (can take a minute)...")
	sys.exit(1)

request_stop = False
mapper = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	#connect to db
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating trips table...")
	mcur.execute(open("SQL/create_trips.sql", 'r').read())
	mconn.commit()

	#Read trips from file
	#Count lines for status indicator
	linecount = 0
	for line in open(config.TRIPS_FILE).xreadlines(): 
		linecount += 1

	#parse trips
	print("Loading trips...")
	mapper = util.MapReduce(read_trip, upload_trip, num_workers = 4, initializer = init)
	trips = mapper(enumerate(open(config.TRIPS_FILE, 'r').xreadlines()), length = linecount, pipe = True, chunksize = 1000, out = False)
	