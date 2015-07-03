import time
import psycopg2 #for postgres DB access

import util, config #local modules

if __name__ == '__main__':
	#connect to db
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Exporting...")
	sql = "SELECT COUNT(*) FROM trips_patched WHERE commute_direction = 0"
	mcur.execute(sql)
	count = mcur.fetchone()[0]

	sql = "SELECT agent_id, commute_direction, orig_taz, dest_taz, cellpath FROM trips_patched WHERE commute_direction = 0"
	mcur.execute(sql)
	start = time.time()
	f = open(config.TRIPS_PATCHED_FILE, 'a')
	f.write("agent_id,commute_direction,orig_TAZ,dest_TAZ,cells_ID_string")
	for i, (agent_id, commute_direction, orig_taz, dest_taz, cellpath) in enumerate(mcur):
		if i % 10000 == 1:
			est = datetime.datetime.now() + datetime.timedelta(seconds = (time.time()-start)/i*(count-i))
			sys.stderr.write('\rdone {0:%}'.format(float(i)/count) + "  ETA " + est.strftime("%Y-%m-%d %H:%M"))
		
		f.write("{},{},{:.1f},{:.1f},{}".format(agent_id, commute_direction, orig_TAZ, dest_TAZ, " ".join(cellpath)))

	f.close()
	mconn.close()