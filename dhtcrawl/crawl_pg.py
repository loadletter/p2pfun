import os, sys, binascii, time
import psycopg2
from dbconf import DSN, PEERID, FETCHN, UPDATEN
from dht import DHT, DHTError

DB_EXEC_INIT = """CREATE TABLE IF NOT EXISTS magnets (mid SERIAL UNIQUE, magnet CHAR(64) NOT NULL PRIMARY KEY, lastupdated INTEGER)
CREATE TABLE IF NOT EXISTS addrmap (mid INTEGER REFERENCES magnets(mid) ON DELETE CASCADE, aid INTEGER REFERENCES addresses(aid) ON DELETE CASCADE, firstseen INTEGER, lastseen INTEGER, PRIMARY KEY(mid, aid))
CREATE TABLE IF NOT EXISTS addresses (aid SERIAL UNIQUE, ipaddr INET NOT NULL, iport INTEGER NOT NULL, PRIMARY KEY(ipaddr, iport))"""

DB_EXEC_UPSERT_MAGNETS = """LOCK TABLE magnets IN SHARE ROW EXCLUSIVE MODE
WITH new_magnets (magnet, lastupdated) AS (
  values 
    (%s, %s)

),
upsert AS
( 
    update magnets m
        SET lastupdated = nm.lastupdated
    FROM new_magnets nm
    WHERE m.magnet = nm.magnet
    RETURNING m.*
)
INSERT INTO magnets (magnet, lastupdated)
SELECT magnet, lastupdated
FROM new_magnets
WHERE NOT EXISTS (SELECT 1 
                  FROM upsert up
                  WHERE up.magnet = new_magnets.magnet)
"""

DB_EXEC_UPSERT_ADDRMAP = """LOCK TABLE addrmap IN SHARE ROW EXCLUSIVE MODE
WITH new_addrmap (mid, aid, lastseen) AS (
  values 
    (%s, %s, %s, %s)

),
upsert AS
( 
    update addrmap a
        SET lastseen = na.lastseen
    FROM new_addrmap na
    WHERE a.mid = na.mid AND a.aid = na.aid
    RETURNING a.*
)
INSERT INTO addrmap (mid, aid, firstseen, lastseen)
SELECT mid, aid, lastseen, lastseen
FROM new_addrmap
WHERE NOT EXISTS (SELECT 1 
                  FROM upsert up
                  WHERE up.mid = new_addrmap.mid AND up.aid = new_addrmap.aid)
"""

DB_EXEC_INSERT_ADDRESSES = """
WITH new_addresses (ipaddr, iport) AS (
  values
    (%s, %s)
)
INSERT INTO addresses (ipaddr, iport)
SELECT 1
FROM new_addresses
WHERE NOT EXISTS (
        SELECT aid FROM addresses WHERE ipaddr = new_addresses.ipaddr AND iport = new_addresses.iport)
"""

class RateLimit:
	def __init__(self, callback_func, flush_time=10):
		self.callback_func = callback_func
		self.flush_time = flush_time
		self.lastflush = time.time()
	
	def do(self, **kwargs):
		if time.time() > self.lastflush + self.flush_time:
			apply(self.callback_func, kwargs)
			self.lastflush = time.time()

class DHTCrawler(DHT):
	def _print_stats(self):
		nodes = self.nodes(self.IPV4)
		print "Nodes:", repr(nodes)
		
	def _search_do(self):
		try:
			mag = binascii.a2b_hex(self.searchqueue.pop())
		except IndexError:
			with self.conn.cursor() as cur:
				cur.execute('SELECT magnet FROM magnets WHERE mid % %s = %s ORDER BY lastupdated ASC LIMIT %s', (self.numworkers, self.workid, FETCHN))
				self.searchqueue.extend(map(lambda x: x[0], cur))
		while self.search(mag) == True and len(self.searchqueue) > 0:
			mag = binascii.a2b_hex(self.searchqueue.pop())
	
	def _result_do(self):
		if len(self.resultqueue) > UPDATEN:
			#todo: write to db
	
	def loop(self):
		self.searchqueue = []
		self.resultqueue = []
		self.tempresults = {}
		self.tempresults[self.DHT_EVENT_VALUES] = {}
		self.tempresults[self.DHT_EVENT_VALUES6] = {}
		stats = RateLimit(self._print_stats)
		search = RateLimit(self._search_do, 2)
		result = RateLimit(self._result_do, 2)
		while True:
			self.do()
			stats.do()
			search.do()
			result.do()
			
	def on_search(self, ev, infohash, data):
		#TODO: append data to a list in tempresults, when event is search done move to resultqueue as a tuple ready to crunched by postgres
		if ev in self.tempresults and len(data) > 0:
			tmpres = self.tempresults[ev]
			if infohash in self.tempresults:
				tmpres[infohash].update(data)
			else:
				tmpres[infohash] = set(data)
		#TODO
		if ev == self.EVENT_SEARCH_DONE and infohash in tmpres:
			res = self.tempresults[self.DHT_EVENT_VALUES].pop(infohash)
			self.resultqueue.append(res)
		if ev == self.EVENT_SEARCH_DONE6 and infohash in self.tempresults:
			res = self.tempresults.pop(infohash)
			self.resultqueue.append(res)
		print "Nodes", repr(self.nodes(self.IPV4))
		print "Nodes6", repr(self.nodes(self.IPV6))
		print "Event", repr(ev)
		print "Hash", repr(infohash)
		print "Data", repr(data)

def insert_magnets(cur, fpath):
	maglist = []
	linec = 0
	with open(fpath) as f:
		for line in f:
			linec += 1
			l = line.strip()
			if len(l) == 64:
				maglist.append((l, 0))
	print len(maglist), "/", linec
	cur.executemany(DB_EXEC_UPSERT_MAGNETS, maglist)
	
def main():
	port = int(sys.argv[1])
	Crawler = DHTCrawler(PEERID, port)
	Crawler.conn = psycopg2.connect(DSN)
	Crawler.numworkers = int(sys.argv[2])
	Crawler.workid = int(sys.argv[3])
	with conn.cursor() as initcur:
		initcur.execute(DB_EXEC_INIT)
		if len(sys.argv) >= 5:
			insert_magnets(initcur, sys.argv[4])
	Crawler.loop()
