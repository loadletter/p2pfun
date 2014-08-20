import os, sys, binascii, time
import psycopg2
from dbconf import DSN, PEERID
from dht import DHT, DHTError

DB_EXEC_INIT = """CREATE TABLE IF NOT EXISTS magnets (mid SERIAL UNIQUE, magnet CHAR(64) NOT NULL PRIMARY KEY, lastupdated INTEGER)
CREATE TABLE IF NOT EXISTS addrmap (mid INTEGER REFERENCES magnets(mid) ON DELETE CASCADE, aid INTEGER REFERENCES addresses(aid) ON DELETE CASCADE, firstseen INTEGER, lastseen INTEGER, PRIMARY KEY(mid, aid))
CREATE TABLE IF NOT EXISTS addresses (aid SERIAL UNIQUE, ipaddr INET NOT NULL PRIMARY KEY)"""

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
WITH new_addresses (ipaddr) AS (
  values 
    (%s)
)
INSERT INTO addresses (ipaddr)
SELECT 1
FROM new_addresses
WHERE
    NOT EXISTS (
        SELECT aid FROM addresses WHERE ipaddr = new_addresses
    )
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
		
	def search_do(self):
		try:
			mag = binascii.a2b_hex(self.searchqueue.pop())
		except IndexError:
			#TODO: select some from db order by lastmodified and append them to searchqueue
			#lastmod should be updated just after the select so another instance doesn't pick the same up
			#or use SELECT WHERE mid % nshards = 0
		while self.search(mag) == True and len(self.searchqueue) > 0:
			mag = binascii.a2b_hex(self.searchqueue.pop())
		
			
	def loop(self):
		self.searchqueue = []
		stats = RateLimit(self._print_stats)
		search = RateLimit(self.search, 0.01)
		while True:
			self.do()
			stats.do()
			search.do()
			
	def on_search(self, ev, infohash, data):
		#TODO
		#if ev == self.EVENT_SEARCH_DONE or ev == EVENT_SEARCH_DONE6:
		print "Nodes", repr(self.nodes(self.IPV4))
		print "Nodes6", repr(self.nodes(self.IPV6))
		print "Event", repr(ev)
		print "Hash", repr(infohash)
		print "Data", repr(data)

def insert_magnets(cur):
	maglist = []
	linec = 0
	with open(sys.argv[2]) as f:
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
	with conn.cursor() as initcur:
		initcur.execute(DB_EXEC_INIT)
		if len(sys.argv) >= 3:
			insert_magnets(initcur)
	Crawler.loop()
