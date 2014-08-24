import os, sys, binascii, time
import psycopg2
from dbconf import DSN, FETCHN, BOOTSTRAPN, BOOTLIMIT
from dht import DHT, DHTError

PEERID = os.urandom(20)

class RateLimit:
	def __init__(self, callback_func, flush_time=10):
		self.callback_func = callback_func
		self.flush_time = flush_time
		self.lastflush = time.time()
	
	def do(self, args=()):
		if time.time() > self.lastflush + self.flush_time:
			apply(self.callback_func, args)
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
				cur.execute('SELECT magnet FROM magnets WHERE mid %% %s = %s ORDER BY lastupdated ASC LIMIT %s', (self.numworkers, self.workid, FETCHN))
				self.searchqueue.extend(map(lambda x: x[0], cur))
			self.conn.commit()
		else:
			while self.search(mag) == True and len(self.searchqueue) > 0:
				mag = binascii.a2b_hex(self.searchqueue.pop())
	
	def _bootstrap_do(self):
		n = self.nodes(self.IPV4)
		if n[0] > BOOTLIMIT:
			return
		with self.conn.cursor() as cur:
			cur.execute('SELECT ipaddr, iport FROM addresses OFFSET RANDOM() * (SELECT COUNT(*) FROM addresses) LIMIT %s', (BOOTSTRAPN,))
			for row in cur:
				apply(self.ping, row)
		self.conn.commit()
	
	def _results_process(self, infohash, data):
		if len(data) == 0:
			return
		data_ascii_hash = binascii.b2a_hex(infohash)
		data_lastmod = int(time.time())
		data_denorm = map(lambda x: (data_ascii_hash, data_lastmod) + x, data)
		with self.conn.cursor() as cur:
			cur.executemany('SELECT insert_update_addr(%s, %s, %s, %s)', data_denorm)
		self.conn.commit()
				
	def loop(self):
		self.searchqueue = []
		self.halfresults = {}
		self.tempresults = {}
		self.tempresults[self.EVENT_VALUES] = {}
		self.tempresults[self.EVENT_VALUES6] = {}
		self.tempresults[self.EVENT_SEARCH_DONE] = self.tempresults[self.EVENT_VALUES]   #references to previous dicts
		self.tempresults[self.EVENT_SEARCH_DONE6] = self.tempresults[self.EVENT_VALUES6] #
		stats = RateLimit(self._print_stats)
		search = RateLimit(self._search_do, 2)
		bootstrap = RateLimit(self._bootstrap_do, 60)
		while True:
			self.do()
			stats.do()
			search.do()
			bootstrap.do()
			
	def on_search(self, ev, infohash, data):
		if ev == self.EVENT_NONE:
			return
		tmpres = self.tempresults[ev] #another reference, to automatically handle 4/6
		if len(data) > 0:
			if infohash in self.tempresults:
				tmpres[infohash].update(data)
			else:
				tmpres[infohash] = set(data)
		if ev in [self.EVENT_SEARCH_DONE, self.EVENT_SEARCH_DONE6] and infohash in tmpres:
			res = tmpres.pop(infohash)
			if infohash in self.halfresults:
				self.halfresults[infohash].update(res)
				#alredy in complete, must have completed both v6 and v4
				compresult = self.halfresults.pop(infohash)
				self._results_process(infohash, compresult)
			else:
				self.halfresults[infohash] = res

		print "Nodes", repr(self.nodes(self.IPV4))
		print "Nodes6", repr(self.nodes(self.IPV6))
		print "Event", repr(ev)
		print "Hash", repr(infohash)
		print "Data", repr(data)
	
def main():
	port = int(sys.argv[1])
	Crawler = DHTCrawler(PEERID, port)
	Crawler.conn = psycopg2.connect(DSN)
	Crawler.numworkers = int(sys.argv[2])
	Crawler.workid = int(sys.argv[3])
	try:
		Crawler.loop()
	except KeyboardInterrupt, SystemExit:
		Crawler.conn.close()

if __name__ == "__main__":
	main()
