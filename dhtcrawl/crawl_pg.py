import os, sys, binascii, time, logging
import psycopg2
from dbconf import DSN, FETCHN, BOOTSTRAPN, BOOTLIMIT, LOGFILE
from dht import DHT, DHTError

#LOGFILE=''
#FETCHN=4000
#BOOTSTRAPN=8
#BOOTLIMIT=40
#LOGLEVEL='INFO'

try:
	from dbconf import PEERID
except ImportError:
	PEERID = os.urandom(20)

try:
	from dbconf import LOGLEVEL
except ImportError:
	LOGLEVEL = logging.DEBUG
else:
	LOGLEVEL = eval('logging.%s' % LOGLEVEL)

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
		logging.info("Nodes: %s", repr(nodes))
		
	def _search_do(self):
		n = self.nodes(self.IPV4)
		if n[0] < BOOTLIMIT/2:
			return
		try:
			mag = self.searchqueue.pop()
		except IndexError:
			with self.conn.cursor() as cur:
				cur.execute('''SELECT encode(magnet, 'hex') FROM magnets WHERE mid %% %s = %s ORDER BY lastupdated ASC LIMIT %s''', (self.numworkers, self.workid, FETCHN))
				self.searchqueue.extend(map(lambda x: x[0], cur))
			self.conn.commit()
		else:
			search_mag = True
			while search_mag and len(self.searchqueue) > 0:
				search_mag = self.search(binascii.a2b_hex(mag))
				if search_mag:
					logging.debug("SCH: %s", mag)
					mag = self.searchqueue.pop()
				else:
					logging.debug("SCH FULL AT %s", mag)
					self.searchqueue.append(mag)
	
	def _bootstrap_do(self):
		n = self.nodes(self.IPV4)
		if n[0] > BOOTLIMIT:
			return
		with self.conn.cursor() as cur:
			cur.execute('SELECT ipaddr, iport FROM addresses OFFSET RANDOM() * (SELECT COUNT(*) FROM addresses) LIMIT %s', (BOOTSTRAPN,))
			for row in cur:
				logging.debug("BOOT: %s", repr(row))
				apply(self.ping, row)
		self.conn.commit()
	
	def _results_process(self, infohash, data):
		data_lastmod = int(time.time())
		if len(data) == 0:
			with self.conn.cursor() as cur:
				cur.execute('UPDATE magnets SET lastupdated = %s WHERE magnet = %s', (data_lastmod, psycopg2.Binary(infohash)))
		else:
			data_denorm = map(lambda x: (psycopg2.Binary(infohash), data_lastmod) + x, data)
			with self.conn.cursor() as cur:
				cur.executemany('SELECT insert_update_addr(%s, %s, %s, %s)', data_denorm)
		self.conn.commit()
				
	def loop(self):
		self.searchqueue = []
		stats = RateLimit(self._print_stats)
		search = RateLimit(self._search_do, 5)
		bootstrap = RateLimit(self._bootstrap_do, 30)
		while True:
			self.do()
			stats.do()
			search.do()
			bootstrap.do()
			
	def on_search(self, ev, infohash, data):
		if ev == self.EVENT_NONE:
			return
		ascii_infohash = binascii.b2a_hex(infohash)
		if ev in [self.EVENT_VALUES, self.EVENT_VALUES6]:
			self._results_process(infohash, data)
			logging.debug("VAL: %s %i", ascii_infohash, len(data))
		if ev in [self.EVENT_SEARCH_DONE, self.EVENT_SEARCH_DONE6]:
			self._results_process(infohash, data)
			logging.debug("DONE: %s", ascii_infohash)
					
def main():
	if len(sys.argv) != 4:
		logging.error("usage: %s listenport numworkers workid", sys.argv[0])
		return
	port = int(sys.argv[1])
	Crawler = DHTCrawler(PEERID, port)
	Crawler.conn = psycopg2.connect(DSN)
	Crawler.numworkers = int(sys.argv[2])
	Crawler.workid = int(sys.argv[3])
	if Crawler.workid < 0 or Crawler.workid >= Crawler.numworkers:
		logging.error("workid spans from 0 to numworkers - 1")
		return
	try:
		Crawler.loop()
	except KeyboardInterrupt, SystemExit:
		with Crawler.conn.cursor() as cur:
			n, n6 = Crawler.get_nodes()
			cur.executemany('SELECT insert_new_address(%s, %s)', n + n6)
			logging.info("SAVED: %i (%i/%i)", len(n) + len(n6), len(n), len(n6))
		Crawler.conn.commit()
		Crawler.conn.close()

if __name__ == "__main__":
	if LOGFILE == '':
		logging.basicConfig(level=LOGLEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
	else:
		logging.basicConfig(filename=LOGFILE, level=LOGLEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
	main()
