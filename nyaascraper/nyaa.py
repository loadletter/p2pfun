import sys, json, re, shutil, os
import requests
from tpool import ThreadPool

HTTP_PROXY = ""
NYAA_URL = "http://www.nyaa.se"
SKIP_EXISTING = True
NUM_THREADS = 10
NUM_CHUNK = 200

def i2hex(tid):
	m = tid % 256
	h = hex(m).replace('0x', '')
	if len(h) == 1:
		h = '0' + h
	return h
	
FILENAME_REGEX=re.compile('^inline; filename="(.+)"$')

class TorrentStore:
	def __init__(self, destdir):
		self.destdir = destdir
		self.tmpdir = os.path.join(self.destdir, '.tmp')
		shutil.rmtree(self.tmpdir, ignore_errors=True)
		os.mkdir(self.tmpdir)
	
	def exists(self, tid):
		path = os.path.join(self.destdir, i2hex(tid))
		if not os.path.isdir(path):
			return False
		_, _, bucketfiles = os.walk(path).next()
		return tid in map(lambda x: int(x.split('-', 1)[0]), bucketfiles)
	
	def add(self, tid, filename, data):
		filepath = os.path.join(self.tmpdir, '%i-%s' % (tid, filename))
		with open(filepath, 'wb') as f:
			f.write(data)
	
	def commit(self):
		_, _, bucketfiles = os.walk(self.tmpdir).next()
		splitnames = map(lambda x: x.split('-', 1), bucketfiles)
		splitnames.sort(key=lambda x: int(x[0]))
		for i, n in splitnames:
			fname = '%s-%s' % (i, n)
			s = os.path.join(self.tmpdir, fname)
			subd = os.path.join(self.destdir, i2hex(int(i)))
			try:
				os.mkdir(subd)
			except OSError:
				pass
			d = os.path.join(subd, fname)
			os.rename(s, d)

class NyaaScraper:
	def __init__(self, destdir, startid, endid):
		self.startid = startid
		self.endid = endid
		self.store = TorrentStore(destdir)
		proxies = {}
		if HTTP_PROXY:
			proxies['http'] = HTTP_PROXY
		self.session = requests.Session(proxies=proxies)
		
	def crawl(self):
		i = self.startid
		pool = ThreadPool(NUM_THREADS)
		while i <= self.endid:
			print "queue: %i ==> %i" % (i, i + NUM_CHUNK)
			for j in xrange(i, i + NUM_CHUNK):
				if j > self.endid:
					break
				pool.add_task(self.downloader, j)
				i += 1
			pool.wait_completion()
			self.store.commit()
		self.store.commit()
	
	def downloader(self, tid):
		if SKIP_EXISTING and self.store.exists(tid):
			return
		try:
			req = self.session.get("%s/?page=download&tid=%i" % (NYAA_URL, tid), timeout=120, allow_redirects=False)
			if req.status_code == 200:
				filename = re.findall(FILENAME_REGEX, req.headers['content-disposition'])[0]
				self.store.add(tid, filename, req.content)
				print "found:", tid
			elif req.status_code == 404 or req.status_code in [302, 303]:
				print "torrent/%i:NotFound" % tid
			else:
				print "torrent/%i: %i %s" % (tid, req.status_code, req.error)
		except Exception, e:
			print "torrent/%i:Exception %s" % (tid, repr(e))

if __name__ == "__main__":
	if len(sys.argv) == 4 and os.path.isdir(sys.argv[1]):
		s = NyaaScraper(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
		s.crawl()
	else:
		print "usage: ", sys.argv[0], "destinationdir startid endid"
