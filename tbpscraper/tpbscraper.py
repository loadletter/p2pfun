import sys, json
import requests
from BeautifulSoup import BeautifulSoup
from sqlitedict import SqliteMultithread
from tpool import ThreadPool

HTTP_PROXY = ""
TPB_URL = "http://uj3wazyk5u4hnvtk.onion"
SKIP_EXISTING = True
NUM_THREADS = 20
NUM_CHUNK = 200

def find_tag(tagname, alt, *dls):
	for d in dls:
		for i, r in enumerate(d.findAll('dt')):
			if tagname in r.text:
				if type(alt) == str:
					return d.findAll('dd')[i].text
				else:
					return d.findAll('dd')[i]
	return alt

def page_parse(data):
	soup = BeautifulSoup(data)
	dl1 = soup.findAll('dl', {'class' : 'col1'})[0]
	dl2 = soup.findAll('dl', {'class' : 'col2'})[0]
	size = find_tag("Size:", '', dl1, dl2).replace('&nbsp;',' ')
	category = soup.findAll('a', {'title' : 'More from this category'})[0].text.replace(' &gt; ', '>')
	tags = []
	rawtags = find_tag("Tag(s):", None, dl1, dl2)
	if rawtags:
		tags = map(lambda x: x.text, rawtags.findAll('a'))
	title = soup.find(id='title').text
	dateup = find_tag("Uploaded:", '', dl1, dl2)
	byuser = find_tag("By:", None, dl1, dl2)
	user = ''
	if byuser.findAll('a'):
		user = byuser.findAll('a')[0].text
	elif byuser.findAll('i'):
		user = byuser.findAll('i')[0].text
	seeders = find_tag("Seeders:", '0', dl1, dl2)
	leechers = find_tag("Leechers:", '0', dl1, dl2)
	magnet = soup.findAll('a', {'title' : 'Get this torrent'})[0]['href'].split('&')[0]
	comment = soup.findAll('div', {'class' : 'nfo'})[0].text
	return (title, user, dateup, int(seeders), int(leechers), comment, magnet, category, json.dumps(tags))
	
class TPBScraper:
	def __init__(self, dbpath, startid, endid):
		self.database = SqliteMultithread(dbpath, autocommit=False, journal_mode="DELETE")
		self.database.execute('CREATE TABLE IF NOT EXISTS tpb (tid INTEGER PRIMARY KEY, title TEXT, user TEXT, date TEXT, seeders INTEGER, leechers INTEGER, comment TEXT, magnet TEXT, category TEXT, tags TEXT)')
		self.database.commit()
		self.startid = startid
		self.endid = endid
		proxies = {}
		if HTTP_PROXY:
			proxies['http'] = HTTP_PROXY
		self.session = requests.Session(proxies=proxies)
	
	def exists(self, tid):
		return self.database.select_one('SELECT 1 FROM tpb WHERE tid = ?', (tid,)) is not None
		
	def crawl(self):
		i = self.startid
		while i <= self.endid:
			pool = ThreadPool(NUM_THREADS)
			print "queue: %i ==> %i" % (i, i + NUM_CHUNK)
			for j in xrange(i, i + NUM_CHUNK):
				if j > self.endid:
					break
				pool.add_task(self.downloader, j)
				i += 1
			pool.wait_completion()
			self.database.commit()
		self.database.commit()
		self.database.close()
			
	def downloader(self, tid):
		if SKIP_EXISTING and self.exists(tid):
			return
		try:
			req = self.session.get("%s/torrent/%i" % (TPB_URL, tid), timeout=120, headers={"accept-language": "en"})
			if req.status_code == 200:
				print "found:", tid
				self.database.execute('INSERT OR REPLACE INTO tpb (tid, title, user, date, seeders, leechers, comment, magnet, category, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (tid,) + page_parse(req.text))
			elif req.status_code == 404:
				print "torrent/%i:NotFound" % tid
			else:
				print "torrent/%i: %i %s" % (tid, req.status_code, req.error)
		except Exception, e:
			print "torrent/%i:Exception %s" % repr(e)

if __name__ == "__main__":
	if len(sys.argv) == 4:
		s = TPBScraper(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
		s.crawl()
	else:
		print "usage: ", sys.argv[0], "database startid endid"
