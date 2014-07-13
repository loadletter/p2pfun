import sys, json
import requests
from BeautifulSoup import BeautifulSoup
from sqlitedict import SqliteMultithread

HTTP_PROXY = ""
TPB_URL = "http://uj3wazyk5u4hnvtk.onion"
SKIP_EXISTING = True
NUM_THREADS = 20
NUM_CHUNK = 200


def page_parse(data):
	soup = BeautifulSoup(data)
	dl1 = soup.findAll('dl', {'class' : 'col1'})[0]
	size = dl1.findAll('dd')[2].text.replace('&nbsp;',' ')
	category = dl1.findAll('a', {'title' : 'More from this category'})[0].text.replace(' &gt; ', '>')
	tags = []
	if len(dl1.findAll('dd')) > 3:
		tags = map(lambda x: x.text, dl1.findAll('dd')[3].findAll('a'))
	title = soup.find(id='title').text
	dl2 = soup.findAll('dl', {'class' : 'col2'})[0]
	dateup = dl2.findAll('dd')[0].text
	byuser = dl2.findAll('dd')[1].text
	seeders = dl2.findAll('dd')[2].text
	leechers = dl2.findAll('dd')[3].text
	magnet = soup.findAll('a', {'title' : 'Get this torrent'})[0]['href'].split('&')[0]
	comment = soup.findAll('div', {'class' : 'nfo'})[0].text
	return (title, byuser, dateup, seeders, leechers, comment, magnet, category, json.dumps(tags))
	
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
		
	def crawl(self):
		i = self.startid
		while i <= self.endid:
			pool = ThreadPool(NUM_THREADS)
			print "queue: %i ==> %i" % (i, i + NUM_CHUNK)
			for j in xrange(i, i + NUM_CHUNK):
				if j > self.endid:
					break
				
				#dlthreads = []
				#for n in range(0, DOWN_THREADS):
					#DlThread = Thread(target=worker, args=(namelist, resultlist))
					#DlThread.daemon=True
					#DlThread.start()
					#dlthreads.append(DlThread)
				#for dlthread in dlthreads:
					#dlthread.join()

				#or
				
				#start thread
				#while dlthreads > NUM_TREADS: sleep 1


				i += 1
			pool.close()
			pool.join()
			self.database.commit()
		self.database.commit()
		self.database.close()
			
	def downloader(self, tid):
		if SKIP_EXISTING and str(tid) in self.database:
			return
		req = self.session.get("%s/torrent/%i" % (TPB_URL, tid), timeout=120, headers={"accept-language": "en"})
		isfound = not "<title>Not Found | The Pirate Bay" in req.text
		if req.status_code == 200 and isfound:
			print "found:", tid
			self.database.execute('INSERT OR REPLACE INTO tpb (tid, title, user, date, seeders, leechers, comment, magnet, category, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (tid,) + page_parse(req.text))
		elif req.status_code == 404 or not isfound:
			print "torrent/%i: Not found" % tid
		else:
			print "torrent/%i: %i %s" % (tid, req.status_code, req.error)

if __name__ == "__main__":
	if len(sys.argv) == 4:
		s = TPBScraper(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
		s.crawl()
	else:
		print "usage: ", sys.argv[0], "database startid endid"
