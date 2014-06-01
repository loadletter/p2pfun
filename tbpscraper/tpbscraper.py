import sys
from multiprocessing.pool import ThreadPool
import requests
from BeautifulSoup import BeautifulSoup
from sqlitedict import SqliteDict

HTTP_PROXY = ""
TPB_URL = "http://uj3wazyk5u4hnvtk.onion"
NUM_THREADS = 20
NUM_CHUNK = 200


def page_parse(data):
	soup = BeautifulSoup(data)
	dl1 = soup.findAll('dl', {'class' : 'col1'})[0]
	size = dl1.findAll('dd')[2].text.replace('&nbsp;',' ')
	title = soup.find(id='title').text
	dl2 = soup.findAll('dl', {'class' : 'col2'})[0]
	dateup = dl2.findAll('dd')[0].text
	byuser = dl2.findAll('dd')[1].text
	seeders = dl2.findAll('dd')[2].text
	leechers = dl2.findAll('dd')[3].text
	magnet = soup.findAll('a', {'title' : 'Get this torrent'})[0]['href']
	comment = soup.findAll('div', {'class' : 'nfo'})[0].text
	return {'title': title, 'user' : byuser, 'date' : dateup, 'seeders' : seeders, 'leechers' : leechers, 'comment' : comment, 'magnet' : magnet}
	
class TPBScraper:
	def __init__(self, dbpath, startid, endid):
		self.database = SqliteDict(dbpath, tablename="tpb", autocommit=False)
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
				pool.apply_async(self.downloader, (j,))
				i += 1
			pool.close()
			pool.join()
			self.database.commit()
		self.database.commit()
		self.database.close()
			
	def downloader(self, tid):
		req = self.session.get("%s/torrent/%i" % (TPB_URL, tid), timeout=120)
		isfound = not "<title>Not Found | The Pirate Bay" in req.text
		if req.status_code == 200 and isfound:
			print "found:", tid
			self.database[str(tid)] = page_parse(req.text)
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
