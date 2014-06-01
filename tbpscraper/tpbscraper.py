import sys
from multiprocessing.pool import ThreadPool
import requests
from sqlitedict import SqliteDict

HTTP_PROXY = ""
TPB_URL = "http://uj3wazyk5u4hnvtk.onion"
NUM_THREADS = 20
NUM_CHUNK = 200



def crawl(startid, endid, d):
	proxies = {}
	if HTTP_PROXY:
		proxies['http'] = HTTP_PROXY
	sess = requests.Session(proxies=proxies)
	i = startid
	while i <= endid:
		pool = ThreadPool(NUM_THREADS)
		print "queue: %i ==> %i" % (i, i + NUM_CHUNK)
		for j in xrange(i, i + NUM_CHUNK):
			if j > endid:
				break
			pool.apply_async(downloader, (sess, d, j))
			i += 1
		pool.close()
		pool.join()
		d.commit()
	d.close()	
		
def downloader(session, db, tid):
	req = session.get("%s/torrent/%i" % (TPB_URL, tid), timeout=120)
	if req.status_code == 200 and not "<title>Not Found | The Pirate Bay" in req.text:
		db[str(tid)] = page_parse(req.text)
	elif req.status_code == 200:
		print "torrent/%i: Not found" % tid
	else:
		print "torrent/%i: %i %s" % (tid, req.status_code, req.error)
	
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
	return {'title': title, 'user' : byuser, 'date' : dateup, 'seeders' : seeders, 'leechers' : leechers, 'magnet' : magnet}

if __name__ == "__main__":
	if len(sys.argv) == 4:
		database = SqliteDict(sys.argv[1], tablename="tpb", autocommit=False)
		crawl(int(sys.argv[2]), int(sys.argv[3]), database)
	else:
		print "usage: ", sys.argv[0], "database startid endid"
