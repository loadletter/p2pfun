#!/usr/bin/python
# -*- coding: utf-8 -*-
import SimpleHTTPServer, SocketServer, socket, urlparse, requests, re
########################################################################
TRACKER = 'http://october.bakabt.me:2710/cdf9a433d3e2c0f89862f68bc8d23aaf/announce.php'
PORT = 8025
HTTP_UA = 'uTorrent/2210(25302)'
PEER_ID = '-UT2210-%d6b%cb%d3R%d9%c7%e3%bcvp%86'
########################################################################
HOSTNAME = socket.gethostbyaddr(socket.gethostname())[0]
ANNOUNCE_URL = TRACKER
BASE_URL, ANNOUNCE_KW = ANNOUNCE_URL.rsplit('/', 1)
SCRAPE_URL = ''.join((BASE_URL, '/', 'scrape', ANNOUNCE_KW.replace('announce', '')))
REQ_HEADERS = {'User-Agent' : HTTP_UA, 'Connection' : 'close', 'Accept-Encoding' : 'gzip'}

class webDispatcher(SimpleHTTPServer.SimpleHTTPRequestHandler):
	def req_hello(self, qs):
		self.send_response(200)
		self.send_header("Content-Type","text/html")
		self.end_headers()       
		self.wfile.write('Hello.')

	def req_announce(self, qs):
		newquery = re.sub('peer_id=[^&]+', 'peer_id=' + PEER_ID, qs)
		req = requests.get(ANNOUNCE_URL + '?' + newquery, headers=REQ_HEADERS)
		self.send_response(req.status_code)
		self.end_headers()
		self.wfile.write(req.content)

	def req_scrape(self, qs):
		newquery = re.sub('peer_id=[^&]+&', '', qs)
		req = requests.get(SCRAPE_URL + '?' + newquery, headers=REQ_HEADERS)
		self.send_response(req.status_code)
		self.end_headers()
		self.wfile.write(req.content)
			
	def do_GET(self):
		url_parsed = urlparse.urlparse(self.path)
		action = url_parsed.path[1:]
		if action=="": action="hello"
		methodname = "req_"+action
		try:
			getattr(self, methodname)(url_parsed.query)
		except AttributeError:
			self.send_response(404)
			self.send_header("Content-Type","text/html")
			self.end_headers()       
			self.wfile.write("404 - Not found")       
		except TypeError:  # URL not called with the proper parameters
			self.send_response(400)
			self.send_header("Content-Type","text/html")
			self.end_headers()      
			self.wfile.write("400 - Bad request")
		  
httpd = SocketServer.ThreadingTCPServer(('', PORT), webDispatcher)
print u"Server listening at http://%s:%s" % (HOSTNAME, PORT)
httpd.serve_forever()
