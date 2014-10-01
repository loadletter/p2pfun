#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, SimpleHTTPServer, SocketServer, socket, cgi, urlparse, requests, urllib

TRACKER = 'http://october.bakabt.me:2710/cdf9a433d3e2c0f89862f68bc8d23aaf/announce.php'
PORT = 8025
HTTP_UA = 'uTorrent/2210(25302)'
PEER_ID = '-UT2210-%d6b%cb%d3R%d9%c7%e3%bcvp%86'
HOSTNAME = socket.gethostbyaddr(socket.gethostname())[0]
ANNOUNCE_URL = TRACKER
BASE_URL, ANNOUNCE_KW = ANNOUNCE_URL.rsplit('/', 1)
SCRAPE_URL = ''.join((BASE_URL, '/', 'scrape', ANNOUNCE_KW.replace('announce', '')))

class webDispatcher(SimpleHTTPServer.SimpleHTTPRequestHandler):

	def req_hello(self):
		self.send_response(200)
		self.send_header("Content-Type","text/html")
		self.end_headers()       
		self.wfile.write('Hello.')

	def req_announce(self, **kwargs):
		keys = tuple(kwargs)
		values = map(lambda x: kwargs[x][0], keys)
		args = dict(zip(keys, values))
		args['peer_id'] = PEER_ID
		#re.sub('peer_id=[^&]+',  'asdasd', url)
		print kwargs
		print args
		dest_url = ANNOUNCE_URL + '?' + urllib.urlencode(args)
		print dest_url
		req = requests.get(dest_url, headers={'User-Agent' : HTTP_UA})
		print req.url
		self.send_response(req.status_code)
		self.end_headers()
		self.wfile.write(req.content)

	def req_scrape(self, **kwargs):
		keys = tuple(kwargs)
		values = map(lambda x: kwargs[x][0], keys)
		args = dict(zip(keys, values))
		if peer_id in args:
			del args['peer_id']
		req = requests.get(SCRAPE_URL + '?' + urllib.urlencode(args), headers={'User-Agent' : HTTP_UA})
		self.send_response(req.status_code)
		self.end_headers()
		self.wfile.write(req.content)
			
	def do_GET(self):
		params = cgi.parse_qs(urlparse.urlparse(self.path).query)
		action = urlparse.urlparse(self.path).path[1:]
		if action=="": action="hello"
		methodname = "req_"+action
		try:
			getattr(self, methodname)(**params)
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
print u"Server listening at http://%s:%s" % (HOSTNAME,PORT)
httpd.serve_forever()
