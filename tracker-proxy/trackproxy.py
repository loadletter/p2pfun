#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, SimpleHTTPServer, SocketServer, socket, cgi, urlparse, requests

PORT = 8025
HOSTNAME = socket.gethostbyaddr(socket.gethostname())[0]

class webDispatcher(SimpleHTTPServer.SimpleHTTPRequestHandler):

	def req_hello(self):
		self.send_response(200)
		self.send_header("Content-Type","text/html")
		self.end_headers()       
		self.wfile.write('Hello.')

	def req_announce(self, **kwargs):
		self.send_response(200)
		self.end_headers()       
		self.wfile.write(requests.get('''http://october.bakabt.me:2710/cdf9a433d3e2c0f89862f68bc8d23aaf/announce.php?info_hash=%13%15%86%2bd%19O%94%97%c6%922%cd%e6%19w%3d%02c%5d&peer_id=-UT2210-%d6b%cb%d3R%d9%c7%e3%bcvp%86&port=44589&uploaded=0&downloaded=0&left=1693878276&corrupt=0&key=2E0A2949&event=started&numwant=200&compact=1&no_peer_id=1&ipv6=2001%3a0%3a5ef5%3a79fd%3a386c%3a330f%3aa007%3aa26''', headers={'User-Agent' : 'uTorrent/2210(25302)'}).content)

	def req_scrape(self, **kwargs):
		self.send_response(200)
		self.end_headers()    
		self.wfile.write(requests.get('http://october.bakabt.me:2710/cdf9a433d3e2c0f89862f68bc8d23aaf/scrape.php?info_hash=%13%15%86%2bd%19O%94%97%c6%922%cd%e6%19w%3d%02c%5d', headers={'User-Agent' : 'uTorrent/2210(25302)'}).content)
			
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
