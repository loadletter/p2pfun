import bcode, os, sys, posixpath, urllib

def extract(torrent_file, dest=sys.stdout):
	with open(torrent_file) as f:
		data = f.read()
		decoded = bcode.bdecode(data)
	if not 'url-list' in decoded:
		print >>sys.stderr, "No webseeds found!"
		sys.exit(2)
	wseed = decoded['url-list'][0]
	infoh = decoded['info']
	tname = infoh['name']
	if 'length' in infoh:
		#single file torrent
		print >>dest, posixpath.join(wseed, urllib.pathname2url(tname))
	else:
		#multi file torrent
		files = map(lambda x: posixpath.join(*(x['path'])), infoh['files'])
		for fi in files:
			fpath = urllib.pathname2url(posixpath.join(tname, fi))
			print >>dest, posixpath.join(wseed, fpath)

if __name__ == "__main__":
	if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
		print "Usage: extract.py file.torrent"
		sys.exit(1)
	extract(sys.argv[1])
