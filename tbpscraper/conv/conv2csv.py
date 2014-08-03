import sqlite3, json, base64, zlib, sys

MINCOMPRESSLEN = 150

def commentencode(data):
	assert type(data) == unicode
	encdata = data.encode('utf-8')
	if len(encdata) > MINCOMPRESSLEN:
		compdata = zlib.compress(encdata, 9)
		if len(compdata) < len(encdata):
			return ''.join(['C', base64.b64encode(compdata)])
	return ''.join(['U', encdata])

def commentdecode(row):
	if row.startswith('U'):
		return json.loads(row)
	if row.startswuth('C'):
		decdata = base64.b64decode(row[1:])
		uncdata = zlib.decompress(decdata)
		return json.loads(uncdata)
	assert False

def run(dbfile, outfile):
	conn = sqlite3.connect(dbfile)
	with open(outfile, "wb") as f:
		curs = conn.execute('SELECT tid, date, size, seeders, leechers, magnet, category, tags, title, user, comment FROM tpb ORDER BY tid')
		for data in curs:
			st = map(str, data[0:7])
			for i in range(1, len(st)*2, 2):
				st.insert(i, '|')
			st.append(json.dumps([json.loads(data[7]), data[8], data[9], commentencode(data[10])]))
			st.append('\n')
			f.write(''.join(st))
			
if __name__ == "__main__":
	run(sys.argv[1], sys.argv[2])
