import sqlite3, json, sys, os

def run(dbfile, outdir):
	conn = sqlite3.connect(dbfile)
	categoryfiles = {}
	entrycount = 0
	tmpcurs = conn.execute('SELECT MAX(_ROWID_) FROM tpb LIMIT 1')
	rownum = tmpcurs.fetchone()[0]
	#real stuff
	curs = conn.execute('SELECT tid, date, size, seeders, leechers, magnet, category, tags, title, user, comment FROM tpb ORDER BY tid')
	for data in curs:
		st = map(str, data[0:6]) #tid, date, size, seeders, leechers, magnet
		for i in range(1, len(st)*2, 2):
			st.insert(i, '|')
		st.append(json.dumps([json.loads(data[7]), data[8], data[9], data[10]])) #tags, title, user, comment
		st.append('\n')
		#write to file
		treecat = data[6].replace('/', '_').replace('-', '').replace('>', '-').replace(' ', '').strip()
		if not treecat in categoryfiles:
			categoryfiles[treecat] = open(os.path.join(outdir, treecat), 'wb')
		f = categoryfiles[treecat]
		f.write(''.join(st))
		#useless stuff
		entrycount += 1
		if entrycount % 25 == 0:
			sys.stdout.write("\r%.2f%% (%i/%i)" % ((100/float(rownum))*entrycount, entrycount, rownum))
			sys.stdout.flush()
	print ", done."
	for fd in categoryfiles.itervalues():
		fd.close()
	conn.close()
			
if __name__ == '__main__':
	run(sys.argv[1], sys.argv[2])
