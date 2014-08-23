import sys
import re
import psycopg2
from dbconf import DSN

ASCIIHEX40_REGEX = re.compile("^[0-9a-f]{40}$")
CHUNKSIZE = 2000

def chunks(l, n):
	""" Yield successive n-sized chunks from l.
	"""
	for i in xrange(0, len(l), n):
		yield l[i:i+n]

def insert_magnets(cur, fpath):
	maglist = []
	linec = 0
	with open(fpath) as f:
		for line in f:
			linec += 1
			l = line.strip()
			if ASCIIHEX40_REGEX.match(l):
				maglist.append((l,))
	print len(maglist), "/", linec
	for num, c in enumerate(chunks(maglist, CHUNKSIZE)):
		cur.executemany('SELECT insert_new_magnet(%s)', c)
		conn.commit()
		print num * CHUNKSIZE

if __name__ == "__main__":
	conn = psycopg2.connect(DSN)
	with conn.cursor() as cur:
		insert_magnets(cur, sys.argv[1])
	conn.commit()
	conn.close()
