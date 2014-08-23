import sys
import re
import psycopg2
from dbconf import DSN

DB_EXEC_INSERT_NEW_MAGNETS = """LOCK TABLE magnets IN SHARE ROW EXCLUSIVE MODE
WITH new_magnets (magnet, lastupdated) AS (
  values 
    (%s, %s)
)
INSERT INTO magnets (magnet, lastupdated)
SELECT magnet, lastupdated
FROM new_magnets
WHERE NOT EXISTS (SELECT 1 
                  FROM magnets m
                  WHERE m.magnet = new_magnets.magnet)
"""

ASCIIHEX64_REGEX = re.compile("^[0-9a-f]{64}$")

def insert_magnets(cur, fpath):
	maglist = []
	linec = 0
	with open(fpath) as f:
		for line in f:
			linec += 1
			l = line.strip()
			if ASCIIHEX64_REGEX.match(l):
				maglist.append((l, 0))
	print len(maglist), "/", linec
	cur.executemany(DB_EXEC_INSERT_NEW_MAGNETS, maglist)

if __name__ == "__main__":
	conn = psycopg2.connect(DSN)
	with conn.cursor() as initcur:
		insert_magnets(initcur, sys.argv[1])
