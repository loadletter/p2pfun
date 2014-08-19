import psycopg2
import dht

DB_EXEC_INIT = """CREATE TABLE IF NOT EXISTS magnets (mid SERIAL UNIQUE, magnet CHAR(64) NOT NULL PRIMARY KEY, lastupdated INTEGER)
CREATE TABLE IF NOT EXISTS addrmap (mid INTEGER REFERENCES magnets(mid) ON DELETE CASCADE, aid INTEGER REFERENCES addresses(aid) ON DELETE CASCADE, firstseen INTEGER, lastseen INTEGER, PRIMARY KEY(mid, aid))
CREATE TABLE IF NOT EXISTS addresses (aid SERIAL UNIQUE, ipaddr INET NOT NULL PRIMARY KEY, firstseen INTEGER, lastseen INTEGER)"""

DB_EXEC_UPSERT_MAGNETS = """LOCK TABLE magnets IN SHARE ROW EXCLUSIVE MODE
WITH new_magnets (magnet, lastupdated) AS (
  values 
     (%s, %s)

),
upsert AS
( 
    update magnets m
        SET lastupdated = nm.lastupdated
    FROM new_magnets nm
    WHERE m.magnet = nm.magnet
    RETURNING m.*
)
INSERT INTO magnets (magnet, lastupdated)
SELECT magnet, lastupdated
FROM new_magnets
WHERE NOT EXISTS (SELECT 1 
                  FROM upsert up
                  WHERE up.magnet = new_magnets.magnet)
"""
