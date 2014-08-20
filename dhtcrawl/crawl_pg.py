import psycopg2
from dht import DHT, DHTError

DB_EXEC_INIT = """CREATE TABLE IF NOT EXISTS magnets (mid SERIAL UNIQUE, magnet CHAR(64) NOT NULL PRIMARY KEY, lastupdated INTEGER)
CREATE TABLE IF NOT EXISTS addrmap (mid INTEGER REFERENCES magnets(mid) ON DELETE CASCADE, aid INTEGER REFERENCES addresses(aid) ON DELETE CASCADE, firstseen INTEGER, lastseen INTEGER, PRIMARY KEY(mid, aid))
CREATE TABLE IF NOT EXISTS addresses (aid SERIAL UNIQUE, ipaddr INET NOT NULL PRIMARY KEY)"""

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

DB_EXEC_UPSERT_ADDRMAP = """LOCK TABLE addrmap IN SHARE ROW EXCLUSIVE MODE
WITH new_addrmap (mid, aid, lastseen) AS (
  values 
    (%s, %s, %s, %s)

),
upsert AS
( 
    update addrmap a
        SET lastseen = na.lastseen
    FROM new_addrmap na
    WHERE a.mid = na.mid AND a.aid = na.aid
    RETURNING a.*
)
INSERT INTO addrmap (mid, aid, firstseen, lastseen)
SELECT mid, aid, lastseen, lastseen
FROM new_addrmap
WHERE NOT EXISTS (SELECT 1 
                  FROM upsert up
                  WHERE up.mid = new_addrmap.mid AND up.aid = new_addrmap.aid)
"""

DB_EXEC_INSERT_ADDRESSES = """
WITH new_addresses (ipaddr) AS (
  values 
    (%s)
)
INSERT INTO addresses (ipaddr)
SELECT 1
FROM new_addresses
WHERE
    NOT EXISTS (
        SELECT aid FROM addresses WHERE ipaddr = new_addresses
    )
"""

