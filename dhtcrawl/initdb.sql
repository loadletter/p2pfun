CREATE TABLE IF NOT EXISTS magnets (mid SERIAL UNIQUE, magnet BYTEA NOT NULL PRIMARY KEY, lastupdated INTEGER);
CREATE TABLE IF NOT EXISTS addresses (aid SERIAL UNIQUE, ipaddr INET NOT NULL, iport INTEGER NOT NULL, PRIMARY KEY(ipaddr, iport));
CREATE TABLE IF NOT EXISTS addrmap (mid INTEGER REFERENCES magnets(mid) ON DELETE CASCADE, aid INTEGER REFERENCES addresses(aid) ON DELETE CASCADE, firstseen INTEGER, lastseen INTEGER, PRIMARY KEY(mid, aid));

CREATE OR REPLACE FUNCTION insert_update_addr(new_magnet BYTEA, new_lastupdated INTEGER, new_ipaddr INET, new_iport INTEGER) RETURNS integer AS $$
DECLARE
	new_mid INTEGER;
	new_aid INTEGER;
	retval INTEGER;
BEGIN
	LOCK TABLE magnets IN SHARE ROW EXCLUSIVE MODE;
	WITH magnets_up AS
	(
		UPDATE magnets m
			SET lastupdated = new_lastupdated
		WHERE m.magnet = new_magnet
		RETURNING m.*
	)
	INSERT INTO magnets (magnet, lastupdated)
	SELECT new_magnet, new_lastupdated
	WHERE NOT EXISTS (SELECT 1 
					  FROM magnets_up up
					  WHERE up.magnet = new_magnet)
	RETURNING mid INTO new_mid;

	INSERT INTO addresses (ipaddr, iport)
	SELECT new_ipaddr, new_iport
	WHERE NOT EXISTS (
			SELECT aid FROM addresses WHERE addresses.ipaddr = new_ipaddr AND addresses.iport = new_iport)
	RETURNING aid INTO new_aid;
	
	-- if the row alredy exists insert ... returning wont exec
	IF new_mid IS NULL THEN
		new_mid := (SELECT mid FROM magnets WHERE magnet = new_magnet);
	END IF;
	IF new_aid IS NULL THEN
		new_aid := (SELECT aid FROM addresses WHERE addresses.ipaddr = new_ipaddr AND addresses.iport = new_iport);
	END IF;
	
	LOCK TABLE addrmap IN SHARE ROW EXCLUSIVE MODE;
	WITH addrmap_up AS
	( 
		UPDATE addrmap a
			SET lastseen = new_lastupdated
		WHERE a.mid = new_mid AND a.aid = new_aid
		RETURNING a.*
	)
	INSERT INTO addrmap (mid, aid, firstseen, lastseen)
	SELECT new_mid, new_aid, new_lastupdated, new_lastupdated
	WHERE NOT EXISTS (SELECT 1 
					  FROM addrmap_up up
					  WHERE up.mid = new_mid AND up.aid = new_aid)
	RETURNING lastseen INTO retval;
	RETURN retval;	
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_new_magnet(new_magnet BYTEA) RETURNS void AS $$
BEGIN
	SET LOCAL synchronous_commit TO OFF;
	INSERT INTO magnets (magnet, lastupdated)
	SELECT new_magnet, 0
	WHERE NOT EXISTS (SELECT 1 
					  FROM magnets m
					  WHERE m.magnet = new_magnet);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_new_address(new_ipaddr INET, new_iport INTEGER) RETURNS void AS $$
BEGIN
	SET LOCAL synchronous_commit TO OFF;
	INSERT INTO addresses (ipaddr, iport)
	SELECT new_ipaddr, new_iport
	WHERE NOT EXISTS (
			SELECT aid FROM addresses WHERE addresses.ipaddr = new_ipaddr AND addresses.iport = new_iport);
END;
$$ LANGUAGE plpgsql;
