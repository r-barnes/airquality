 1. Install postgresql
 2. Install PostGIS (`sudo apt-get install postgresql-9.1-postgis`)
 3. Create a PostGRES database
   1. Log in to the server using `sudo -u postgres psql`
   2. `CREATE USER airq WITH PASSWORD 'password';`
   3. `CREATE DATABASE airquality;`
   4. `GRANT ALL PRIVILEGES ON DATABASE airquality TO airq;`
 4. Load the PostGIS extension
   1. `sudo -u postgres createlang plpgsql airquality;`
   2. `sudo -u postgres psql -d airquality -f /usr/share/postgresql/9.1/contrib/postgis-1.5/postgis.sql`
   3. `sudo -u postgres psql -d airquality -f /usr/share/postgresql/9.1/contrib/postgis-1.5/spatial_ref_sys.sql`


To connect from a remote machine setup an SSH tunnel:

    ssh -L 3333:localhost:5432 USER@107.170.182.238

Then you can tap into the database like so:

    psql -h localhost -p 3333 airquality airq


API calls
/measurements/stationID (optional limit)
	Get a list of measurements taken from a specific station. Limits at 1000 if one is not specified

/stations
	Get a list of stations

/last/limit
	Get a list of the last limit measurements
