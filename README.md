 1. Install postgresql
 2. Install PostGIS (`sudo apt-get install postgresql-9.1-postgis`)
 3. Create a PostGRES database
   1. Log in to the server using `sudo -u postgres psql`
   2. `CREATE USER user WITH PASSWORD 'password';`
   3. `CREATE DATABASE omgtransit;`
   4. `GRANT ALL PRIVILEGES ON DATABASE omgtransit TO user;`
 4. Load the PostGIS extension
   1. `sudo -u postgres createlang plpgsql omgtransit;`
   2. `sudo -u postgres psql -d omgtransit -f /usr/share/postgresql/9.1/contrib/postgis-1.5/postgis.sql`
   3. `sudo -u postgres psql -d omgtransit -f /usr/share/postgresql/9.1/contrib/postgis-1.5/spatial_ref_sys.sql`


To connect from a remoet machine setup an SSH tunnel:

    ssh -L 3333:localhost:5432 USER@107.170.182.238

Then you can tap into the database like so:

    psql -h localhost -p 3333 airquality airq