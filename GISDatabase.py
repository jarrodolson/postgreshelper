import psycopg2 as pg
import csv

conn = pg.connect(dbname='geo', user='postgres', password='pw')
conn.autocommit = True
cur = conn.cursor()

##cur.execute("CREATE DATABASE geo;")
##cur.execute("CREATE EXTENSION postgis;")
##cur.execute("CREATE EXTENSION postgis_topology;")
##cur.execute("CREATE EXTENSION fuzzystrmatch;")
##cur.execute("CREATE EXTENSION postgis_tiger_geocoder;")
##cur.execute("SELECT na.address, na.streetname,na.streettypeabbrev, na.zip FROM normalize_address('1 Devonshire Place, Boston, MA 02109') AS na;")
##print(cur.fetchone())
#cur.execute("SELECT na.address, na.streetname,na.streettypeabbrev, na.zip FROM normalize_address('12741 NE 170th Ln, Woodinville, WA 98072') AS na;")
