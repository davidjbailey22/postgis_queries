**Overview**

postgis queries for use in python via psycopg2

**Example**

first define database connections variables:

db = "database name" user = "database user name" host = "host" passw = "password for user name"

join PostGIS polygon fields to a PostGIS point based on spatial intersect
spatial_join("prod_data.points", "prod_data.poly", "name")
