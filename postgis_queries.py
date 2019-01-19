"""
psycopg2 PostGIS spatial queries for use in Python ETL's
By D. Bailey
"""

import psycopg2

# function to connect to PostGres
def con(db, user, host, passw):
    # connection variables
    db1 = "dbname='{}'".format(db)
    user1 = "user='{}'".format(user)
    host1 = "host='{}'".format(host)
    passw1 = "password={}".format(passw)

    return db1 + user1 + host1 + passw1

# connection variables
db = 'database'
user = 'user'
host = 'host'
passw = "password"

# connect to postgis
connection = psycopg2.connect(con(db, user, host, passw))
cursor = connection.cursor()

# SELECT operation - join PostGIS polygon fields to a PostGIS point based on spatial intersect
def spatial_join(pt, poly, poly_field, *args):

    # pt: point table to be used in intersection
    # poly: polygon table to be used in intersection
    # poly_field: polygon field (other than shape) to be included in output
    # args (OPTIONAL): point field to be used in output (Use this for less clutter)

    # check if args was used in function
    # no args
    if args == ():
        # intersect point with polygon in PostGIS and get field from polygon
        squery = "select a.*, b.{} from {} as a, {} as b where ST_Intersects(a.point, b.polygon)".format(poly_field, pt, poly)

        cursor.execute(squery)
        record = cursor.fetchall()

        for rec in record:
            print rec

    # args was used
    else:
        # get first item in args list
        args = args[0]
        # intersect point with polygon in PostGIS and get field from polygon
        squery = "select a.{}, b.{} from {} as a, {} as b where ST_Intersects(a.point, b.polygon)".format(args, poly_field, pt, poly)

        cursor.execute(squery)
        record = cursor.fetchall() #fetchmany

        print record
        for rec in record:
            print rec

    # commit to Database
    connection.commit()
    

# SELECT operation - get list of points based on a spatial intersect with polygon and table join
def list_points(name, list_table, pt, poly, join_table_field1, join_table_field2):

    # name: field from list_table
    # list_table: table that holds fields
    # pt: point table to be used in intersection
    # poly: polygon table to be used in intersection
    # join_table_field1: column in polygon table to be used in join
    # join_table_field2: column in point table to be used in join

    # intersect point with polygon, get name in polygon, join name based on field in another table
    # list all names
    lsquery = "select c.{} from {} as c, {} as a, {} as b where ST_Intersects(a.point, b.polygon) and (b.{} = c.{})".format(name, list_table, pt, poly, join_table_field1, join_table_field2)
    cursor.execute(lsquery)
    names = cursor.fetchall()
    print names

    # commit to Database
    connection.commit()
