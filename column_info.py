'''
Created on May 7, 2011

@author: etherealite
'''
import os, sys
proj_dir = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(proj_dir, 'packages')
sys.path.append(packages_dir)


import settings
from mysql_env import cursor, db_connect

# Create a new cursor for mysql introspection DB.
info_c = db_connect('information_schema')

cursor.execute("DROP TABLE IF EXISTS column_dups")
create_info = """
    CREATE TABLE column_dups
    (
        id INT(11) AUTO_INCREMENT,
        attribute varchar(60),
        value varchar(60),
        duplicates INT(11),
        source varchar(60),
        PRIMARY KEY(id)
    )
"""
cursor.execute(create_info)

get_columns = """
SELECT column_name FROM COLUMNS
WHERE table_schema = '%s'
AND table_name = 'master'
""" % settings.DATABASE
info_c.execute(get_columns)

columns = info_c.fetchall()

columns = tuple(map(lambda col: col[0], columns ))

cursor.execute("SELECT distinct(source) FROM master")

sources = tuple(map(lambda res: res[0], cursor.fetchall()))

do_tally_base = r"""
SELECT * FROM
(SELECT %s as sb1, COUNT(*) as sb2, source as sb3 FROM master
WHERE source = '%s'
AND %s IS NOT NULL
GROUP BY %s ORDER BY COUNT(*) DESC) as t2
WHERE sb2 > 1
"""

for source in sources:
    for column in columns:
        do_tally = do_tally_base % (column, source, column, column)
        cursor.execute(do_tally)
        stuff = cursor.fetchall()
        if len(stuff) > 0: print stuff
        
    
