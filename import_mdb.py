'''
Created on Apr 11, 2011

@author: etherealite
'''
import os, sys
import pdb
proj_dir = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(proj_dir, 'packages')
sys.path.append(packages_dir)

from subprocess import check_output
import re

import settings
# functions call cusor from the global scope.
from mysql_env import cursor


# dictionary of mdb files to import
databases = settings.MDB_FILES

# path to mdb tools
mdbtools_path = settings.MDB_TOOLS


def tablefrom_mdb(table_name, mdb_file):
    """inserts a new table from an mdb file given the table name provided
    in mdb_file. Returns a list of 2 queries, a drop table and a create
    table."""
    global mdbtools_path
    mdb_table = table_name
    mdb_file = mdb_file

    # Name and path of mdb-schema bin file.
    schema_bin = "mdb-schema"
    schema_bin = mdbtools_path + schema_bin
    mysql_schema = \
    check_output([
                 schema_bin,
                  "-S", #sanitize names
                  "-T", mdb_table, #only for {mdb_table} table
                  mdb_file
                ])

    # lazy fixes for the garbage that mdb-schema puts out.
    mysql_schema = re.sub('DROP TABLE .*;', '', mysql_schema)
    #mysql_schema = mysql_schema.replace(
    #        'DROP TABLE', 'DROP TABLE IF EXISTS')
    mysql_schema = mysql_schema.replace('Long Integer', 'INTEGER')
    mysql_schema = mysql_schema.replace('Text (', 'VARCHAR(')
    mysql_schema = mysql_schema.replace(' (Short)', '')
    # get rid of comment lines
    mysql_schema = re.split('\n', mysql_schema)
    mysql_schema = [line for line in mysql_schema if line and not 
                    re.match('^-', line)]
    mysql_schema = '\n'.join(mysql_schema)
    return mysql_schema

def table_attr(table_name):
    """Read in table name and return relevent column(attributes) info"""
    global cursor
    columns = []
    line_pos = 0
    for line in body:
        groups = re.match(column_patt, line, flags=regx_flags)
        max = groups.group('max')
        if max:
            max = int(max)
        column = [groups.group('name'), groups.group('type').lower()]
        column.append(max)
        column.append(line_pos)
        columns.append(tuple(column))

        line_pos += 1
    return tuple(columns)

def datafrom_mdb(mdb_file, table_name):
    """Retrun mysql compatible insert queries to import data
    from mdb database file"""
    global mdbtools_path
    mysql_inserts = \
    check_output([
                  mdbtools_path + 'mdb-export',
                  '-S', #sanitize
                  '-R', ';\n', #set delimiter
                  '-X', '\\', #set escape character
                  '-I', #insert statements, not CSV
                  mdb_file, 
                  table_name
                  ])
    # mdb-tools chodes, and gives us some fields containing only a single
    # backslash, MySQLdb no likey that.
    mysql_inserts = mysql_inserts.replace('"\\",', '"x",')
    # gotta scrape off that last empty string that split() leaves behind
    # wtih the [:-1]
    mysql_inserts = re.split(';\n', mysql_inserts)[:-1]
    return mysql_inserts



## Read MS Access MDB file into mdb-tools and using the target
## table, create in the MySQL database, an identical table.
for database_key in databases.keys():
    mdb_filename = databases[database_key]['file']

    # mdb_tablename_sanitized is needed because mdbtools replaces \w
    # with _ on table names.
    mdb_tablename = databases[database_key]['table']
    mdb_tablename_sanitized = re.sub('[^\w]+', '_', mdb_tablename)
    final_tablename = database_key

#    # MySQL being a fuckup
#    # Drop all possible conflicting tables
#    drop_query = """DROP IF EXISTS "%s", "%s","%s";""" % (
#            mdb_tablename, mdb_tablename_sanitized, final_tablename
#            )
#    print drop_query
#    cursor.execute(drop_query)

    print "working on database: %s" % final_tablename
    print "inserting table: %s, from file: %s" % (mdb_tablename, mdb_filename)

    create_query = tablefrom_mdb(mdb_tablename, mdb_filename)
    cursor.execute(create_query)

    insert_queries = datafrom_mdb(mdb_filename, mdb_tablename)
    for query in insert_queries:
        cursor.execute(query)

    # Name the table according to the key in the settings dict.
    rename_query = "RENAME TABLE `%s` TO `%s`;" % (
            mdb_tablename_sanitized, final_tablename
            )
    cursor.execute(rename_query)

    addsource_col = """
    ALTER TABLE %s 
    ADD COLUMN source varchar(60),
    ADD COLUMN id int(10) PRIMARY KEY AUTO_INCREMENT FIRST;
    """ % final_tablename
    cursor.execute(addsource_col)


### build collection of all columns contained in each table as proudced
### by mdb-tools
#
#def create_table(table_name, columns):
#    col_def = "%s %s%s, "
#    def_body = ''
#    for pos in sorted(columns.keys()):
#        col_name = columns[pos][0]
#        col_type = columns[pos][1]
#        col_max = columns[pos][2]
#        if not col_max:
#            col_max = ''
#        else:
#            col_max = '(' + str(columns[pos][2]) + ')'
#        def_body += col_def % (col_name, col_type, col_max)
#    def_body = def_body[:-2]
#
#    query_string = """CREATE TABLE %s (%s);""" % (table_name, def_body)
#    cursor.execute(query_string)
#    schemas[friendly_name] = table_attr(create_statement)
#
#
### create a union of columns across all tables
#cols_union = {}
#union_pos = 0
#for table in schemas:
#    table_colpos = 0
#    table_schema = schemas[table]
#    for column in table_schema:
#        table_colname = column[0]
#        table_coltype = column[1]
#        table_colmax = column[2]
#
#        # make sure union has the largest field width between all tables.
#        if cols_union.has_key(table_colname):
#            union_coltype = cols_union[table_colname][0]
#            union_colmax = cols_union[table_colname][1]
#            if table_colmax > union_colmax:
#                assert union_coltype == table_coltype
#                cols_union[table_colname][1] = table_colmax
#        else:
#            if table_colpos > 0:
#                predecessor = table_schema[table_colpos - 1][0]
#            else:
#                assert union_pos == 0
#                predecessor = None
#            cols_union[table_colname] = [table_coltype, table_colmax,
#                                          predecessor, union_pos]
#            union_pos +=1
#
#        table_colpos += 1
#
### create dictionary of union with column position as keys.
#union_by_pos = {}
#for column in cols_union.items():
#    col_name = column[0]
#    col_type = column[1][0]
#    col_max = column[1][1]
#    col_pos = column[1][3]
#    union_by_pos[col_pos] = (col_name, col_type, col_max)
#query = """DROP TABLE IF EXIST %s""" % MDB_MERGED
#cursor.execute('DROP TABLE IF EXISTS master')
#create_table('', union_by_pos)
#cursor.execute('ALTER TABLE master ADD COLUMN source varchar(60);')
#
## copy data from mdb tables into the master table.
#for table_name in schemas:
#    columns_all = []
#    for column in schemas[table_name]:
#        columns_all.append(column[0])
#    columns_all.append('source')
#    columns_all_str = ', '.join(columns_all)
#    select_qry = """SELECT %s FROM %s""" \
#     % (columns_all_str, table_name)
#
#    cursor.execute(select_qry)
#    table_values = cursor.fetchall()
#
#    insert_qry = """INSERT INTO master (%s)
#    VALUES (%s)""" % \
#    (columns_all_str, (r'%s, ' * len(columns_all))[:-2])
#    cursor.executemany(insert_qry, table_values)
