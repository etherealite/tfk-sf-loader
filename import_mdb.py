'''
Created on Apr 11, 2011

@author: etherealite
'''
import os, sys
proj_dir = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(proj_dir, 'packages')
sys.path.append(packages_dir)

from subprocess import check_output
import re

import settings
from mysql_env import cursor, create_table



# directory holding the mdb files
sources_dir = settings.SOURCES_DIR
# dictionary of mdb files to import
databases = settings.MDB_FILES
# merged table name
#MERGED_TABLE = settings.MERGED_TABLE

# path to mdb tools
mdbtools_path = settings.MDB_TOOLS

# Get database settings
host = settings.HOST
database = settings.DATABASE
user = settings.USER
passwd = settings.PASSWD


def tablefrom_mdb(table_name, file_path):
    """inserts a new table from an mdb file given the table name provided
    in file_path. Returns a list of 2 queries, a drop table and a create
    table."""
    global mdbtools_path
    mdb_table = table_name
    mdb_file = file_path
    
    mysql_schema = \
    check_output([
                 mdbtools_path + "mdb-schema",
                  "-S", #sanitize names
                  "-T", mdb_table, #only for {mdb_table} table
                  mdb_file
                ])
    
## lazy fixes for mysql incompatibilities.
    mysql_schema = \
    mysql_schema.replace('DROP TABLE', 'DROP TABLE IF EXISTS')
    mysql_schema = mysql_schema.replace('Long Integer', 'INTEGER')
    mysql_schema = mysql_schema.replace('Text (', 'VARCHAR(')
    mysql_schema = mysql_schema.replace(' (Short)', '')
## get rid of comment lines
    mysql_schema = re.split('\n', mysql_schema)
    mysql_schema = [line for line in mysql_schema if line and not 
                    re.match('^-', line)]
    mysql_schema = '\n'.join(mysql_schema)
    # gotta scrape off that last empty string that split() leaves behind
    # wtih the [:-1]
    mysql_schema = mysql_schema.split(';')[:-1]
    return mysql_schema

def parse_schema(schema):
    """read in a table create statement and parse out the column
    names, data types and maximum field width. Return in a
     nested tuple"""
    regx_flags = re.DOTALL + re.IGNORECASE
    body_patt = r'.*create[\s]table.*?[(](.*)[)]'
    column_patt = \
    r'\s*(?P<name>\w+)\s+(?P<type>\w+)[(]?(?P<max>\d+)?'
    column_decs = \
    re.match(body_patt, schema, flags=regx_flags).group(1)
    body = [line for line in re.split('\n', column_decs) if line]
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
    
def datafrom_mdb(mdb_file, mdb_table):
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
                  mdb_table
                  ])
    # mdb-tools chodes, and gives us some fields containing only a single
    # backslash, MySQLdb no likey that.
    mysql_inserts = mysql_inserts.replace('"\\",', '"x",')
    # gotta scrape off that last empty string that split() leaves behind
    # wtih the [:-1]
    mysql_inserts = re.split(';\n', mysql_inserts)[:-1]
    return mysql_inserts

def reset_and_rename(target, new_name, cursor):
    """get rid of any tables with name conflicting with new_name and
    rename target table. """
    drop_table = "DROP TABLE IF EXISTS `%s`;" % new_name
    cursor.execute(drop_table)
    
    rename_table = "RENAME TABLE `%s` TO `%s`;" % (target, new_name)
    cursor.execute(rename_table)

## Read MS Access MDB file into mdb-tools and using the target 
## table, create in the MySQL database, an identical table.
schemas = {}
for database_key in databases.keys():
    mdb_file = databases[database_key]['file']
    mdb_table = databases[database_key]['table']
    friendly_name = database_key
    
    print "working on database: %s" % friendly_name
    print "inserting table: %s, from file: %s" % (mdb_table, mdb_file)
    
    # create_schema includes a DROP IF EXIST statement
    create_schema = tablefrom_mdb(mdb_table, mdb_file)
    for query in create_schema:
        cursor.execute(query)

    create_statement = create_schema[1] #[1] is actual create statement
    insert_queries = datafrom_mdb(mdb_file, mdb_table)
    for query in insert_queries:
        cursor.execute(query)
    
    # sanitized_name needed to match table name in schema after mdb_tools
    # sanitizes names
    mdb_name = re.sub('\s', '_', mdb_table) 
    reset_and_rename(mdb_name, friendly_name, cursor)
    
    addsource_col = """
    ALTER TABLE %s ADD COLUMN source varchar(60);
    """ % friendly_name
    
    cursor.execute(addsource_col)
    
    # insert source column and set value to the name
    # of the table for all records.
    insert_source = """
    UPDATE %s SET source='%s';
    """ % (friendly_name, friendly_name)
    cursor.execute(insert_source)
    
## build collection of all columns contained in each table as proudced
## by mdb-tools
    schemas[friendly_name] = parse_schema(create_statement)


## create a union of columns across all tables
cols_union = {}
union_pos = 0
for table in schemas:
    table_colpos = 0
    table_schema = schemas[table]
    for column in table_schema:
        table_colname = column[0]
        table_coltype = column[1]
        table_colmax = column[2]
        
        # make sure union has the largest field width between all tables.
        if cols_union.has_key(table_colname):
            union_coltype = cols_union[table_colname][0]
            union_colmax = cols_union[table_colname][1]
            if table_colmax > union_colmax:
                assert union_coltype == table_coltype
                cols_union[table_colname][1] = table_colmax
        else:
            if table_colpos > 0:
                predecessor = table_schema[table_colpos - 1][0]
            else:
                assert union_pos == 0
                predecessor = None
            cols_union[table_colname] = [table_coltype, table_colmax,
                                          predecessor, union_pos]
            union_pos +=1
            
        table_colpos += 1

## create dictionary of union with column position as keys.
union_by_pos = {}
for column in cols_union.items():
    col_name = column[0]
    col_type = column[1][0]
    col_max = column[1][1]
    col_pos = column[1][3]
    union_by_pos[col_pos] = (col_name, col_type, col_max)
cursor.execute('DROP TABLE IF EXISTS master')
create_table('master', union_by_pos)
cursor.execute('ALTER TABLE master ADD COLUMN source varchar(60);')

# copy data from mdb tables into the master table.
for table_name in schemas:
    columns_all = []
    for column in schemas[table_name]:
        columns_all.append(column[0])
    columns_all.append('source')
    columns_all_str = ', '.join(columns_all)
    select_qry = """SELECT %s FROM %s""" \
     % (columns_all_str, table_name)
    
    cursor.execute(select_qry)
    table_values = cursor.fetchall()
    
    insert_qry = """INSERT INTO master (%s)
    VALUES (%s)""" % \
    (columns_all_str, (r'%s, ' * len(columns_all))[:-2])
    cursor.executemany(insert_qry, table_values)
    
