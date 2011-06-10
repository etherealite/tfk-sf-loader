'''
Created on Apr 6, 2011

@author: etherealite
'''
import MySQLdb
import _mysql_exceptions
import settings

# get our db connection setup
def db_connect(database=settings.DATABASE):        
    db=MySQLdb.connect(
                       host=settings.HOST,
                       user=settings.USER,
                       passwd=settings.PASSWD,
                       db=database
                       )
    db.autocommit(True) #need this for innodb
    
    cursor = db.cursor()
    return cursor

cursor = db_connect()


def insert(table, data):
    global cursor
    
    if isinstance(data, dict):
        data = data.items()
        
        
    escape_chars = ["'", '"',]
    str_columns, str_values = '',''
    for pair in data:
        key,value = pair
        key = str(key)
        value = str(value)
        for char in escape_chars:
            value = value.replace(char, "\\" + char)
        
        str_columns += key + ', '
        str_values += "'" + value + "'" +', '
        
    str_columns = str_columns[:-2]
    str_values = str_values[:-2]
    insert_str = """
    INSERT INTO %s
        (%s)
        VALUES(%s)
    """ % (table, str_columns, str_values)
    cursor.execute(insert_str)



def create_table(table_name, columns):
    col_def = "%s %s%s, "
    def_body = ''
    for pos in sorted(columns.keys()):
        col_name = columns[pos][0]
        col_type = columns[pos][1]
        col_max = columns[pos][2]
        if not col_max:
            col_max = ''
        else:
            col_max = '(' + str(columns[pos][2]) + ')'
        def_body += col_def % (col_name, col_type, col_max)
    def_body = def_body[:-2]
    
    query_string = """CREATE TABLE %s (%s);""" % (table_name, def_body)
    cursor.execute(query_string)