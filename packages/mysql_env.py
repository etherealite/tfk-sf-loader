'''
Created on Apr 6, 2011

@author: etherealite
'''
import sys, os
settings_path = os.path.abspath(__file__)
settings_path = os.path.dirname(os.path.dirname(settings_path))
sys.path.append(settings_path)


import MySQLdb
import _mysql_exceptions
import settings

class Cursor(object):
    def __init__(self, settings_obj):
        """Opens a database and installs a cursor instance Supply the
        constructor with settings object."""
        # Read in initial settings
        self._setfrom_obj(settings_obj)

        # Open a database connection and install a cursor
        self._opendb()
        self._getcursor()


    def _opendb(self, database=''):
        """Installs a MySQLdb connection object for internal use."""
        # allow overiding of the database string.
        if database:
            self.database = database

        # Install a connection object wich will be used to create a cusor
        # that will be proxied by objects of this class.
        self._mysqldb_db = MySQLdb.connect(
                        host=self.host,
                        user=self.user,
                        passwd=self.passwd,
                        db=self.database
                        )
        # Without this, changes made to the DB will not be saved unless
        # commit is called on the cursor(using innodb).
        self._mysqldb_db.autocommit(True)


    def _getcursor(self):
        """install a cursor into the base proxy object"""
        self.cursor = self._mysqldb_db.cursor()


    def __getattr__(self, name):
        """Send attribute requests to the wrapped MySQLdb cursor
        instance."""
        if hasattr(self.cursor, name):
            return getattr(self.cursor, name)
        else:
            return object.__getattribute__(self, name)


    def _setfrom_obj(self, settings_obj):
        """Set needed instance attributes from settings object"""
        self.host = settings_obj.HOST
        self.user = settings_obj.USER
        self.database = settings_obj.DATABASE
        self.passwd = settings_obj.PASSWD


    def use_db(self, database):
        """Change the db selected by the cursor"""
        self._opendb(database)
        self._getcursor()

cursor = Cursor(settings)

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
    return insert_str
    cursor.execute(insert_str)

