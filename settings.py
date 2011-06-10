'''
Created on Apr 9, 2011

@author: etherealite
'''

import os, sys
proj_dir = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(proj_dir, 'packages')
sys.path.append(packages_dir)

# path to mdb tools
MDB_TOOLS = '/usr/bin/'

# Mysql host DB settings
HOST = 'localhost'
DATABASE = 'tfkdb'
USER = 'root'
PASSWD = 'password'



###
# expand file paths
def expand_paths():
    for mdb_file,ldif_file in zip(MDB_FILES, LDIF_FILES.keys()):
        MDB_FILES[mdb_file]['file'] = SOURCES_DIR + "/" + MDB_FILES[mdb_file]['file']
        LDIF_FILES[ldif_file]['file'] = SOURCES_DIR + "/" + LDIF_FILES[ldif_file]['file']
# sources file dir

SOURCES_DIR = '/home/etherealite/projects/tfk/sources'
# Access databases and their table name in the form of 

# key : { 'file' : filename, 'table' : relevent table)
MDB_FILES = {
             'rides' : {'file' : 'Dbase_client_New.mdb',
                        'table': 'TBL Client'},
             'rolodex' : {'file' : 'tfkrolo7.mdb', 
                          'table' : 'tbl rolodex'},
             'national' : {'file': 'National.mdb', 
                           'table' : 'NATIONAL MASTER'}
             }

###
# Settings for thunder bird import.

# Thunderbird ldif file import settings
LDIF_FILES = {
              'national address' : {'file' : 'natl_address_4-1.ldif'},
              'address' : {'file' : 'address_4-9-2011.ldif'},
              'ride address' : {'file' : 'ride_address_4.8.2011.ldif'}
              }
# set table names for import
PEOPLE_TABLE = 'thunderpeople'
GROUP_TABLE = 'thundergroup'


expand_paths()





