'''
Created on Apr 16, 2011

@author: etherealite
'''

import os, sys
proj_dir = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(proj_dir, 'packages')
sys.path.append(packages_dir)

import settings
from mysql_env import cursor,insert
from ldif import LDIFParser

# Settings from config file settings.py
GROUP_TABLE = settings.GROUP_TABLE
PEOPLE_TABLE = settings.PEOPLE_TABLE
LDIFS = settings.LDIF_FILES


# DB stuff
c = cursor


###
# Setup tables and stuff
def tablesetup():
    drop_tables = "DROP TABLE IF EXISTS %s,%s" % \
    (PEOPLE_TABLE, GROUP_TABLE)
    c.execute(drop_tables)
    
    create_people = """
        CREATE TABLE %s
        (
            id INT(11) AUTO_INCREMENT,
            dn VARCHAR(300),
            first_name VARCHAR(60),
            last_name VARCHAR(60),
            nick_name VARCHAR(60),
            email VARCHAR(60),
            second_email VARCHAR(60),
            instant_msg VARCHAR(60),
            description TINYTEXT,
            organization VARCHAR(60),
            home_phone VARCHAR(60),
            work_phone VARCHAR(60),
            mobile_phone VARCHAR(60),
            source VARCHAR(60),
            PRIMARY KEY(id)
        )
    """ % PEOPLE_TABLE
    c.execute(create_people)
    
    create_groups = """
        CREATE TABLE %s
        (
            id INT(11) AUTO_INCREMENT,
            cn VARCHAR(120),
            member VARCHAR(300),
            source VARCHAR(60),
            thunderpeople_id INT(11),
            PRIMARY KEY(id)
        )
    """ % GROUP_TABLE
    c.execute(create_groups)


###
# Make the parser actually do something by overriding its handle() method

class LDIFBird(LDIFParser):
    
    def handle(self, dn, entry):
        source = self.source
        if 'person' in entry['objectclass']:
            add_person(dn, entry, source)
        else:
            add_group(dn, entry, source)


###
# Functions to write the parsed entries into the DB

def add_person(dn, entry, source):
    person = {'dn' : dn}
    for attribute in entry:
        if attribute in person2table.keys():
            person[person2table[attribute]] = entry[attribute][0]
    person['source'] = source
    print person
    insert(PEOPLE_TABLE, person)

def add_group(dn, entry, source):
    group = {
             'cn' : entry['cn'][0],
             'source' : source,
             }
    for dn in entry['member']:
        insert_group = dict(group, **{'member' : dn})
        insert(GROUP_TABLE,  insert_group)


###
# Mappings to translate field names

# Table field names mapped to LDIF parser equivalents.
table2person = {
             'first_name' : 'givenName',
             'last_name' : 'sn',
             'nick_name' : 'mozillaNickname',
             'email' : 'mail',
             'second_email' : 'mozillaSecondEmail',
             'instant_msg' : 'nsAIMid',
             'description' : 'description',
             'organization' : 'o',
             'home_phone' : 'homePhone',
             'work_phone' : 'telephoneNumber',
             'mobile_phone' : 'mobile',
             }
#Inverse of person2table
person2table = dict([[v,k] for k,v in table2person.items()])

###
# Finally, run the overridden parser
def load(): 
    for source in LDIFS:
        ldif_file = open(LDIFS[source]['file'], 'rb')
        parser = LDIFBird(ldif_file)
        parser.source = source
        parser.parse()

###
# Insert foreign keys from each for thunderpeople field into thundergroup
# that match have a member field matching the dn that thunderpeople 
# record.
#
# NOTE:
#    This leaves a few thundergroup records without thunderpeople_ids,
#    probably due to Thunderbird not counting commas as significant
#    in string matching. I hope they go to do hell.
def putkeys():
    cursor.execute("SELECT id, dn FROM thunderpeople")
    people = cursor.fetchall()
    
    for person in people:
        person_id = person[0]
        person_dn = person[1]
        
        # Handle email addresses with pesky quotes.
        person_dn = person_dn.replace('"', r'\"') 
        
        query = 'UPDATE thundergroup SET thunderpeople_id' \
        ' = "%s" WHERE member = "%s"' % (person_id, person_dn)
        print query
        cursor.execute(query)
        
        
    query = 'SELECT * FROM thundergroup WHERE thunderpeople_id' \
    ' IS NULL;'
    cursor.execute(query)
    result = cursor.fetchall()
    if result:
        try:
            raise Exception('F*ck thunder bird')
        except Exception:
            print "thundergroup records with no foreign key:"
            for record in result:
                print record
            raise

#tablesetup()
#load()
#putkeys()
