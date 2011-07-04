import re
from sqlalchemy import create_engine, MetaData, select
tfkload = create_engine('mysql://root:password@localhost/tfkload', echo=True)
meta = MetaData()
meta.reflect(bind=tfkload)


rolodex = meta.tables['rolodex']

conn = tfkload.connect()
s = select([rolodex.c.id, rolodex.c.FIRSTNAME, rolodex.c.LASTNAME])

result = conn.execute(s)

for row in result:
    if row[1] and re.match("[A-Za-z]+(([ ]+([&]|and|or)[ ]+))[A-Za-z]+", row[1]):
        print row[1]
        first_names = re.findall('(\w+?)(?:(?:\sand\s)|(?:\s&\s))(\w+)', row[1])
        print first_names, row[2]
