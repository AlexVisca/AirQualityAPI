import sqlite3

conn = sqlite3.connect('data/stats.sqlite')

crs = conn.cursor()
crs.execute('''
          DELETE FROM Stats;
          ''')
crs.execute('''
          VACUUM;
          ''')
conn.commit()
conn.close()