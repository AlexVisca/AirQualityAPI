import sqlite3

conn = sqlite3.connect('data/stats.sqlite')

crs = conn.cursor()
crs.execute('''
          DROP TABLE stats
          ''')

conn.commit()
conn.close()
