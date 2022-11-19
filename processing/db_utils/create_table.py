import sqlite3


conn = sqlite3.connect('data/stats.sqlite')

c = conn.cursor()
c.execute('''
    CREATE TABLE Stats
    (id_ INTEGER PRIMARY KEY ASC,
    count INTEGER NOT NULL,
    temp_buffer FLOAT NOT NULL,
    max_temp FLOAT,
    min_temp FLOAT,
    avg_temp FLOAT,
    max_pm2_5 INTEGER,
    max_co_2 INTEGER,
    last_updated VARCHAR(100) NOT NULL)
''')
conn.commit()
conn.close()