import mysql.connector


conn = mysql.connector.connect(
    host="api.lxvdev.xyz", 
    port=3306, 
    user="storage", 
    password="storeboughtsecrets", 
    database="telemetry")

crs = conn.cursor()
crs.execute('''
            DROP TABLE temperature, environment;
            ''')

conn.commit()
conn.close()