import mysql.connector


conn = mysql.connector.connect(
    host="20.106.90.66", 
    port=3306, 
    user="storage", 
    password="store", 
    database="telemetry")

crs = conn.cursor()
crs.execute('''
            DROP TABLE temperature, environment;
            ''')

conn.commit()
conn.close()