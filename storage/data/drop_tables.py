import mysql.connector


conn = mysql.connector.connect(
    host="api-lxvdev.westus3.cloudapp.azure.com", 
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