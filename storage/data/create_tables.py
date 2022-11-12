import mysql.connector


conn = mysql.connector.connect(
    host="ec2-35-93-78-123.us-west-2.compute.amazonaws.com", 
    port=3306, 
    user="storage", 
    password="store", 
    database="telemetry")

crs = conn.cursor()
crs.execute('''
    CREATE TABLE temperature
    (id_ INT NOT NULL AUTO_INCREMENT,
    date_created VARCHAR(100) NOT NULL,
    device_id VARCHAR(250) NOT NULL,
    location VARCHAR(250) NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    timestamp VARCHAR(100) NOT NULL,
    trace_id VARCHAR(250) NOT NULL,
    CONSTRAINT temperature_pk PRIMARY KEY (id_))
    ''')

crs.execute('''
    CREATE TABLE environment
    (id_ INT NOT NULL AUTO_INCREMENT,
    date_created VARCHAR(100) NOT NULL,
    device_id VARCHAR(250) NOT NULL,
    location VARCHAR(250) NOT NULL,
    pm2_5 INTEGER NOT NULL,
    co_2 INTEGER NOT NULL,
    timestamp VARCHAR(100) NOT NULL,
    trace_id VARCHAR(250) NOT NULL,
    CONSTRAINT environment_pk PRIMARY KEY (id_))
    ''')

conn.commit()
conn.close()
