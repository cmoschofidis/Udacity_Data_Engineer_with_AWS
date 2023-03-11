import psycopg2
from config import studentdb


# Connecting to database
try:
    conn = psycopg2.connect(f"host= {studentdb['host']} dbname={studentdb['dbname']} user={studentdb['user']} password={studentdb['password']}")
    print("Connection successfull")
except psycopg2.Error as e:
   print(f"Error: Could not make connection to the {studentdb['dbname']} database")
   print(e)


# Connect to cursor
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print(f"Error: Could not get cursor to the {studentdb['dbname']} database")
    print(e)

# Enable autocommit
conn.set_session(autocommit=True) 

# Test Connection

# try:
#     cur.execute("Select * FROM udacity.music_library")
# except psycopg2.Error as e:
#     print(e)


# Let's create it

# try:
#     cur.execute("CREATE DATABASE udacity IF NOT EXISTS ")
# except psycopg2.Error as e:
#     print(e)

try:
    conn.close()
except psycopg2.Error as e:
    print(e)

try:
    conn = psycopg2.connect("dbname=udacity")
    print(f'Established connection to the {conn} database')
except psycopg2.Error as e:
    print(f'Error: Could not establish connection to the {conn} database')
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print(f'Error: Could not get cursor to the {conn} database')
    print(e)
conn.set_session(autocommit=True)

try:
    cur.execute("""
                CREATE TABLE IF NOT EXISTS music_library
                (album_name varchar,
                artist_name varchar,
                year int);
                """)
except psycopg2.Error as e:
    print("Error creating music_library table")
    print(e)
    
#print(cur.fetchall())

# Insert rows
# try:
#     cur.execute("""
#                 INSERT INTO music_library
#                 (album_name,
#                 artist_name,
#                 year)
#                 VALUES(%s, %s, %s)
#             """, ("Rubber Soul", "The Beatles", 1965))
# except psycopg2.Error as e:
#     print("Error: Inserting Rows")
#     print(e)

# Validate that rows are inserted
try:
    cur.execute("SELECT * FROM music_library;")
except psycopg2.Error as e:
    print("Error: SELECT *")

row = cur.fetchone()
print(row)
while row:
    print(row)
    row = cur.fetchone()

# DROP TABLE

try:
    cur.execute("DROP TABLE music_library;")
    print("music_library is dropped")
except psycopg2.Error as e:
    print("Error: Dropping Table")

cur.close()
conn.close()