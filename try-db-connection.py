#example of how to store data in a database with pyscopg2
import pika, os
import urllib.parse as up
import psycopg2

try:
  up.uses_netloc.append("postgres")
  BK_URL = 'postgres://aznyoiqv:1DiWjnHFB_5_u9tgYkK6RC094q-drRmn@tuffi.db.elephantsql.com:5432/aznyoiqv'
  DB_URL = os.environ.get('DATABASE_URL', BK_URL)
  url = up.urlparse(DB_URL)
  connection = psycopg2.connect(database=url.path[1:],
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port
  )

  cursor = connection.cursor()

  #Print PostgreSQL Connection properties
  print('connection.get_dsn_parameters()','\n')
  print(connection.get_dsn_parameters(),"\n")
  #Print PostgreSQL version
  cursor.execute("SELECT version();")
  record = cursor.fetchone()
  print("You are connected to - ", record,"\n")
except (Exception, psycopg2.Error) as error :
  print("Error while connecting to PostgreSQL", error)
finally:
  #closing database connection.
  if(connection):
    connection.close()
    print("PostgreSQL connection is closed")
