#microservice number 2, receives data from a message queue and stores in a database
import pika, os
import urllib.parse as up
import psycopg2
from datetime import datetime
import json
import datetime

#Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
URLMQ = 'amqp://jrwecwqb:jerz42ZTdOoWl-K5HdLTBB_76YA7_EqW@mosquito.rmq.cloudamqp.com/jrwecwqb'
# url_local = 'amqp://guest:guest@localhost:5672/%2f'
url = os.environ.get('CLOUDAMQP_URL', URLMQ)
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel
channel.queue_declare(queue='weather') # Declare a queue

def store(ch, method, properties, body):
    body = json.loads(body)
    if body:
        body = body[0]
    print("[X] Received time:" + str(body["t"]) + " and temperature: " + str(body["T"]))
    # print("Received body:   ", body)
    try:
        up.uses_netloc.append("postgres")
        URLDB = 'postgres://aznyoiqv:1DiWjnHFB_5_u9tgYkK6RC094q-drRmn@tuffi.db.elephantsql.com:5432/aznyoiqv'
        # url = up.urlparse(os.environ["ELEPHANTSQL_URL"])
        url = up.urlparse(URLDB)
        connection = psycopg2.connect(database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        cursor = connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS weather (id SERIAL PRIMARY KEY, time TIMESTAMP, temperature integer);")

        postgres_insert_query = """ INSERT INTO weather (TIME, TEMPERATURE) VALUES (%s,%s)"""
        time = datetime.datetime.utcfromtimestamp(float(body['t'])/1000.)
        record_to_insert = (time, int(body["T"]))
        cursor.execute(postgres_insert_query, record_to_insert)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into weather table")

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
        if(connection):
            connection.close()
            print("PostgreSQL connection is closed")

channel.basic_consume('weather', store, auto_ack=True)

print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()
