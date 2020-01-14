#microservice number 1, Raspberry Pi sending data to Microservice number 2
import pika
import os
import time
import json

message_interval = 20  # seconds
reading_interval = 5  # seconds
# sensor = 11 # might need to be changed depending on the pi setup
# pin = 4 # might need to be changed depending on the pi setup

# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = 'amqp://jrwecwqb:jerz42ZTdOoWl-K5HdLTBB_76YA7_EqW@mosquito.rmq.cloudamqp.com/jrwecwqb'
# url_local = 'amqp://guest:guest@localhost:5672/%2f'
url = os.environ.get('CLOUDAMQP_URL', url)
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
# start a channel
channel = connection.channel()
# Declare a queue
channel.queue_declare(queue='weather')

isSimulation = 1
if isSimulation:
  import random
  def genrand():
      return random.random(), random.random() * 10
else:
  import Adafruit_DHT

while True:
  body = []
  timeout = time.time() + message_interval
  while True:
    if time.time() > timeout:
      break
    if isSimulation:
      humidity, temperature = genrand()
    else:
        humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)

    read_time = time.time()
    d = {'t': read_time, 'T': temperature, 'H': humidity}
    body.append(d)
    time.sleep(reading_interval)

    print('sending data')
    channel.basic_publish(exchange='',
                        routing_key='weather',
                        body=json.dumps(body))

connection.close()
