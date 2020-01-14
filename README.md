# First steps with microservices

Follow this tutorial : [Tutorial](https://www.cloudamqp.com/blog/2019-09-11-a-developers-guide-through-the-microservice-jungle.html)

I do not have a raspberry, so there is a flag in the script in order to generate random data.
(There is a flag on the script)

For this, I followed: [Doc](https://www.cloudamqp.com/docs/python.html)

# Technologies used

- Microservices (ok, this is not a tech, but it is worth mentioning because it is the objective of this project)
- Python
- Heroku
- RabbitQM
- ElephantSQL (PostgreSQL)

# Usage

## Run microservices to generate and store data

To use microservice01 (generate data) and microservice02 (store data in DB) 
locally, run in different terminals both files

```
$ python microservice01-raspberry.py
sending data
sending data
```

```
python microservice02-store-data-in-db.py  

[*] Waiting for messages:

[X] Received time:1579041490.4134362 and temperature: 0.49375389629322086
1 Record inserted successfully into weather table
PostgreSQL connection is closed
[X] Received time:1579041490.4134362 and temperature: 0.49375389629322086
1 Record inserted successfully into weather table
PostgreSQL connection is closed
```

## Run microservice to show data

It is a simple web done in Flask to show data. Deployed to Heroku

https://mytemperature--itegram.herokuapp.com/
