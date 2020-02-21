1.Install Kafka services - I used Confluent community platform.
 My guide: https://docs.confluent.io/current/quickstart/ce-quickstart.html
 
2.Configure Debezium - Load MongoDB Connector
```bash
 > confluent-hub install mongodb/kafka-connect-mongodb:latest
```
 or
```html
 https://debezium.io/documentation/reference/0.10/install.html
```
3.use Source connector or Sink connector (Source Produce to Kafka and Sink Consume from Kafka)
![pic5](https://github.com/arashkhavari/mongodb-mysql-cdc/blob/master/img/img5.png)
 from Control Center or
```bash
 > curl -X PUT http://localhost:8083/connectors/source-mongodb-inventory/config -H "Content-Type: application/json" -d '{
      "tasks.max":1,
      "connector.class":"com.mongodb.kafka.connect.MongoSourceConnector",
      "key.converter":"org.apache.kafka.connect.storage.StringConverter",
      "value.converter":"org.apache.kafka.connect.storage.StringConverter",
      "connection.uri":"<>",
      "database":"BigBoxStore",
      "collection":"inventory",
      "pipeline":"[{\"$match\": { \"$and\": [ { \"updateDescription.updatedFields.quantity\" : { \"$lte\": 5 } },   {\"operationType\": \"update\"}]}}]", 
      "topic.prefix": ""  
  }'
```
4.Check the topic and producer working
![pic4](https://github.com/arashkhavari/mongodb-mysql-cdc/blob/master/img/img4.png)
 From Control Center

5.Develope Kafka Consumer

 I am using Python Kafka Consumer module as mention below

 https://github.com/dpkp/kafka-python
```python
 from kafka import KafkaConsumer
 import sys
 import json
 import mysql.connector
 consumer = KafkaConsumer('nekso.rates')
 for message in consumer: 
   json_map = json.loads(message.value.decode('utf-8'))
   mydb = mysql.connector.connect(host="", user="", passwd="", database="")
   mycursor = mydb.cursor()
     if json_map['operationType'] == "insert":
       _id = json_map['fullDocument']['_id']['$oid']
       created_date = json_map['fullDocument']['createdDate']['$date']
       try:
         type(created_date)
       except KeyError:
         created_date = "NULL"
       varias="insert into rates (_id,created_date) values(%s, %s)"
       values=_id,created_date
       mycursor.execute(varias, values)
       mydb.commit()
       mycursor.close()
       with open("<PATH>/consumer.log", "a") as logfile:
         logfile.write("topic=%s offset=%d \n" % (message.topic, message.offset))
     else:
       with open("<PATH>/consumer.log", "a") as logfile:
         logfile.write("topic=%s offset=%d value=%s \n" % (message.topic, message.offset, message.value.decode('utf-8')))
```
  If Consumer get the new data this code writes a log file(offset and topic).
  HINT: Producer and Consumer should hava a same traffic.
![pic3](https://github.com/arashkhavari/mongodb-mysql-cdc/blob/master/img/img3.png)
6.use Supervisord for run python script
  add end of file in /etc/supervisor.conf
```bash
  [program:consumer]
  command=python <PATH>/consumer.py
  process_name=%(program_name)s_%(process_num)02d
  numprocs=1
  priority=999 
  autostart=true
  autorestart=true 
  startsecs=1
  startretries=3
  user=root
  redirect_stderr=true
  stdout_logfile=<PATH>/consumer.log
```
 Uncomment WEB UI for monitoring
```bash
  [inet_http_server]
  port=0.0.0.0:9001
  username=username
  password=password
```
![pic2](https://github.com/arashkhavari/mongodb-mysql-cdc/blob/master/img/img2.png)

note:
    replace env_sample to .env_pro
