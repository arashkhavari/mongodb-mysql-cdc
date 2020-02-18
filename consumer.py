#!/bin/python3

from kafka import KafkaConsumer, TopicPartition
import json
import update_extractor
import insert_extractor
import logging
import mysql.connector
from mysql.connector import errors
from dotenv import load_dotenv
from pathlib import Path
import os

# Load enviroment from .env_pro
env_path = Path('.') / '.env_pro'
load_dotenv(dotenv_path=env_path)



# Insert log to file
logging.basicConfig(filename=os.getenv("log_file") , filemode='w',
                   format='%(asctime)s %(name)s %(levelname)s %(message)s',level=os.getenv("log_level"))



# MySQL Connection
my_connection = mysql.connector.connect(host=os.getenv("mysql_host"), user=os.getenv("mysql_user"),
                           passwd=os.getenv("mysql_pass"),
                           database=os.getenv("mysql_db"))


# List for items updated
def update_list(data):
    main_list = []
    key_list = json.loads(os.getenv("item_list"))
    for value in key_list:
        var = eval("update_extractor.%s(data)" % value)
        main_list.append(var)
    zipped = zip(key_list, main_list)
    filter_none = list(filter(lambda x: x[1] is not None, zipped))
    unzipped_a = [x[0] for x in filter_none]
    unzipped_b = [x[1] for x in filter_none]
    logging.debug(f"update_list: {unzipped_a} {unzipped_b}")
    return unzipped_a+unzipped_b



# List for items inserted
def insert_list(data):
    main_list = []
    key_list = json.loads(os.getenv("item_list"))
    for value in key_list:
        var = eval("insert_extractor.%s(data)" % value)
        main_list.append(var)
    zipped = zip(key_list, main_list)
    filter_none = list(filter(lambda x: x[1] is not None, zipped))
    unzipped_a = [x[0] for x in filter_none]
    unzipped_b = [x[1] for x in filter_none]
    logging.debug(f"insert_list: {unzipped_a} {unzipped_b}")
    return unzipped_a+unzipped_b



# Update query set
def update_query(get_list):
    mean = len(get_list)//2
    whkey = get_list[0]
    whval = get_list[mean]
    table = os.getenv("mysql_table")
    cnt = 1
    meancnt = mean+1
    query_data = ""
    while cnt < mean:
        if isinstance(get_list[meancnt], int):
            if meancnt == len(get_list)-1:
                query_data += f"{get_list[cnt]} = {get_list[meancnt]}"
            else:
                query_data += f"{get_list[cnt]} = {get_list[meancnt]}, "
        else:
            if meancnt == len(get_list)-1:
                query_data += f"{get_list[cnt]} = '{get_list[meancnt]}'"
            else:
                query_data += f"{get_list[cnt]} = '{get_list[meancnt]}', "
        cnt += 1
        meancnt += 1
    query = f"update {table} set {query_data} where {whkey} = '{whval}'"
    return query


# Insert query set
def insert_query(get_list):
    mean = len(get_list)//2
    table = os.getenv("mysql_table")
    cnt = 0
    query_item = ""
    query_value = ""
    while cnt < mean:
        meancnt = cnt+mean
        if isinstance(get_list[meancnt], int):
            if meancnt == len(get_list)-1:
                query_item += f"{get_list[cnt]}"
                query_value += f"{get_list[meancnt]}"
            else:
                query_item += f"{get_list[cnt]}, "
                query_value += f"{get_list[meancnt]}, "
        else:
            if meancnt == len(get_list)-1:
                query_item += f"{get_list[cnt]}"
                query_value += f"'{get_list[meancnt]}'"
            else:
                query_item += f"{get_list[cnt]}, "
                query_value += f"'{get_list[meancnt]}', "
        cnt += 1
    query = f"insert into {table}({query_item}) values({query_value})"
    return query


# Delete query
def delete_query(get_id):
    table = os.getenv("mysql_table")
    mean = len(get_id)//2
    query = f"delete from `{table}` where `id` = '{get_id[mean]}'"
    return query


# Execute update query to MySQL
def update_execute(data):
    return my_connection
    val = data
    try:
        my_cursor = my_connection.cursor()
        my_cursor.execute(val)
        my_connection.commit()
    finally:
        my_cursor.close()
        my_connection.close()


# Execute query to MySQL
def insert_execute(data,redata):
    return my_connection
    val = data
    re_val = redata
    try:
        my_cursor = my_connection.cursor()
        my_cursor.execute(val)
        my_connection.commit()
    except mysql.connector.IntegrityError as e:
        logging.info(f"This query {data} is duplicate")
        my_cursor = my_connection.cursor()
        my_cursor.execute(re_val)
        my_connection.commit()
        logging.info(f"This query {val} duplicated is delete")
        my_cursor = my_connection.cursor()
        my_cursor.execute(val)
        my_connection.commit()
        logging.info(f"This query {val} deleted is duplicate")
    finally:
        my_cursor.close()
        my_connection.close()


# Consume from Kafka Broker
consumer = KafkaConsumer(auto_offset_reset=os.getenv("offset_reset_from"),
                         enable_auto_commit=os.getenv("auto_commit"),
                         group_id=os.getenv("counsumer_group"),
                         bootstrap_servers=[os.getenv("kafka_host")])
consumer.assign([TopicPartition(os.getenv("kafka_topic"), 0)])
while True:
    msg = next(consumer)
    logging.info(f"Offset: {msg.offset}, Topic: {msg.topic}, Message: {msg.value.decode('utf-8')}")
    json_map = json.loads(msg.value.decode('utf-8'))
    operation_type = json_map['operationType']
    if operation_type == 'insert' or operation_type == 'replace':
        insert_execute(insert_query(insert_list(json_map)),delete_query(insert_list(json_map)))
        logging.info(f"Offset: {msg.offset} executed")
    elif operation_type == 'update':
        if len(update_list(json_map)) == 2:
            logging.info(f"Offset: {msg.offset} update items not in list {json_map}")
            pass
        else:
            update_execute(update_query(update_list(json_map)))
            logging.info(f"Offset: {msg.offset} executed")
    else:
        pass
    consumer.commit()
