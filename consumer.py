#!/bin/python3

import json
import logging
import os
from pathlib import Path
import update_extractor
import insert_extractor
import mysql.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

# Load Environments from .env_pro
env_path = Path('.') / '.env_pro'
load_dotenv(dotenv_path=env_path)

# Insert log to file
logging.basicConfig(filename=os.getenv("log_file"), filemode='w',
                    format='%(asctime)s %(name)s %(levelname)s %(message)s', level=os.getenv("log_level"))


# List for items updated
def update_list(data):
    main_list = []
    key_list = json.loads(os.getenv("item_list"))
    for value in key_list:
        var = getattr(update_extractor, value)(data)
        main_list.append(var)
    aggregation_list = zip(key_list, main_list)
    filter_none = list(filter(lambda x: x[1] is not None, aggregation_list))
    item_list = [x[0] for x in filter_none]
    value_list = [x[1] for x in filter_none]
    logging.debug(f"update_list: {item_list} {value_list}")
    return item_list, value_list


# List for items inserted
def insert_list(data):
    main_list = []
    key_list = json.loads(os.getenv("item_list"))
    for value in key_list:
        var = getattr(insert_extractor, value)(data)
        main_list.append(var)
    aggregation_list = zip(key_list, main_list)
    filter_none = list(filter(lambda x: x[1] is not None, aggregation_list))
    item_list = [x[0] for x in filter_none]
    value_list = [x[1] for x in filter_none]
    logging.debug(f"insert_list: {item_list} {value_list}")
    return item_list, value_list


# Update query set
def update_query(get_list):
    table_name = os.getenv("mysql_table")
    key_list = get_list[0]
    value_list = get_list[1]
    id_key = get_list[0][0]
    id_value = get_list[1][0]
    del key_list[0]
    del value_list[0]
    aggregation_query_key = zip(key_list, value_list)
    aggregation_query_key = ", ".join("%s = '%s'" % tup for tup in aggregation_query_key)
    upd_query = f"update {table_name} set {aggregation_query_key} where {id_key} = '{id_value}'"
    return upd_query


# Insert query set
def insert_query(get_list):
    table_name = os.getenv("mysql_table")
    query_key = ','.join(map(str, get_list[0]))
    query_value = ",".join(list(map(lambda a: str(a) if isinstance(a, bool) else f'"{str(a)}"', get_list[1])))
    ins_query = f"insert into {table_name}({query_key}) values({query_value})"
    return ins_query


# Delete query
def delete_query(get_id):
    table_name = os.getenv("mysql_table")
    mean = len(get_id) // 2
    del_query = f"delete from `{table_name}` where `id` = '{get_id[mean]}'"
    return del_query


# Execute update query to MySQL
def update_execute(data):
    my_connection = mysql.connector.connect(host=os.getenv("mysql_host"), user=os.getenv("mysql_user"),
                                            passwd=os.getenv("mysql_pass"),
                                            database=os.getenv("mysql_db"))
    val = data
    try:
        my_cursor = my_connection.cursor()
        my_cursor.execute(val)
        my_connection.commit()
    finally:
        my_cursor.close()
        my_connection.close()


# Execute query to MySQL
def insert_execute(data, redata):
    my_connection = mysql.connector.connect(host=os.getenv("mysql_host"), user=os.getenv("mysql_user"),
                                            passwd=os.getenv("mysql_pass"),
                                            database=os.getenv("mysql_db"))
    insert_record = data
    delete_record = redata
    try:
        my_cursor = my_connection.cursor()
        my_cursor.execute(insert_record)
        my_connection.commit()
    except mysql.connector.IntegrityError as e:
        logging.info(f"This query {data} is duplicate")
        my_cursor = my_connection.cursor()
        my_cursor.execute(delete_record)
        my_connection.commit()
        logging.info(f"This query {insert_record} duplicated is delete")
        my_cursor = my_connection.cursor()
        my_cursor.execute(insert_record)
        my_connection.commit()
        logging.info(f"This query {insert_record} deleted is duplicate")
    finally:
        my_cursor.close()
        my_connection.close()


# Consume from Kafka Broker
consumer = KafkaConsumer(auto_offset_reset=os.getenv("offset_reset_from"),
                         enable_auto_commit=os.getenv("auto_commit"),
                         group_id=os.getenv("consumer_group"),
                         bootstrap_servers=[os.getenv("kafka_host")])
consumer.assign([TopicPartition(os.getenv("kafka_topic"), 0)])
while True:
    msg = next(consumer)
    logging.info(f"Offset: {msg.offset}, Topic: {msg.topic}, Message: {msg.value.decode('utf-8')}")
    json_map = json.loads(msg.value.decode('utf-8'))
    operation_type = json_map['operationType']
    if operation_type == 'insert' or operation_type == 'replace':
        insert_execute(insert_query(insert_list(json_map)), delete_query(insert_list(json_map)))
        logging.info(f"Offset: {msg.offset} executed")
    elif operation_type == 'update':
        update_execute(update_query(update_list(json_map)))
        logging.info(f"Offset: {msg.offset} executed")
    consumer.commit()
