#!/bin/python3

import json
import mysql.connector
from mysql.connector import errors
import logging
import import_extractor
from dotenv import load_dotenv
from pathlib import Path
import os

# Load environments from .env_pro
env_path = Path('.') / '.env_pro'
load_dotenv(dotenv_path=env_path)

# Insert log to file
logging.basicConfig(filename=os.getenv("log_file"), filemode='w',
                    format='%(asctime)s %(name)s %(levelname)s %(message)s', level=os.getenv("log_level"))


# List for items imported
def import_list(data):
    main_list = []
    key_list = json.loads(os.getenv("item_list"))
    for value in key_list:
        var = eval("import_extractor.%s(data)" % value)
        main_list.append(var)
    aggregation_list = zip(key_list, main_list)
    filter_none = list(filter(lambda x: x[1] is not None, aggregation_list))
    item_list = [x[0] for x in filter_none]
    value_list = [x[1] for x in filter_none]
    logging.debug(f"import_list: {item_list}")
    return item_list + value_list


# Import query set
def import_query(get_list):
    mean = len(get_list) // 2
    table_name = os.getenv("mysql_table")
    counter = 0
    query_item = ""
    query_value = ""
    while mean > counter:
        mean_counter = counter + mean
        if isinstance(get_list[mean_counter], int):
            if counter + 1 == mean:
                query_item += f"{get_list[counter]}"
                query_value += f"{get_list[mean_counter]}"
            else:
                query_item += f"{get_list[counter]}, "
                query_value += f"{get_list[mean_counter]}, "
        else:
            if counter + 1 == mean:
                query_item += f"{get_list[counter]}"
                query_value += f"'{get_list[mean_counter]}'"
            else:
                query_item += f"{get_list[counter]}, "
                query_value += f"'{get_list[mean_counter]}',"
        counter += 1
    imp_query = f"INSERT INTO {table_name}({query_item}) VALUES({query_value})"
    return imp_query


# Delete query
def delete_query(get_id):
    table_name = os.getenv("mysql_table")
    mean = len(get_id) // 2
    del_query = f"delete from `{table_name}` where `id` = '{get_id[mean]}'"
    return del_query


# Insert to MySQL
with open(os.getenv("mongo_data_path")) as fp:
    line = fp.readline()
    cnt = 1
    my_connection = mysql.connector.connect(host=os.getenv("mysql_host"), user=os.getenv("mysql_user"),
                                            passwd=os.getenv("mysql_pass"),
                                            database=os.getenv("mysql_db"))

    my_cursor = my_connection.cursor()
    while line:
        line = fp.readline()
        json_map = json.loads(line)
        if cnt % 20 == 0:
            my_cursor.close()
            my_cursor = my_connection.cursor()
        insert_record = import_query(import_list(json_map))
        delete_record = delete_query(import_list(json_map))
        try:
            cnt += 1
            my_cursor.execute(insert_record)
            my_connection.commit()
        except mysql.connector.IntegrityError as e:
            logging.info(f"This query {insert_record} is duplicate")
            my_cursor = my_connection.cursor()
            my_cursor.execute(delete_record)
            my_connection.commit()
            logging.info(f"This query {insert_record} duplicated is delete")
            my_cursor = my_connection.cursor()
            my_cursor.execute(insert_record)
            my_connection.commit()
            logging.info(f"This query {insert_record} deleted is duplicate")
